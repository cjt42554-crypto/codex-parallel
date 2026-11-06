"""Real dispatcher/control subprocess integration with fake model workers."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import unittest

import test_dispatch as fixtures


class BackgroundIntegrationTests(fixtures.DispatchFixture):
    def setUp(self):
        super().setUp()
        self.batches = []

    def tearDown(self):
        try:
            for batch in self.batches:
                self.run_cli('--batch', batch, '--cancel', '--wait-seconds', '4')
        finally:
            # A failed cancellation assertion must not leak the test's detached workers.
            for path in (self.root / 'workers').glob('*/receipt.json'):
                try:
                    pid = json.loads(path.read_text()).get('pid')
                    if pid and str(self.root).encode() in Path(f'/proc/{pid}/cmdline').read_bytes():
                        os.killpg(pid, signal.SIGKILL)
                except (OSError, ValueError):
                    pass
            super().tearDown()

    def submit(self, *extra):
        result = self.run_cli('--submit', *extra)
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.batches.append(data['batch_id'])
        self.assertNotEqual(data['state'], 'startup_failed', data)
        return data

    def query(self, batch, *extra):
        result = self.run_cli('--batch', batch, *extra)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_submit_live_attention_cancel_waiter_and_explicit_cancel(self):
        self.jobs(self.root, ['a', 'b'])
        self.behavior(self.root, {'a': 'blocked', 'b': 'hang'})
        started = time.monotonic()
        batch = self.submit()['batch_id']
        self.assertLess(time.monotonic() - started, 2)
        event = self.query(batch, '--wait', '--after', '0', '--wait-seconds', '3')
        self.assertEqual(event['event'], 'attention')
        self.assertEqual([row['id'] for row in event['items']], ['a'])
        self.assertEqual(event['state'], 'running')
        cursor = event['cursor']
        page = self.query(batch, '--results', '--after', '0')
        self.assertEqual(page['items'][0]['status'], 'blocked')
        expired = self.query(batch, '--wait', '--after', str(cursor), '--wait-seconds', '.1')
        self.assertTrue(expired['waiting_timed_out'])
        self.assertEqual(self.receipt('b')['status'], 'running')
        waiter = subprocess.Popen(self.command(None, '--batch', batch, '--wait', '--after', str(cursor)),
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(waiter)
        time.sleep(.1)
        waiter.terminate()
        waiter.wait(timeout=3)
        self.assertTrue(self.query(batch, '--status')['scheduler_live'])
        cancelled = self.query(batch, '--cancel', '--wait-seconds', '5')
        self.assertTrue(cancelled['stopped'], cancelled)
        self.assertEqual(self.receipt('b')['status'], 'interrupted')
        after = self.query(batch, '--results', '--after', str(cursor))
        self.assertEqual([row['id'] for row in after['items']], ['b'])

    def test_old_invocation_handle_does_not_cancel_new_retry(self):
        self.behavior(self.root, {'a': 'partial'})
        first = self.submit()['batch_id']
        self.query(first, '--wait', '--wait-seconds', '3')
        self.behavior(self.root, {'a': 'hang'})
        second = self.submit('--retry-failed')['batch_id']
        until = time.monotonic() + 3
        while time.monotonic() < until:
            receipt = self.receipt()
            if receipt['attempt'] == 2 and receipt['status'] == 'running':
                break
            time.sleep(.03)
        self.assertEqual(receipt['attempt'], 2)
        old_cancel = self.query(first, '--cancel', '--wait-seconds', '1')
        self.assertTrue(old_cancel['stopped'])
        self.assertTrue(self.query(second, '--status')['scheduler_live'])
        old_row = self.query(first, '--results')['items'][0]
        self.assertEqual(json.loads(Path(old_row['receipt_path']).read_text())['status'], 'partial')
        self.assertTrue(self.query(second, '--cancel', '--wait-seconds', '5')['stopped'])

    def test_killed_scheduler_cleanup_and_new_invocation_resume(self):
        self.behavior(self.root, {'a': 'hang'})
        submitted = self.submit('--status-interval-seconds', '10')
        batch = submitted['batch_id']
        self.wait_started()
        os.kill(submitted['pid'], signal.SIGKILL)
        until = time.monotonic() + 3
        while time.monotonic() < until:
            status = self.query(batch, '--status')
            if status['state'] == 'orphaned':
                break
            time.sleep(.03)
        self.assertEqual(status['state'], 'orphaned', status)
        cancelled = self.query(batch, '--cancel', '--wait-seconds', '5')
        self.assertTrue(cancelled['stopped'], cancelled)
        self.behavior(self.root, {})
        resumed = self.submit('--retry-failed')['batch_id']
        self.query(resumed, '--wait', '--wait-seconds', '3')
        self.assertTrue(self.query(resumed, '--report')['ok'])
        self.assertEqual(self.receipt()['attempt'], 2)


if __name__ == '__main__':
    unittest.main()
