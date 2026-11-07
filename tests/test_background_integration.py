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
            for path in (self.root / 'workers').glob('*/receipt-*.json'):
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

    def wait_terminal(self, batch):
        cursor = 0
        while True:
            event = self.query(batch, '--wait', '--after', str(cursor), '--wait-seconds', '3')
            if event['event'] != 'result':
                self.assertEqual(event['event'], 'terminal', event)
                return event
            cursor = event['cursor']

    def test_task_only_submit_and_wait_with_same_directory_finishes_with_summary(self):
        (self.root / 'COMMON.md').unlink()
        self.jobs(self.root, ['a', 'b', 'c'])
        self.behavior(self.root, {'a': 'research', 'b': 'research', 'c': 'research'})
        self.submit('--ramp-seconds', '0')
        answers = []
        while True:
            # No init command, shared file, batch-handle copy, cursor, or report query.
            response = self.run_cli('--wait', '--wait-seconds', '3', '--limit', '2')
            self.assertEqual(response.returncode, 0, response.stderr)
            event = json.loads(response.stdout)
            answers.extend(event['items'])
            if event['event'] == 'terminal':
                self.assertTrue(event['summary']['ok'])
                self.assertEqual(event['summary']['counts']['complete'], 3)
                break
            self.assertEqual(event['event'], 'result')
        self.assertEqual({r['id'] for r in answers}, {'a', 'b', 'c'})
        self.assertEqual(len(answers), 3)
        self.assertFalse((self.root / 'COMMON.md').exists())
        self.assertTrue(all('研究已完成' in r['answer'] for r in answers))

    def test_submit_live_result_cancel_waiter_and_explicit_cancel(self):
        self.jobs(self.root, ['a', 'b'])
        self.behavior(self.root, {'b': 'hang'})
        started = time.monotonic()
        batch = self.submit()['batch_id']
        self.assertLess(time.monotonic() - started, 2)
        event = self.query(batch, '--wait', '--after', '0', '--wait-seconds', '3')
        self.assertEqual(event['event'], 'result')
        self.assertEqual([(row['id'], row['status'], row['answer']) for row in event['items']],
                         [('a', 'complete', 'body')])
        self.assertEqual(event['state'], 'running')
        cursor = event['cursor']
        expired = self.query(batch, '--wait', '--after', str(cursor), '--wait-seconds', '.1')
        self.assertTrue(expired['waiting_timed_out'])
        self.assertEqual(expired['cursor'], cursor)
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
        after = self.query(batch, '--wait', '--after', str(cursor), '--wait-seconds', '3')
        self.assertEqual(after['event'], 'terminal')
        self.assertEqual([row['id'] for row in after['items']], ['b'])

    def test_old_invocation_handle_does_not_cancel_new_retry(self):
        self.behavior(self.root, {'a': 'partial'})
        first = self.submit()['batch_id']
        self.wait_terminal(first)
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
        self.wait_terminal(resumed)
        self.assertTrue(self.query(resumed, '--report')['ok'])
        self.assertEqual(self.receipt()['attempt'], 2)


if __name__ == '__main__':
    unittest.main()
