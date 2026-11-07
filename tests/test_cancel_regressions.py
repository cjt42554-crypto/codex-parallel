"""Exercise invocation selection and cancellation against local disposable processes."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import unittest

import test_dispatch as fixtures

import batch_control as control
import process_ownership as ownership


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class CancelTests(fixtures.DispatchFixture):
    def cli(self, *args):
        result = self.run_cli(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_failed_submission_stays_latest_while_active_batch_publishes(self):
        self.behavior(self.root, {'a': 'hang'})
        a = self.cli('--submit', '--status-interval-seconds', '.1')
        try:
            self.wait_started()
            b = self.cli('--submit')
            self.assertEqual(b['state'], 'startup_failed', b)
            self.assertIn('active dispatcher or worker', b['error'])
            self.assertNotEqual(a['batch_id'], b['batch_id'])
            heartbeat = control.read_status(a['batch_id'])['heartbeat_utc']
            end = time.monotonic() + 3
            while time.monotonic() < end:
                if control.read_status(a['batch_id'])['heartbeat_utc'] != heartbeat:
                    break
                time.sleep(.02)
            else:
                self.fail('A did not publish a new heartbeat after B failed')
            workspace = self.cli('--status')
            self.assertEqual(workspace['latest_submission'], b['batch_id'])
            self.assertEqual(workspace['active_batch'], a['batch_id'])
            self.assertEqual(workspace['state'], 'startup_failed')
            no_batch = self.run_cli('--cancel', '--wait-seconds', '0')
            self.assertNotEqual(no_batch.returncode, 0)
            self.assertIn('explicit --batch', no_batch.stderr)
            child = self.receipt()['pid']
            stopped = self.cli('--batch', b['batch_id'], '--cancel', '--wait-seconds', '3')
            self.assertTrue(stopped['stopped'], stopped)
            self.assertFalse(stopped['dispatcher_term_sent'])
            self.assertEqual(stopped['attempts'], {})
            self.assertTrue(control.read_status(a['batch_id'])['scheduler_live'])
            self.assertEqual(self.receipt()['pid'], child)
            self.assertTrue(control._process(child).live)
            self.assertFalse((Path(a['batch_id']) / 'cancel_request.json').exists())
        finally:
            self.cli('--batch', a['batch_id'], '--cancel', '--wait-seconds', '4')

    def spawn(self, owner=None):
        env = {**os.environ}
        if owner:
            env[ownership.OWNER_ENV] = owner['marker']
        proc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                start_new_session=True, env=env)
        self.procs.append(proc)
        return proc

    def test_explicit_cli_cancel_accepts_request_despite_damaged_projections(self):
        self.behavior(self.root, {'a': 'hang'})
        submitted = self.cli('--submit')
        batch = Path(submitted['batch_id'])
        self.wait_started()
        pid = self.receipt()['pid']
        saved = {name: (batch / name).read_bytes() for name in ('status.json', 'config.json')}
        try:
            for name in saved:
                (batch / name).write_text('{invalid')
            result = self.cli('--batch', str(batch), '--cancel', '--wait-seconds', '4')
            self.assertTrue((batch / 'cancel_request.json').exists())
            self.assertTrue(result['dispatcher_term_sent'])
            self.assertFalse(result['scheduler_live'])
            process = control._process(pid)
            self.assertTrue(process is None or not process.live)
            self.assertFalse(result['stopped'])  # Damaged launch metadata remains unverified.
        finally:
            for name, value in saved.items():
                (batch / name).write_bytes(value)
            control.cancel(batch, 4)

    def test_cancel_terminal_batch_visits_all_attempts_but_not_other_batches_or_reuse(self):
        scheduler = self.spawn()
        scope = dict(boot_id=control._boot_id(), pid_namespace=os.readlink('/proc/self/ns/pid'),
                     uid=os.geteuid(), pid=scheduler.pid,
                     process_start_ticks=control._process(scheduler.pid).ticks)
        scheduler.terminate()
        scheduler.wait(timeout=3)
        a, b, c = (self.root / 'runs' / str(i) for i in (1, 2, 3))
        for batch in (a, b, c):
            identity = dict(batch_id=str(batch), run_id=batch.name, **scope)
            write(batch / 'config.json', dict(**identity, jobs=['a'], attempt_journal_version=1))
            write(batch / 'status.json', dict(**identity, state='complete'))
            (batch / 'attempts').mkdir()

        directory = self.root / 'workers' / 'a'

        def attempt(batch, number):
            owner = ownership.prepare_owner()
            receipt = directory / f'receipt-{number:02d}.json'
            base = dict(task_id='a', batch_id=str(batch), run_id=batch.name, attempt=number, owner=owner)
            # Leave the PID unset to cover spawn before the dispatcher saves it.
            write(receipt, {**base, 'status': 'running', 'execution': 'prepared'})
            binding = dict(**base, source='execution', receipt_path=str(receipt))
            write(batch / 'attempts' / 'a' / f'{number:02d}.json', binding)
            return self.spawn(owner), binding

        first, _ = attempt(a, 1)
        second, reused = attempt(a, 2)
        other, _ = attempt(b, 3)
        write(c / 'bindings' / 'a.json', {**reused, 'source': 'reuse'})

        consumer = control.cancel(c, 3)
        self.assertTrue(consumer['stopped'], consumer)
        self.assertEqual(consumer['attempts'], {})
        self.assertTrue(all(p.poll() is None for p in (first, second, other)))
        result = control.cancel(a, 4)
        self.assertTrue(result['stopped'], result)
        self.assertEqual(set(result['attempts']), {'a/01', 'a/02'})
        self.assertEqual(first.wait(timeout=2), -signal.SIGTERM)
        self.assertEqual(second.wait(timeout=2), -signal.SIGTERM)
        self.assertIsNone(other.poll())
        self.assertFalse((b / 'cancel_request.json').exists())

        request = (a / 'cancel_request.json').read_bytes()
        ledgers = [directory / f'ownership-{n:02d}.json' for n in (1, 2)]
        observed = [path.read_bytes() for path in ledgers]
        repeated = control.cancel(a, 4)
        self.assertTrue(repeated['stopped'], repeated)
        self.assertEqual((a / 'cancel_request.json').read_bytes(), request)
        self.assertEqual([path.read_bytes() for path in ledgers], observed)
        for key in ('a/01', 'a/02'):
            self.assertEqual(repeated['attempts'][key]['signals'], result['attempts'][key]['signals'])

        # A terminal snapshot and an empty candidate list do not cover an absent journal.
        (c / 'attempts').rmdir()
        unverified = control.cancel(c, 1)
        self.assertFalse(unverified['stopped'])
        self.assertIn('launch_evidence_missing', [reason['reason'] for reason in unverified['uncertain']])


if __name__ == '__main__':
    unittest.main()
