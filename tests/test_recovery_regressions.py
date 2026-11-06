"""Recovery across dispatcher crashes, using only local fake EXEC workers."""
import json
from datetime import datetime
from pathlib import Path
import subprocess
import time

import test_dispatch as fixtures


class RecoveryTests(fixtures.DispatchFixture):
    def setUp(self):
        super().setUp()
        fake = self.scripts / 'codexexec'
        fake.write_text(fixtures.FAKE.replace(
            "print(json.dumps({'type':'turn.started'}),flush=True)",
            "with (worker/'effects').open('a') as log:log.write(str(attempt)+'\\n')\n"
            "print(json.dumps({'type':'turn.started'}),flush=True)\n"
            "if mode=='held':\n"
            " while (root/'hold').exists():time.sleep(.01)"))
        self.directory = self.root / 'workers/a'

    def cli(self, *args):
        result = self.run_cli(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def row(self):
        return json.loads(self.cli('--results').stdout)['items'][0]

    def effects(self):
        return (self.directory / 'effects').read_text().splitlines()

    def numbered(self, attempt):
        return self.directory / f'receipt-{attempt:02d}.json'

    def control(self):
        return json.loads((self.directory / 'control.json').read_text())

    def run_wrapped(self, script_source, *args):
        script = self.scripts / 'injected_dispatch.py'
        script.write_text(script_source)
        command = self.command(None, *args)
        command[1] = str(script)
        return subprocess.run(command, capture_output=True, text=True, timeout=20)

    def wait_for(self, check):
        until = time.monotonic() + 6
        while time.monotonic() < until:
            try:
                if value := check():
                    return value
            except (FileNotFoundError, ValueError):
                pass
            time.sleep(.01)
        self.fail('timed out waiting for the fake worker')

    def test_missing_and_stale_alias_cannot_repeat_a_completed_attempt(self):
        self.behavior(self.root, {'a': ['partial', 'complete']})
        self.cli()
        old = self.numbered(1).read_bytes()
        self.cli('--retry-failed')
        alias = self.directory / 'receipt.json'
        for stale in (False, True):
            with self.subTest(stale=stale):
                if stale:
                    alias.write_bytes(old)
                else:
                    alias.unlink()
                self.cli('--retry-failed')
                row = self.row()
                self.assertEqual((row['status'], row['attempt'], row['reused']), ('complete', 2, True))
                self.assertEqual(Path(row['receipt_path']), self.numbered(2))
                self.assertEqual(self.effects(), ['1', '2'])

    def test_missing_result_does_not_erase_success_or_allow_explicit_retry(self):
        self.cli()
        alias = self.directory / 'receipt.json'
        before = alias.read_bytes()
        result = self.directory / 'result-01.json'
        saved = result.read_bytes()
        result.unlink()
        for args in ((), ('--retry-failed',)):
            self.cli(*args)
            row = self.row()
            self.assertEqual((row['status'], row['attempt']), ('complete', 1))
            self.assertTrue(any('result unavailable' in issue for issue in row['issues']))
            self.assertFalse(json.loads(self.cli('--report').stdout)['ok'])
            self.assertEqual(alias.read_bytes(), before)
            self.assertFalse(self.numbered(2).exists())
        result.write_bytes(saved)
        self.cli()
        self.assertTrue(json.loads(self.cli('--report').stdout)['ok'])
        self.assertEqual(self.effects(), ['1'])

    def test_result_after_dispatcher_loss_is_not_proof_of_worker_exit(self):
        self.behavior(self.root, {'a': 'held'})
        hold = self.root / 'hold'
        hold.touch()
        dispatcher = self.start()
        try:
            receipt = self.wait_for(lambda: (r if (r := self.receipt()).get('turn_started_utc') else None))
            dispatcher.kill()
            dispatcher.wait(timeout=5)
            self.assertIn('active dispatcher or worker', self.run_cli('--retry-failed').stderr)
        finally:
            hold.unlink(missing_ok=True)
        self.wait_for(lambda: (self.directory / 'result-01.json').is_file())
        stat = Path(f'/proc/{receipt["pid"]}/stat')
        self.wait_for(lambda: not stat.exists() or stat.read_text().rsplit(')', 1)[1].split()[0] == 'Z')
        self.cli()
        row = self.row()
        self.assertEqual((row['status'], row['recovery_state'], row['attempt']), ('interrupted', 'unknown', 1))
        self.assertEqual(self.effects(), ['1'])
        self.assertEqual(self.receipt()['status'], 'running')
        self.cli('--retry-failed')
        self.assertEqual(self.effects(), ['1', '2'])
        self.assertEqual(self.receipt()['status'], 'complete')

    def test_checkpoint_with_successful_exit_and_turn_recovers_original_attempt(self):
        # Kill the dispatcher exactly after saving the exit checkpoint, before
        # it can commit the task's final status.
        crash = self.run_wrapped('''import asyncio, os
import dispatch
save = dispatch.Batch.save_receipt
def checkpoint(directory, receipt):
    path = save(directory, receipt)
    if receipt.get('execution') == 'succeeded' and receipt.get('reported_status') == 'unknown':
        os._exit(91)
    return path
dispatch.Batch.save_receipt = staticmethod(checkpoint)
asyncio.run(dispatch.main())
''')
        self.assertEqual(crash.returncode, 91, crash.stderr)
        checkpoint = self.receipt()
        self.assertEqual((checkpoint['execution'], checkpoint['reported_status'], checkpoint['exit_code']),
                         ('succeeded', 'unknown', 0))
        self.cli()
        row = self.row()
        self.assertEqual((row['status'], row['recovery_state'], row['attempt']), ('complete', 'recovered', 1))
        self.assertEqual(self.effects(), ['1'])
        self.assertEqual(self.receipt()['status'], 'complete')

    def test_crash_in_backoff_resumes_original_deadline_and_total_budget(self):
        self.behavior(self.root, {'a': 'transient'})
        dispatcher = self.start(None, '--max-retries', '2', '--retry-backoff-seconds', '1.3',
                                '--status-interval-seconds', '.1')
        intent = self.wait_for(lambda: (c['retry'] if (c := self.control())['phase'] == 'backoff' else None))
        self.assertEqual((intent['count'], intent['limit'], intent['next_attempt']), (1, 2, 2))
        dispatcher.kill()
        dispatcher.wait(timeout=5)
        self.cli('--max-retries', '0', '--retry-backoff-seconds', '0')
        second = json.loads(self.numbered(2).read_text())
        self.assertGreaterEqual(datetime.fromisoformat(second['start_utc']).timestamp(), intent['ready_at'] - .05)
        self.assertEqual(second['automatic_retry_count'], 1)
        self.assertEqual(self.receipt()['automatic_retry_count'], 2)
        self.assertEqual(self.effects(), ['1', '2', '3'])
        self.assertEqual(json.loads(self.cli('--report').stdout)['automatic_retries'], 2)
        self.assertEqual(json.loads(self.cli('--report').stdout)['automatic_retries_started'], 2)
        self.cli()
        self.assertEqual(self.effects(), ['1', '2', '3'])

    def test_execution_success_with_unavailable_result_never_replays(self):
        for mode, observation in (('missing', 'unavailable'), ('bad_schema', 'invalid')):
            with self.subTest(mode=mode):
                self.root = self.make_root(mode)
                self.directory = self.root / 'workers/a'
                self.behavior(self.root, {'a': mode})
                self.cli()
                self.cli('--retry-failed')
                row = self.row()
                self.assertEqual((row['status'], row['execution'], row['reported_status'], row['result_observation']),
                                 ('blocked', 'succeeded', 'unknown', observation))
                self.assertEqual(self.effects(), ['1'])
                self.assertEqual(self.control()['phase'], 'settled')

    def test_cancelled_takeover_does_not_revive_and_old_cancel_does_not_close_new_owner(self):
        self.behavior(self.root, {'a': ['transient', 'complete']})
        first = self.start(None, '--retry-backoff-seconds', '12', '--status-interval-seconds', '.1')
        state_a = self.wait_for(lambda: (c if (c := self.control())['phase'] == 'backoff' else None))
        first.kill()
        first.wait(timeout=5)
        run_a = self.root / 'runs' / state_a['owner_run_id']
        second = self.start(None, '--retry-backoff-seconds', '0', '--status-interval-seconds', '.1')
        state_b = self.wait_for(lambda: (c if (c := self.control())['owner_run_id'] != run_a.name else None))
        run_b = self.root / 'runs' / state_b['owner_run_id']
        self.assertEqual((state_b['phase'], state_b['chain_id'], state_b['retry']),
                         ('backoff', state_a['chain_id'], state_a['retry']))
        self.cli('--batch', str(run_a), '--cancel', '--wait-seconds', '0')
        self.assertEqual(self.control(), state_b)
        self.cli('--batch', str(run_b), '--cancel', '--wait-seconds', '5')
        second.wait(timeout=6)
        self.assertEqual((self.control()['phase'], self.control()['reason']), ('settled', 'cancelled'))
        self.cli()
        self.assertEqual(self.effects(), ['1'])
        self.assertEqual(self.row()['recovery_state'], 'retry_cancelled')
        self.assertEqual(json.loads(self.numbered(1).read_text())['status'], 'failed')
        self.cli('--retry-failed', '--retry-backoff-seconds', '0')
        self.assertEqual(self.effects(), ['1', '2'])
        self.assertNotEqual(self.control()['chain_id'], state_a['chain_id'])

    def test_compatibility_receipt_failure_does_not_diverge_retry_schedule(self):
        self.behavior(self.root, {'a': ['transient', 'complete']})
        run = self.run_wrapped('''import asyncio
import dispatch
write = dispatch.write_json
def injected(path, value):
    if path.name == 'receipt.json':
        raise PermissionError('injected compatibility write failure')
    write(path, value)
dispatch.write_json = injected
asyncio.run(dispatch.main())
''', '--retry-backoff-seconds', '0')
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual((json.loads(self.numbered(2).read_text())['execution'], self.control()['phase']),
                         ('succeeded', 'settled'))
        self.assertEqual(self.effects(), ['1', '2'])
        self.cli('--retry-failed')
        self.assertEqual(self.effects(), ['1', '2'])

    def test_authoritative_prepare_failure_does_not_launch(self):
        run = self.run_wrapped('''import asyncio
import dispatch
write = dispatch.write_json
def injected(path, value):
    if path.name == 'receipt-01.json':
        raise PermissionError('injected authoritative write failure')
    write(path, value)
dispatch.write_json = injected
asyncio.run(dispatch.main())
''')
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertFalse((self.directory / 'effects').exists())
        self.assertFalse(self.numbered(1).exists())
        self.assertEqual(self.row()['status'], 'failed')
