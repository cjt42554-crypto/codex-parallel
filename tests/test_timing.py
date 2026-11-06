"""Execution budgets and warning semantics, using only fake local EXEC workers."""
import json
from pathlib import Path
import time
import unittest

import test_dispatch as fixtures


class TimingTests(fixtures.DispatchFixture):
    def test_queue_wait_is_excluded_from_each_execution_budget(self):
        self.jobs(self.root, ['a', 'b', 'c'])
        self.behavior(self.root, {key: 'brief' for key in ('a', 'b', 'c')})
        result = self.run_cli('--concurrency', '1', '--task-timeout-seconds', '.6')
        self.assertEqual(result.returncode, 0, result.stderr)
        for key in ('a', 'b', 'c'):
            receipt = self.receipt(key)
            self.assertEqual(receipt['status'], 'complete')
            self.assertFalse(receipt['timed_out'])
            self.assertLess(receipt['execution_elapsed_seconds'], .6)
        self.assertGreater(self.receipt('c')['queue_wait_seconds'], .6)
        report = json.loads(self.run_cli('--report').stdout)
        self.assertGreater(report['batch_elapsed_seconds'], .9)

    def test_per_task_override_can_disable_or_shorten_default_budget(self):
        manifest = dict(defaults=dict(model='fake', effort='low', task_timeout_seconds=.1), tasks=[
            dict(id='a', prompt='work'), dict(id='b', prompt='work', task_timeout_seconds=0)])
        (self.root / 'tasks.json').write_text(json.dumps(manifest))
        self.behavior(self.root, {'a': 'brief', 'b': 'brief'})
        result = self.run_cli('--task-timeout-seconds', '5')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.receipt('a')['status'], 'failed')
        self.assertTrue(self.receipt('a')['timed_out'])
        self.assertEqual(self.receipt('a')['attempt'], 1)
        self.assertEqual(self.receipt('b')['status'], 'complete')
        self.assertEqual(self.receipt('b')['task_timeout_seconds'], 0)

    def test_silence_warning_does_not_cancel_unlimited_task(self):
        self.behavior(self.root, {'a': 'hang'})
        proc = self.start(None, '--idle-warning-seconds', '.1', '--status-interval-seconds', '.1')
        self.wait_started()
        until = time.monotonic() + 4
        while time.monotonic() < until:
            status = json.loads(self.run_cli('--status').stdout)
            if status.get('stalled_ids') == ['a']:
                break
            time.sleep(.03)
        self.assertEqual(status.get('stalled_ids'), ['a'])
        self.assertTrue(status['scheduler_live'])
        self.assertIsNone(proc.poll())
        self.assertEqual(self.receipt()['status'], 'running')
        self.assertEqual(self.receipt()['task_timeout_seconds'], 0)
        proc.terminate()
        proc.wait(timeout=12)
        self.assertEqual(self.receipt()['status'], 'interrupted')

    def test_transient_retry_publishes_only_final_outcome_and_separate_backoff(self):
        self.behavior(self.root, {'a': ['transient', 'complete']})
        result = self.run_cli('--retry-backoff-seconds', '.3')
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(self.run_cli('--report').stdout)
        rows = [json.loads(line) for line in (Path(report['batch_id']) / 'result-events.jsonl').read_text().splitlines()]
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]['cursor'], rows[0]['attempt'], rows[0]['status']), (1, 2, 'complete'))
        self.assertGreater(rows[0]['retry_backoff_seconds'], .2)
        self.assertLess(rows[0]['queue_wait_seconds'], .2)
        self.assertEqual(report['automatic_retries'], 1)

    def test_invalid_task_budgets_are_rejected_before_launch(self):
        for value in (-1, float('nan'), float('inf'), True, '10'):
            self.jobs(self.root, ['a'])
            manifest = json.loads((self.root / 'tasks.json').read_text())
            manifest[0]['task_timeout_seconds'] = value
            (self.root / 'tasks.json').write_text(json.dumps(manifest))
            with self.subTest(value=value):
                result = self.run_cli()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.root / 'workers').exists())


if __name__ == '__main__':
    unittest.main()
