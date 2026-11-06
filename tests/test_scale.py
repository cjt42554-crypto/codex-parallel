"""Batch semantics and scale checks. All EXEC processes are local test doubles."""
import argparse
import asyncio
from contextlib import redirect_stderr, redirect_stdout
import fcntl
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import tracemalloc
import unittest
from unittest.mock import patch

import test_dispatch as fixtures


class ScaleTests(fixtures.DispatchFixture):
    def module(self):
        spec = importlib.util.spec_from_file_location('scale_dispatch', self.scripts / 'dispatch.py')
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def report(self):
        result = self.run_cli('--report')
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def batch_args(self, **changes):
        defaults = dict(manifest='tasks.json', only=None, channel='sub', retry_failed=False,
                        timeout_seconds=5, concurrency=1, ramp_seconds=0,
                        status_interval_seconds=.1, retry_backoff_seconds=0, max_retries=2)
        return argparse.Namespace(**{**defaults, **changes})

    def test_manifest_defaults_and_overrides_are_explicit_in_every_command(self):
        manifest = dict(defaults=dict(model='default-model', effort='medium'), tasks=[
            dict(id='a', prompt='work'), dict(id='b', model='other-model', effort='high', prompt='other work')])
        (self.root / 'tasks.json').write_text(json.dumps(manifest))
        result = self.run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        for name, model, effort in [('a', 'default-model', 'medium'), ('b', 'other-model', 'high')]:
            cmd = self.receipt(name)['command']
            self.assertEqual(cmd[cmd.index('-m')+1], model)
            self.assertIn('model_reasoning_effort=' + json.dumps(effort), cmd)
        # Changing inherited execution inputs is still caught before any worker is launched.
        manifest['defaults']['effort'] = 'high'
        (self.root / 'tasks.json').write_text(json.dumps(manifest))
        self.assertIn('inputs/configuration changed', self.run_cli().stderr)
        self.assertEqual(self.receipt()['attempt'], 1)

    def test_transient_retries_are_bounded_and_preserve_evidence(self):
        ids = ['a', 'exhausted', 'blocked', 'partial', 'quota', 'bad_schema', 'tool']
        self.jobs(self.root, ids)
        self.behavior(self.root, dict(a=['transient', 'complete'], exhausted='transient',
                                     blocked='blocked', partial='partial', quota='quota',
                                     bad_schema='bad_schema', tool='transient_tool'))
        result = self.run_cli('--retry-backoff-seconds', '0', '--ramp-seconds', '0')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.receipt()['status'], 'complete')
        self.assertEqual(self.receipt()['attempt'], 2)
        self.assertEqual(self.receipt('exhausted')['attempt'], 3)
        for name in ids[2:]:
            self.assertEqual(self.receipt(name)['attempt'], 1, name)
        first = json.loads((self.root / 'workers/a/receipt-01.json').read_text())
        self.assertEqual(first['status'], 'failed')
        self.assertEqual(self.receipt()['model_requested'], first['model_requested'])
        self.assertEqual(self.receipt()['reasoning_effort_requested'], first['reasoning_effort_requested'])
        self.assertEqual(self.report()['automatic_retries'], 3)
        # A subsequent ordinary invocation reuses failed results too; it must not silently retry them.
        self.assertEqual(self.run_cli('--retry-backoff-seconds', '0').returncode, 0)
        self.assertEqual(self.receipt('exhausted')['attempt'], 3)
        self.assertEqual(self.report()['automatic_retries'], 0)

    def test_retry_safe_opt_in_and_disable(self):
        self.jobs(self.root, ['a', 'safe'])
        jobs = json.loads((self.root / 'tasks.json').read_text())
        jobs[1]['retry_safe'] = True
        (self.root / 'tasks.json').write_text(json.dumps(jobs))
        self.behavior(self.root, {'a': 'transient', 'safe': ['transient_tool', 'complete']})
        self.assertEqual(self.run_cli('--max-retries', '0').returncode, 0)
        self.assertEqual(self.receipt('safe')['attempt'], 1)
        self.assertEqual(self.run_cli('--only', 'safe', '--retry-failed', '--retry-backoff-seconds', '0').returncode, 0)
        self.assertEqual(self.receipt('safe')['status'], 'complete')
        # Retry-safe allows a fresh failure after tool use to receive an automatic retry.
        other = self.make_root('safe-run')
        (other / 'tasks.json').write_text(json.dumps(dict(defaults=dict(model='fake', effort='low', retry_safe=True),
                                                       tasks=[dict(id='a', prompt='work')])) )
        self.behavior(other, {'a': ['transient_tool', 'complete']})
        self.assertEqual(self.run_cli('--retry-backoff-seconds', '0', root=other).returncode, 0)
        self.assertEqual(json.loads((other / 'workers/a/receipt.json').read_text())['attempt'], 2)

    def test_backoff_does_not_hold_worker_slot(self):
        self.jobs(self.root, ['a', 'b'])
        self.behavior(self.root, {'a': ['transient', 'complete']})
        result = self.run_cli('--concurrency', '1', '--retry-backoff-seconds', '.6')
        self.assertEqual(result.returncode, 0, result.stderr)
        report = self.report()
        events = [json.loads(line) for line in (Path(report['summary_path']).parent / 'events.jsonl').read_text().splitlines()]
        starts = [event['task_id'] for event in events if event['event'] == 'worker_started']
        self.assertEqual(starts, ['a', 'b', 'a'])
        self.assertEqual(report['peak_workers'], 1)

    def test_interrupt_during_backoff_retains_failed_attempt(self):
        self.behavior(self.root, {'a': 'transient'})
        proc = self.start(None, '--status-interval-seconds', '.1')
        until = time.monotonic() + 5
        status = {}
        while time.monotonic() < until:
            try:
                status = json.loads((self.root / 'status-tasks.json').read_text())
            except (OSError, ValueError):
                pass
            if status.get('retrying_ids') == ['a']:
                break
            time.sleep(.03)
        self.assertEqual(status.get('retrying_ids'), ['a'])
        self.assertEqual(status['running'], 0)
        proc.terminate()
        proc.wait(timeout=12)
        report = self.report()
        self.assertEqual(report['state'], 'interrupted')
        self.assertEqual(report['counts'], {'failed': 1})
        self.assertFalse(report['ok'])
        self.assertEqual(self.receipt()['attempt'], 1)

    def test_compact_report_attention_and_result_pagination(self):
        ids = [f'task-{i:03}' for i in range(30)]
        self.jobs(self.root, ids)
        self.behavior(self.root, {ids[1]: 'blocked', ids[2]: 'issues'})
        result = self.run_cli('--concurrency', '8', '--ramp-seconds', '0')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(result.stdout.splitlines()), 2)
        self.assertNotIn('task-029', result.stderr)
        report = self.report()
        self.assertFalse(report['ok'])
        self.assertEqual(report['attention_count'], 2)
        page = json.loads(self.run_cli('--results', '--attention', '--limit', '1').stdout)
        self.assertEqual(page['total'], 2)
        self.assertEqual(page['items'][0]['id'], ids[1])
        self.assertEqual(page['next_offset'], 1)
        page = json.loads(self.run_cli('--results', '--attention', '--limit', '1', '--offset', '1').stdout)
        self.assertEqual(page['items'][0]['id'], ids[2])
        self.assertIsNone(page['next_offset'])
        all_rows = [json.loads(line) for line in Path(report['results_path']).read_text().splitlines()]
        self.assertEqual(len(all_rows), 30)
        self.assertEqual(all_rows[-1]['summary'], 'body')

    def test_hundreds_of_workers_respect_bound_and_throttle_sampling(self):
        self.jobs(self.root, [f'task-{i:03}' for i in range(128)])
        start = time.monotonic()
        result = self.run_cli('--concurrency', '16', '--ramp-seconds', '0')
        elapsed = time.monotonic() - start
        self.assertEqual(result.returncode, 0, result.stderr)
        report = self.report()
        self.assertTrue(report['ok'])
        self.assertEqual(report['counts'], {'complete': 128})
        self.assertLessEqual(report['peak_workers'], 16)
        self.assertGreater(report['peak_workers'], 1)
        self.assertEqual(len(result.stdout.splitlines()), 2)
        samples = (Path(report['summary_path']).parent / 'resources.jsonl').read_text().splitlines()
        self.assertLessEqual(len(samples), int(elapsed / 2) + 3)
        print(json.dumps(dict(benchmark='128_fake_exec', elapsed_seconds=round(elapsed, 3),
                              peak_workers=report['peak_workers'], status_samples=len(samples))))

    def test_thousand_cached_jobs_reuse_fast_without_common_copies(self):
        self.jobs(self.root, [f'task-{i:04}' for i in range(1000)])
        (self.root / 'COMMON.md').write_text('C' * 65536)
        mod = self.module()
        args = argparse.Namespace(manifest='tasks.json', only=None, channel='sub', retry_failed=False)
        tracemalloc.start()
        jobs = mod.prepare_jobs(self.root, args)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertLess(peak, 16 * 1024 * 1024)
        for job in jobs:
            directory = self.root / 'workers' / job['id']
            directory.mkdir(parents=True)
            (directory / 'receipt.json').write_text(json.dumps(dict(status='complete', attempt=1,
                input_fingerprint=job['_fingerprint'], result_file='result-01.json')))
            (directory / 'result-01.json').write_text(json.dumps(dict(status='complete', summary='cached', artifacts=[], issues=[])))
        start = time.monotonic()
        result = self.run_cli()
        elapsed = time.monotonic() - start
        self.assertEqual(result.returncode, 0, result.stderr)
        report = self.report()
        self.assertEqual(report['reused_count'], 1000)
        self.assertEqual(report['peak_workers'], 0)
        self.assertEqual(report['counts'], {'complete': 1000})
        self.assertLess(elapsed, 8, 'cached results must not pay a per-task launch delay')
        self.assertLess(len(result.stdout) + len(result.stderr), 8000)
        self.assertFalse(any((self.root / 'workers').glob('*/started')))
        print(json.dumps(dict(benchmark='1000_cached_64KiB_common', elapsed_seconds=round(elapsed, 3),
                              prepare_peak_bytes=peak, output_bytes=len(result.stdout) + len(result.stderr))))

    def test_result_evidence_stays_bound_to_native_and_legacy_attempts(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                root = self.make_root('legacy') if legacy else self.root
                self.behavior(root, {'a': 'partial'})
                self.assertEqual(self.run_cli(root=root).returncode, 0)
                directory = root / 'workers/a'
                if legacy:
                    receipt = json.loads((directory / 'receipt.json').read_text())
                    receipt.pop('result_file')
                    (directory / 'receipt.json').write_text(json.dumps(receipt))
                    (directory / 'receipt-01.json').unlink()
                    self.assertEqual(self.run_cli(root=root).returncode, 0)
                row = json.loads(self.run_cli('--results', root=root).stdout)['items'][0]
                self.behavior(root, {})
                self.assertEqual(self.run_cli('--only', 'a', '--retry-failed', root=root).returncode, 0)
                saved = json.loads(Path(row['receipt_path']).read_text())
                self.assertEqual((saved['status'], saved['attempt']), ('partial', 1))
                self.assertEqual(json.loads(Path(row['result_path']).read_text())['status'], 'partial')
                self.assertEqual(json.loads((directory / 'receipt.json').read_text())['attempt'], 2)

    def test_damaged_saved_result_reference_is_reported_without_losing_peers(self):
        self.behavior(self.root, {'a': 'partial'})
        self.assertEqual(self.run_cli().returncode, 0)
        receipt = self.receipt()
        receipt['result_file'] = None
        (self.root / 'workers/a/receipt.json').write_text(json.dumps(receipt))
        self.jobs(self.root, ['a', 'b'])
        result = self.run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.report()['counts'], {'partial': 1, 'complete': 1})
        attention = json.loads(self.run_cli('--results', '--attention').stdout)['items'][0]
        self.assertIn('invalid result filename', attention['issues'][0])

    def test_relative_run_directory_reports_live_scheduler(self):
        self.behavior(self.root, {'a': 'hang'})
        proc = subprocess.Popen(self.command('run'), cwd=self.base,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(proc)
        self.wait_started()
        status = json.loads(self.run_cli('--status').stdout)
        self.assertEqual(status['state'], 'running')
        self.assertTrue(status['scheduler_live'])
        status['process_start_ticks'] = 'different-process'
        self.assertFalse(self.module().has_live_worker(status, 'dispatch.py'))
        proc.terminate()
        proc.wait(timeout=12)

    def test_ready_retry_reports_memory_wait_and_recovers(self):
        self.behavior(self.root, {'a': ['transient', 'complete']})
        mod = self.module()
        args = self.batch_args()
        jobs = mod.prepare_jobs(self.root, args)
        memory = {'kib': 3 * 1024 * 1024}

        async def scenario():
            with (self.root / '.dispatch.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                batch = mod.Batch(self.root, args, jobs, lock.fileno())
                original = batch.worker

                async def worker(job, force=False):
                    await original(job, force)
                    if batch.results[job['id']]['status'] == 'failed':
                        memory['kib'] = 0

                batch.worker = worker
                task = asyncio.create_task(batch.run())
                try:
                    until = time.monotonic() + 3
                    while time.monotonic() < until:
                        await asyncio.sleep(.03)
                        status = json.loads((self.root / 'status-tasks.json').read_text())
                        if status['waiting_for_memory']:
                            break
                    self.assertTrue(status['waiting_for_memory'])
                    self.assertEqual(status['retrying_ids'], ['a'])
                    self.assertEqual(status['running'], 0)
                    memory['kib'] = 3 * 1024 * 1024
                    await asyncio.wait_for(task, 3)
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

        with patch.object(mod, 'available_kib', side_effect=lambda: memory['kib']), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            asyncio.run(scenario())
        self.assertEqual(self.receipt()['attempt'], 2)
        self.assertTrue(self.report()['ok'])

    def test_repeated_cancellation_during_timeout_cleanup_reaps_process_group(self):
        (self.scripts / 'codexexec').write_text('''#!/usr/bin/env python3
import json,signal,subprocess,sys,time
from pathlib import Path
worker=Path(sys.argv[sys.argv.index('-C')+1])
sys.stdin.read()
signal.signal(signal.SIGTERM,signal.SIG_IGN)
child=subprocess.Popen([sys.executable,'-c','import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(30)'],close_fds=False)
(worker/'child.pid').write_text(str(child.pid))
print(json.dumps({'type':'turn.started'}),flush=True)
time.sleep(30)
''')
        mod = self.module()
        mod.TERMINATE_GRACE_SECONDS = .3
        args = self.batch_args(timeout_seconds=.2)
        jobs = mod.prepare_jobs(self.root, args)

        async def scenario():
            started = asyncio.Event()
            processes = []
            original = mod.stop_worker

            async def stop(proc):
                processes.append(proc)
                started.set()
                await original(proc)

            with (self.root / '.dispatch.lock').open('a') as lock, patch.object(mod, 'stop_worker', side_effect=stop):
                fcntl.flock(lock, fcntl.LOCK_EX)
                batch = mod.Batch(self.root, args, jobs, lock.fileno())
                task = asyncio.create_task(batch.run())
                try:
                    await asyncio.wait_for(started.wait(), 3)
                    task.cancel()
                    await asyncio.sleep(.03)
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(task, 3)
                    self.assertEqual(self.receipt()['status'], 'interrupted')
                    self.assertEqual(batch.results['a']['status'], 'interrupted')
                    pid = int((self.root / 'workers/a/child.pid').read_text())
                    try:
                        child_state = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[0]
                    except FileNotFoundError:
                        child_state = None
                    self.assertIn(child_state, (None, 'Z'))
                finally:
                    if not task.done():
                        task.cancel()
                    for proc in processes:
                        try:
                            os.killpg(proc.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        await proc.wait()
                    await asyncio.gather(task, return_exceptions=True)

        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            asyncio.run(scenario())
        # A survivor retaining the inherited flock would reject this next invocation.
        result = self.run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
