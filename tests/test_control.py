"""Control-plane tests use local, fake dispatchers and disposable process groups."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
import batch_control as control


FAKE = r'''
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

def write(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value))
    temp.replace(path)

def ticks(pid):
    return Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]

p = argparse.ArgumentParser()
p.add_argument('--run-dir')
p.add_argument('--_run-id')
p.add_argument('--manifest', default='hold')
p.add_argument('--only', nargs='*')
for flag in ('channel', 'concurrency', 'ramp-seconds', 'task-timeout-seconds',
             'max-retries', 'retry-backoff-seconds', 'status-interval-seconds', 'idle-warning-seconds'):
    p.add_argument('--' + flag)
for flag in ('retry-failed', 'no-dashboard'):
    p.add_argument('--' + flag, action='store_true')
args = p.parse_args()
run = Path(args.run_dir) / 'runs' / args._run_id
print('fake stdout', flush=True)
print('fake stderr', file=sys.stderr, flush=True)
if args.manifest == 'invalid':
    print('dispatch: invalid manifest', file=sys.stderr, flush=True)
    sys.exit(2)

status = dict(state='running', batch_id=str(run), run_id=run.name, manifest=args.manifest,
              pid=os.getpid(), process_start_ticks=ticks(os.getpid()), cursor=0,
              boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
              heartbeat_utc='old on purpose', running_ids=['peer'], stalled_ids=['peer'])
def stop(*_):
    write(run / 'status.json', {**status, 'state': 'interrupted'})
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
write(run / 'config.json', dict(**vars(args), sid=os.getsid(0), executable=sys.executable,
                                stdin_closed=sys.stdin.read() == ''))
if args.manifest == 'delayed':
    while not (run / 'ready').exists():
        time.sleep(.02)
write(run / 'status.json', status)
while not (run / 'finish').exists():
    time.sleep(.02)
write(run / 'status.json', {**status, 'state': 'complete'})
'''


class ControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fake = self.root / 'dispatch.py'
        self.fake.write_text(FAKE)
        patcher = patch.object(control, 'DISPATCHER', self.fake)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.stop_fake_dispatchers)

    def stop_fake_dispatchers(self):
        for path in self.root.glob('runs/*/submission.json'):
            meta = json.loads(path.read_text())
            fd = control._pin(meta, control._boot_id())
            if fd is not None:
                try:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                finally:
                    os.close(fd)

    def launch(self, manifest='hold', **kwargs):
        return control.submit(self.root, argparse.Namespace(manifest=manifest, **kwargs))

    @staticmethod
    def append_rows(run, rows):
        with (run / 'result-events.jsonl').open('a') as stream:
            for cursor, state, issues in rows:
                stream.write(json.dumps(dict(cursor=cursor, id=f't{cursor}', status=state,
                                             issues=issues, summary='', artifacts=[])) + '\n')

    def wait_until(self, predicate, timeout=4):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if result := predicate():
                return result
            time.sleep(.02)
        self.fail('condition did not become true')

    def test_detached_submission_survives_submitter_exit_and_copies_options(self):
        options = dict(manifest='hold', channel='api', concurrency=3, ramp_seconds=0, timeout_seconds=1.25,
                       max_retries=0, retry_backoff_seconds=3.5, status_interval_seconds=.3,
                       idle_warning_seconds=.7, only=['a', 'b'], retry_failed=True, no_dashboard=True)
        code = ('import argparse,json,sys; from pathlib import Path; import batch_control as b; '
                'b.DISPATCHER=Path(sys.argv[1]); '
                'print(json.dumps(b.submit(Path(sys.argv[2]), argparse.Namespace(**json.loads(sys.argv[3])))))')
        proc = subprocess.run([sys.executable, '-B', '-c', code, str(self.fake), str(self.root),
                               json.dumps(options)], env={**os.environ, 'PYTHONPATH': str(SCRIPTS)},
                              capture_output=True, text=True, check=True, timeout=8)
        submitted = json.loads(proc.stdout)
        run = Path(submitted['batch_id'])
        self.assertEqual(submitted['state'], 'running')
        self.assertEqual(run.parent, self.root / 'runs')
        self.assertEqual(run.name, submitted['run_id'])
        config = json.loads((run / 'config.json').read_text())
        recorded = json.loads((run / 'submission.json').read_text())
        self.assertEqual(config['sid'], recorded['pid'])
        self.assertTrue(recorded['process_start_ticks'])
        self.assertEqual(config['executable'], sys.executable)
        self.assertTrue(config['stdin_closed'])
        for option, value in options.items():
            key = 'task_timeout_seconds' if option == 'timeout_seconds' else option
            self.assertEqual(config[key], str(value) if type(value) in (int, float) else value)
        self.assertEqual(Path(submitted['log_path']).read_text().splitlines(), ['fake stdout', 'fake stderr'])
        self.assertTrue(control.read_status(run)['scheduler_live'])

    def test_starting_timeout_uses_own_invocation_and_does_not_kill(self):
        old = self.launch()
        (self.root / 'status-delayed.json').write_text(json.dumps(old))
        with patch.object(control, 'STARTUP_WAIT_SECONDS', .12):
            submitted = self.launch('delayed')
        self.assertEqual(submitted['state'], 'starting')
        self.assertNotEqual(submitted['batch_id'], old['batch_id'])
        self.assertTrue(submitted['scheduler_live'])
        run = Path(submitted['batch_id'])
        (run / 'ready').touch()
        self.wait_until(lambda: control.read_status(run)['state'] == 'running')

    def test_invalid_startup_keeps_log_and_exit_code(self):
        submitted = self.launch('invalid')
        self.assertEqual(submitted['state'], 'startup_failed')
        self.assertEqual(submitted['exit_code'], 2)
        self.assertIn('invalid manifest', submitted['error'])
        self.assertIn('invalid manifest', Path(submitted['log_path']).read_text())
        self.assertFalse(Path(submitted['status_path']).exists())
        again = control.wait_for_event(submitted['batch_id'], 0, 1)
        self.assertEqual(again['state'], 'startup_failed')
        self.assertFalse(again['waiting_timed_out'])

    def test_wait_coalesces_successes_and_expires_without_stopping(self):
        run = Path(self.launch()['batch_id'])
        self.append_rows(run, [(1, 'complete', []), (2, 'complete', [])])
        result = control.wait_for_event(run, 0, .25)
        self.assertTrue(result['waiting_timed_out'])
        self.assertEqual(result['items'], [])
        self.assertEqual(result['cursor'], 2)
        status = control.read_status(run)
        self.assertTrue(status['scheduler_live'])
        self.assertEqual(status['stalled_ids'], ['peer'])
        (run / 'finish').touch()
        self.assertEqual(control.wait_for_event(run, result['cursor'], 2)['state'], 'complete')

    def test_attention_precedes_final_status_while_peer_runs(self):
        run = Path(self.launch()['batch_id'])
        self.append_rows(run, [(1, 'complete', []), (2, 'blocked', []),
                               (3, 'complete', ['review needed']), (4, 'complete', [])])
        result = control.wait_for_event(run, 0, 2, 1)
        self.assertEqual(result['state'], 'running')
        self.assertEqual(result['event'], 'attention')
        self.assertEqual([row['id'] for row in result['items']], ['t2'])
        self.assertEqual(result['cursor'], 2)
        self.assertEqual(control.read_status(run)['running_ids'], ['peer'])
        self.assertEqual(control.read_status(run)['cursor'], 0)  # Commit precedes the next snapshot.
        second = control.wait_for_event(run, result['cursor'], 0, 1)
        self.assertEqual([row['id'] for row in second['items']], ['t3'])
        self.assertEqual(second['cursor'], 3)
        self.assertEqual(control.wait_for_event(run, second['cursor'], 0)['cursor'], 4)

    def test_cursor_and_offset_pagination_do_not_skip_results(self):
        run = Path(self.launch()['batch_id'])
        self.append_rows(run, [(1, 'complete', []), (2, 'failed', []), (3, 'complete', []),
                               (4, 'partial', []), (5, 'complete', [])])
        with (run / 'result-events.jsonl').open('a') as stream:
            stream.write('{"cursor":6')
        page = control.results_page(run, after_cursor=0, limit=2)
        self.assertEqual((page['cursor'], page['high_watermark']), (2, 5))
        next_page = control.results_page(run, after_cursor=page['cursor'], limit=2)
        self.assertEqual([row['id'] for row in next_page['items']], ['t3', 't4'])
        selected = control.results_page(run, attention=True, offset=1, limit=1)
        self.assertEqual([row['id'] for row in selected['items']], ['t4'])
        self.assertEqual(selected['total'], 2)
        self.assertIsNone(selected['next_offset'])
        self.assertEqual(control.results_page(run, limit=2)['next_offset'], 2)

    def test_wait_retries_partial_tail_and_stops_at_attention_limit(self):
        run = Path(self.launch()['batch_id'])
        path = run / 'result-events.jsonl'
        self.append_rows(run, [(1, 'complete', [])])
        waits = iter(('{"cursor":2,"status":', '"blocked","id":"t2"}\n'
                      '{"cursor":3,"status":"failed","id":"t3"}\n'))

        def append_after_poll(_):
            with path.open('a') as stream:
                stream.write(next(waits))
            return True

        with patch.object(control, '_pause', side_effect=append_after_poll):
            first = control.wait_for_event(run, 0, 2, 1)
        self.assertEqual([row['id'] for row in first['items']], ['t2'])
        self.assertEqual(first['cursor'], 2)
        second = control.wait_for_event(run, first['cursor'], 0, 1)
        self.assertEqual([row['id'] for row in second['items']], ['t3'])

    def test_legacy_final_index_and_snapshot(self):
        run = self.root / 'runs' / '1'
        run.mkdir(parents=True)
        (run / 'status.json').write_text(json.dumps(dict(state='complete', run_id='1', utc='then')))
        (run / 'results.jsonl').write_text('{"id":"a","status":"complete"}\n{"id":"b","status":"partial"}\n')
        self.assertEqual(control.read_status(run)['heartbeat_utc'], 'then')
        result = control.wait_for_event(run, 1, 0)
        self.assertEqual(result['state'], 'complete')
        self.assertEqual(result['cursor'], 2)
        self.assertEqual([row['id'] for row in result['items']], ['b'])

    def test_final_offset_keeps_commit_order_and_can_resume_with_cursor(self):
        run = Path(self.launch()['batch_id'])
        self.append_rows(run, [(1, 'failed', []), (2, 'partial', []), (3, 'blocked', [])])
        rows = (run / 'result-events.jsonl').read_text().splitlines()
        (run / 'results.jsonl').write_text('\n'.join(reversed(rows)) + '\n')
        (run / 'finish').touch()
        self.wait_until(lambda: control.read_status(run)['state'] == 'complete')
        page = control.results_page(run, limit=1)
        self.assertEqual([row['id'] for row in page['items']], ['t1'])
        self.assertEqual(page['cursor'], 1)
        self.assertEqual(page['next_offset'], 1)
        live = control.results_page(run, after_cursor=page['cursor'], limit=1)
        self.assertEqual([row['id'] for row in live['items']], ['t2'])

    def test_cancel_signals_only_the_requested_invocation(self):
        old, new = self.launch(), self.launch()
        (self.root / 'status-hold.json').write_text(json.dumps(new))
        result = control.cancel(old['batch_id'], 3)
        self.assertTrue(result['dispatcher_term_sent'])
        # This legacy fixture has no launch journal; stopping its dispatcher
        # cannot certify that all historically launched tools are gone.
        self.assertFalse(result['stopped'])
        self.assertEqual(result['state'], 'interrupted')
        self.assertEqual(json.loads(Path(result['cancel_path']).read_text())['outcome'], 'worker_cleanup_unconfirmed')
        self.assertTrue(control.read_status(new['batch_id'])['scheduler_live'])

    def spawn_worker(self, code='import time; time.sleep(30)'):
        worker = subprocess.Popen([sys.executable, '-c', code], start_new_session=True)
        def cleanup():
            # The test owns this session. Its process cannot have recycled before wait().
            try:
                os.killpg(worker.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            worker.wait(timeout=4)
        self.addCleanup(cleanup)
        return worker

    def kill_dispatcher(self, status):
        fd = control._pin(status, control._boot_id())
        self.assertIsNotNone(fd)
        try:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
        finally:
            os.close(fd)

    def test_scheduler_loss_uses_worker_start_event_and_escalates_group(self):
        status = self.launch()
        run = Path(status['batch_id'])
        ready = run / 'descendant'
        code = ('import signal,subprocess,sys,time; '
                f'p=subprocess.Popen([sys.executable,"-c","import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); open({str(ready)!r}, \'w\').close(); time.sleep(30)"]); '
                'time.sleep(30)')
        worker = self.spawn_worker(code)
        self.wait_until(ready.exists)
        row = dict(event='worker_started', task_id='peer', pid=worker.pid,
                   process_start_ticks=control._process(worker.pid).ticks, boot_id=control._boot_id())
        (run / 'events.jsonl').write_text(json.dumps(row) + '\n')
        self.kill_dispatcher(status)
        lost = control.wait_for_event(run, 0, 3)
        self.assertEqual(lost['state'], 'orphaned')
        self.assertFalse(lost['waiting_timed_out'])
        result = control.cancel(run, 3)
        self.assertFalse(result['stopped'], result)
        self.assertEqual(result['outcome'], 'worker_cleanup_unconfirmed')
        signals = result['attempts']['peer/00']['signals']
        self.assertTrue(any(row['signal'] == 'SIGKILL' and row['outcome'] == 'sent' for row in signals))
        self.assertEqual(worker.wait(timeout=2), -signal.SIGTERM)
        self.assertFalse(control._process(worker.pid))

    def test_scheduler_loss_recovers_only_exact_batch_receipts(self):
        status, new = self.launch(), self.launch()
        run = Path(status['batch_id'])
        owned, unrelated = self.spawn_worker(), self.spawn_worker()
        for task, batch, worker in (('peer', status, owned), ('other', new, unrelated)):
            directory = self.root / 'workers' / task
            directory.mkdir(parents=True)
            receipt = dict(batch_id=batch['batch_id'], task_id=task, status='running', attempt=1,
                           pid=worker.pid, process_start_ticks=control._process(worker.pid).ticks,
                           boot_id=control._boot_id())
            (directory / 'receipt.json').write_text(json.dumps(receipt))
        self.kill_dispatcher(status)
        self.assertEqual(control.wait_for_event(run, 0, 3)['state'], 'orphaned')
        cancelled = control.cancel(run, 2)
        self.assertFalse(cancelled['stopped'], cancelled)
        self.assertEqual(list(cancelled['attempts']), ['peer/01'])
        self.assertEqual(owned.wait(timeout=2), -signal.SIGTERM)
        self.assertIsNone(unrelated.poll())
        self.assertTrue(control.read_status(new['batch_id'])['scheduler_live'])

    def test_cancel_leaves_unverified_worker_alone(self):
        status = self.launch()
        run = Path(status['batch_id'])
        worker = self.spawn_worker()
        wrong_ticks = str(int(control._process(worker.pid).ticks) + 1)
        snapshot = json.loads((run / 'status.json').read_text())
        snapshot['running_workers'] = dict(peer=dict(pid=worker.pid, process_start_ticks=wrong_ticks))
        (run / 'status.json').write_text(json.dumps(snapshot))
        self.kill_dispatcher(status)
        self.wait_until(lambda: not control.read_status(run)['scheduler_live'])
        result = control.cancel(run, 1)
        self.assertFalse(result['stopped'])
        attempt = result['attempts']['peer/00']
        self.assertFalse(attempt['stopped'])
        self.assertTrue(attempt['uncertain'])
        self.assertEqual(attempt['signals'], [])
        self.assertIsNone(worker.poll())


if __name__ == '__main__':
    unittest.main()
