"""Dashboard regressions with temporary batches and fake terminal launchers."""

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch


SOURCE = Path(__file__).resolve().parents[1] / 'scripts' / 'dashboard.py'
SPEC = importlib.util.spec_from_file_location('dashboard', SOURCE)
dashboard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dashboard)


def write_json(path, **value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value))
    temp.replace(path)


@contextmanager
def observe_reads():
    reads = []
    read_text = Path.read_text

    def track(path, *args, **kwargs):
        reads.append(path)
        return read_text(path, *args, **kwargs)

    with patch.object(Path, 'read_text', track):
        yield reads


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / 'batch'
        self.registry = self.base / 'registry'
        self.registry.mkdir()
        registry = patch.object(dashboard, 'registry_directory', return_value=self.registry)
        registry.start()
        self.addCleanup(registry.stop)

    def manifest(self, *ids, root=None):
        root = root or self.root
        write_json(root / 'tasks.json', defaults=dict(model='worker', effort='high', retry_safe=True),
                   tasks=[dict(id=task_id, prompt=f'draft {task_id}') for task_id in ids])

    def worker(self, task_id, *, attempt=1, status='complete', root=None):
        directory = (root or self.root) / 'workers' / task_id
        write_json(directory / 'receipt.json', status=status, attempt=attempt,
                   result_file=f'result-{attempt:02d}.json')
        (directory / f'prompt-{attempt:02d}.txt').write_text(f'sent {task_id}/{attempt}')
        write_json(directory / f'result-{attempt:02d}.json', summary=f'answer {task_id}/{attempt}')
        return directory

    def register(self, root, time=1):
        write_json(self.registry / 'batches' / f'{root.name}.json', run_dir=str(root), registered_at=time)

    def status(self, root=None, **changes):
        status = dict(run_id='1', pid=123, manifest='tasks.json', state='running', total=3,
                      counts={}, pending=0, running=0, running_ids=[], queued_ids=[], retrying_ids=[],
                      selected_ids=[])
        status.update(changes)
        write_json((root or self.root) / 'status-tasks.json', **status)

    def test_ui_reads_only_selected_attempt_details(self):
        ids = [f'task{i:02d}' for i in range(25)]
        self.manifest(*ids)
        for task_id in ids:
            self.worker(task_id)
        screen = Mock()
        screen.getmaxyx.return_value = (28, 110)
        screen.getch.side_effect = [-1, ord('j'), -1, ord('q')]
        with (patch.object(dashboard.curses, 'curs_set'),
              patch.object(dashboard.curses, 'has_colors', return_value=False),
              patch.object(dashboard.time, 'monotonic', return_value=1), observe_reads() as reads):
            dashboard.ui(screen, self.root, 'tasks.json')
        detail_reads = [(p.parent.name, p.name) for p in reads if p.name.startswith(('prompt-', 'result-'))]
        self.assertEqual(detail_reads, [(task_id, name) for task_id in ids[:2]
                                        for name in ('prompt-01.txt', 'result-01.json')])

    def test_detail_cache_is_bounded_and_refreshes_on_file_change_and_new_attempt(self):
        ids = [str(i) for i in range(12)]
        self.manifest(*ids)
        for task_id in ids:
            self.worker(task_id)
        view = dashboard.BatchView(self.root, 'tasks.json')
        _, rows = view.rows()
        with observe_reads() as reads:
            self.assertEqual(view.detail(rows[0])['answer'], 'answer 0/1')
            self.assertEqual(view.detail(rows[0])['prompt'], 'sent 0/1')
        self.assertEqual([p.name for p in reads], ['prompt-01.txt', 'result-01.json'])
        write_json(self.root / 'workers/0/result-01.json', summary='replacement')
        self.assertEqual(view.detail(rows[0])['answer'], 'replacement')
        self.worker('0', attempt=2)
        _, rows = view.rows()
        self.assertEqual(view.detail(rows[0])['answer'], 'answer 0/2')
        self.assertEqual(view.detail(rows[0])['prompt'], 'sent 0/2')
        for row in rows:
            view.detail(row)
        self.assertLess(len(view.details.entries), len(rows))
        with observe_reads() as reads:
            view.detail(rows[0])
        self.assertEqual([p.name for p in reads], ['prompt-02.txt', 'result-02.json'])

    def test_current_queue_and_retry_hide_old_receipts_and_attempt_details(self):
        self.manifest('queued', 'retry', 'finished')
        self.worker('queued')
        self.worker('retry', status='failed')
        self.worker('finished')
        self.status(queued_ids=['queued', 'retry'], retrying_ids=['retry'],
                    selected_ids=['queued', 'retry', 'finished'])
        with patch.object(dashboard, 'live', return_value=True), observe_reads() as reads:
            data = dashboard.snapshot(self.root, 'tasks.json')
        self.assertTrue(data['scheduler_live'])
        q, r, f = data['tasks']
        self.assertEqual([row['status'] for row in (q, r, f)], ['queued', 'retrying', 'complete'])
        for row in (q, r):
            self.assertEqual((row['title'], row['model'], row['effort']), (row['id'], 'worker', 'high'))
            self.assertIn('draft ' + row['id'], row['prompt'])
            self.assertEqual((row['attempt'], row['prompt_sent'], row['answer']), (0, False, None))
        self.assertFalse(any(p.parent.name in ('queued', 'retry') for p in reads))
        with patch.object(dashboard, 'live', return_value=False):
            rows = dashboard.snapshot(self.root, 'tasks.json', include_prompts=False)['tasks']
        self.assertEqual([row['status'] for row in rows], ['complete', 'failed', 'complete'])

    def test_overview_uses_only_registry_and_aggregate_for_modern_batches(self):
        other = self.base / 'other'
        self.register(self.root)
        self.register(other, time=2)
        write_json(self.registry / 'latest.json', run_dir=str(self.root), registered_at=1)
        self.status(counts={'complete': 1, 'retrying': 1}, pending=1, running=1)
        self.status(other, state='complete', total=2, counts={'complete': 1, 'failed': 1})
        overview = dashboard.Overview()
        with (patch.object(dashboard.BatchView, 'rows', side_effect=AssertionError('worker scan')),
              patch.object(dashboard, 'live', return_value=True) as live, observe_reads() as reads):
            running, finished = overview.rows()
            self.assertEqual((running['root'], running['total'], running['active']), (self.root, 3, True))
            self.assertEqual((running['counts']['running'], running['counts']['retrying']), (1, 1))
            self.assertEqual((finished['root'], finished['complete'], finished['active']), (other, 1, False))
            self.assertEqual(live.call_count, 1)
            self.assertCountEqual([p.name for p in reads], ['batch.json', 'other.json', 'latest.json',
                                                           'status-tasks.json', 'status-tasks.json'])
            reads.clear()
            overview.rows()
            self.assertEqual(reads, [])
            self.status(state='interrupted', counts={'complete': 1, 'interrupted': 2})
            self.assertFalse(any(row['active'] for row in overview.rows()))
            self.assertEqual(reads, [self.root / 'status-tasks.json'])

    def test_legacy_overview_caches_stopped_batch_until_new_run(self):
        self.root.mkdir()
        (self.root / 'tasks.json').write_text(json.dumps([{'id': 'one'}]))
        worker = self.worker('one')
        config = self.root / 'runs/1/config.json'
        write_json(config, jobs=['one'])
        self.register(self.root)
        overview = dashboard.Overview()
        with observe_reads() as reads:
            self.assertEqual(overview.rows()[0]['complete'], 1)
            self.assertIn(config, reads)
            self.assertIn(worker / 'receipt.json', reads)
            self.assertNotIn(worker / 'result-01.json', reads)
            reads.clear()
            self.assertEqual(overview.rows()[0]['complete'], 1)
            self.assertEqual(reads, [])
            self.worker('one', attempt=2, status='failed')
            newer = self.root / 'runs/2/config.json'
            write_json(newer, jobs=['one'])
            self.assertEqual(overview.rows()[0]['counts']['failed'], 1)
            self.assertIn(newer, reads)

    def test_launcher_falls_back_on_spawn_error_or_unsuccessful_exit(self):
        command = ['python', 'dashboard.py']
        for first in (OSError('missing executable'), Mock(**{'poll.return_value': 7})):
            with (self.subTest(first=first), patch.object(dashboard.shutil, 'which', side_effect=lambda x: x),
                  patch.object(dashboard.subprocess, 'Popen', side_effect=[first, Mock(**{'poll.return_value': 0})]) as popen):
                dashboard.launch_in_terminal(command)
            self.assertEqual([call.args[0][0] for call in popen.call_args_list],
                             ['xdg-terminal-exec', 'x-terminal-emulator'])
            self.assertEqual(popen.call_args.args[0][1:], ['-e', *command])

    def test_live_launcher_outlasts_startup_and_is_left_running(self):
        proc = Mock(**{'poll.return_value': None})
        now = 0

        def advance(_):
            nonlocal now
            now += 1
            self.assertLess(now, 20)

        with (patch.object(dashboard.shutil, 'which', side_effect=lambda x: x),
              patch.object(dashboard.subprocess, 'Popen', return_value=proc) as popen,
              patch.object(dashboard.time, 'monotonic', side_effect=lambda: now),
              patch.object(dashboard.time, 'sleep', side_effect=advance),
              patch.object(dashboard, 'monitor_is_open', side_effect=lambda _: now >= 12)):
            dashboard.launch_in_terminal(['python', 'dashboard.py'], self.registry / 'monitor.lock')
        self.assertGreater(now, 10)
        self.assertEqual(popen.call_count, 1)
        self.assertTrue(popen.call_args.kwargs['start_new_session'])
        proc.terminate.assert_not_called()
        proc.kill.assert_not_called()
        proc.wait.assert_not_called()

    def test_clean_launcher_exit_waits_for_detached_monitor_to_claim_lock(self):
        proc = Mock(**{'poll.return_value': 0})
        with (patch.object(dashboard.shutil, 'which', side_effect=lambda x: x),
              patch.object(dashboard.subprocess, 'Popen', return_value=proc) as popen,
              patch.object(dashboard.time, 'monotonic', side_effect=[0, 0, 0.1]),
              patch.object(dashboard.time, 'sleep'),
              patch.object(dashboard, 'monitor_is_open', side_effect=[False, False, True]) as ready):
            dashboard.launch_in_terminal(['python', 'dashboard.py'], self.registry / 'monitor.lock')
        self.assertEqual(ready.call_count, 3)
        self.assertEqual(popen.call_count, 1)

    def test_automatic_startup_and_monitor_locks_cover_all_batches(self):
        other = self.base / 'other'
        session = 'a' * 20
        first_session = dashboard.automatic_locks(session)
        second_session = dashboard.automatic_locks('b' * 20)
        calls = []

        def terminal(command, lock=None):
            calls.append((command, lock))
            if lock == first_session[1]:
                dashboard.launch_monitor(other, 'other.json')
                self.assertEqual(len(calls), 1)

        with (patch.object(dashboard, 'graphical_session', return_value=session) as current_session,
              patch.object(dashboard, 'launch_in_terminal', side_effect=terminal)):
            dashboard.launch_monitor(self.root, 'tasks.json')
            self.assertEqual(calls[0][0][2:], ['--session-monitor', session])
            self.assertEqual(calls[0][1], first_session[1])
            with dashboard.try_lock(first_session[1]) as acquired:
                self.assertTrue(acquired)
                dashboard.launch_monitor(other, 'other.json')
                self.assertEqual(len(calls), 1)
                dashboard.launch_monitor(other, 'other.json', allow_multiple=True)
                self.assertEqual(calls[1][0][2:], ['--run-dir', str(other), '--manifest', 'other.json',
                                                   '--allow-multiple'])
                self.assertIsNone(calls[1][1])
                current_session.return_value = 'b' * 20
                dashboard.launch_monitor(other, 'other.json')
                self.assertEqual(calls[2][1], second_session[1])

        def wrapper(_application, root, _manifest):
            self.assertIsNone(root)
            self.assertTrue(dashboard.monitor_is_open(first_session[1]))

        with patch.object(dashboard.curses, 'wrapper', side_effect=wrapper) as mock_wrapper:
            dashboard.main(['--session-monitor', session])
        mock_wrapper.assert_called_once()
        self.assertFalse(dashboard.monitor_is_open(first_session[1]))


if __name__ == '__main__':
    unittest.main()
