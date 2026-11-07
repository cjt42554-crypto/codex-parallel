"""Exercise the dashboard with dispatcher evidence and no real EXEC workers."""

import json
from pathlib import Path
import time
from unittest.mock import Mock, patch

from test_dashboard import dashboard, observe_reads, write_json
from test_dispatch import DispatchFixture


class DashboardInvocationTests(DispatchFixture):
    def run_batch(self, *args):
        result = self.run_cli(*args)
        self.assertEqual(result.returncode, 0, result.stderr)

    def saved_run(self, ids, state='complete', **status):
        run = self.root / 'runs' / '1'
        run.mkdir(parents=True)
        write_json(self.root / 'status-tasks.json', manifest='tasks.json', run_id='1',
                   batch_id=str(run), selected_ids=ids, state=state, **status)
        return run

    def saved_rows(self, run, *rows):
        (run / 'result-events.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))

    def test_only_uses_the_invocations_selection_even_if_the_manifest_changes(self):
        self.jobs(self.root, ['a', 'b'])
        self.behavior(self.root, {'b': 'blocked'})
        self.run_batch()
        self.run_batch('--only', 'a')

        # The original manifest may be edited before the batch is viewed again.
        for ids in (['a', 'b'], ['b', 'c']):
            with self.subTest(ids=ids):
                self.jobs(self.root, ids)
                rows = dashboard.snapshot(self.root, 'tasks.json')['tasks']
                self.assertEqual([(r['id'], r['status'], r['attempt']) for r in rows],
                                 [('a', 'complete', 1)])
                self.assertEqual((rows[0]['prompt_sent'], rows[0]['answer']), (True, 'body'))

    def test_each_manifest_retains_its_own_receipt_prompt_and_answer(self):
        # Make the fake worker outputs distinguishable across attempts.
        worker = self.scripts / 'codexexec'
        worker.write_text(worker.read_text().replace("'answer':'body'", "'answer':f'{mode} {attempt}'"))
        (self.root / 'other.json').write_bytes((self.root / 'tasks.json').read_bytes())
        self.behavior(self.root, {'a': 'blocked'})
        self.run_batch()
        self.behavior(self.root, {})
        self.run_batch('--manifest', 'other.json', '--retry-failed')

        with observe_reads() as reads:
            first, = dashboard.snapshot(self.root, 'tasks.json')['tasks']
            second, = dashboard.snapshot(self.root, 'other.json')['tasks']
        self.assertEqual((first['status'], first['attempt'], first['answer']), ('blocked', 1, 'blocked 1'))
        self.assertEqual((second['status'], second['attempt'], second['answer']), ('complete', 2, 'complete 2'))
        for kind, suffix in (('receipt', 'json'), ('prompt', 'txt'), ('result', 'json')):
            self.assertEqual([p.name for p in reads if p.name.startswith(f'{kind}-')],
                             [f'{kind}-01.{suffix}', f'{kind}-02.{suffix}'])

    def test_cancelled_retry_does_not_display_an_old_attempt_for_unstarted_work(self):
        self.jobs(self.root, ['c', 'b'])
        self.behavior(self.root, {'b': 'blocked'})
        self.run_batch('--only', 'b')
        self.behavior(self.root, {'c': 'hang'})
        proc = self.start(None, '--only', 'c', 'b', '--retry-failed', '--concurrency', '1',
                          '--status-interval-seconds', '30')
        started = self.root / 'workers/c/started'
        until = time.monotonic() + 5
        while not started.exists() and time.monotonic() < until:
            time.sleep(.02)
        self.assertTrue(started.exists())
        # A real fake worker has started, while the initial snapshot still has no worker receipt.
        status = json.loads((self.root / 'status-tasks.json').read_text())
        self.assertEqual(status['running_workers'], {})
        with patch.object(dashboard, 'live', return_value=False):
            a, b = dashboard.snapshot(self.root, 'tasks.json', include_prompts=False)['tasks']
        self.assertEqual((a['id'], a['status'], a['attempt']), ('c', 'unknown', 1))
        self.assertEqual((b['id'], b['status'], b['attempt']), ('b', 'not_started', 0))
        proc.terminate()
        proc.wait(timeout=12)

        # The dispatcher must preserve the original input even for a task that never launched.
        write_json(self.root / 'tasks.json', tasks=[dict(id='b', model='new-model', effort='high',
                                                       prompt='modified task')])
        (self.root / 'COMMON.md').write_text('modified common')
        rows = dashboard.snapshot(self.root, 'tasks.json')['tasks']
        self.assertEqual([(r['id'], r['status'], r['attempt']) for r in rows],
                         [('c', 'interrupted', 1), ('b', 'not_started', 0)])
        self.assertEqual((rows[1]['prompt_sent'], rows[1]['answer']), (False, None))
        self.assertEqual((rows[1]['model'], rows[1]['effort']), ('fake', 'low'))
        self.assertTrue(rows[1]['prompt'].startswith('shared\n\ntest\n'))
        self.assertEqual(self.receipt('b')['status'], 'blocked')

    def test_finished_rows_use_the_index_and_load_only_the_selected_attempt(self):
        ids = [f'task-{i:04d}' for i in range(1000)]
        self.jobs(self.root, ids)
        run = self.saved_run(ids)
        self.saved_rows(run, *(dict(id=i, status='complete', attempt=1) for i in ids))
        directory = self.root / 'workers' / ids[0]
        write_json(directory / 'receipt-01.json', status='complete', attempt=1, result_file='result-01.json')
        (directory / 'prompt-01.txt').write_text('sent')
        write_json(directory / 'result-01.json', status='complete', answer='answer')
        view = dashboard.BatchView(self.root, 'tasks.json')

        with (patch.object(Path, 'is_symlink', autospec=True, side_effect=Path.is_symlink) as symlink,
              observe_reads() as reads):
            _, rows = view.rows()
            self.assertEqual(len(rows), len(ids))
            self.assertFalse(any('workers' in p.parts for p in reads))
            self.assertFalse(any('workers' in call.args[0].parts[:-1] for call in symlink.call_args_list))
            reads.clear()
            self.assertEqual(view.detail(rows[0])['answer'], 'answer')
            self.assertEqual([p.name for p in reads], ['receipt-01.json', 'prompt-01.txt', 'result-01.json'])

        opened = []
        path_open = Path.open

        def observe_open(path, *args, **kwargs):
            opened.append(path)
            return path_open(path, *args, **kwargs)

        with patch.object(Path, 'open', observe_open):
            view.rows()
            view.detail(rows[0])
        self.assertEqual(opened, [])

    def test_unstarted_history_uses_its_frozen_config_and_common(self):
        run = self.saved_run(['a'], 'interrupted')
        common = run / 'common.md'
        common.write_text('frozen common')
        write_json(run / 'config.json', jobs=['a'], common_prompt_file=str(common),
                   job_metadata={'a': dict(title='frozen title', model='old-model', effort='high',
                                           prompt='frozen task')})
        self.saved_rows(run, dict(id='a', status='not_started'))
        write_json(self.root / 'tasks.json', tasks=[dict(id='a', model='new-model', effort='low',
                                                       prompt='modified task')])
        (self.root / 'COMMON.md').write_text('modified common')

        with observe_reads() as reads:
            row, = dashboard.snapshot(self.root, 'tasks.json')['tasks']
        self.assertEqual((row['title'], row['model'], row['effort']), ('frozen title', 'old-model', 'high'))
        self.assertTrue(row['prompt'].startswith('frozen common\n\nfrozen task\n'))
        self.assertFalse(row['prompt_sent'])
        self.assertFalse(any(path.name in ('tasks.json', 'COMMON.md') for path in reads))

    def test_binding_precedes_stale_snapshot_and_preserves_an_unindexed_success(self):
        run = self.saved_run(['a', 'b'], 'running', queued_ids=['a', 'b'], running_workers={})
        write_json(run / 'config.json', jobs=['a', 'b'], job_metadata={'a': {}, 'b': {}}, attempt_journal_version=1)
        directory = self.root / 'workers/a'
        path = run / 'bindings/a.json'
        write_json(path, task_id='a', attempt=1, run_id=run.name, batch_id=str(run),
                   source='execution', execution='prepared',
                   receipt_path=str(directory / 'receipt-01.json'))
        directory.mkdir(parents=True)
        (directory / 'prompt-01.txt').write_text('prepared input')
        view = dashboard.BatchView(self.root, 'tasks.json')

        with patch.object(dashboard, 'live', return_value=True):
            _, rows = view.rows()
        self.assertEqual([(r['status'], r['attempt']) for r in rows], [('starting', 1), ('queued', 0)])
        with patch.object(dashboard, 'live', return_value=False):
            _, rows = view.rows()
        self.assertEqual([(r['status'], r['attempt']) for r in rows], [('unknown', 1), ('not_started', 0)])
        self.assertTrue(rows[0]['needs_attention'])
        detail = view.detail(rows[0])
        self.assertEqual((detail['prompt'], detail['prompt_sent']), ('prepared input', False))

        # A newer binding plus numbered receipt can outlive the scheduler and its result index.
        write_json(path, task_id='a', attempt=2, run_id=run.name, batch_id=str(run),
                   source='execution', execution='prepared',
                   receipt_path=str(directory / 'receipt-02.json'))
        write_json(directory / 'receipt-02.json', attempt=2, run_id=run.name, batch_id=str(run),
                   status='complete', execution='succeeded',
                   reported_status='complete', result_observation='valid', result_file='result-02.json')
        write_json(directory / 'result-02.json', status='complete', answer='already completed')
        with patch.object(dashboard, 'live', return_value=False):
            _, rows = view.rows()
        row = rows[0]
        self.assertEqual((row['status'], row['attempt'], row['execution']), ('complete', 2, 'succeeded'))
        self.assertTrue(row['needs_attention'])
        self.assertEqual(view.detail(row)['answer'], 'already completed')

    def test_completed_result_with_missing_evidence_displays_attention(self):
        run = self.saved_run(['a'])
        receipt = self.root / 'workers/a/receipt-01.json'
        write_json(receipt, status='complete', attempt=1, result_file='result-01.json')
        self.saved_rows(run, dict(id='a', title='task a', status='complete', attempt=1,
                                  receipt_path=str(receipt), execution='succeeded', reported_status='complete',
                                  result_observation='unavailable', recovery_state='recovered',
                                  issues=['result unavailable: numbered result was deleted']))
        view = dashboard.BatchView(self.root, 'tasks.json')
        _, (row,) = view.rows()
        self.assertTrue(row['needs_attention'])
        self.assertEqual((row['status'], row['execution'], row['result_observation'], row['recovery_state']),
                         ('complete', 'succeeded', 'unavailable', 'recovered'))
        self.assertIn('result unavailable: numbered result was deleted', row['issues'])
        self.assertIsNone(view.detail(row)['answer'])

        screen = Mock()
        screen.getmaxyx.return_value = (28, 120)
        screen.getch.return_value = ord('q')
        with (patch.object(dashboard.curses, 'curs_set'),
              patch.object(dashboard.curses, 'has_colors', return_value=False)):
            dashboard.ui(screen, self.root, 'tasks.json')
        shown = '\n'.join(call.args[2] for call in screen.addstr.call_args_list)
        self.assertIn('! 已完成 · 告警', shown)
        self.assertIn('告警：执行已成功。', shown)
        self.assertIn('result unavailable: numbered result was deleted', shown)
