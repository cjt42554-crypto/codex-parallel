"""Every two-field EXEC answer is delivered; no research-content verification."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from test_dispatch import DispatchFixture
from test_dashboard import dashboard
import batch_control
from result_contract import needs_attention, valid_result


class ResultContractTests(unittest.TestCase):
    def test_only_current_two_field_result_is_accepted(self):
        value = dict(status='complete', answer='资料有限，成果在 /missing/report.txt')
        original = copy.deepcopy(value)
        self.assertTrue(valid_result(value))
        self.assertFalse(needs_attention(value))
        self.assertEqual(value, original)
        for invalid in (dict(value, needs_attention=False),
                        dict(status='complete', summary='old', artifacts=[], issues=[])):
            self.assertFalse(valid_result(invalid))

    def test_status_not_answer_wording_controls_abnormal_result_filter(self):
        for status, expected in [('complete', False), ('partial', True), ('blocked', True)]:
            with self.subTest(status=status):
                value = dict(status=status, answer='Missing dependency; 未完成更多验证；报告未知。')
                self.assertEqual(needs_attention(value), expected)
        self.assertTrue(needs_attention(dict(status='complete', issues=['result unavailable'])))

    def test_running_wait_delivers_every_answer_once_then_times_out(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / 'runs/123'
            run.mkdir(parents=True)
            status = dict(state='running', run_id='123', batch_id=str(run))
            event = run / 'result-events.jsonl'
            first = dict(id='a', cursor=1, status='complete', answer='尚未验证当前盈利。')
            event.write_text(json.dumps(first) + '\n')
            with patch.object(batch_control, '_read_status', return_value=status):
                completed = batch_control.wait_for_event(run, 0, 0)
                self.assertEqual((completed['event'], completed['cursor'], completed['items']), ('result', 1, [first]))
                quiet = batch_control.wait_for_event(run, completed['cursor'], 0)
                self.assertEqual((quiet['event'], quiet['cursor'], quiet['items']), ('timeout', 1, []))
                second = dict(id='b', cursor=2, status='partial', answer='还有一个交付未完成。')
                with event.open('a') as out:
                    out.write(json.dumps(second) + '\n')
                action = batch_control.wait_for_event(run, 1, 0)
                self.assertEqual((action['event'], action['cursor'], action['items']), ('result', 2, [second]))
                self.assertEqual(batch_control.results_page(run, after_cursor=0)['items'], [first, second])
                self.assertEqual(batch_control.results_page(run, after_cursor=0, attention=True)['items'], [second])


class ResultContractIntegrationTests(DispatchFixture):
    def report(self):
        return json.loads(self.run_cli('--report').stdout)

    def test_completed_research_is_delivered_without_warnings_or_content_checks(self):
        self.jobs(self.root, ['a', 'b'])
        self.behavior(self.root, {'a': 'research', 'b': 'research'})
        run = self.run_cli('--ramp-seconds', '0')
        self.assertEqual(run.returncode, 0, run.stderr)
        report = self.report()
        self.assertTrue(report['ok'])
        self.assertEqual(report['attention_count'], 0)
        self.assertEqual(Path(report['attention_path']).read_text(), '')
        page = json.loads(self.run_cli('--results', '--after', '0').stdout)
        self.assertEqual(len(page['items']), 2)
        for row in page['items']:
            self.assertFalse(row['needs_attention'])
            self.assertIn('/not/a/real/artifact.txt', row['answer'])
            self.assertNotIn('summary', row)
            self.assertNotIn('artifacts', row)
        wait = json.loads(self.run_cli('--wait', '--after', '0', '--wait-seconds', '0').stdout)
        self.assertEqual((wait['event'], wait['cursor']), ('terminal', 2))
        self.assertEqual(wait['items'], page['items'])
        self.assertEqual(wait['summary']['reused_count'], 0)
        first = json.loads(self.run_cli('--wait', '--after', '0', '--wait-seconds', '0', '--limit', '1').stdout)
        second = json.loads(self.run_cli('--wait', '--after', str(first['cursor']), '--wait-seconds', '0', '--limit', '1').stdout)
        self.assertEqual(first['event'], 'result')
        self.assertEqual(second['event'], 'terminal')
        self.assertEqual(first['items'] + second['items'], page['items'])
        self.assertEqual(second['summary']['counts']['complete'], 2)
        self.assertTrue(second['summary']['ok'])
        empty = json.loads(self.run_cli('--wait', '--after', '2', '--wait-seconds', '0').stdout)
        self.assertEqual(empty['items'], [])
        for task_id in ('a', 'b'):
            raw = json.loads((self.root / 'workers' / task_id / 'result-01.json').read_text())
            self.assertEqual(set(raw), {'status', 'answer'})
        # Separate CLI processes remember delivered task IDs without --after.
        auto_first = json.loads(self.run_cli('--wait', '--wait-seconds', '0', '--limit', '1').stdout)
        auto_second = json.loads(self.run_cli('--wait', '--wait-seconds', '0', '--limit', '1').stdout)
        self.assertEqual(auto_first['items'] + auto_second['items'], page['items'])
        self.assertEqual(json.loads(self.run_cli('--wait', '--wait-seconds', '0').stdout)['items'], [])
        self.assertEqual(json.loads(self.run_cli('--results', '--attention', '--after', '0').stdout)['items'], [])
        snapshot = dashboard.snapshot(self.root, 'tasks.json')
        for task in snapshot['tasks']:
            self.assertFalse(task['needs_attention'])
            self.assertFalse(dashboard.display_warnings(task))
            self.assertIn('/not/a/real/artifact.txt', task['answer'])
        # Reuse the completed EXEC without changing its prompt fingerprint or rerunning it.
        again = self.run_cli('--ramp-seconds', '0')
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(self.report()['reused_count'], 2)
        self.assertTrue(self.report()['ok'])
        reused = json.loads(self.run_cli('--wait', '--wait-seconds', '0').stdout)
        self.assertEqual(reused['summary']['reused_count'], 2)

    def test_mixed_batch_only_reports_followup_and_mechanical_failures(self):
        modes = {'a': 'research', 'b': 'action', 'c': 'simple_partial', 'd': 'simple_blocked',
                 'e': 'missing', 'f': 'failed_turn', 'g': 'bad_schema'}
        self.jobs(self.root, list(modes))
        self.behavior(self.root, modes)
        run = self.run_cli('--ramp-seconds', '0')
        self.assertEqual(run.returncode, 0, run.stderr)
        report = self.report()
        self.assertFalse(report['ok'])
        self.assertEqual(report['attention_count'], 6)
        rows = json.loads(self.run_cli('--results', '--attention', '--after', '0').stdout)['items']
        self.assertEqual({r['id'] for r in rows}, set(modes) - {'a'})
        action = next(r for r in rows if r['id'] == 'b')
        self.assertEqual(action['issues'], [])
        self.assertIn('请主会话', action['answer'])
        self.assertIn('请主会话', run.stderr)
        snapshot = dashboard.snapshot(self.root, 'tasks.json')
        self.assertEqual({r['id'] for r in snapshot['tasks'] if dashboard.display_warnings(r)}, set(modes) - {'a'})


if __name__ == '__main__':
    unittest.main()
