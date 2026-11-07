"""Display declared follow-up without interpreting research answers."""
import copy
import unittest

from test_dashboard import dashboard


class WarningPolicyTests(unittest.TestCase):
    def completed(self, issues):
        return dict(status='complete', execution='succeeded', reported_status='complete',
                    result_observation='valid', issues=issues)

    def test_completed_research_findings_are_notes_not_warnings(self):
        task = dict(status='complete',
                    answer='仅两场历史；未完成真实成交验证，净收益未知。报告在 /missing/report.txt。')
        original = copy.deepcopy(task)
        self.assertEqual(dashboard.display_warnings(task), [])
        self.assertEqual(task, original)

    def test_scheduler_errors_are_visible_for_completed_exec(self):
        issues = ['无法保存本次尝试的回执。']
        self.assertEqual(dashboard.display_warnings(self.completed(issues)), issues)

    def test_abnormal_completion_and_unreadable_result_are_warned(self):
        for status in ('partial', 'blocked', 'failed', 'interrupted', 'unknown'):
            with self.subTest(status=status):
                self.assertTrue(dashboard.display_warnings(dict(status=status, issues=[])))
        task = self.completed([])
        task['result_observation'] = 'unavailable'
        self.assertTrue(dashboard.display_warnings(task))
        for status in ('running', 'queued', 'starting'):
            self.assertFalse(dashboard.display_warnings(dict(status=status, issues=[])))

    def test_dependency_failures_in_english(self):
        for issue in ('Missing dependency: pyarrow', 'Rust compiler not found',
                      'Could not complete the replay', 'ModuleNotFoundError: numpy'):
            with self.subTest(issue=issue):
                self.assertEqual(dashboard.display_warnings(self.completed([issue])), [issue])


if __name__ == '__main__':
    unittest.main()
