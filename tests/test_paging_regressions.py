"""Result pages continue the same sequence across batch state changes."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import batch_control as control


def result(task, cursor, status='complete', issues=None):
    return dict(id=task, cursor=cursor, status=status, issues=issues or [])


class ResultPagingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runs = Path(self.temp.name) / 'runs'

    def set_state(self, run, state):
        run.mkdir(parents=True, exist_ok=True)
        (run / 'status.json').write_text(json.dumps(dict(state=state, run_id=run.name)))

    def append(self, path, *rows):
        with path.open('a') as stream:
            for row in rows:
                stream.write(json.dumps(row) + '\n')

    def test_cursor_continues_in_commit_order_after_terminal_status(self):
        for run_id, terminal in enumerate(('complete', 'interrupted'), start=1):
            with self.subTest(terminal=terminal):
                run = self.runs / str(run_id)
                self.set_state(run, 'running')
                a, b, c = result('a', 3), result('b', 1), result('c', 2)
                events = run / 'result-events.jsonl'
                self.append(events, b, c)
                first = control.results_page(run, limit=1)
                self.assertEqual(first['state'], 'running')

                self.append(events, a)
                self.append(run / 'results.jsonl', a, b, c)
                self.set_state(run, terminal)
                second = control.results_page(run, after_cursor=first['cursor'], limit=1)
                self.assertEqual(second['state'], terminal)
                third = control.results_page(run, after_cursor=second['cursor'], limit=1)
                self.assertEqual([page['items'][0]['id'] for page in (first, second, third)],
                                 ['b', 'c', 'a'])
                self.assertEqual([page['cursor'] for page in (first, second, third)], [1, 2, 3])

    def test_attention_pages_skip_successes_without_losing_results(self):
        run = self.runs / '1'
        self.set_state(run, 'running')
        a = result('a', 4, 'not_started')
        b = result('b', 1, 'failed')
        c = result('c', 3, issues=['review'])
        d = result('d', 2)
        events = run / 'result-events.jsonl'
        self.append(events, b, d, c)
        first = control.results_page(run, attention=True, limit=1)
        self.assertEqual((first['items'], first['cursor']), ([b], 1))

        self.append(events, a)
        self.append(run / 'results.jsonl', a, b, c, d)
        self.set_state(run, 'interrupted')
        page = control.results_page(run, attention=True, after_cursor=first['cursor'])
        self.assertEqual((page['items'], page['cursor'], page['high_watermark'], page['total']),
                         ([c, a], 4, 4, 3))

if __name__ == '__main__':
    unittest.main()
