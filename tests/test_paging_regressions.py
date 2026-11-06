"""Result pages continue the same sequence across batch state changes."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import batch_control as control


def result(task, cursor=None, status='complete', issues=None):
    row = dict(id=task, status=status, issues=issues or [])
    if cursor is not None:
        row['cursor'] = cursor
    return row


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

    def test_offsets_continue_in_commit_order_after_terminal_status(self):
        for run_id, terminal in enumerate(('complete', 'interrupted'), start=1):
            with self.subTest(terminal=terminal):
                run = self.runs / str(run_id)
                self.set_state(run, 'running')
                a, b, c = result('a', 3), result('b', 1), result('c', 2)
                events = run / 'result-events.jsonl'
                self.append(events, b, c)
                first = control.results_page(run, limit=1)
                self.assertEqual(first['state'], 'running')
                self.assertEqual(first['next_offset'], 1)

                self.append(events, a)
                self.append(run / 'results.jsonl', a, b, c)
                self.set_state(run, terminal)
                second = control.results_page(run, offset=first['next_offset'], limit=1)
                self.assertEqual(second['state'], terminal)
                self.assertEqual(second['next_offset'], 2)
                third = control.results_page(run, offset=second['next_offset'], limit=1)
                self.assertIsNone(third['next_offset'])
                self.assertEqual([page['items'][0]['id'] for page in (first, second, third)],
                                 ['b', 'c', 'a'])
                self.assertEqual([page['cursor'] for page in (first, second, third)], [1, 2, 3])

    def test_attention_offsets_and_cursors_follow_the_same_sequence(self):
        run = self.runs / '1'
        self.set_state(run, 'running')
        a = result('a', 4, 'not_started')
        b = result('b', 1, 'failed')
        c = result('c', 3, issues=['review'])
        d = result('d', 2)
        events = run / 'result-events.jsonl'
        self.append(events, b, d, c)
        first = control.results_page(run, attention=True, limit=1)
        self.assertEqual((first['items'], first['cursor'], first['next_offset']), ([b], 1, 1))

        self.append(events, a)
        self.append(run / 'results.jsonl', a, b, c, d)
        self.set_state(run, 'interrupted')
        offset_page = control.results_page(run, attention=True, offset=first['next_offset'])
        cursor_page = control.results_page(run, attention=True, after_cursor=first['cursor'])
        for page in (offset_page, cursor_page):
            self.assertEqual((page['items'], page['cursor'], page['high_watermark'], page['total']),
                             ([c, a], 4, 4, 3))
            self.assertIsNone(page['next_offset'])

    def test_legacy_archive_keeps_saved_order_and_synthetic_cursors(self):
        run = self.runs / '1'
        self.set_state(run, 'complete')
        path = run / 'results.jsonl'
        self.append(path, result('c'), result('a', status='partial'), result('b'))

        first = control.results_page(run, limit=1)
        self.assertEqual((first['items'], first['results_path']), ([result('c', 1)], str(path)))
        rest = control.results_page(run, offset=first['next_offset'])
        self.assertEqual(rest['items'], [result('a', 2, 'partial'), result('b', 3)])
        selected = control.results_page(run, after_cursor=first['cursor'], attention=True)
        self.assertEqual((selected['items'], selected['cursor']), ([result('a', 2, 'partial')], 3))


if __name__ == '__main__':
    unittest.main()
