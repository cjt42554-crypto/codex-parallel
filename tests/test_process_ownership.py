"""Process ownership regressions using disposable local Python processes."""
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import process_ownership as ownership


SLEEP = 'import time; time.sleep(30)'


@unittest.skipUnless(sys.platform == 'linux' and hasattr(os, 'pidfd_open')
                     and hasattr(signal, 'pidfd_send_signal'), 'requires Linux pidfds')
class OwnershipTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        self.owner = ownership.prepare_owner()
        self.receipt = dict(attempt=1, run_id='123', status='running', owner=self.owner)

    def spawn(self, code=SLEEP, *, marker=None, thread=None):
        env = {key: value for key, value in os.environ.items()
               if key not in (ownership.OWNER_ENV, ownership.THREAD_ENV)}
        if marker is not None:
            env[ownership.OWNER_ENV] = marker
        if thread is not None:
            env[ownership.THREAD_ENV] = thread
        proc = subprocess.Popen([sys.executable, '-c', code], env=env, cwd=self.directory,
                                start_new_session=True)

        def cleanup():
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=2)

        self.addCleanup(cleanup)
        return proc

    def wait_for(self, read):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            try:
                if value := read():
                    return value
            except FileNotFoundError:
                pass
            time.sleep(0.01)
        self.fail('isolated process did not reach its checkpoint')

    def terminate(self, *, wait=2):
        return ownership.terminate_owned(self.receipt, self.directory, time.monotonic() + wait)

    def test_spawn_window_and_detached_child_leave_unrelated_sentinel_alone(self):
        # The receipt never acquired a launcher PID, and the tool is in its
        # own session. The marker alone still binds its host identity.
        tool = self.spawn(marker=self.owner['marker'])
        sentinel = self.spawn(marker=ownership.prepare_owner()['marker'])
        self.assertEqual(os.getsid(tool.pid), tool.pid)
        self.assertNotEqual(os.getsid(tool.pid), os.getsid(sentinel.pid))
        self.assertTrue(ownership.has_owned_work(self.receipt, self.directory))
        view = ownership.scan_owned(self.receipt, self.directory)
        self.assertEqual([row['pid'] for row in view['processes']], [tool.pid])
        self.assertEqual(view['uncertain'], [])
        result = self.terminate()
        self.assertTrue(result['stopped'], result)
        self.assertEqual(tool.wait(timeout=2), -signal.SIGTERM)
        self.assertIsNone(sentinel.poll())
        self.assertEqual([row['pid'] for row in result['signals']], [tool.pid])

    def test_observed_descendant_survives_producer_exit_and_keeps_claim(self):
        leader, child = self.fork_tool()
        self.assertEqual(os.getsid(child), child)
        view = ownership.scan_owned(self.receipt, self.directory)
        self.assertEqual({row['pid'] for row in view['processes']}, {leader.pid, child})
        row = next(row for row in view['processes'] if row['pid'] == child)
        self.assertIn('descendant', row['evidence'])
        ledger = self.directory / 'ownership-01.json'
        first = json.loads(ledger.read_text())
        (self.directory / 'release').touch()
        self.assertEqual(leader.wait(timeout=2), 0)
        self.assertNotEqual(ownership._process(child).parent, leader.pid)
        # An expired cancel still retains all prior evidence for a later cancel.
        result = self.terminate(wait=0)
        self.assertFalse(result['stopped'])
        self.assertEqual([row['pid'] for row in result['remaining']], [child])
        self.assertIn('ledger', result['remaining'][0]['evidence'])
        self.assertEqual(json.loads(ledger.read_text()), first)
        self.assertTrue(self.terminate()['stopped'])
        self.assertEqual(json.loads(ledger.read_text()), first)

    def fork_tool(self, marker=None):
        code = r'''
import os, subprocess, sys, time
from pathlib import Path
env = {k: v for k, v in os.environ.items() if k != 'CODEX_PARALLEL_OWNER'}
if marker is not None:
    env['CODEX_PARALLEL_OWNER'] = marker
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(12)'], env=env,
                         start_new_session=True)
Path('child').write_text(str(child.pid))
while not Path('release').exists():
    time.sleep(.01)
'''
        leader = self.spawn(f'marker={marker!r}\n' + code, marker=self.owner['marker'])
        child = self.wait_for(lambda: (self.directory / 'child').read_text())
        child = int(child)
        child_process = ownership._process(child)

        def clean_child():
            if child_process is not None:
                row = ownership._identity(child_process, ownership._scope(), ['test_fixture'])
                try:
                    ownership._send(row, signal.SIGKILL)
                except ProcessLookupError:
                    pass

        self.addCleanup(clean_child)
        return leader, child

    def test_conflicting_child_stays_unverified_across_reparent_and_repeat_cancel(self):
        leader, child = self.fork_tool(ownership.prepare_owner()['marker'])
        view = ownership.scan_owned(self.receipt, self.directory)
        self.assertEqual([row['pid'] for row in view['processes']], [leader.pid])
        self.assertEqual([(row['reason'], row['pid']) for row in view['uncertain']],
                         [('conflicting_process_ownership', child)])
        for _ in range(2):
            result = self.terminate(wait=.2)
            self.assertFalse(result['stopped'], result)
            self.assertIn(child, [row['pid'] for row in result['uncertain']])
            self.assertNotIn(child, [row['pid'] for row in result['signals']])
            self.assertTrue(ownership._process(child).live)
        self.assertEqual(leader.wait(timeout=2), -signal.SIGTERM)
        self.assertNotEqual(ownership._process(child).parent, leader.pid)

    def test_wrong_ticks_do_not_authorize_signaling_a_live_pid(self):
        sentinel = self.spawn()
        ticks = ownership._process(sentinel.pid).ticks
        self.receipt.update(pid=sentinel.pid, process_start_ticks=str(ticks + 1))
        result = self.terminate()
        self.assertTrue(result['stopped'], result)
        self.assertEqual(result['signals'], [])
        self.assertIsNone(sentinel.poll())

    def test_failed_preparation_without_owner_has_no_process_to_stop(self):
        receipt = dict(execution='failed', status='failed', reason='launch_failed')
        result = ownership.terminate_owned(receipt, self.directory, time.monotonic() + .5)
        self.assertTrue(result['stopped'], result)
        self.assertEqual(result['signals'], [])

    def test_pid_without_current_owner_does_not_authorize_signaling(self):
        sentinel = self.spawn()
        receipt = dict(attempt=1, execution='running', status='running',
                       pid=sentinel.pid, boot_id=self.owner['boot_id'],
                       process_start_ticks=str(ownership._process(sentinel.pid).ticks))
        result = ownership.terminate_owned(receipt, self.directory, time.monotonic() + .5)
        self.assertFalse(result['stopped'], result)
        self.assertEqual(result['uncertain'], [{'reason': 'missing_attempt_owner'}])
        self.assertEqual(result['signals'], [])
        self.assertIsNone(sentinel.poll())

    def test_thread_only_matches_positive_binding_and_cannot_override_owner(self):
        thread = 'worker-test-thread'
        tool = self.spawn(thread=thread)
        sentinel = self.spawn(marker=ownership.prepare_owner()['marker'], thread=thread)
        self.assertFalse(ownership.has_owned_work(self.receipt, self.directory))
        self.receipt['thread_id'] = thread
        view = ownership.scan_owned(self.receipt, self.directory)
        self.assertEqual([row['pid'] for row in view['processes']], [tool.pid])
        self.assertEqual([(row['reason'], row['pid']) for row in view['uncertain']],
                         [('conflicting_process_ownership', sentinel.pid)])
        result = self.terminate(wait=.3)
        self.assertFalse(result['stopped'])
        self.assertEqual(tool.wait(timeout=2), -signal.SIGTERM)
        self.assertIsNone(sentinel.poll())

    def test_associated_identity_read_error_blocks_admission_and_cleanup(self):
        tool = self.spawn(marker=self.owner['marker'])
        self.receipt.update(pid=tool.pid, process_start_ticks=str(ownership._process(tool.pid).ticks))
        real_read = ownership._process

        def deny(pid):
            if pid == tool.pid:
                raise PermissionError('test procfs access denied')
            return real_read(pid)

        with patch.object(ownership, '_process', side_effect=deny):
            self.assertTrue(ownership.has_owned_work(self.receipt, self.directory))
            result = self.terminate(wait=0.1)
        self.assertFalse(result['stopped'])
        self.assertEqual(result['uncertain'][0]['reason'], 'process_identity_unreadable')
        self.assertEqual(result['uncertain'][0]['pid'], tool.pid)
        self.assertEqual(result['signals'], [])
        self.assertIsNone(tool.poll())
        self.assertTrue(self.terminate()['stopped'])

    def test_readonly_scan_does_not_claim_and_lost_ledger_is_not_overwritten(self):
        tool = self.spawn(marker=self.owner['marker'])
        ledger = self.directory / 'ownership-01.json'
        self.assertEqual([row['pid'] for row in ownership.scan_owned(
            self.receipt, self.directory, persist=False)['processes']], [tool.pid])
        self.assertFalse(ledger.exists())
        ledger.write_text('{broken')
        result = self.terminate(wait=0.1)
        self.assertFalse(result['stopped'])
        self.assertEqual(result['uncertain'][0]['reason'], 'ownership_ledger_unavailable')
        self.assertEqual(ledger.read_text(), '{broken')
        self.assertIsNone(tool.poll())

    def test_old_opaque_process_does_not_block_new_attempt(self):
        sentinel = self.spawn()
        time.sleep(.04)
        self.receipt['owner'] = ownership.prepare_owner()
        real_environment = ownership._environment

        def deny(pid):
            if pid == sentinel.pid:
                raise PermissionError('test unreadable environment')
            return real_environment(pid)

        with patch.object(ownership, '_environment', side_effect=deny):
            result = self.terminate()
        self.assertTrue(result['stopped'], result)
        self.assertIsNone(sentinel.poll())

    def test_new_same_uid_opaque_process_blocks_even_without_recorded_pid(self):
        tool = self.spawn(marker=self.owner['marker'])
        real_environment = ownership._environment

        def deny(pid):
            if pid == tool.pid:
                raise PermissionError('test unreadable environment')
            return real_environment(pid)

        with patch.object(ownership, '_environment', side_effect=deny):
            self.assertTrue(ownership.has_owned_work(self.receipt, self.directory))
            result = self.terminate(wait=.15)
            self.assertFalse(result['stopped'], result)
            self.assertIn(('opaque_launch_candidate', tool.pid),
                          [(row['reason'], row['pid']) for row in result['uncertain']])
            self.assertEqual(result['signals'], [])
            self.assertIsNone(tool.poll())

    def test_transient_lock_contention_waits_for_other_scan(self):
        tool = self.spawn(marker=self.owner['marker'])
        with (self.directory / 'ownership-01.lock').open('a') as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            release = threading.Timer(.05, fcntl.flock, args=(handle, fcntl.LOCK_UN))
            release.start()
            try:
                result = ownership.scan_owned(self.receipt, self.directory)
            finally:
                release.join()
        self.assertEqual(result['uncertain'], [])
        self.assertEqual([row['pid'] for row in result['processes']], [tool.pid])
        self.assertTrue((self.directory / 'ownership-01.json').exists())


if __name__ == '__main__':
    unittest.main()
