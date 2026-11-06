"""Invocation-local submission, observation and cancellation for dispatch.py."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import NamedTuple

import process_ownership as ownership
import task_control

DISPATCHER = Path(__file__).resolve().with_name('dispatch.py')
STARTUP_WAIT_SECONDS = 5.0
POLL_SECONDS = .2
TERMINAL_STATES = {'complete', 'interrupted', 'startup_failed', 'lost', 'orphaned'}


def _stamp():
    return datetime.now(timezone.utc).isoformat()


def _boot_id():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


class _Process(NamedTuple):
    state: str
    ticks: str

    @property
    def live(self):
        return self.state not in ('Z', 'X', 'x')


def _process(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return _Process(fields[0], fields[19])
    except (FileNotFoundError, ProcessLookupError):
        return None


def _identity(record):
    pid = record.get('pid')
    ticks = record.get('process_start_ticks')
    if isinstance(pid, str) and pid.isascii() and pid.isdecimal():
        pid = int(pid)
    if type(pid) is not int or pid < 2:
        pid = None
    ticks = str(ticks)
    return pid, ticks if ticks.isascii() and ticks.isdecimal() else None


def _different_boot(record, boot_id):
    return bool(record.get('boot_id') and record['boot_id'] != boot_id)


def _same_pid_scope(record):
    namespace = record.get('pid_namespace')
    uid = record.get('uid')
    return (os.readlink('/proc/self') == str(os.getpid()) and
            (namespace is None or namespace == os.readlink('/proc/self/ns/pid')) and
            (uid is None or uid == os.geteuid()))


def _live(record, boot_id):
    """True/False for a known identity; None when evidence is insufficient."""
    if _different_boot(record, boot_id):
        return False
    pid, ticks = _identity(record)
    if pid is None:
        return None
    try:
        if not _same_pid_scope(record):
            return None
        current = _process(pid)
    except OSError:
        return None
    if current is None or not current.live:
        return False
    return current.ticks == ticks if ticks is not None else None


def _pin(record, boot_id):
    """Open before verifying, so PID reuse cannot retarget the eventual signal."""
    pid, ticks = _identity(record)
    if pid is None or ticks is None or _different_boot(record, boot_id):
        return None
    if not _same_pid_scope(record):
        return None
    if not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        raise OSError(errno.ENOSYS, 'safe cancellation requires Linux pidfd support')
    try:
        fd = os.pidfd_open(pid)
    except ProcessLookupError:
        return None
    try:
        current = _process(pid)
        if (current and current.ticks == ticks and current.live and
                Path(f'/proc/{pid}').stat().st_uid == os.geteuid()):
            return fd
    except (FileNotFoundError, ProcessLookupError):
        pass
    except BaseException:
        os.close(fd)
        raise
    os.close(fd)
    return None


def _handle(run_dir):
    path = Path(run_dir).expanduser().resolve()
    if path.parent.name != 'runs' or not path.name.isascii() or not path.name.isdecimal():
        raise ValueError('batch must be an invocation directory: <root>/runs/<numeric id>')
    if not path.is_dir():
        raise FileNotFoundError(f'batch does not exist: {path}')
    return path


def _read_json(path):
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return None
    if not isinstance(data, dict):
        raise ValueError(f'invalid object in {path}')
    return data


def _write_json(path, data):
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                     prefix='.' + path.name + '-', delete=False) as out:
        tmp = Path(out.name)
        try:
            json.dump(data, out, ensure_ascii=False, indent=2)
            out.write('\n')
            out.flush()
            os.fsync(out.fileno())
            tmp.replace(path)
        finally:
            tmp.unlink(missing_ok=True)


def _paths(run_dir):
    return dict(batch_id=str(run_dir), run_id=run_dir.name, root=str(run_dir.parent.parent),
                status_path=str(run_dir / 'status.json'), config_path=str(run_dir / 'config.json'),
                submission_path=str(run_dir / 'submission.json'), log_path=str(run_dir / 'dispatcher.log'))


def _log_error(path, default):
    try:
        with path.open('rb') as stream:
            stream.seek(max(0, os.fstat(stream.fileno()).st_size - 4096))
            lines = stream.read(4096).decode(errors='replace').strip().splitlines()
    except OSError:
        return default
    return lines[-1][:500] if lines else default


def _worker_records(snapshot):
    workers = snapshot.get('running_workers') or {}
    if not isinstance(workers, dict):
        return {}
    return {str(task): {**rec, 'boot_id': rec.get('boot_id') or snapshot.get('boot_id')}
            for task, rec in workers.items() if isinstance(rec, dict)}


def _check_owner(data, run_dir):
    if (str(data.get('run_id', run_dir.name)) != run_dir.name or
            data.get('batch_id', str(run_dir)) != str(run_dir)):
        raise ValueError(f'evidence belongs to a different invocation: {run_dir}')


def _latest_path(root, manifest):
    key = hashlib.sha256(os.fsencode(os.path.abspath(root / manifest))).hexdigest()
    return root / ('.latest-submission-' + key + '.json')


def _read_handle(root, pointer):
    if pointer is None:
        return None
    run = _handle(pointer['batch_id'])
    if run.parent.parent != root:
        raise ValueError(f'invocation pointer belongs to a different workspace: {run}')
    _check_owner(pointer, run)
    return run


def register_submission(run_dir):
    """Publish acceptance once, separately from dispatcher startup or heartbeats."""
    run = _handle(run_dir)
    root = run.parent.parent
    meta = _read_json(run / 'submission.json') or _read_json(run / 'config.json')
    if meta is None:
        raise FileNotFoundError(f'no submission or config for {run}')
    _check_owner(meta, run)
    path = _latest_path(root, meta.get('manifest', 'tasks.json'))
    # An earlier submission may reach this write later. Do not move the index backward.
    with (root / '.submission.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        previous = _read_json(path)
        if previous is None or int(previous['run_id']) < int(run.name):
            _write_json(path, _paths(run))


def latest_submission(root, manifest='tasks.json') -> Path | None:
    root = Path(root).expanduser().resolve()
    index = _read_json(_latest_path(root, manifest))
    if index is not None:
        return _read_handle(root, index)
    # Older invocations predate the index. Submission/config are more authoritative
    # than the manifest heartbeat, which could belong to a different invocation.
    runs = root / 'runs'
    if not runs.is_dir():
        return None
    key = _latest_path(root, manifest)
    for candidate in sorted((p for p in runs.iterdir() if p.is_dir() and
                             p.name.isascii() and p.name.isdecimal()),
                            key=lambda p: int(p.name), reverse=True):
        meta = _read_json(candidate / 'submission.json') or _read_json(candidate / 'config.json')
        if meta and _latest_path(root, meta.get('manifest', 'tasks.json')) == key:
            _check_owner(meta, candidate)
            return candidate
    old = _read_json(root / ('status-' + Path(manifest).stem + '.json'))
    if old and old.get('run_id'):
        return _handle(runs / str(old['run_id']))
    if old and old.get('summary_path'):
        return _read_handle(root, {'batch_id': str(Path(old['summary_path']).parent)})
    return None


def register_active_batch(run_dir):
    """Only the dispatcher holding the workspace execution lock may call this."""
    run = _handle(run_dir)
    _write_json(run.parent.parent / '.active-batch.json', _paths(run))


def active_batch(root) -> Path | None:
    root = Path(root).expanduser().resolve()
    if not _root_lock_held(root):
        return None
    pointer = _read_json(root / '.active-batch.json')
    if pointer is not None:
        run = _read_handle(root, pointer)
        status = read_status(run)
        if status['scheduler_live'] is True or status['state'] == 'orphaned':
            return run
        return None
    # A currently running dispatcher may predate the separate pointer.
    for path in sorted(root.glob('status-*.json')):
        old = _read_json(path)
        if old and old.get('run_id') and _live(old, _boot_id()) is True:
            return _handle(root / 'runs' / str(old['run_id']))
    return None


def _read_status(run_dir, boot_id, *, inspect_workers=True):
    path = run_dir / 'status.json'
    snapshot = _read_json(path)
    if snapshot is None:
        meta = _read_json(run_dir / 'submission.json')
        if meta is None:
            meta = _read_json(run_dir / 'config.json')
        if meta is None:
            raise FileNotFoundError(f'no status or submission evidence for {run_dir}')
        _check_owner(meta, run_dir)
        live = _live(meta, boot_id)
        # A dispatcher can publish status and exit between the first read and the probe.
        snapshot = _read_json(path)
        if snapshot is None:
            failed = meta.get('state') == 'startup_failed' or live is False
            state = 'startup_failed' if failed else 'starting'
            result = {**meta, **_paths(run_dir), 'state': state,
                      'scheduler_live': False if failed else live, 'health': state}
            if failed and not result.get('error'):
                result['error'] = _log_error(run_dir / 'dispatcher.log',
                                             'dispatcher exited before publishing its initial status')
            return result
    _check_owner(snapshot, run_dir)
    live = _live(snapshot, boot_id)
    if snapshot.get('state') == 'running' and live is False:
        # A final snapshot may have been written immediately before process exit.
        snapshot = _read_json(path) or snapshot
        _check_owner(snapshot, run_dir)
        live = _live(snapshot, boot_id)
    state = snapshot.get('state')
    result = {**snapshot, **_paths(run_dir), 'scheduler_live': live,
              'heartbeat_utc': snapshot.get('heartbeat_utc', snapshot.get('utc')),
              'cursor': snapshot.get('cursor', 0)}
    if state == 'running' and live is False and inspect_workers:
        records, uncertain, _ = _attempt_records(run_dir, snapshot)
        workers = {}
        for (task, _), (rec, directory) in records.items():
            observation = ownership.scan_owned(rec, directory, persist=False)
            if observation['processes']:
                workers[task] = {key: rec.get(key) for key in
                                 ('pid', 'process_start_ticks', 'boot_id', 'attempt')}
            uncertain.extend(observation['uncertain'])
        state = 'orphaned' if workers else 'lost'
        result.update(state=state, snapshot_state='running', health=state,
                      running_workers=workers, ownership_unverified=bool(uncertain))
    else:
        result['health'] = ('live' if live else 'unverified') if state == 'running' else state
    return result


def read_status(run_dir) -> dict:
    """Read this invocation's snapshot and check scheduler identity, ignoring heartbeat age."""
    return _read_status(_handle(run_dir), _boot_id())


def submit(root, args) -> dict:
    """Start the same dispatcher in a new session and wait briefly for its own snapshot."""
    root = Path(root).expanduser().resolve()
    runs = root / 'runs'
    runs.mkdir(exist_ok=True)
    run_number = time.time_ns()
    while True:
        run_dir = runs / str(run_number)
        try:
            run_dir.mkdir()
            break
        except FileExistsError:
            run_number += 1
    command = [sys.executable, str(DISPATCHER), '--run-dir', str(root), '--_run-id', run_dir.name]
    for key in ('manifest', 'channel', 'concurrency', 'ramp_seconds', 'timeout_seconds',
                'max_retries', 'retry_backoff_seconds', 'status_interval_seconds',
                'idle_warning_seconds', 'only', 'retry_failed', 'no_dashboard'):
        value = getattr(args, key, None)
        if value is None:
            continue
        flag = '--task-timeout-seconds' if key == 'timeout_seconds' else '--' + key.replace('_', '-')
        if key in ('retry_failed', 'no_dashboard'):
            if value:
                command.append(flag)
        elif key == 'only':
            command.extend([flag, *map(str, value)])
        else:
            command.extend([flag, str(value)])
    boot_id = _boot_id()
    submission = dict(**_paths(run_dir), state='starting', manifest=getattr(args, 'manifest', None) or 'tasks.json',
                      submitted_utc=_stamp(), pid=None, process_start_ticks=None, boot_id=boot_id,
                      spawn_state='prepared', pid_namespace=os.readlink('/proc/self/ns/pid'),
                      uid=os.geteuid())
    path = run_dir / 'submission.json'
    _write_json(path, submission)
    register_submission(run_dir)
    try:
        with (run_dir / 'dispatcher.log').open('ab') as log:
            child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                     close_fds=True, start_new_session=True)
    except OSError as exc:
        submission.update(state='startup_failed', spawn_state='not_started', error=str(exc))
        _write_json(path, submission)
        return _read_status(run_dir, boot_id)

    try:
        current = _process(child.pid)
        submission.update(pid=child.pid, process_start_ticks=current.ticks if current else None,
                          spawn_state='started')
        _write_json(path, submission)
        deadline = time.monotonic() + STARTUP_WAIT_SECONDS
        while True:
            code = child.poll()
            status = _read_status(run_dir, boot_id)
            if code is not None and status['state'] in ('starting', 'startup_failed'):
                submission.update(state='startup_failed', spawn_state='exited', exit_code=code,
                                  error=_log_error(run_dir / 'dispatcher.log',
                                                   f'dispatcher exited before readiness (exit {code})'))
                _write_json(path, submission)
                return _read_status(run_dir, boot_id)
            if status['state'] != 'starting' or not _pause(deadline):
                return status
    finally:
        if child.poll() is None:
            # A long-lived API caller should reap the child too; this thread never signals it.
            threading.Thread(target=child.wait, daemon=True).start()


def _pause(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False
    time.sleep(min(POLL_SECONDS, remaining))
    return True


def _deadline(wait_seconds):
    seconds = float(wait_seconds)
    if not 0 <= seconds <= 60:
        raise ValueError('wait_seconds must be between 0 and 60')
    return time.monotonic() + seconds


def _index(run_dir, status):
    live = run_dir / 'result-events.jsonl'
    if live.is_file() or status['state'] not in ('complete', 'interrupted'):
        return live, False
    return run_dir / 'results.jsonl', True


def _needs_attention(row):
    return row.get('status') != 'complete' or bool(row.get('issues'))


def _page_size(limit):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('limit must be an integer between 1 and 100')


def _nonnegative(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(f'{name} must be a nonnegative integer')


def _rows(stream, legacy, path, previous=0):
    while True:
        position = stream.tell()
        line = stream.readline()
        if not line:
            return
        if not legacy and not line.endswith(b'\n'):
            stream.seek(position)
            return  # Retry from this byte when the producer finishes the row.
        try:
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError('expected a result object')
            cursor = previous + 1 if legacy else row.get('cursor')
            if type(cursor) is not int or cursor <= 0:
                raise ValueError('expected a positive result cursor')
            if cursor <= previous:
                raise ValueError('result cursors must strictly increase')
        except ValueError as exc:
            raise ValueError(f'{path}: {exc}') from exc
        previous = cursor
        yield {**row, 'cursor': cursor}


def _page(path, legacy, after, attention, offset, limit):
    items = []
    cursor, high_watermark, total = after or 0, 0, 0
    try:
        stream = path.open('rb')
    except FileNotFoundError:
        stream = None
    if stream is not None:
        with stream:
            for row in _rows(stream, legacy, path):
                mark = row['cursor']
                high_watermark = max(high_watermark, mark)
                matches = not attention or _needs_attention(row)
                total += matches
                if len(items) >= limit or mark <= (after or 0):
                    continue
                cursor = mark
                if matches and (after is not None or total > offset):
                    items.append(row)
    following = offset + len(items)
    return dict(items=items, cursor=cursor, high_watermark=high_watermark, total=total, limit=limit,
                offset=offset if after is None else None,
                next_offset=following if after is None and following < total else None,
                results_path=str(path))


def results_page(run_dir, after_cursor=None, attention=False, offset=0, limit=20) -> dict:
    """Read final task rows, including those committed by a still-running dispatcher."""
    _page_size(limit)
    _nonnegative(offset, 'offset')
    if after_cursor is not None:
        _nonnegative(after_cursor, 'after_cursor')
    run_dir = _handle(run_dir)
    status = _read_status(run_dir, _boot_id())
    # Keep offsets anchored to the append-only commit order across state changes.
    path, legacy = _index(run_dir, status)
    page = _page(path, legacy, after_cursor, attention, offset, limit)
    return dict(**page, batch_id=status['batch_id'], state=status['state'])


def wait_for_event(run_dir, after_cursor, wait_seconds, limit=20) -> dict:
    """Coalesce ordinary successes until there is attention, a terminal state, or a timeout."""
    _page_size(limit)
    cursor = 0 if after_cursor is None else after_cursor
    _nonnegative(cursor, 'after_cursor')
    deadline = _deadline(wait_seconds)
    run_dir = _handle(run_dir)
    boot_id = _boot_id()
    last_index = identity = None
    position = previous = 0
    items = []
    while True:
        status = _read_status(run_dir, boot_id)
        path, legacy = _index(run_dir, status)
        try:
            st = path.stat()
            version = (path, st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
        except FileNotFoundError:
            version = (path,)
        if version != last_index:
            last_index = version
            try:
                stream = path.open('rb')
            except FileNotFoundError:
                stream = None
            if stream is not None:
                with stream:
                    st = os.fstat(stream.fileno())
                    current = (path, st.st_dev, st.st_ino)
                    if current != identity or st.st_size < position:
                        identity = current
                        position = previous = 0
                    stream.seek(position)
                    for row in _rows(stream, legacy, path, previous):
                        previous = row['cursor']
                        if previous <= cursor:
                            continue
                        cursor = previous
                        if _needs_attention(row):
                            items.append(row)
                            if len(items) == limit:
                                break
                    position = stream.tell()
        terminal = status['state'] in TERMINAL_STATES
        if terminal or items or not _pause(deadline):
            event = 'terminal' if terminal else 'attention' if items else 'timeout'
            return dict(items=items, cursor=cursor, results_path=str(path), limit=limit,
                        batch_id=status['batch_id'], run_id=status['run_id'],
                        state=status['state'], event=event, waiting_timed_out=event == 'timeout',
                        **{key: status[key] for key in ('error', 'log_path', 'report_path', 'summary_path')
                           if key in status})


def _task_name(value):
    return (isinstance(value, str) and value not in ('', '.', '..') and
            Path(value).name == value)


def _attempt_number(value):
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        value = int(value)
    return value if type(value) is int and value > 0 else None


def _startup_never_reached_journal(run_dir, config):
    if config is not None or (run_dir / 'status.json').exists():
        return False
    submission = _read_json(run_dir / 'submission.json')
    return bool(submission and submission.get('state') == 'startup_failed' and
                submission.get('spawn_state') in ('not_started', 'exited'))


def _attempt_records(run_dir, snapshot):
    """Find the attempts launched by this batch, including earlier attempts of a task."""
    records = {}
    uncertain = []
    root = run_dir.parent.parent
    workers = root / 'workers'
    try:
        config = _read_json(run_dir / 'config.json')
        if config is not None:
            _check_owner(config, run_dir)
    except (OSError, ValueError) as exc:
        config = None
        uncertain.append(dict(reason='invocation_config_unverified', error=str(exc)))
    modern = config is not None and config.get('attempt_journal_version') == 1
    try:
        no_launch = _startup_never_reached_journal(run_dir, config)
    except (OSError, ValueError):
        no_launch = False
    if no_launch:
        return records, uncertain, 'startup_failed_before_launch'
    if not modern:
        uncertain.append(dict(reason='legacy_launch_history_incomplete'))
    if workers.is_symlink():
        return records, [*uncertain, dict(reason='worker_directory_is_symlink')], 'unverified'

    def list_dir(path, *, required=False):
        try:
            if path.is_symlink():
                raise ValueError('directory is a symlink')
            return sorted(path.iterdir())
        except FileNotFoundError:
            if not required:
                return []
            uncertain.append(dict(reason='launch_evidence_missing', path=str(path)))
        except (OSError, ValueError) as exc:
            uncertain.append(dict(reason='launch_evidence_unreadable', path=str(path), error=str(exc)))
        return []

    def read(path, *, required=False):
        try:
            if path.is_symlink():
                raise ValueError('evidence is a symlink')
            row = _read_json(path)
        except (OSError, ValueError) as exc:
            uncertain.append(dict(reason='launch_evidence_unreadable', path=str(path), error=str(exc)))
            return None
        if row is None and required:
            uncertain.append(dict(reason='launch_evidence_missing', path=str(path)))
        return row

    def owned_receipt(row, task, attempt):
        return (row and row.get('batch_id') == str(run_dir) and
                str(row.get('run_id', run_dir.name)) == run_dir.name and
                row.get('task_id') == task and _attempt_number(row.get('attempt')) == attempt)

    # Immutable execution bindings are distinct from the result bindings used by the UI.
    attempts = run_dir / 'attempts'
    journal_keys = set()
    for directory in list_dir(attempts, required=modern):
        task = directory.name
        if not _task_name(task):
            uncertain.append(dict(reason='invalid_attempt_task', path=str(directory)))
            continue
        for path in list_dir(directory, required=True):
            if path.name.endswith('.tmp'):
                continue  # Uncommitted temp files never grant launch rights.
            attempt = _attempt_number(path.stem) if path.suffix == '.json' else None
            binding = read(path, required=True) if attempt else None
            worker = workers / task
            receipt_path = worker / f'receipt-{attempt:02d}.json' if attempt else None
            if (not owned_receipt(binding, task, attempt) or binding.get('source') != 'execution' or
                    binding.get('receipt_path') != str(receipt_path) or worker.is_symlink()):
                uncertain.append(dict(reason='invalid_attempt_binding', path=str(path)))
                continue
            key = task, attempt
            journal_keys.add(key)
            receipt = read(receipt_path, required=True)
            if not owned_receipt(receipt, *key) or receipt.get('owner') != binding.get('owner'):
                # The binding was committed by this batch before spawn. Retain its owner
                # even if the corresponding receipt is unavailable or has been replaced.
                uncertain.append(dict(reason='attempt_receipt_unverified', path=str(receipt_path)))
                receipt = binding
            records[key] = (receipt, worker)

    # Check the numbered history, never just receipt.json (another batch can own it).
    # For a current invocation, selected IDs bound this check; legacy configs may not
    # list the selection, so consult the whole historical worker directory.
    selected = config.get('jobs') if config else None
    if isinstance(selected, list) and all(_task_name(task) for task in selected):
        worker_dirs = [workers / task for task in sorted(set(selected))]
    else:
        worker_dirs = list_dir(workers)
        if modern:
            uncertain.append(dict(reason='selection_unverified'))
    for directory in worker_dirs:
        task = directory.name
        for path in list_dir(directory):
            numbered = path.suffix == '.json' and path.stem.startswith('receipt-')
            attempt = _attempt_number(path.stem.removeprefix('receipt-')) if numbered else None
            if attempt is not None and (task, attempt) not in records:
                # Errors for an unbound historical file do not by themselves imply
                # this invocation owns it. The bound receipt was checked above.
                try:
                    receipt = _read_json(path) if not path.is_symlink() else None
                except (OSError, ValueError):
                    continue
                if owned_receipt(receipt, task, attempt):
                    records[task, attempt] = (receipt, directory)
                    if modern and (task, attempt) not in journal_keys:
                        uncertain.append(dict(reason='attempt_missing_from_journal', path=str(path)))
        if not modern:
            receipt = read(directory / 'receipt.json') if directory.is_dir() and not directory.is_symlink() else None
            if receipt and receipt.get('batch_id') == str(run_dir) and receipt.get('task_id') == task:
                attempt = _attempt_number(receipt.get('attempt'))
                records.setdefault((task, attempt or 0), (receipt, directory))

    if not modern:
        # Pre-journal versions wrote start events and snapshots. A later finish event
        # is no proof that this attempt's detached children exited.
        def legacy_record(task, rec):
            if not _task_name(task):
                return
            attempt = _attempt_number(rec.get('attempt')) or 0
            pid = rec.get('pid')
            if any(key[0] == task and val[0].get('pid') == pid for key, val in records.items()):
                return
            directory = workers / task
            key = task, attempt
            if key in records and records[key][0].get('pid') != pid:
                uncertain.append(dict(reason='conflicting_legacy_launch_identity', task_id=task, attempt=attempt))
                return
            records[key] = ({**rec, 'attempt': attempt, 'task_id': task,
                             'boot_id': rec.get('boot_id') or snapshot.get('boot_id')},
                            directory if directory.is_dir() and not directory.is_symlink() else None)

        for task, rec in _worker_records(snapshot).items():
            legacy_record(task, rec)
        try:
            with (run_dir / 'events.jsonl').open('rb') as stream:
                for line in stream:
                    if not line.endswith(b'\n'):
                        break
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(event, dict) and event.get('event') == 'worker_started':
                        legacy_record(event.get('task_id'), event)
        except FileNotFoundError:
            pass
        except OSError as exc:
            uncertain.append(dict(reason='launch_events_unreadable', error=str(exc)))
    return records, uncertain, 'attempt_journal' if modern else 'legacy_evidence'


def _root_lock_held(root):
    """Whether this workspace is locked; this alone does not identify the owner."""
    try:
        with (root / '.dispatch.lock').open('rb') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            return False
    except FileNotFoundError:
        return False


def _trusted_meta(run_dir, name):
    try:
        meta = _read_json(run_dir / name)
        if meta is not None:
            _check_owner(meta, run_dir)
        return meta
    except (OSError, ValueError):
        return None


def _cancel_status(run_dir, boot_id, *, inspect_workers=False):
    try:
        return _read_status(run_dir, boot_id, inspect_workers=inspect_workers)
    except (OSError, ValueError):
        # A broken projection must not block a valid cancellation.
        for name in ('status.json', 'config.json', 'submission.json'):
            meta = _trusted_meta(run_dir, name)
            if meta is not None:
                return {**meta, **_paths(run_dir), 'state': meta.get('state', 'unknown'),
                        'scheduler_live': _live(meta, boot_id), 'snapshot_unverified': True}
        raise


def _dispatcher_identity(run_dir, status, boot_id):
    submission = _trusted_meta(run_dir, 'submission.json')
    config = _trusted_meta(run_dir, 'config.json')
    if submission and submission.get('state') == 'startup_failed' and (
            submission.get('spawn_state') in ('not_started', 'exited') or 'exit_code' in submission):
        return None, False
    for record in (config, submission, status):
        if record and all(_identity(record)):
            # Historical snapshots may omit boot information that submission has.
            if submission and _identity(submission) == _identity(record):
                record = {**submission, **record}
            return record, _live(record, boot_id)
    return None, None


def _stop_dispatcher(run_dir, status, deadline, record, boot_id):
    identity, live = _dispatcher_identity(run_dir, status, boot_id)
    if live is not True:
        return live
    if not identity.get('boot_id'):
        record['dispatcher_signal_error'] = 'dispatcher boot identity unavailable'
        return None
    try:
        fd = _pin(identity, boot_id)
        if fd is None:
            return _live(identity, boot_id)
        try:
            signal.pidfd_send_signal(fd, signal.SIGTERM)
            record['dispatcher_term_sent'] = True
            # Give the normal shutdown path time to write final results and reap
            # its own children; allow time for verification within the caller's wait.
            grace = time.monotonic() + min(4.0, max(0, deadline - time.monotonic()) * .75)
            while _live(identity, boot_id) is True and _pause(grace):
                pass
            if _live(identity, boot_id) is True and time.monotonic() < deadline:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
                record['dispatcher_kill_sent'] = True
                while _live(identity, boot_id) is True and _pause(deadline):
                    pass
        finally:
            os.close(fd)
    except ProcessLookupError:
        pass
    except OSError as exc:
        record['dispatcher_signal_error'] = str(exc)
    return _live(identity, boot_id)


def _cancel(run_dir, deadline, record, previous):
    boot_id = _boot_id()
    status = _cancel_status(run_dir, boot_id)
    before, _, _ = _attempt_records(run_dir, status)
    # Persist known descendants while the dispatcher and CLI can still provide
    # their ancestry. The final scan below includes intents racing with cancel.
    known_live = set()
    for key, (receipt, directory) in before.items():
        try:
            if ownership.scan_owned(receipt, directory)['processes']:
                known_live.add(key)
        except (OSError, ValueError):
            # A later scan reports any persistent observation error; proceed
            # to stop the dispatcher and clean the remaining attempts.
            continue
    live = _stop_dispatcher(run_dir, status, deadline, record, boot_id)
    status = _cancel_status(run_dir, boot_id)
    records, uncertain, evidence = _attempt_records(run_dir, status)
    attempts = dict(previous.get('attempts', {}))
    observed = set()

    def terminate(item):
        (task, attempt), (receipt, directory) = item
        key = f'{task}/{attempt:02d}'
        try:
            cleanup = ownership.terminate_owned(receipt, directory, deadline=deadline)
        except (OSError, ValueError) as exc:
            cleanup = dict(stopped=False, remaining=[], signals=[], scope={},
                           uncertain=[dict(reason='attempt_observation_failed', error=str(exc))])
        return key, {**cleanup, 'task_id': task, 'attempt': attempt,
                     'pid': receipt.get('pid'), 'process_start_ticks': receipt.get('process_start_ticks')}

    # A stubborn earlier attempt should not consume the entire grace period
    # before the other known live attempts get their first signal.
    if records:
        work = sorted(records.items(), key=lambda item: item[0] not in known_live)
        with ThreadPoolExecutor(max_workers=min(8, len(work))) as pool:
            for key, cleanup in pool.map(terminate, work):
                observed.add(key)
                signals = list(attempts.get(key, {}).get('signals', []))
                signals.extend(row for row in cleanup['signals'] if row not in signals)
                attempts[key] = {**cleanup, 'signals': signals}
                uncertain.extend({**item, 'attempt_key': key} for item in cleanup['uncertain'])
    for key in sorted(attempts.keys() - observed):
        uncertain.append(dict(reason='previous_attempt_no_longer_visible', attempt=key))
    if live is not False:
        uncertain.append(dict(reason='dispatcher_still_running' if live else 'dispatcher_unverified'))
    clean = not uncertain and all(item['stopped'] for item in attempts.values())
    # Keep the old task-level display as a projection. It can show only one
    # attempt per task; the cleanup decision above uses the complete history.
    workers = {}
    for item in sorted(attempts.values(), key=lambda item: item['attempt']):
        sent = {row['signal'] for row in item['signals'] if row['outcome'] == 'sent'}
        workers[item['task_id']] = dict(pid=item['pid'], process_start_ticks=item['process_start_ticks'],
                                       term_sent='SIGTERM' in sent, kill_sent='SIGKILL' in sent,
                                       outcome=('stopped' if item['stopped'] else
                                                'still_running' if item['remaining'] else 'unverified'))
    record.update(attempts=attempts, workers=workers, stopped=clean, uncertain=uncertain,
                  scope=dict(launch_evidence=evidence, kind='local_linux_procfs',
                             boot_id=boot_id, pid_namespace=os.readlink('/proc/self/ns/pid'),
                             uid=os.geteuid(),
                             excludes=['processes outside the visible PID namespace',
                                       'untracked descendants that lost all ownership evidence',
                                       'remote work and external side effects']),
                  outcome=('stopped' if record['dispatcher_term_sent'] else
                           'orphan_cleanup_complete' if status['state'] == 'running' else
                           'already_terminal')
                  if clean else ('cleanup_pending' if live is True else 'worker_cleanup_unconfirmed'))
    return _cancel_status(run_dir, boot_id, inspect_workers=True)


def _previous_cleanup(path):
    try:
        return _read_json(path) or {}
    except (OSError, ValueError):
        # A cleanup projection can be rebuilt from the durable request,
        # launch journal and per-attempt ownership ledgers.
        return {}


def cancel(run_dir, wait_seconds=30) -> dict:
    """Persist the request before waiting, then clean only the batch's owned attempts."""
    deadline = _deadline(wait_seconds)
    run_dir = _handle(run_dir)
    path = run_dir / 'cancellation.json'
    # Validate the handle without an ownership scan before accepting cancellation.
    boot_id = _boot_id()
    _cancel_status(run_dir, boot_id)
    prior = _previous_cleanup(path)
    accepted = task_control.request_cancel(
        run_dir, dict(batch_id=str(run_dir), run_id=run_dir.name,
                      requested_utc=prior.get('requested_utc') or _stamp(),
                      reason='user_cancel', cancel_request_path=str(run_dir / 'cancel_request.json')))
    _check_owner(accepted, run_dir)
    base = {**accepted, 'cancel_path': str(path)}
    # This lock serializes projections and signals only. The launch gate is separate
    # and is never held while waiting for a process.
    with (run_dir / '.cancel.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {**_cancel_status(run_dir, boot_id), **prior, **base,
                    'outcome': 'cancel_in_progress', 'stopped': False}
        previous = _previous_cleanup(path)
        record = {**base, 'stopped': False,
                  'dispatcher_term_sent': previous.get('dispatcher_term_sent', False),
                  'dispatcher_kill_sent': previous.get('dispatcher_kill_sent', False)}
        try:
            status = _cancel(run_dir, deadline, record, previous)
        except (OSError, ValueError) as exc:
            status = _cancel_status(run_dir, boot_id)
            record.update(attempts=previous.get('attempts', {}), outcome='cancel_error', error=str(exc))
        record.update(state=status['state'], finished_utc=_stamp())
        _write_json(path, record)
    return {**status, **record}
