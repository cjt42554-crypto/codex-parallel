"""Local Linux process evidence shared by worker admission and cleanup.

The caller must close its launch gate before interpreting a clean scan as a
stopped attempt. This is observational process tracking, not containment.
"""
from __future__ import annotations

from collections import defaultdict, deque
from contextlib import nullcontext
from dataclasses import dataclass
import errno
import fcntl
import json
import os
from pathlib import Path
import secrets
import signal
import tempfile
import time


OWNER_ENV = 'CODEX_PARALLEL_OWNER'
THREAD_ENV = 'CODEX_THREAD_ID'
PROC = Path('/proc')
_GONE = (FileNotFoundError, ProcessLookupError)
_POLL = 0.05


@dataclass(frozen=True)
class _Process:
    pid: int
    ticks: int
    parent: int
    state: str

    @property
    def live(self):
        return self.state not in ('Z', 'X', 'x')


def _boot_id():
    value = (PROC / 'sys/kernel/random/boot_id').read_text().strip()
    if not value:
        raise ValueError('missing Linux boot ID')
    return value


def _scope():
    # pidfd_open interprets its number in our own PID namespace. Verify that
    # procfs resolves self to that same numeric namespace before using its PIDs.
    if os.readlink(PROC / 'self') != str(os.getpid()):
        raise ValueError('procfs and pidfd PID namespaces differ')
    return dict(kind='local_linux_procfs', boot_id=_boot_id(),
                pid_namespace=os.readlink(PROC / 'self/ns/pid'), uid=os.geteuid(),
                excludes=['remote jobs and external effects', 'other UIDs and invisible PID namespaces',
                          'detached processes that dropped ownership evidence before observation'])


def prepare_owner():
    """Create launch metadata. Persist it in the numbered receipt before spawn."""
    scope = _scope()
    # /proc stat start times are in boot-clock ticks. Round down and allow one
    # tick of granularity at the boundary; the marker itself is the proof.
    now_ticks = int(time.clock_gettime(time.CLOCK_BOOTTIME) * os.sysconf('SC_CLK_TCK'))
    return dict(marker=secrets.token_hex(24), uid=scope['uid'], boot_id=scope['boot_id'],
                pid_namespace=scope['pid_namespace'], not_before_ticks=max(0, now_ticks - 1))


def _number(value, minimum=0):
    if isinstance(value, str) and value.isascii() and value.isdecimal() and len(value) <= 32:
        value = int(value)
    return value if type(value) is int and value >= minimum else None


def _process(pid):
    try:
        fields = (PROC / str(pid) / 'stat').read_bytes().rsplit(b')', 1)[1].split()
        return _Process(pid, int(fields[19]), int(fields[1]), fields[0].decode('ascii'))
    except _GONE:
        return None


def _uid(pid):
    # /proc/PID directory ownership changes for nondumpable processes. Read
    # the kernel's effective UID rather than relying on the directory owner.
    for line in (PROC / str(pid) / 'status').read_bytes().splitlines():
        if line.startswith(b'Uid:'):
            return int(line.split()[2])
    raise ValueError('missing process UID')


def _environment(pid):
    data = (PROC / str(pid) / 'environ').read_bytes().split(b'\0')
    keys = (os.fsencode(OWNER_ENV), os.fsencode(THREAD_ENV))
    values = {key: set() for key in keys}
    for item in data:
        key, separator, value = item.partition(b'=')
        if separator and key in values and value:
            values[key].add(value)
    return tuple(values[key] for key in keys)


def bind_worker_identity(pid, receipt):
    """Return start ticks only if the freshly launched PID still proves its owner."""
    owner = receipt.get('owner') or {}
    try:
        scope = _scope()
        if any(owner.get(key) != scope[key] for key in ('uid', 'boot_id', 'pid_namespace')):
            return None
        if not owner.get('marker') or _number(pid, 2) != pid:
            return None
        before = _process(pid)
        if before is None or not before.live or before.ticks < owner['not_before_ticks']:
            return None
        if _uid(pid) != owner['uid'] or _environment(pid)[0] != {os.fsencode(owner['marker'])}:
            return None
        after = _process(pid)
        if after is not None and after.live and before.ticks == after.ticks:
            return str(after.ticks)
    except (OSError, ValueError, IndexError, KeyError, TypeError):
        pass
    return None


def _identity(process, scope, evidence):
    return dict(pid=process.pid, process_start_ticks=str(process.ticks), boot_id=scope['boot_id'],
                pid_namespace=scope['pid_namespace'], uid=scope['uid'], evidence=evidence)


def _key(row):
    return row['pid'], int(row['process_start_ticks'])


def _ledger_key(receipt, owner):
    return dict(attempt=receipt.get('attempt'), run_id=receipt.get('run_id'),
                batch_id=receipt.get('batch_id'), marker=owner['marker'])


def _read_ledger(path, key, scope):
    try:
        document = json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return {}, {}
    if (not isinstance(document, dict) or document.get('key') != key
            or not isinstance(document.get('processes'), list)
            or not isinstance(document.get('concerns'), list)):
        raise ValueError('ownership ledger does not match attempt')
    def identities(rows):
        result = {}
        for row in rows:
            if (not isinstance(row, dict) or _number(row.get('pid'), 2) is None
                    or type(row.get('pid')) is not int or _number(row.get('process_start_ticks')) is None
                    or row.get('uid') != scope['uid'] or row.get('boot_id') != scope['boot_id']
                    or row.get('pid_namespace') != scope['pid_namespace']):
                raise ValueError('invalid process identity in ownership ledger')
            result[_key(row)] = row
        return result
    return identities(document['processes']), identities(document['concerns'])


def _write_ledger(path, key, claims, concerns):
    with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=path.parent,
                                     prefix=f'.{path.name}-', delete=False) as stream:
        tmp = Path(stream.name)
        try:
            json.dump(dict(key=key, processes=list(claims.values()), concerns=list(concerns.values())),
                      stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
            tmp.replace(path)
        finally:
            tmp.unlink(missing_ok=True)


def _ledger_lock(path, deadline):
    lock = path.with_suffix('.lock').open('a')
    try:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return lock
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(.01, remaining))
    except BaseException:
        lock.close()
        raise


def _missing_known_pid(pid, uncertain):
    """A missing procfs entry is not necessarily a missing local PID."""
    if not hasattr(os, 'pidfd_open'):
        uncertain.append(dict(reason='process_visibility_unverified', pid=pid))
        return
    try:
        fd = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    except OSError as exc:
        uncertain.append(dict(reason='process_visibility_unverified', pid=pid, error=str(exc)))
    else:
        os.close(fd)
        uncertain.append(dict(reason='process_visibility_unverified', pid=pid))


def _collect(receipt, owner, scope, retained, prior_concerns, uncertain):
    """Find exact markers, explicitly bound thread IDs, and observed ancestry."""
    root_pid = _number(receipt.get('pid'), 2)
    root_ticks = _number(receipt.get('process_start_ticks'))
    known = set(retained) | set(prior_concerns)
    if root_pid is not None and root_ticks is not None:
        known.add((root_pid, root_ticks))
    tracked_pids = {pid for pid, _ in known}
    if root_pid is not None:
        tracked_pids.add(root_pid)
    marker = os.fsencode(owner['marker'])
    thread_id = receipt.get('thread_id')
    thread = os.fsencode(thread_id) if isinstance(thread_id, str) and thread_id else None
    earliest = owner['not_before_ticks']

    table, checks, parent_edges = {}, {}, {}
    opaque, opaque_candidates = set(), {}
    try:
        pids = {int(path.name) for path in PROC.iterdir()
                if path.name.isascii() and path.name.isdecimal() and int(path.name) >= 2}
    except OSError as exc:
        uncertain.append(dict(reason='procfs_enumeration_failed', error=str(exc)))
        pids = set()
    for pid in pids | tracked_pids:
        try:
            process = _process(pid)
        except (OSError, ValueError, IndexError) as exc:
            if pid in tracked_pids:
                uncertain.append(dict(reason='process_identity_unreadable', pid=pid, error=str(exc)))
            else:
                opaque.add(pid)
            continue
        if process is None:
            if pid in tracked_pids:
                _missing_known_pid(pid, uncertain)
            continue
        if not process.live:
            continue
        if process.ticks < earliest and pid not in tracked_pids:
            continue
        table[pid] = process
        parent_edges[pid] = {process.parent}
        try:
            uid = _uid(pid)
        except _GONE:
            table.pop(pid)
            if pid in tracked_pids:
                _missing_known_pid(pid, uncertain)
            continue
        except (OSError, ValueError, IndexError) as exc:
            checks[pid] = None, None, None, str(exc)
            continue
        if uid != scope['uid']:
            checks[pid] = uid, set(), set(), None
            continue
        try:
            markers, threads = _environment(pid)
        except _GONE:
            table.pop(pid)
            if pid in tracked_pids:
                _missing_known_pid(pid, uncertain)
            continue
        except (OSError, ValueError) as exc:
            markers, threads, error = None, None, str(exc)
        else:
            error = None
        try:
            current = _process(pid)
        except (OSError, ValueError, IndexError) as exc:
            checks[pid] = None, markers, threads, str(exc)
            continue
        if current is None or not current.live or current.ticks != process.ticks:
            table.pop(pid)
            continue
        table[pid] = current
        parent_edges[pid].add(current.parent)
        checks[pid] = uid, markers, threads, error

    owned, blocked, concerns = {}, set(), {}
    descendants = defaultdict(list)
    for process in table.values():
        for parent_pid in parent_edges[process.pid]:
            parent = table.get(parent_pid)
            if parent and parent.ticks <= process.ticks:
                descendants[parent.pid].append(process.pid)

    def classify(pid, inherited=False):
        process = table[pid]
        uid, markers, threads, error = checks[pid]
        key = pid, process.ticks
        retained_match = key in retained
        prior_concern = key in prior_concerns
        recorded_pid = pid == root_pid and process.ticks == root_ticks
        in_window = process.ticks >= earliest
        owner_match = bool(markers and marker in markers and in_window)
        thread_match = bool(thread and threads and thread in threads and in_window)
        incomplete_root = pid == root_pid and root_ticks is None
        related = (retained_match or inherited or owner_match or thread_match or recorded_pid
                   or incomplete_root or prior_concern)
        if not related:
            if error:
                opaque.add(pid)
                if uid == scope['uid'] and in_window:
                    opaque_candidates[pid] = error
            return False
        if pid == os.getpid():
            reason = 'attempt_identity_matches_observer'
        elif uid != scope['uid']:
            reason = 'process_uid_unverified' if uid is None else 'process_uid_changed'
        elif ((markers and markers != {marker})
              or (thread and threads and threads != {thread})):
            reason = 'conflicting_process_ownership'
        elif pid == root_pid and root_ticks is not None and not recorded_pid and not retained_match:
            reason = 'conflicting_process_start_ticks'
        elif prior_concern and not (owner_match or thread_match):
            reason = 'retained_ownership_uncertainty'
        elif not (retained_match or owner_match or thread_match) and error:
            reason = 'process_ownership_unreadable'
        elif not (retained_match or owner_match or thread_match or inherited):
            reason = 'worker_identity_unverified'
        else:
            evidence = [kind for kind, found in (('receipt_pid', recorded_pid), ('ledger', retained_match),
                                                 ('owner_marker', owner_match), ('bound_thread', thread_match),
                                                 ('descendant', inherited)) if found]
            if error:
                opaque.add(pid)
            owned[pid] = _identity(process, scope, evidence)
            return True
        if pid not in blocked:
            row = {**_identity(process, scope, []), 'reason': reason, **({'error': error} if error else {})}
            uncertain.append(row)
            concerns[key] = row
            blocked.add(pid)
        return False

    for pid in table:
        classify(pid)
    queue = deque(owned)
    while queue:
        for pid in descendants[queue.popleft()]:
            if pid not in owned and pid not in blocked and classify(pid, inherited=True):
                queue.append(pid)
    for pid, error in opaque_candidates.items():
        if pid not in owned and pid not in blocked:
            uncertain.append(dict(reason='opaque_launch_candidate', pid=pid, error=error))
    if opaque:
        scope['opaque_processes'] = len(opaque)
    # Once a new positive observation resolves a concern, only its claim need
    # remain. Unknown identities can never act as authority for a signal.
    return sorted(owned.values(), key=lambda row: (int(row['process_start_ticks']), row['pid'])), concerns


def _scan(receipt, directory, persist, carry=(), carry_concerns=(), lock_deadline=None):
    uncertain = []
    try:
        scope = _scope()
    except (OSError, ValueError) as exc:
        return dict(processes=[], uncertain=[dict(reason='procfs_scope_unverified', error=str(exc))],
                    scope=dict(kind='local_linux_procfs'))

    owner = receipt.get('owner')
    if owner is None:
        # The dispatcher assigns an owner before publishing a numbered attempt.
        if (receipt.get('attempt') is None and receipt.get('pid') is None
                and receipt.get('execution') in ('prepared', 'failed', 'interrupted')):
            return dict(processes=[], uncertain=[], scope=scope)
        return dict(processes=[], uncertain=[dict(reason='missing_attempt_owner')], scope=scope)
    if (not isinstance(owner, dict) or not isinstance(owner.get('marker'), str) or not owner['marker']
            or type(owner.get('uid')) is not int or not owner.get('boot_id') or not owner.get('pid_namespace')
            or type(owner.get('not_before_ticks')) is not int or owner['not_before_ticks'] < 0):
        return dict(processes=[], uncertain=[dict(reason='invalid_attempt_owner')], scope=scope)
    if receipt.get('boot_id') and receipt['boot_id'] != owner['boot_id']:
        return dict(processes=[], uncertain=[dict(reason='conflicting_attempt_boot_identity')], scope=scope)
    if owner['boot_id'] != scope['boot_id']:
        return dict(processes=[], uncertain=[], scope=scope)
    if any(owner[key] != scope[key] for key in ('uid', 'pid_namespace')):
        return dict(processes=[], uncertain=[dict(reason='attempt_outside_observer_scope')], scope=scope)

    attempt = _number(receipt.get('attempt'), 1)
    path = Path(directory) / f'ownership-{attempt:02d}.json' if directory is not None and attempt else None
    if directory is not None and path is None:
        uncertain.append(dict(reason='ownership_ledger_unavailable', error='attempt number is missing'))
    claims = {_key(row): row for row in carry}
    concerns = {_key(row): row for row in carry_concerns}
    lock = nullcontext()
    if path is not None and persist:
        try:
            lock = _ledger_lock(path, time.monotonic() + .5 if lock_deadline is None else lock_deadline)
        except OSError as exc:
            uncertain.append(dict(reason='ownership_ledger_unavailable', error=str(exc)))
    with lock:
        key = _ledger_key(receipt, owner)
        try:
            if path is not None:
                retained, prior_concerns = _read_ledger(path, key, scope)
                claims.update(retained)
                concerns.update(prior_concerns)
        except (OSError, ValueError) as exc:
            uncertain.append(dict(reason='ownership_ledger_unavailable', error=str(exc)))
        processes, observed_concerns = _collect(receipt, owner, scope, claims, concerns, uncertain)
        additional = {_key(row): row for row in processes if _key(row) not in claims}
        all_concerns = {**concerns, **{key: row for key, row in observed_concerns.items() if key not in concerns}}
        for row in processes:
            all_concerns.pop(_key(row), None)
        if path is not None and persist and (additional or all_concerns != concerns) and not any(
                row['reason'] == 'ownership_ledger_unavailable' for row in uncertain):
            try:
                _write_ledger(path, key, {**claims, **additional}, all_concerns)
            except OSError as exc:
                uncertain.append(dict(reason='ownership_ledger_unavailable', error=str(exc)))
    return dict(processes=processes, uncertain=uncertain, scope=scope)


def scan_owned(receipt, directory=None, *, persist=True):
    """Observe the attempt's live claims and relevant uncertainty.

    Pass the worker directory for a per-attempt ledger. A read-only scan can
    consume retained identities without claiming new ones on disk.
    """
    return _scan(receipt, directory, persist)


def has_owned_work(receipt, directory=None):
    result = scan_owned(receipt, directory)
    return bool(result['processes'] or result['uncertain'])


def _send(row, sig, receipt=None):
    if not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        raise OSError(errno.ENOSYS, 'safe cleanup requires Linux pidfd support')
    pid = row['pid']
    fd = os.pidfd_open(pid)
    try:
        scope = _scope()
        if any(row[k] != scope[k] for k in ('boot_id', 'pid_namespace', 'uid')):
            raise ValueError('signal observer does not match process identity')
        current = _process(pid)
        if current is None or not current.live or current.ticks != int(row['process_start_ticks']):
            raise ProcessLookupError(pid)
        if _uid(pid) != row['uid']:
            raise PermissionError('process UID no longer matches claim')
        if receipt is not None:
            try:
                markers, threads = _environment(pid)
            except PermissionError:
                # An already proven identity remains valid if its environment
                # is unreadable. A positive conflicting marker never does.
                pass
            else:
                marker = os.fsencode(receipt['owner']['marker'])
                thread = receipt.get('thread_id')
                if ((markers and markers != {marker})
                        or (thread and threads and threads != {os.fsencode(thread)})):
                    raise ValueError('conflicting ownership at signal time')
        signal.pidfd_send_signal(fd, sig)
    finally:
        os.close(fd)


def terminate_owned(receipt, directory=None, deadline=None):
    """Retain claims, signal verified identities, and confirm scoped absence.

    The deadline is an absolute monotonic time. Do not interpret `stopped` as
    confirmation of scheduler shutdown or of work outside the returned scope.
    """
    started = time.monotonic()
    if deadline is None:
        deadline = started + 10
    grace = started + min(2, max(0, deadline - started) / 2)
    carried, carried_concerns, attempted, failures, signals = {}, {}, set(), {}, []
    clear = False
    while True:
        scan = _scan(receipt, directory, True, tuple(carried.values()) if directory is None else (),
                     tuple(carried_concerns.values()) if directory is None else (), lock_deadline=deadline)
        processes = scan['processes']
        uncertain = scan['uncertain']
        now = time.monotonic()
        if not processes and not uncertain:
            if clear:
                return dict(stopped=True, remaining=[], uncertain=[], scope=scan['scope'], signals=signals)
            clear = True
        else:
            clear = False
        if now >= deadline:
            if clear:
                uncertain.append(dict(reason='cleanup_recheck_deadline'))
            uncertain.extend(failures[_key(row)] for row in processes if _key(row) in failures)
            return dict(stopped=False, remaining=processes, uncertain=uncertain, scope=scan['scope'], signals=signals)

        carried.update({_key(row): row for row in processes})
        carried_concerns.update({_key(row): row for row in uncertain if 'process_start_ticks' in row})
        for row in processes:
            carried_concerns.pop(_key(row), None)
        # A claim must survive this call before we can stop its producer.
        if not any(row['reason'] == 'ownership_ledger_unavailable' for row in uncertain):
            for row in processes:
                if time.monotonic() >= deadline:
                    break
                key = _key(row)
                sig = signal.SIGKILL if now >= grace and (key, signal.SIGTERM) in attempted else signal.SIGTERM
                if (key, sig) in attempted:
                    continue
                attempted.add((key, sig))
                result = dict(pid=row['pid'], process_start_ticks=row['process_start_ticks'], signal=sig.name)
                try:
                    _send(row, sig, receipt)
                    result['outcome'] = 'sent'
                    failures.pop(key, None)
                except _GONE:
                    result['outcome'] = 'exited'
                except (OSError, ValueError, IndexError) as exc:
                    result.update(outcome='failed', error=str(exc))
                    failures[key] = dict(reason='process_signal_failed', pid=row['pid'], error=str(exc))
                signals.append(result)
        # Static evidence gaps (e.g. missing owner or a different scope) will
        # not be resolved by polling once we have stopped any verified claims.
        if not processes and uncertain and all(row['reason'] in {
                'missing_attempt_owner', 'invalid_attempt_owner',
                'attempt_outside_observer_scope', 'conflicting_attempt_boot_identity',
        } for row in uncertain):
            return dict(stopped=False, remaining=[], uncertain=uncertain, scope=scan['scope'], signals=signals)
        time.sleep(min(_POLL, max(0, deadline - time.monotonic())))
