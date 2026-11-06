"""Short metadata arbitration for task launch, retry takeover and cancellation."""
from contextlib import contextmanager
import fcntl
import json
from pathlib import Path


@contextmanager
def locked(root):
    # The execution lock is held for the whole invocation and inherited by EXEC.
    # This separate lock must never cover subprocess creation, signals, or waits.
    with (Path(root) / '.task-control.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _read_json(path):
    try:
        value = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    if not isinstance(value, dict):
        raise ValueError(f'expected object in {path}')
    return value


def _write_json(path, value):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    temp.replace(path)


def read(directory):
    try:
        state = _read_json(Path(directory) / 'control.json')
    except NotADirectoryError:
        return None
    if state is None:
        return None
    if (state.get('version') != 1 or state.get('phase') not in ('running', 'backoff', 'settled')
            or not isinstance(state.get('input_fingerprint'), str)
            or not isinstance(state.get('chain_id'), str)
            or type(state.get('attempt')) is not int or state['attempt'] < 1
            or not isinstance(state.get('owner_run_id'), str) or not state['owner_run_id'].isascii()
            or not state['owner_run_id'].isdecimal() or not isinstance(state.get('retry'), dict)):
        raise ValueError(f'invalid task control: {directory}')
    return state


def write(directory, state):
    _write_json(Path(directory) / 'control.json', state)


def close(state, reason):
    return {**state, 'phase': 'settled', 'reason': reason}


def is_cancelled(run_dir):
    # The legacy cancellation file was also written before signalling. For
    # newer invocations cancel_request.json is the immutable request.
    return ((Path(run_dir) / 'cancel_request.json').exists()
            or (Path(run_dir) / 'cancellation.json').exists())


def request_cancel(run_dir, request):
    """Accept a sticky request before releasing the lock or touching processes."""
    run_dir = Path(run_dir)
    root = run_dir.parent.parent
    with locked(root):
        path = run_dir / 'cancel_request.json'
        accepted = _read_json(path)
        if accepted is None:
            accepted = request
            _write_json(path, accepted)
        try:
            config = _read_json(run_dir / 'config.json') or {}
        except (OSError, ValueError):
            config = {}
        jobs = config.get('jobs', [])
        for task_id in jobs if isinstance(jobs, list) else []:
            if not isinstance(task_id, str) or task_id in ('', '.', '..') or Path(task_id).name != task_id:
                continue
            directory = root / 'workers' / task_id
            if directory.is_symlink() or directory.parent.is_symlink():
                continue
            try:
                state = read(directory)
                if state and state['owner_run_id'] == run_dir.name and state['phase'] in ('running', 'backoff'):
                    write(directory, close(state, 'cancelled'))
            except (OSError, ValueError):
                # The accepted request already revokes launch/takeover rights.
                # Recovery can settle the individual file when it becomes readable.
                continue
        return accepted
