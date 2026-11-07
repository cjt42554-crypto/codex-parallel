#!/usr/bin/env python3
"""Bounded, resumable Codex CLI worker queue. No business-data mutations."""
from __future__ import annotations
import argparse
import asyncio
from collections import Counter, deque
from datetime import datetime, timezone
import hashlib
import heapq
import fcntl
import random
import re
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import textwrap
import unicodedata
import uuid
from typing import NamedTuple

import process_ownership
import task_control
from result_contract import RESULT_STATES, needs_attention, result_issues, valid_result

SCRIPTS = Path(__file__).resolve().parent
TERMINATE_GRACE_SECONDS = 10
DEFAULT_MAX_SUBAGENTS = 3
MULTI_AGENT_FIELDS = ('multi_agent', 'max_subagents', 'subagent_model', 'subagent_effort')

def remember_run(root, manifest):
    """Optional monitor discovery; failures must not affect execution."""
    try:
        from monitor_registry import registry_directory
        directory = registry_directory()
        entry = {'run_dir': str(root), 'manifest': manifest, 'registered_at': time.time()}
        batches = directory / 'batches'
        batches.mkdir(exist_ok=True)
        key = hashlib.sha256(json.dumps([str(root), manifest]).encode()).hexdigest()
        batch_temp = batches / f'{key}-{os.getpid()}.tmp'
        batch_temp.write_text(json.dumps(entry))
        batch_temp.replace(batches / f'{key}.json')
        temp = directory / f'latest-{os.getpid()}.tmp'
        temp.write_text(json.dumps(entry))
        temp.replace(directory / 'latest.json')
    except Exception:
        pass

def launch_dashboard(root, manifest):
    """Best-effort, detached observer. Never block or fail scheduling."""
    def launch():
        try:
            with (root / 'dashboard.log').open('ab') as log:
                subprocess.Popen(
                    [sys.executable, str(SCRIPTS / 'dashboard.py'),
                     '--run-dir', str(root), '--manifest', manifest, '--launch'],
                    stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                    start_new_session=True, close_fds=True)
        except Exception as exc:
            try:
                print(f'Dashboard unavailable (scheduling continues): {exc}', file=sys.stderr)
            except Exception:
                pass
    try:
        threading.Thread(target=launch, daemon=True).start()
    except Exception:
        pass

def stamp():
    return datetime.now(timezone.utc).isoformat()

def write_json(path, value):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    temp.replace(path)

def available_kib():
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1])
    return 0

def process_snapshot(root_pids):
    if not root_pids:
        return {'process_count': 0, 'rss_kib': 0}
    table = {}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            parts = (entry / 'stat').read_text().rsplit(')', 1)[1].split()
            table[int(entry.name)] = (int(parts[1]), int(parts[21]) * os.sysconf('SC_PAGE_SIZE') // 1024)
        except (OSError, ValueError, IndexError):
            continue
    owned = set(root_pids)
    while True:
        more = {pid for pid, (ppid, _) in table.items() if ppid in owned}
        if more <= owned:
            break
        owned |= more
    return {'process_count': len(owned), 'rss_kib': sum(table.get(pid, (0, 0))[1] for pid in owned)}

class ConfigurationError(ValueError):
    pass


def read_common(root):
    """Shared context is optional for task-only workspaces."""
    try:
        return (root / 'COMMON.md').read_text()
    except FileNotFoundError:
        return ''


def load_jobs(root, args):
    manifest = json.loads((root / args.manifest).read_text())
    defaults = {}
    if isinstance(manifest, dict):
        defaults = manifest.get('defaults', {})
        fields = ('model', 'effort', 'web_search', 'write_dirs', 'retry_safe', 'task_timeout_seconds',
                  'idle_warning_seconds', *MULTI_AGENT_FIELDS)
        if not isinstance(defaults, dict) or set(defaults) - set(fields):
            raise ConfigurationError('unsupported defaults; use ' + ', '.join(fields))
        jobs = manifest.get('tasks')
    else:
        jobs = manifest
    if not isinstance(jobs, list):
        raise ConfigurationError('manifest must be a task array or an object with defaults and tasks')
    seen = set()
    normalized = []
    for index, job in enumerate(jobs):
        if not isinstance(job, dict):
            raise ConfigurationError(f'task {index}: expected an object')
        job = {**defaults, **job}
        job.setdefault('title', job.get('id'))
        for field in ('id', 'title', 'model', 'effort', 'prompt'):
            if not isinstance(job.get(field), str) or not job[field].strip():
                raise ConfigurationError(f'task {index}: {field} must be a nonempty string')
        task_id = job['id']
        if task_id in ('.', '..') or not re.fullmatch(r'[\w.-]+', task_id):
            raise ConfigurationError(f'unsafe task id: {task_id!r}')
        if task_id in seen:
            raise ConfigurationError(f'duplicate task id: {task_id}')
        seen.add(task_id)
        workers = root / 'workers'
        if workers.is_symlink() or (workers / task_id).is_symlink():
            raise ConfigurationError(f'task {task_id}: worker directories must not be symlinks')
        if job.get('web_search', 'disabled') not in ('disabled', 'cached', 'live'):
            raise ConfigurationError(f'task {task_id}: invalid web_search mode')
        if not isinstance(job.get('retry_safe', False), bool):
            raise ConfigurationError(f'task {task_id}: retry_safe must be a boolean')
        multi_agent = job.get('multi_agent', False)
        if not isinstance(multi_agent, bool):
            raise ConfigurationError(f'task {task_id}: multi_agent must be a boolean')
        limit = job.get('max_subagents', DEFAULT_MAX_SUBAGENTS)
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ConfigurationError(f'task {task_id}: max_subagents must be a positive integer in TOML range')
        for field in ('subagent_model', 'subagent_effort'):
            if multi_agent or field in job:
                if not isinstance(job.get(field), str) or not job[field].strip():
                    raise ConfigurationError(f'task {task_id}: {field} must be a nonempty string')
                job[field] = job[field].strip()
        for field in ('task_timeout_seconds', 'idle_warning_seconds'):
            if field in job and (type(job[field]) not in (int, float) or not math.isfinite(job[field]) or job[field] < 0):
                raise ConfigurationError(f'task {task_id}: {field} must be a finite nonnegative number (0 disables)')
        dirs = job.get('write_dirs', [])
        if not isinstance(dirs, list) or any(not isinstance(p, str) or not Path(p).is_absolute()
                                           or not Path(p).is_dir() for p in dirs):
            raise ConfigurationError(f'task {task_id}: write_dirs must contain existing absolute directories')
        normalized.append(job)
    jobs = normalized
    if args.only is not None:
        unknown = set(args.only) - seen
        if not args.only or unknown:
            raise ConfigurationError(f'--only requires known task IDs; unknown: {sorted(unknown)}')
        jobs = [job for job in jobs if job['id'] in args.only]
    return jobs


def task_spec(job, prompt, channel):
    spec = dict(prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest(),
                model=job['model'], effort=job['effort'], channel=channel,
                web_search=job.get('web_search', 'disabled'),
                write_dirs=sorted(set(str(Path(p).resolve()) for p in job.get('write_dirs', []))))
    # Child settings affect task identity only when child agents are enabled.
    if job.get('multi_agent'):
        spec.update(multi_agent=True, max_subagents=job.get('max_subagents', DEFAULT_MAX_SUBAGENTS),
                    subagent_model=job['subagent_model'], subagent_effort=job['subagent_effort'])
    return spec


def multi_agent_args(spec):
    if not spec.get('multi_agent'):
        return ['--disable', 'multi_agent']
    return ['--enable', 'multi_agent', '-c', 'agents.enabled=true',
            '-c', f'agents.max_concurrent_threads_per_session={spec["max_subagents"]}',
            '-c', 'agents.default_subagent_model=' + json.dumps(spec['subagent_model']),
            '-c', 'agents.default_subagent_reasoning_effort=' + json.dumps(spec['subagent_effort'])]


def fingerprint(spec):
    return hashlib.sha256(json.dumps(spec, sort_keys=True, ensure_ascii=False).encode()).hexdigest()




def read_receipt(path):
    if not path.exists():
        return None
    receipt = json.loads(path.read_text())
    if not isinstance(receipt, dict) or receipt.get('status') not in (
            'running', 'complete', 'partial', 'blocked', 'failed', 'interrupted'):
        raise ValueError(f'invalid receipt: {path}')
    return receipt


ATTEMPT_FILE = re.compile(r'(events|prompt|stderr|receipt|result)-(\d+)\.(jsonl|txt|log|json)')


class AttemptEvidence(NamedTuple):
    latest: dict | None
    source: Path | None
    number: int
    problem: str | None
    receipts: list[dict]
    ownership_gap: bool = False


def recover_attempt(directory):
    """Reconcile the newest attempt without treating worker-produced output as a receipt."""
    records, number, unreadable = {}, 0, None
    try:
        files = sorted(directory.iterdir())
    except (FileNotFoundError, NotADirectoryError):
        return AttemptEvidence(None, None, 0, None, [])
    except OSError as exc:
        return AttemptEvidence(None, None, 0, f'cannot inspect previous attempts: {exc}', [], True)
    numbered = []
    evidence_numbers = set()
    for path in files:
        match = ATTEMPT_FILE.fullmatch(path.name)
        if match:
            current = int(match[2])
            number = max(number, current)
            evidence_numbers.add(current)
            if match[1] == 'receipt' and match[3] == 'json':
                numbered.append((current, path))

    for current, path in numbered:
        try:
            receipt = read_receipt(path)
            if receipt is None:
                continue
            if receipt.get('task_id') != directory.name:
                raise ValueError('receipt belongs to a different task')
            declared = receipt.get('attempt')
            if type(declared) is not int or declared < 1:
                raise ValueError('invalid attempt number in receipt')
            if declared != current:
                raise ValueError('receipt attempt does not match its filename')
            number = max(number, current)
            records[current] = receipt, path
        except (OSError, ValueError) as exc:
            unreadable = f'cannot read previous attempt evidence {path.name}: {exc}'

    old, source = records[max(records)] if records else (None, None)
    receipts = [receipt for receipt, _ in records.values()]
    gap = bool(evidence_numbers - records.keys())
    if number not in records and number:
        detail = f'no readable receipt for attempt {number}'
        return AttemptEvidence(old, source, number, detail, receipts, True)
    if old is None:
        return AttemptEvidence(None, None, number, unreadable, receipts, bool(unreadable))
    return AttemptEvidence(old, source, number, None, receipts, gap)


def next_attempt(directory):
    numbers = [0]
    for path in directory.iterdir():
        match = ATTEMPT_FILE.fullmatch(path.name)
        if match:
            numbers.append(int(match[2]))
    return max(numbers) + 1


def read_result(directory, receipt):
    name = receipt.get('result_file')
    if not isinstance(name, str) or Path(name).name != name or name in ('', '.', '..'):
        raise ValueError('invalid result filename in receipt')
    path = directory / name
    result = json.loads(path.read_text())
    expected = receipt.get('reported_status', receipt['status'])
    if not valid_result(result) or (expected in RESULT_STATES and result['status'] != expected):
        raise ValueError('saved result does not match receipt')
    return result, path


def result_observation(directory, receipt):
    try:
        result, path = read_result(directory, receipt)
        return 'valid', result, path, None
    except OSError as exc:
        return 'unavailable', None, None, f'result unavailable: {exc}'
    except ValueError as exc:
        return 'invalid', None, None, f'result invalid: {exc}'


def successful_exit(receipt):
    return (type(receipt.get('exit_code')) is int and receipt['exit_code'] == 0
            and receipt.get('turn_completed_utc') and not receipt.get('turn_failed_utc'))


def validate_retry(intent, attempt):
    return (isinstance(intent, dict) and intent.get('next_attempt') == attempt + 1
            and type(intent.get('count')) is int and type(intent.get('limit')) is int
            and 1 <= intent['count'] <= intent['limit'] <= 10
            and all(type(intent.get(k)) in (int, float) and 0 <= intent[k] <= upper
                    for k, upper in (('delay_seconds', 120), ('base_delay', 120), ('ready_at', 1e12))))




def transient_error(message):
    """Only explicit transport/capacity errors, never business or quota failures."""
    text = str(message).lower()
    if any(s in text for s in ('quota', 'usage limit', 'usage_limit',
                               'unauthorized', 'authentication', 'invalid_api_key')):
        return False
    return bool(re.search(r'server_is_overloaded|rate_limit_exceeded|too many requests|\b429\b|'
                         r'\b(?:502|503|504)\b|connection reset|connection closed|'
                         r'stream disconnected|temporarily unavailable|server overloaded', text))


def compact_status(status):
    return {k: v for k, v in status.items()
            if k not in ('queued_ids', 'retrying_ids', 'selected_ids', 'running_ids', 'running_workers')}


async def stop_worker(proc, receipt, directory):
    """Use the same attempt ownership evidence as admission and batch cancel."""
    result = await asyncio.to_thread(process_ownership.terminate_owned, receipt, directory,
                                     time.monotonic() + TERMINATE_GRACE_SECONDS)
    if not result['stopped']:
        raise RuntimeError(f'owned work not confirmed stopped: {result}')
    if proc is not None:
        await proc.wait()


def has_owned_work(receipts, directory):
    return any(process_ownership.has_owned_work(receipt, directory) for receipt in receipts)


async def finish_cleanup(awaitable):
    """Finish cleanup even if cancellation arrives again while it is in progress."""
    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while True:
        try:
            return await asyncio.shield(task), cancelled
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True


def format_report(jobs, final, result_rows):
    """Render the already published results for the terminal."""
    labels = {'complete': '已完成', 'partial': '部分完成', 'blocked': '阻塞',
              'failed': '失败', 'interrupted': '已中断', 'not_started': '未启动'}
    totals = '，'.join(f'{labels.get(s, s)} {n}' for s, n in final['counts'].items()) or '无任务'
    if len(jobs) > 20:
        return (f'\n批次结束：{totals}\n复用 {len(final.get("reused", []))} 项。'
                f'\n简报：{final["report_path"]}'
                f'\n详细结果：{final["results_path"]}')
    reused = set(final.get('reused', []))
    rows = []

    def clean(value):
        return ' '.join(''.join(c if not unicodedata.category(c).startswith('C')
                               else ' ' for c in str(value)).split())

    def width(value):
        return sum(0 if unicodedata.combining(c) else
                   2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in value)

    for job in jobs:
        task_id = job['id']
        receipt = final['results'].get(task_id, {})
        result = result_rows[task_id]
        status = result['status']
        reasons = result_issues(result)
        if status == 'not_started':
            reasons = ['本轮未启动']
        elif status in RESULT_STATES and (status != 'complete' or result.get('needs_attention')):
            reasons.insert(0, result['answer'])
        task = job['title'] if job['title'] == task_id else f'{job["title"]} [{task_id}]'
        row = [task, receipt.get('pid'), labels.get(status, status), receipt.get('exit_code'),
               receipt.get('attempt'), '历史' if task_id in reused else '本轮' if receipt else '—',
               '；'.join(dict.fromkeys(clean(v) for v in reasons if v)) or '—']
        rows.append([clean(v) if v is not None else '—' for v in row])
    header = ['任务', 'PID', '状态', '退出码', '尝试', '来源', '原因']
    widths = [max(width(row[i]) for row in [header, *rows]) for i in range(len(header)-1)]

    def line(row):
        return '  '.join(value + ' ' * (widths[i] - width(value))
                         for i, value in enumerate(row[:-1])) + '  ' + row[-1]

    output = ['', f'批次结束：{totals}', '', line(header)]
    for row in rows:
        reason_lines = textwrap.wrap(row[-1], width=60) or ['—']
        output.append(line([*row[:-1], reason_lines[0]]))
        output.extend(line([''] * (len(header)-1) + [part]) for part in reason_lines[1:])
    output += ['', '来源为“历史”表示本轮未重跑，PID 和退出码来自之前的执行。' if reused else '',
               f'详细结果：{final["results_path"]}']
    return '\n'.join(output)


class Batch:
    def __init__(self, root, args, jobs, lock_fd):
        self.root, self.args, self.jobs, self.lock_fd = root, args, jobs, lock_fd
        supplied_id = getattr(args, '_run_id', None)
        if supplied_id is not None and not str(supplied_id).isdigit():
            raise ConfigurationError('invalid internal run ID')
        self.run_dir = root / 'runs' / (str(supplied_id) if supplied_id else str(time.time_ns()))
        self.run_dir.mkdir(parents=True, exist_ok=bool(supplied_id))
        if (self.run_dir / 'config.json').exists():
            raise ConfigurationError('invocation already exists; submit a new invocation to resume tasks')
        self.running, self.results = {}, {}
        self.receipt_paths = {}
        self.final_rows = {}
        self.cursor = self.attention_count = 0
        self.last_progress = {}
        self.result_events = self.run_dir / 'result-events.jsonl'
        self.result_events.touch(exist_ok=False)
        for name in ('attempts', 'bindings'):
            (self.run_dir / name).mkdir(exist_ok=True)
        self.reused = set()
        self.queued = {job['id'] for job in jobs}
        self.retrying = {}
        self.retries = Counter()
        self.automatic_retries_started = 0
        self.inflight = {}
        self.status_interval = getattr(args, 'status_interval_seconds', 2.0)
        self.last_publish = -float('inf')
        self.waiting_for_memory = False
        self.process_start_ticks = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]
        self.peak_running = self.peak_turns = 0
        self.started = time.monotonic()
        self.boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.pid_namespace = os.readlink('/proc/self/ns/pid')
        for job in jobs:
            job['_queued_at'] = self.started
        self.status_path = root / ('status-' + Path(args.manifest).stem + '.json')
        common = self.run_dir / 'common.md'
        common.write_text(jobs[0]['_common'] if jobs else read_common(root))
        metadata = {j['id']: {**{k: j[k] for k in ('title', 'model', 'effort', 'prompt')},
                              **{k: j['_spec'][k] for k in MULTI_AGENT_FIELDS if k in j['_spec']},
                              'web_search': j.get('web_search', 'disabled'),
                              'task_timeout_seconds': j.get('task_timeout_seconds', args.timeout_seconds),
                              'idle_warning_seconds': j.get('idle_warning_seconds',
                                                           getattr(args, 'idle_warning_seconds', 600))}
                    for j in jobs}
        write_json(self.run_dir / 'config.json', {'started_utc': stamp(), 'pid': os.getpid(), **vars(args),
                   'process_start_ticks': self.process_start_ticks, 'boot_id': self.boot_id,
                   'pid_namespace': self.pid_namespace, 'uid': os.geteuid(),
                   'batch_id': str(self.run_dir), 'run_id': self.run_dir.name,
                   'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   'jobs': [j['id'] for j in jobs], 'job_metadata': metadata,
                   'attempt_journal_version': 1, 'common_prompt_file': str(common)})

    def binding(self, directory, receipt, path, source):
        task_id, attempt = receipt['task_id'], receipt.get('attempt')
        name = receipt.get('result_file')
        result = directory / name if isinstance(name, str) and Path(name).name == name and name not in ('', '.', '..') else None
        record = dict(task_id=task_id, attempt=attempt, receipt_path=str(path),
                      result_path=str(result) if result else None, source=source,
                      execution=receipt.get('execution'), run_id=self.run_dir.name, batch_id=str(self.run_dir))
        if source == 'execution':
            record['owner'] = receipt['owner']
            journal = self.run_dir / 'attempts' / task_id
            journal.mkdir(exist_ok=True)
            write_json(journal / f'{attempt:02d}.json', record)
        write_json(self.run_dir / 'bindings' / f'{task_id}.json', record)

    def receipt_snapshot(self, task_id, receipt):
        path = self.run_dir / 'receipts' / f'{task_id}.json'
        path.parent.mkdir(exist_ok=True)
        write_json(path, receipt)
        return path

    def record(self, **event):
        with (self.run_dir / 'events.jsonl').open('a') as out:
            out.write(json.dumps({'utc': stamp(), **event}, ensure_ascii=False) + '\n')

    def active_turns(self):
        return sum(bool(r['turn_started_utc']) and not r['turn_completed_utc']
                   and not r.get('turn_failed_utc') for r in self.running.values())

    def counts(self):
        active = self.queued | set(self.retrying) | {j['id'] for j in self.inflight.values()}
        totals = Counter(r['status'] for task_id, r in self.results.items() if task_id not in active)
        return {status: totals[status]
                for status in ('complete', 'partial', 'blocked', 'failed', 'interrupted')}

    def publish(self, state='running', **extra):
        now = time.monotonic()
        if state == 'running' and now - self.last_publish < self.status_interval:
            return
        first = self.last_publish == -float('inf')
        self.last_publish = now
        counts = self.counts()
        running_ids = sorted(j['id'] for j in self.inflight.values())
        counts.update(queued=len(self.queued) if state == 'running' else 0,
                      retrying=len(self.retrying), running=len(running_ids))
        if state != 'running':
            counts['not_started'] = len(self.queued)
        snapshot = dict(utc=stamp(), run_id=self.run_dir.name, pid=os.getpid(),
                        batch_id=str(self.run_dir), boot_id=self.boot_id, heartbeat_utc=stamp(),
                        pid_namespace=self.pid_namespace, uid=os.geteuid(),
                        process_start_ticks=self.process_start_ticks,
                        manifest=self.args.manifest, state=state, total=len(self.jobs),
                        elapsed_seconds=round(now - self.started, 2),
                        batch_elapsed_seconds=round(now - self.started, 2),
                        pending=len(self.queued) + len(self.retrying), running=len(running_ids),
                        active_model_turns=self.active_turns(), counts=counts,
                        peak_workers=self.peak_running, peak_active_turns=self.peak_turns,
                        automatic_retries_started=self.automatic_retries_started,
                        mem_available_kib=available_kib(),
                        waiting_for_memory=state == 'running' and self.waiting_for_memory,
                        waiting_for_service=any(ready > now for ready in self.retrying.values()),
                        stalled_ids=sorted(task_id for task_id, receipt in self.running.items()
                                           if receipt.get('idle_warning_seconds', 0) > 0
                                           and now - self.last_progress.get(task_id, now) >= receipt['idle_warning_seconds']),
                        cursor=self.cursor, result_count=len(self.final_rows), attention_count=self.attention_count,
                        results_path=str(self.result_events),
                        running_workers={task_id: dict(pid=r['pid'], process_start_ticks=r.get('process_start_ticks'),
                                                      attempt=r['attempt'], boot_id=self.boot_id,
                                                      directory=str(self.root / 'workers' / task_id),
                                                      last_event_utc=r.get('last_event_utc'))
                                         for task_id, r in self.running.items()},
                        running_ids=running_ids, queued_ids=sorted(self.queued),
                        retrying_ids=sorted(self.retrying), selected_ids=[j['id'] for j in self.jobs],
                        **self.counts(), **process_snapshot([r['pid'] for r in self.running.values()]))
        snapshot.update(extra)
        write_json(self.run_dir / 'status.json', snapshot)
        write_json(self.status_path, snapshot)
        compact = compact_status(snapshot)
        with (self.run_dir / 'resources.jsonl').open('a') as out:
            out.write(json.dumps(compact) + '\n')
        if first or state != 'running' or getattr(self.args, 'progress', False):
            try:
                print(json.dumps(compact), flush=True)
            except (OSError, ValueError):
                pass

    def result_row(self, job):
        task_id = job['id']
        receipt = self.results.get(task_id, {})
        directory = self.root / 'workers' / task_id
        receipt_path = self.receipt_paths.get(task_id)
        if receipt and (receipt_path is None or not receipt_path.is_file()):
            # Unknown outcomes need an invocation-local snapshot.
            receipt_path = self.receipt_snapshot(task_id, receipt)
        row = dict(id=task_id, title=job['title'], status=receipt.get('status', 'not_started'),
                   answer='', issues=[], attempt=receipt.get('attempt'),
                   execution_elapsed_seconds=receipt.get('execution_elapsed_seconds'),
                   queue_wait_seconds=receipt.get('queue_wait_seconds'),
                   retry_backoff_seconds=receipt.get('retry_backoff_seconds', 0),
                   task_timeout_seconds=receipt.get('task_timeout_seconds'),
                   timed_out=receipt.get('timed_out', False),
                   reused=task_id in self.reused, receipt_path=str(receipt_path) if receipt else None)
        for key in ('attempt_status', 'recovery_state', 'execution', 'reported_status', 'result_observation', 'reason'):
            if key in receipt:
                row[key] = receipt[key]
        if receipt.get('recovery_state') != 'unknown' and (receipt.get('status') in RESULT_STATES
                                                          or receipt.get('execution') == 'succeeded'):
            row.setdefault('execution', 'succeeded')
            row.setdefault('reported_status', receipt['status'])
            observation, result, path, issue = result_observation(directory, receipt)
            row['result_observation'] = observation
            if result is not None:
                row.update(result, result_path=str(path))
                row['reported_status'] = result['status']
            elif issue:
                row['issues'].append(issue)
        row['issues'].extend(str(receipt[k]) for k in ('last_error', 'receipt_error', 'cleanup_error', 'log_error', 'projection_error')
                             if receipt.get(k) and (k != 'last_error' or receipt.get('status') != 'complete'))
        if row['status'] == 'not_started':
            row['issues'].append('not started in this invocation')
        row['needs_attention'] = needs_attention(row)
        return row

    def commit_result(self, job):
        """Publish each task once, after its retry policy has reached a final outcome."""
        if job['id'] in self.final_rows:
            return
        row = self.result_row(job)
        row['cursor'] = self.cursor + 1
        with self.result_events.open('a') as out:
            out.write(json.dumps(row, ensure_ascii=False) + '\n')
            out.flush()
        self.cursor = row['cursor']
        self.final_rows[job['id']] = row
        if row['needs_attention']:
            self.attention_count += 1

    def save_report(self, state):
        """Full evidence stays on disk; the controller reads one bounded report."""
        result_index = self.run_dir / 'results.jsonl'
        attention_path = self.run_dir / 'attention.jsonl'
        counts, preview, attention_count = Counter(), [], 0
        with result_index.open('w') as results, attention_path.open('w') as attention:
            for job in self.jobs:
                self.commit_result(job)
                row = self.final_rows[job['id']]
                counts[row['status']] += 1
                line = json.dumps(row, ensure_ascii=False) + '\n'
                results.write(line)
                if row['needs_attention']:
                    attention.write(line)
                    attention_count += 1
                    if len(preview) < 20:
                        preview.append(dict(id=row['id'], status=row['status'], answer=row['answer'][:500],
                                            issues=[v[:300] for v in row['issues'][:3]]))
        report = dict(run_id=self.run_dir.name, batch_id=str(self.run_dir), cursor=self.cursor,
                      state=state, completed_utc=stamp(), total=len(self.jobs),
                      ok=state == 'complete' and attention_count == 0, counts=dict(counts),
                      elapsed_seconds=round(time.monotonic() - self.started, 2),
                      batch_elapsed_seconds=round(time.monotonic() - self.started, 2),
                      peak_workers=self.peak_running, peak_active_turns=self.peak_turns,
                      reused_count=len(self.reused),
                      automatic_retries_started=self.automatic_retries_started,
                      attention_count=attention_count, attention_preview=preview,
                      results_path=str(result_index), attention_path=str(attention_path),
                      report_path=str(self.run_dir / 'report.json'))
        final = dict(**report, results=self.results,
                     unstarted=[j['id'] for j in self.jobs if j['id'] not in self.results], reused=sorted(self.reused))
        write_json(self.run_dir / 'report.json', report)
        return final

    async def worker(self, job, force=False):
        task_id = job['id']
        directory = self.root / 'workers' / task_id
        old, source = job.get('_old'), job.get('_old_path')
        if job['_decision'] in ('reuse', 'review') and not force:
            receipt = job['_outcome']
            if job.get('_promote_recovered'):
                # Finalize an in-progress record only after independent exit/turn
                # evidence and the matching attempt result agree.
                try:
                    self.receipt_paths[task_id] = self.save_receipt(directory, receipt)
                except OSError as exc:
                    receipt = {**receipt, 'receipt_error': f'could not save recovered receipt: {exc}'}
            elif receipt is old:
                self.receipt_paths[task_id] = source
            if (job.get('_control') or {}).get('phase') == 'running' and receipt.get('execution') == 'succeeded':
                try:
                    with task_control.locked(self.root):
                        current = task_control.read(directory)
                        if current == job['_control'] and current['attempt'] == receipt.get('attempt'):
                            task_control.write(directory, task_control.close(current, 'succeeded'))
                except (OSError, ValueError) as exc:
                    receipt = {**receipt, 'receipt_error': f'could not settle task scheduling state: {exc}'}
            self.results[task_id] = receipt
            self.reused.add(task_id)
            path = self.receipt_paths.get(task_id) or self.receipt_snapshot(task_id, receipt)
            self.binding(directory, receipt, path, 'reuse')
            return
        previous = self.results.get(task_id)
        evidence = recover_attempt(directory)
        if evidence.ownership_gap or has_owned_work(evidence.receipts, directory):
            job.pop('_scheduled_retry', None)
            self.receipt_paths.pop(task_id, None)
            self.results[task_id] = review_outcome(job, previous or old,
                                                   (previous or old or {}).get('attempt'),
                                                   'previous attempt ownership is incomplete' if evidence.ownership_gap
                                                   else 'previous attempt has live or unverified owned work')
            self.reused.add(task_id)
            return
        if force:
            self.results.pop(task_id, None)
            self.receipt_paths.pop(task_id, None)
            job.pop('_scheduled_retry', None)
        prompt = job['_common'] + job['_suffix']
        proc = err = result_path = None
        execution_started = None
        attempt = None
        prepared = False
        chain_id = job.get('_chain_id') or uuid.uuid4().hex
        retry_policy = job.get('_retry_policy') or dict(limit=getattr(self.args, 'max_retries', 2),
                                                       base_delay=getattr(self.args, 'retry_backoff_seconds', 30))
        receipt = dict(task_id=task_id, title=job['title'], model_requested=job['model'],
                       run_id=self.run_dir.name, batch_id=str(self.run_dir),
                       reasoning_effort_requested=job['effort'], input_fingerprint=job['_fingerprint'],
                       start_utc=stamp(), status='failed', execution='prepared', thread_id=None,
                       task_timeout_seconds=job.get('task_timeout_seconds', self.args.timeout_seconds), timed_out=False,
                       idle_warning_seconds=job.get('idle_warning_seconds', getattr(self.args, 'idle_warning_seconds', 600)),
                       queue_wait_seconds=round(max(0, time.monotonic() - job['_queued_at']), 3),
                       retry_backoff_seconds=job.get('_retry_delay', 0),
                       automatic_retry_count=self.retries[task_id], retry_policy=retry_policy,
                       turn_started_utc=None, turn_completed_utc=None, turn_failed_utc=None, usage=None)
        cancelled = False
        try:
            directory.mkdir(parents=True, exist_ok=True)
            attempt = next_attempt(directory)
            receipt['attempt'] = attempt
            result_path = directory / f'result-{attempt:02d}.json'
            receipt['result_file'] = result_path.name
            receipt['prompt_sha256'] = job['_spec']['prompt_sha256']
            cmd = [str(SCRIPTS / 'codexexec'), '--' + self.args.channel, '--ignore-user-config',
                   '-c', 'approval_policy="never"', '--skip-git-repo-check',
                   '--disable', 'apps', *multi_agent_args(job['_spec']), '--disable', 'memories',
                   '--disable', 'browser_use', '--disable', 'computer_use', '-s', 'workspace-write',
                   '-m', job['model'], '-c', 'model_reasoning_effort=' + json.dumps(job['effort']),
                   '-c', 'web_search=' + json.dumps(job.get('web_search', 'disabled')),
                   '-C', str(directory), '--json', '--output-schema',
                   str(SCRIPTS.parent / 'references/result-schema.json'), '-o', str(result_path)]
            for writable in job['_spec']['write_dirs']:
                cmd += ['--add-dir', writable]
            cmd.append('-')
            receipt.update(command=cmd, status='running', owner=process_ownership.prepare_owner(), boot_id=self.boot_id)
            with task_control.locked(self.root):
                if task_control.is_cancelled(self.run_dir):
                    raise asyncio.CancelledError
                current = task_control.read(directory)
                if force and (not current or current['phase'] != 'backoff'
                              or current['owner_run_id'] != self.run_dir.name
                              or current['chain_id'] != chain_id
                              or current['retry'].get('next_attempt') != attempt):
                    raise ConfigurationError('retry authorization changed before launch')
                (directory / f'prompt-{attempt:02d}.txt').write_text(prompt)
                path = self.save_receipt(directory, receipt)
                self.binding(directory, receipt, path, 'execution')
                task_control.write(directory, dict(version=1, phase='running', owner_run_id=self.run_dir.name,
                                                   chain_id=chain_id, attempt=attempt, input_fingerprint=job['_fingerprint'],
                                                   retry={'count': self.retries[task_id], **retry_policy}))
                prepared = True
                job['_chain_id'] = chain_id
            env = os.environ.copy()
            env.pop('CODEX_THREAD_ID', None)
            env[process_ownership.OWNER_ENV] = receipt['owner']['marker']
            env.update(OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1',
                       NUMEXPR_NUM_THREADS='1', PYTHONDONTWRITEBYTECODE='1', TOKENIZERS_PARALLELISM='false')
            err = (directory / f'stderr-{attempt:02d}.log').open('wb')
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=err,
                env=env, start_new_session=True, pass_fds=(self.lock_fd,), limit=16*1024*1024)
            receipt.update(pid=proc.pid, execution='running')
            if self.retries[task_id]:
                self.automatic_retries_started += 1
            receipt['process_start_ticks'] = process_ownership.bind_worker_identity(proc.pid, receipt)
            receipt['boot_id'] = self.boot_id
            execution_started = time.monotonic()
            receipt['execution_started_utc'] = stamp()
            receipt['last_event_utc'] = receipt['execution_started_utc']
            self.last_progress[task_id] = execution_started
            self.running[task_id] = receipt
            self.peak_running = max(self.peak_running, len(self.running))
            self.save_receipt(directory, receipt)
            self.record(event='worker_started', task_id=task_id, pid=proc.pid,
                        process_start_ticks=receipt['process_start_ticks'], boot_id=self.boot_id,
                        attempt=attempt, directory=str(directory))

            async def exchange():
                proc.stdin.write(prompt.encode())
                await proc.stdin.drain()
                proc.stdin.close()
                with (directory / f'events-{attempt:02d}.jsonl').open('wb') as out:
                    while raw := await proc.stdout.readline():
                        out.write(raw)
                        out.flush()
                        try:
                            event = json.loads(raw)
                        except (ValueError, UnicodeDecodeError):
                            continue
                        if not isinstance(event, dict):
                            continue
                        self.last_progress[task_id] = time.monotonic()
                        receipt['last_event_utc'] = stamp()
                        kind = event.get('type')
                        if kind in ('item.started', 'item.completed', 'item.updated'):
                            item = event.get('item')
                            if not isinstance(item, dict) or item.get('type') not in ('agent_message', 'reasoning'):
                                receipt['tools_observed'] = True
                        elif kind not in ('thread.started', 'turn.started', 'turn.completed', 'turn.failed', 'error'):
                            # Unknown protocol events cannot prove that replay is side-effect free.
                            receipt['tools_observed'] = True
                        if kind == 'thread.started':
                            receipt['thread_id'] = event.get('thread_id')
                        elif kind == 'turn.started':
                            receipt['turn_started_utc'] = stamp()
                            self.peak_turns = max(self.peak_turns, self.active_turns())
                        elif kind == 'turn.completed':
                            receipt['turn_completed_utc'] = stamp()
                            receipt['usage'] = event.get('usage')
                        elif kind == 'turn.failed':
                            receipt['turn_failed_utc'] = stamp()
                            receipt['last_error'] = str(event.get('error', 'model turn failed'))[:1500]
                            receipt['transient_error'] = transient_error(receipt['last_error'])
                        elif kind == 'error':
                            receipt['last_error'] = str(event.get('message', ''))[:1500]
                            receipt['transient_error'] = transient_error(receipt['last_error'])
                        else:
                            continue
                        self.save_receipt(directory, receipt)
                        self.record(event=kind, task_id=task_id)
                return await proc.wait()

            code = await asyncio.wait_for(exchange(), receipt['task_timeout_seconds'] or None)
            receipt['exit_code'] = code
            if not successful_exit(receipt):
                receipt['retryable'] = bool(receipt.get('transient_error'))
                raise ValueError(receipt.get('last_error') or f'worker did not complete successfully (exit {code})')
            # Record execution success separately from reading the worker's answer.
            receipt.update(execution='succeeded', status='blocked', reported_status='unknown')
            receipt.pop('last_error', None)
            self.save_receipt(directory, receipt)
            observation, result, _, _ = result_observation(directory, receipt)
            receipt['result_observation'] = observation
            if result is not None:
                receipt.update(status=result['status'], reported_status=result['status'])
        except asyncio.CancelledError:
            cancelled = True
            if receipt['execution'] != 'succeeded':
                receipt.update(status='interrupted', execution='interrupted', reason='cancelled',
                               last_error='dispatcher interrupted')
        except Exception as exc:
            receipt['timed_out'] = isinstance(exc, asyncio.TimeoutError)
            if receipt['execution'] == 'succeeded':
                receipt['receipt_error'] = f'{type(exc).__name__}: {exc}'
            else:
                receipt.update(status='failed', execution='failed',
                               reason='timeout' if receipt['timed_out'] else 'execution_failed' if proc else 'launch_failed',
                               last_error='worker deadline exceeded' if receipt['timed_out'] else f'{type(exc).__name__}: {exc}')
        finally:
            if not prepared:
                if cancelled and previous is not None:
                    self.results[task_id] = previous
                elif not cancelled:
                    receipt.pop('attempt', None)
                    receipt.setdefault('receipt_error', 'attempt could not be prepared')
                    self.results[task_id] = receipt
            else:
                if execution_started is not None:
                    receipt['execution_elapsed_seconds'] = round(time.monotonic() - execution_started, 3)
                if receipt['execution'] in ('failed', 'interrupted'):
                    try:
                        _, cleanup_cancelled = await finish_cleanup(stop_worker(proc, receipt, directory))
                        if cleanup_cancelled:
                            cancelled = True
                            receipt.update(status='interrupted', execution='interrupted', reason='cancelled',
                                           last_error='dispatcher interrupted during cleanup')
                    except Exception as exc:
                        receipt['cleanup_error'] = str(exc)
                    if proc is not None:
                        receipt['exit_code'] = proc.returncode
                if err is not None:
                    try:
                        err.close()
                    except OSError as exc:
                        receipt['log_error'] = f'closing worker log: {exc}'
                receipt['end_utc'] = stamp()
                try:
                    self.receipt_paths[task_id] = self.save_receipt(directory, receipt)
                    with task_control.locked(self.root):
                        current = task_control.read(directory)
                        if (current and current['owner_run_id'] == self.run_dir.name
                                and current['attempt'] == attempt and current['phase'] == 'running'):
                            if task_control.is_cancelled(self.run_dir):
                                task_control.write(directory, task_control.close(current, 'cancelled'))
                            elif intent := self.plan_retry(job, receipt, retry_policy):
                                task_control.write(directory, {**current, 'phase': 'backoff', 'retry': intent})
                                job['_scheduled_retry'] = intent
                            else:
                                task_control.write(directory, task_control.close(current, receipt['execution']))
                except Exception as exc:
                    receipt['receipt_error'] = str(exc)
                self.results[task_id] = receipt
                self.running.pop(task_id, None)
                self.last_progress.pop(task_id, None)
                try:
                    self.record(event='worker_finished', task_id=task_id, status=receipt['status'])
                except Exception as exc:
                    receipt['log_error'] = str(exc)
            if cancelled:
                raise asyncio.CancelledError

    @staticmethod
    def save_receipt(directory, receipt):
        path = directory / f'receipt-{receipt["attempt"]:02d}.json'
        write_json(path, receipt)
        return path

    def plan_retry(self, job, receipt, policy):
        count = self.retries[job['id']] + 1
        if (receipt['status'] == 'failed' and receipt.get('retryable')
                and not receipt.get('cleanup_error') and not receipt.get('receipt_error') and not receipt.get('log_error')
                and (job.get('retry_safe', False) or not receipt.get('tools_observed'))
                and count <= policy['limit']):
            delay = min(120, policy['base_delay'] * 2 ** (count - 1) * random.uniform(.8, 1.2))
            return dict(next_attempt=receipt['attempt'] + 1, count=count,
                        delay_seconds=round(delay, 3), ready_at=time.time() + delay, **policy)
        return None

    def take_over_retry(self, job):
        directory = self.root / 'workers' / job['id']
        old, expected = job['_old'], job['_control']
        with task_control.locked(self.root):
            if task_control.is_cancelled(self.run_dir):
                raise asyncio.CancelledError
            current = task_control.read(directory)
            previous_owner = current['owner_run_id'] if current else old.get('run_id')
            if previous_owner and task_control.is_cancelled(self.root / 'runs' / previous_owner):
                if current and current['phase'] == 'backoff':
                    task_control.write(directory, task_control.close(current, 'cancelled'))
                job.update(_decision='reuse', _outcome=cancelled_retry(old))
                return False
            if current != expected:
                job.update(_decision='review', _outcome=review_outcome(job, old, old.get('attempt'),
                                                                      'retry authorization changed before takeover'))
                return False
            chain = current['chain_id'] if current else uuid.uuid4().hex
            task_control.write(directory, dict(version=1, phase='backoff', owner_run_id=self.run_dir.name,
                                               chain_id=chain, attempt=old['attempt'],
                                               input_fingerprint=job['_fingerprint'], retry=job['_resume_retry']))
            job['_chain_id'] = chain
            return True

    async def run(self):
        # Reuse needs no subprocess or memory headroom. Keep it ahead of new work.
        pending = deque((j, False) for j in sorted(self.jobs, key=lambda j: j['_decision'] == 'launch')
                        if j['_decision'] != 'resume')
        delayed = []
        sequence = 0

        def enqueue_retry(job, intent):
            nonlocal sequence
            task_id = job['id']
            self.retries[task_id] = intent['count']
            job['_retry_policy'] = dict(limit=intent['limit'], base_delay=intent['base_delay'])
            job['_retry_delay'] = intent['delay_seconds']
            job.setdefault('_chain_id', intent.get('chain_id') or uuid.uuid4().hex)
            ready = time.monotonic() + max(0, min(120, intent['ready_at'] - time.time()))
            self.retrying[task_id] = ready
            sequence += 1
            heapq.heappush(delayed, (ready, sequence, job))

        state = 'interrupted'
        try:
            for job in self.jobs:
                if job['_decision'] == 'resume':
                    if not self.take_over_retry(job):
                        pending.appendleft((job, False))
                        continue
                    task_id = job['id']
                    self.queued.discard(task_id)
                    self.results[task_id] = job['_old']
                    intent = job['_resume_retry']
                    enqueue_retry(job, intent)
                    self.record(event='retry_resumed', task_id=task_id, attempt=intent['next_attempt'],
                                retry=intent['count'], ready_at=intent['ready_at'])
            while pending or self.inflight or delayed:
                if task_control.is_cancelled(self.run_dir):
                    raise asyncio.CancelledError
                now = time.monotonic()
                while delayed and delayed[0][0] <= now:
                    ready, _, job = heapq.heappop(delayed)
                    job['_queued_at'] = ready
                    pending.append((job, True))
                elapsed = time.monotonic() - self.started
                capacity = min(self.args.concurrency, 4 if elapsed < self.args.ramp_seconds else
                               12 if elapsed < 2*self.args.ramp_seconds else self.args.concurrency)
                self.waiting_for_memory = False
                while pending and len(self.inflight) < capacity:
                    job, force = pending[0]
                    if (force or job['_decision'] == 'launch') and available_kib() < 1536*1024:
                        self.waiting_for_memory = True
                        break
                    pending.popleft()
                    self.queued.discard(job['id'])
                    self.retrying.pop(job['id'], None)
                    self.inflight[asyncio.create_task(self.worker(job, force))] = job
                self.publish()
                wait = min(1., max(.01, self.status_interval - (time.monotonic() - self.last_publish)))
                if delayed:
                    wait = min(wait, max(.01, delayed[0][0] - time.monotonic()))
                if self.inflight:
                    done, _ = await asyncio.wait(self.inflight, timeout=wait, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        job = self.inflight.pop(task)
                        task.result()
                        task_id = job['id']
                        receipt = self.results[task_id]
                        intent = job.pop('_scheduled_retry', None)
                        if task_id not in self.reused and intent and not receipt.get('receipt_error'):
                            enqueue_retry(job, intent)
                            self.record(event='retry_scheduled', task_id=task_id,
                                        attempt=receipt['attempt'], retry=intent['count'],
                                        delay_seconds=intent['delay_seconds'], ready_at=intent['ready_at'])
                        else:
                            self.commit_result(job)
                elif pending or delayed:
                    await asyncio.sleep(wait)
            state = 'complete'
        finally:
            for task in self.inflight:
                task.cancel()
            _, cleanup_cancelled = await finish_cleanup(asyncio.gather(*self.inflight, return_exceptions=True))
            if cleanup_cancelled:
                state = 'interrupted'
            cancelled = task_control.is_cancelled(self.run_dir)
            for task_id, receipt in tuple(self.results.items()):
                if task_id in self.final_rows or receipt['status'] != 'failed':
                    continue
                try:
                    current = task_control.read(self.root / 'workers' / task_id)
                except (OSError, ValueError):
                    current = None
                if current and current['owner_run_id'] == self.run_dir.name and current['attempt'] == receipt.get('attempt'):
                    if cancelled and 'next_attempt' in current['retry']:
                        self.results[task_id] = cancelled_retry(receipt)
                    elif current['phase'] == 'backoff':
                        self.results[task_id] = {**receipt, 'status': 'interrupted', 'attempt_status': 'failed',
                                                 'recovery_state': 'retry_pending',
                                                 'last_error': 'invocation interrupted before the planned retry'}
                    else:
                        continue
                    self.receipt_paths.pop(task_id, None)
            self.inflight.clear()
            self.retrying.clear()
            self.queued = {j['id'] for j in self.jobs if j['id'] not in self.results}
            final = self.save_report(state)
            self.publish(state, report_path=final['report_path'],
                         attention_count=final['attention_count'], reused_count=final['reused_count'], ok=final['ok'])
            # A closed output pipe must not turn a saved batch result into a failure.
            try:
                print(format_report(self.jobs, final, self.final_rows), file=sys.stderr, flush=True)
            except (OSError, ValueError):
                pass


def review_outcome(job, receipt, attempt, problem):
    row = dict(task_id=job['id'], status='interrupted', execution='unknown', recovery_state='unknown',
               reason='outcome_unverified', last_error=problem)
    if attempt:
        row['attempt'] = attempt
    if receipt and receipt.get('attempt') == attempt:
        row['attempt_status'] = receipt['status']
        row.update({k: receipt[k] for k in ('pid', 'exit_code', 'boot_id', 'process_start_ticks') if k in receipt})
    return row


def cancelled_retry(receipt):
    return {**receipt, 'status': 'interrupted', 'attempt_status': receipt['status'],
            'recovery_state': 'retry_cancelled', 'reason': 'cancelled',
            'last_error': 'invocation cancelled before the planned retry'}


def recovery_decision(root, directory, job, args):
    evidence = recover_attempt(directory)
    old, source, number, problem = evidence[:4]
    job.update(_old=old, _old_path=source)
    control_error = None
    try:
        control = task_control.read(directory)
    except (OSError, ValueError) as exc:
        control, control_error = None, f'cannot read task scheduling state: {exc}'
    job['_control'] = control
    previous = old and old.get('input_fingerprint')
    if (old and previous != job['_fingerprint']) or (control and control['input_fingerprint'] != job['_fingerprint']):
        raise ConfigurationError(f'task {job["id"]}: inputs/configuration changed or old inputs cannot be verified; '
                                 'use a new task ID or run directory (no tasks launched)')
    if control and control['attempt'] != number:
        control_error = 'task scheduling state and latest attempt evidence disagree'

    # Confirmed success is monotonic, even when the model result was not recorded.
    if old and (old.get('execution') == 'succeeded' or successful_exit(old)):
        if problem:
            return 'review', {**review_outcome(job, old, number, problem),
                              'execution': 'succeeded' if old.get('attempt', 0) == number else 'unknown',
                              'result_observation': 'unavailable'}
        reported = old.get('reported_status', 'unknown')
        if old['status'] not in RESULT_STATES or old.get('reported_status') == 'unknown':
            updated = {**old, 'task_id': job['id'], 'execution': 'succeeded',
                       'status': 'blocked', 'reported_status': 'unknown', 'recovery_state': 'recovered'}
            if old.get('attempt') and old.get('result_file') == f'result-{old["attempt"]:02d}.json':
                observation, result, _, _ = result_observation(directory, updated)
            else:
                observation, result = 'unavailable', None
            updated['result_observation'] = observation
            if result is not None:
                updated.update(status=result['status'], reported_status=result['status'])
            if old != updated and old.get('attempt') and not problem:
                updated['end_utc'] = old.get('end_utc') or stamp()
                updated.pop('last_error', None)
                job['_promote_recovered'] = True
            return 'reuse', updated
        if not (args.retry_failed and reported in ('partial', 'blocked') and not (problem or control_error)
                and result_observation(directory, old)[0] == 'valid'):
            return 'reuse', old
        return 'launch', None

    if control_error or problem or (old and old['status'] == 'running'):
        detail = control_error or problem or 'previous worker exited without a verified task outcome'
        can_retry = (not control_error and not evidence.ownership_gap and old and old.get('attempt', 0) == number
                     and old.get('receipt_error') is None and (old.get('owner') or old.get('pid') or old.get('thread_id')))
        if args.retry_failed and can_retry:
            return 'launch', None
        if args.retry_failed and not can_retry:
            detail += '; cannot verify replay safety from the saved attempt'
        return 'review', review_outcome(job, old, number, f'{detail}; review before repeating the task')

    if old is None:
        return 'launch', None
    cancelled = bool(control and (control.get('reason') == 'cancelled'
                     or task_control.is_cancelled(root / 'runs' / control['owner_run_id'])))
    if cancelled and 'next_attempt' in control['retry']:
        return ('launch', None) if args.retry_failed else ('reuse', cancelled_retry(old))
    if control and control['phase'] == 'backoff' and not cancelled:
        intent = control['retry']
        if old['status'] != 'failed' or not validate_retry(intent, old.get('attempt', 0)):
            return 'review', review_outcome(job, old, number, 'previous retry authorization cannot be verified')
    else:
        intent = None
    if intent and (job.get('retry_safe') or not old.get('tools_observed')):
        job['_resume_retry'] = intent
        return 'resume', None
    if args.retry_failed and not old.get('receipt_error'):
        return 'launch', None
    return 'reuse', old


def prepare_jobs(root, args):
    jobs = load_jobs(root, args)
    common = read_common(root)
    for job in jobs:
        directory = root / 'workers' / job['id']
        # Share COMMON in memory instead of storing a full copy for every queued job.
        job['_common'] = common
        job['_suffix'] = '\n\n' + job['prompt'] + '\n\nYour assigned writable directory: ' + str(directory) + '\n'
        job['_spec'] = task_spec(job, common + job['_suffix'], args.channel)
        job['_fingerprint'] = fingerprint(job['_spec'])
        job['_decision'], job['_outcome'] = recovery_decision(root, directory, job, args)
    return jobs

async def main():
    ap = argparse.ArgumentParser(description='Run a batch of independent Codex tasks.')
    ap.add_argument('--run-dir', help='Task workspace; required for init, submit or foreground execution')
    ap.add_argument('--batch', help='Durable batch handle returned by --submit (absolute invocation directory)')
    ap.add_argument('--_run-id', dest='_run_id', help=argparse.SUPPRESS)
    action = ap.add_mutually_exclusive_group()
    action.add_argument('--init', action='store_true')
    action.add_argument('--submit', action='store_true', help='Start a detached batch and return its durable handle')
    action.add_argument('--wait', action='store_true', help='Receive unread final answers; delivered task IDs are remembered per batch')
    action.add_argument('--cancel', action='store_true', help='Stop this invocation and its owned workers')
    action.add_argument('--status', action='store_true', help='Read compact current batch status')
    action.add_argument('--report', action='store_true', help='Read the bounded final report, or current status if running')
    action.add_argument('--results', action='store_true', help='Read a page of final task results')
    ap.add_argument('--attention', action='store_true', help='With --results, show only tasks needing attention')
    ap.add_argument('--limit', type=int, default=20, help='Result page size (1–100)')
    ap.add_argument('--after', type=int, help='Explicit result cursor; with --wait, replay without changing automatic delivery state')
    ap.add_argument('--wait-seconds', type=float, default=60, help='Bound this wait/cancel call, 0–60s; not an execution timeout')
    ap.add_argument('--channel', choices=('sub', 'api'), default='sub')
    ap.add_argument('--manifest', default='tasks.json')
    ap.add_argument('--concurrency', type=int, default=24)
    ap.add_argument('--ramp-seconds', type=int, default=20)
    ap.add_argument('--task-timeout-seconds', dest='timeout_seconds',
                    type=float, default=0,
                    help='Per EXEC attempt time limit after launch, excluding queue/backoff; 0 disables (default)')
    ap.add_argument('--idle-warning-seconds', type=float, default=600,
                    help='Flag a live worker with no new events for this long; warning only, 0 disables')
    ap.add_argument('--only', nargs='*')
    ap.add_argument('--retry-failed', action='store_true')
    ap.add_argument('--max-retries', type=int, default=2, help='Automatic retries per task for transient failures (0 disables)')
    ap.add_argument('--retry-backoff-seconds', type=float, default=30, help='Initial automatic retry delay; exponential jitter, capped at 120s')
    ap.add_argument('--status-interval-seconds', type=float, default=2, help='Status/resource sampling interval (at least 0.1s)')
    ap.add_argument('--progress', action='store_true', help='Also emit periodic status JSON to stdout (default: start/end only)')
    ap.add_argument('--no-dashboard', action='store_true', help='Do not launch the optional terminal workflow monitor')
    args = ap.parse_args()
    if args.concurrency < 1 or args.timeout_seconds < 0 or args.ramp_seconds < 0:
        ap.error('Concurrency must be positive; task timeout and ramp must be nonnegative')
    if (not math.isfinite(args.timeout_seconds) or not math.isfinite(args.idle_warning_seconds)
            or args.idle_warning_seconds < 0 or not 0 <= args.wait_seconds <= 60):
        ap.error('task timeout and idle warning must be finite and nonnegative; wait must be 0–60s')
    if args.after is not None and (args.after < 0 or not (args.results or args.wait)):
        ap.error('--after requires --results or --wait and a nonnegative cursor')
    if (not 0 <= args.max_retries <= 10 or not 0 <= args.retry_backoff_seconds <= 120
            or not .1 <= args.status_interval_seconds <= 60):
        ap.error('retries must be 0–10, backoff 0–120s, status interval 0.1–60s')
    if not 1 <= args.limit <= 100 or (args.attention and not args.results):
        ap.error('--results accepts --limit 1–100 and optional --attention')
    if args.cancel and not args.batch:
        ap.error('--cancel requires an explicit --batch')
    reader = args.status or args.report or args.results or args.wait or args.cancel
    root = Path(args.run_dir).expanduser().resolve() if args.run_dir else None
    target = None
    if args.batch:
        target = Path(args.batch).expanduser().resolve()
        if not reader or not target.name.isdigit() or target.parent.name != 'runs' or not target.is_dir():
            ap.error('--batch requires a valid invocation directory and --status/--report/--results/--wait/--cancel')
        batch_root = target.parent.parent
        if root is not None and root != batch_root:
            ap.error('--batch belongs to a different --run-dir')
        root = batch_root
    if root is None:
        ap.error('provide --run-dir for submission or --batch for an existing invocation')
    if args._run_id is not None and (not args._run_id.isdigit() or reader or args.submit or args.init):
        ap.error('invalid internal run ID')
    if args.init:
        root.mkdir(parents=True, exist_ok=False)
        (root / 'COMMON.md').write_text('Shared context and task boundaries.\n')
        (root / 'tasks.json').write_text('[]\n')
        print(root)
        return
    if args.submit or reader:
        import batch_control as control
        if args.submit:
            result = control.submit(root, args)
        elif args.cancel:
            # Accept the sticky request before consulting fallible projections.
            result = control.cancel(target, args.wait_seconds)
        else:
            scoped_reader = target is not None
            if target is None:
                target = control.latest_submission(root, args.manifest)
                if target is None:
                    raise ConfigurationError('no invocation recorded for this manifest; use --batch for an existing invocation')
            if args.wait:
                result = control.wait_for_event(target, args.after, args.wait_seconds, args.limit)
            elif args.results:
                result = control.results_page(target, after_cursor=args.after, attention=args.attention,
                                              limit=args.limit)
            else:
                status = control.read_status(target)
                if args.report and status.get('state') in ('complete', 'interrupted') and status.get('report_path'):
                    result = json.loads(Path(status['report_path']).read_text())
                else:
                    result = compact_status(status)
            if not scoped_reader:
                active = control.active_batch(root)
                result = {**result, 'latest_submission': str(target), 'active_batch': str(active) if active else None}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    # Keep the lock inode; deleting it would allow another process to lock a new inode.
    with (root / '.dispatch.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ConfigurationError('this run directory already has an active dispatcher or worker')
        jobs = prepare_jobs(root, args)
        batch = Batch(root, args, jobs, lock.fileno())
        import batch_control as control
        loop = asyncio.get_running_loop()
        main_task = asyncio.current_task()

        def cancel_from_signal(signum):
            try:
                task_control.request_cancel(batch.run_dir, dict(batch_id=str(batch.run_dir), run_id=batch.run_dir.name,
                                                                requested_utc=stamp(), reason=signum.name))
            finally:
                main_task.cancel()

        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, cancel_from_signal, signum)
        try:
            # Submission readiness means validation passed, ownership is locked, and cancellation is installed.
            if args._run_id is None:
                control.register_submission(batch.run_dir)
            control.register_active_batch(batch.run_dir)
            batch.publish()
            remember_run(root, args.manifest)
            if not args.no_dashboard:
                launch_dashboard(root, args.manifest)
            await batch.run()
        except asyncio.CancelledError:
            if not task_control.is_cancelled(batch.run_dir):
                raise
        finally:
            for signum in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(signum)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (ConfigurationError, OSError, ValueError) as exc:
        sys.exit(f'dispatch: {exc}')
