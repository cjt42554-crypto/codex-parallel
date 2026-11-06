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
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import textwrap
import unicodedata

SCRIPTS = Path(__file__).resolve().parent
TERMINATE_GRACE_SECONDS = 10

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


def load_jobs(root, args):
    manifest = json.loads((root / args.manifest).read_text())
    defaults = {}
    if isinstance(manifest, dict):
        defaults = manifest.get('defaults', {})
        if not isinstance(defaults, dict) or set(defaults) - {'model', 'effort', 'web_search', 'write_dirs', 'retry_safe'}:
            raise ConfigurationError('defaults supports model, effort, web_search, write_dirs and retry_safe only')
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
    return dict(prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest(),
                model=job['model'], effort=job['effort'], channel=channel,
                web_search=job.get('web_search', 'disabled'),
                write_dirs=sorted(set(str(Path(p).resolve()) for p in job.get('write_dirs', []))))


def fingerprint(spec):
    return hashlib.sha256(json.dumps(spec, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def legacy_fingerprint(receipt):
    """Recover only the execution inputs actually recorded by the old dispatcher."""
    cmd = receipt.get('command', [])
    if not isinstance(cmd, list) or not all(isinstance(v, str) for v in cmd):
        return None
    try:
        channel = 'sub' if '--sub' in cmd else 'api' if '--api' in cmd else None
        web = next(json.loads(v.split('=', 1)[1]) for v in cmd if v.startswith('web_search='))
        spec = dict(prompt_sha256=receipt['prompt_sha256'], model=receipt['model_requested'],
                    effort=receipt['reasoning_effort_requested'], channel=channel, web_search=web,
                    write_dirs=sorted(set(str(Path(cmd[i + 1]).resolve())
                                          for i, v in enumerate(cmd) if v == '--add-dir')))
        return fingerprint(spec) if channel else None
    except (KeyError, IndexError, ValueError, StopIteration):
        return None


def read_receipt(path):
    if not path.exists():
        return None
    receipt = json.loads(path.read_text())
    if not isinstance(receipt, dict) or receipt.get('status') not in (
            'running', 'complete', 'partial', 'blocked', 'failed', 'interrupted'):
        raise ValueError(f'invalid receipt: {path}')
    return receipt


def has_live_worker(receipt, directory):
    try:
        pid = int(receipt.get('pid'))
        stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return (stat[0] != 'Z' and str(directory).encode() in Path(f'/proc/{pid}/cmdline').read_bytes()
                and (receipt.get('process_start_ticks') is None or receipt['process_start_ticks'] == stat[19]))
    except (OSError, TypeError, ValueError, IndexError):
        return False


def next_attempt(directory):
    numbers = [0]
    for path in directory.iterdir():
        match = re.fullmatch(r'(?:events|prompt|stderr|receipt|result)-(\d+)\.(?:jsonl|txt|log|json)', path.name)
        if match:
            numbers.append(int(match[1]))
    try:
        old = read_receipt(directory / 'receipt.json')
        if old and isinstance(old.get('attempt'), int):
            numbers.append(old['attempt'])
    except (OSError, ValueError):
        pass
    return max(numbers) + 1


def valid_result(value):
    return (isinstance(value, dict) and set(value) == {'status', 'summary', 'artifacts', 'issues'}
            and value['status'] in ('complete', 'partial', 'blocked')
            and isinstance(value['summary'], str)
            and all(isinstance(value[k], list) and all(isinstance(v, str) for v in value[k])
                    for k in ('artifacts', 'issues')))


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
            if k not in ('queued_ids', 'retrying_ids', 'selected_ids', 'running_ids')}


async def stop_worker(proc):
    """Terminate this attempt's process group, also on setup/stream exceptions."""
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(proc.wait(), TERMINATE_GRACE_SECONDS)
    except asyncio.TimeoutError:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    await proc.wait()


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


def format_report(root, jobs, final):
    """Human-readable per-task results; stdout remains the existing JSON stream."""
    labels = {'complete': '已完成', 'partial': '部分完成', 'blocked': '阻塞',
              'failed': '失败', 'interrupted': '已中断', 'not_started': '未启动'}
    if len(jobs) > 20:
        counts = Counter(final['results'].get(j['id'], {}).get('status', 'not_started') for j in jobs)
        totals = '，'.join(f'{labels.get(s, s)} {n}' for s, n in counts.items())
        return (f'\n批次结束：{totals}\n复用 {len(final.get("reused", []))} 项。'
                f'\n简报：{final.get("report_path", final["summary_path"])}'
                f'\n详细结果：{final["summary_path"]}')
    reused = set(final.get('reused', []))
    rows, counts = [], {}

    def clean(value):
        return ' '.join(''.join(c if not unicodedata.category(c).startswith('C')
                               else ' ' for c in str(value)).split())

    def width(value):
        return sum(0 if unicodedata.combining(c) else
                   2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in value)

    for job in jobs:
        task_id = job['id']
        receipt = final['results'].get(task_id, {})
        status = receipt.get('status', 'not_started')
        counts[status] = counts.get(status, 0) + 1
        reasons = []
        if status in ('failed', 'interrupted'):
            reasons = [receipt[k] for k in ('last_error', 'receipt_error', 'cleanup_error', 'log_error')
                       if receipt.get(k)]
        elif status in ('partial', 'blocked'):
            try:
                result = json.loads((root / 'workers' / task_id /
                                     receipt.get('result_file', 'result.json')).read_text())
                if isinstance(result, dict):
                    if isinstance(result.get('summary'), str):
                        reasons.append(result['summary'])
                    if isinstance(result.get('issues'), list):
                        reasons.extend(v for v in result['issues'] if isinstance(v, str))
            except (OSError, ValueError, TypeError):
                reasons.append('结果文件不可读取，详见任务回执')
        elif status == 'not_started':
            reasons = ['本轮未启动']
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

    totals = '，'.join(f'{labels.get(s, s)} {n}' for s, n in counts.items()) or '无任务'
    output = ['', f'批次结束：{totals}', '', line(header)]
    for row in rows:
        reason_lines = textwrap.wrap(row[-1], width=60) or ['—']
        output.append(line([*row[:-1], reason_lines[0]]))
        output.extend(line([''] * (len(header)-1) + [part]) for part in reason_lines[1:])
    output += ['', '来源为“历史”表示本轮未重跑，PID 和退出码来自之前的执行。' if reused else '',
               f'详细结果：{final["summary_path"]}']
    return '\n'.join(output)


class Batch:
    def __init__(self, root, args, jobs, lock_fd):
        self.root, self.args, self.jobs, self.lock_fd = root, args, jobs, lock_fd
        self.run_dir = root / 'runs' / str(time.time_ns())
        self.run_dir.mkdir(parents=True)
        self.running, self.results = {}, {}
        self.reused = set()
        self.queued = {job['id'] for job in jobs}
        self.retrying = {}
        self.retries = Counter()
        self.inflight = {}
        self.status_interval = getattr(args, 'status_interval_seconds', 2.0)
        self.last_publish = -float('inf')
        self.waiting_for_memory = False
        self.process_start_ticks = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]
        self.peak_running = self.peak_turns = 0
        self.started = time.monotonic()
        self.status_path = root / ('status-' + Path(args.manifest).stem + '.json')
        write_json(self.run_dir / 'config.json', {'started_utc': stamp(), 'pid': os.getpid(), **vars(args),
                   'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   'jobs': [j['id'] for j in jobs]})

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
                        process_start_ticks=self.process_start_ticks,
                        manifest=self.args.manifest, state=state, total=len(self.jobs),
                        elapsed_seconds=round(now - self.started, 2),
                        pending=len(self.queued) + len(self.retrying), running=len(running_ids),
                        active_model_turns=self.active_turns(), counts=counts,
                        peak_workers=self.peak_running, peak_active_turns=self.peak_turns,
                        automatic_retries=sum(self.retries.values()),
                        mem_available_kib=available_kib(),
                        waiting_for_memory=state == 'running' and self.waiting_for_memory,
                        running_ids=running_ids, queued_ids=sorted(self.queued),
                        retrying_ids=sorted(self.retrying), selected_ids=[j['id'] for j in self.jobs],
                        **self.counts(), **process_snapshot([r['pid'] for r in self.running.values()]), **extra)
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
        attempt = receipt.get('attempt')
        receipt_path = directory / f'receipt-{attempt:02d}.json' if type(attempt) is int and attempt > 0 else None
        if receipt and (receipt_path is None or not receipt_path.is_file()):
            # Legacy/setup-failure receipts may lack an immutable attempt file.
            receipt_path = self.run_dir / 'receipts' / f'{task_id}.json'
            receipt_path.parent.mkdir(exist_ok=True)
            write_json(receipt_path, receipt)
        row = dict(id=task_id, title=job['title'], status=receipt.get('status', 'not_started'),
                   summary='', artifacts=[], issues=[], attempt=receipt.get('attempt'),
                   reused=task_id in self.reused, receipt_path=str(receipt_path) if receipt else None)
        if receipt.get('status') in ('complete', 'partial', 'blocked'):
            try:
                name = receipt.get('result_file', 'result.json')
                if not isinstance(name, str) or Path(name).name != name:
                    raise ValueError('invalid result filename in receipt')
                path = directory / name
                result = json.loads(path.read_text())
                if not valid_result(result) or result['status'] != receipt['status']:
                    raise ValueError('saved result does not match receipt')
                if name == 'result.json':
                    path = self.run_dir / 'legacy-results' / f'{task_id}.json'
                    path.parent.mkdir(exist_ok=True)
                    write_json(path, result)
                row.update(result, result_path=str(path))
            except (OSError, ValueError) as exc:
                row['issues'].append(f'result unavailable: {exc}')
        row['issues'].extend(str(receipt[k]) for k in ('last_error', 'receipt_error', 'cleanup_error', 'log_error')
                             if receipt.get(k) and receipt.get('status') != 'complete')
        if row['status'] == 'not_started':
            row['issues'].append('not started in this invocation')
        return row

    def save_summary(self, state):
        """Full evidence stays on disk; the controller reads one bounded report."""
        result_index = self.run_dir / 'results.jsonl'
        attention_path = self.run_dir / 'attention.jsonl'
        counts, preview, attention_count = Counter(), [], 0
        with result_index.open('w') as results, attention_path.open('w') as attention:
            for job in self.jobs:
                row = self.result_row(job)
                counts[row['status']] += 1
                line = json.dumps(row, ensure_ascii=False) + '\n'
                results.write(line)
                if row['status'] != 'complete' or row['issues']:
                    attention.write(line)
                    attention_count += 1
                    if len(preview) < 20:
                        preview.append(dict(id=row['id'], status=row['status'], summary=row['summary'][:500],
                                            issues=[v[:300] for v in row['issues'][:3]]))
        report = dict(run_id=self.run_dir.name, state=state, completed_utc=stamp(), total=len(self.jobs),
                      ok=state == 'complete' and attention_count == 0, counts=dict(counts),
                      elapsed_seconds=round(time.monotonic() - self.started, 2),
                      peak_workers=self.peak_running, peak_active_turns=self.peak_turns,
                      reused_count=len(self.reused), automatic_retries=sum(self.retries.values()),
                      attention_count=attention_count, attention_preview=preview,
                      results_path=str(result_index), attention_path=str(attention_path),
                      summary_path=str(self.run_dir / 'summary.json'), report_path=str(self.run_dir / 'report.json'))
        final = dict(**report, results=self.results,
                     unstarted=[j['id'] for j in self.jobs if j['id'] not in self.results], reused=sorted(self.reused))
        write_json(self.run_dir / 'summary.json', final)
        write_json(self.run_dir / 'report.json', report)
        return final

    async def worker(self, job, force=False):
        task_id = job['id']
        if force:
            self.results.pop(task_id, None)
        directory = self.root / 'workers' / task_id
        prompt = job['_common'] + job['_suffix']
        proc = err = result_path = None
        attempt = None
        receipt = dict(task_id=task_id, title=job['title'], model_requested=job['model'],
                       reasoning_effort_requested=job['effort'], input_fingerprint=job['_fingerprint'],
                       start_utc=stamp(), status='failed', thread_id=None,
                       turn_started_utc=None, turn_completed_utc=None, turn_failed_utc=None, usage=None)
        cancelled = False
        try:
            directory.mkdir(parents=True, exist_ok=True)
            old = read_receipt(directory / 'receipt.json')
            if old and not force and (old['status'] == 'complete' or
                        (old['status'] != 'running' and not self.args.retry_failed)):
                if old['status'] == 'complete':
                    saved = json.loads((directory / old.get('result_file', 'result.json')).read_text())
                    if not valid_result(saved) or saved['status'] != 'complete':
                        raise ValueError('saved completed result is missing or invalid')
                self.results[task_id] = old
                self.reused.add(task_id)
                return
            attempt = next_attempt(directory)
            receipt['attempt'] = attempt
            result_path = directory / f'result-{attempt:02d}.json'
            receipt['result_file'] = result_path.name
            receipt['prompt_sha256'] = job['_spec']['prompt_sha256']
            # Preserve pre-upgrade receipts/results before refreshing the compatibility files.
            if old and isinstance(old.get('attempt'), int):
                for name in ('receipt', 'result'):
                    source = directory / f'{name}.json'
                    archive = directory / f'{name}-{old["attempt"]:02d}.json'
                    if source.exists() and not archive.exists():
                        archive.write_bytes(source.read_bytes())
            (directory / 'result.json').unlink(missing_ok=True)
            (directory / f'prompt-{attempt:02d}.txt').write_text(prompt)
            cmd = [str(SCRIPTS / 'codexexec'), '--' + self.args.channel, '--ignore-user-config',
                   '-c', 'approval_policy="never"', '--skip-git-repo-check',
                   '--disable', 'apps', '--disable', 'multi_agent', '--disable', 'memories',
                   '--disable', 'browser_use', '--disable', 'computer_use', '-s', 'workspace-write',
                   '-m', job['model'], '-c', 'model_reasoning_effort=' + json.dumps(job['effort']),
                   '-c', 'web_search=' + json.dumps(job.get('web_search', 'disabled')),
                   '-C', str(directory), '--json', '--output-schema',
                   str(SCRIPTS.parent / 'references/result-schema.json'), '-o', str(result_path)]
            for writable in job['_spec']['write_dirs']:
                cmd += ['--add-dir', writable]
            cmd.append('-')
            receipt.update(command=cmd, status='running')
            self.save_receipt(directory, receipt)
            env = os.environ.copy()
            env.update(OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1',
                       NUMEXPR_NUM_THREADS='1', PYTHONDONTWRITEBYTECODE='1', TOKENIZERS_PARALLELISM='false')
            err = (directory / f'stderr-{attempt:02d}.log').open('wb')
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=err,
                env=env, start_new_session=True, pass_fds=(self.lock_fd,), limit=16*1024*1024)
            receipt['pid'] = proc.pid
            self.running[task_id] = receipt
            self.peak_running = max(self.peak_running, len(self.running))
            self.save_receipt(directory, receipt)
            self.record(event='worker_started', task_id=task_id, pid=proc.pid)

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

            code = await asyncio.wait_for(exchange(), self.args.timeout_seconds)
            receipt['exit_code'] = code
            if code != 0 or not receipt['turn_completed_utc'] or receipt['turn_failed_utc']:
                receipt['retryable'] = bool(receipt.get('transient_error'))
                raise ValueError(receipt.get('last_error') or f'worker did not complete successfully (exit {code})')
            result = json.loads(result_path.read_text())
            if not valid_result(result):
                raise ValueError('invalid result schema for this attempt')
            receipt['status'] = result['status']
            write_json(directory / 'result.json', result)
        except asyncio.CancelledError:
            cancelled = True
            receipt.update(status='interrupted', last_error='dispatcher interrupted')
        except Exception as exc:
            receipt.update(status='failed', last_error='worker deadline exceeded' if isinstance(exc, asyncio.TimeoutError)
                           else f'{type(exc).__name__}: {exc}')
        finally:
            # A reused receipt has already been accounted for; never rewrite its history.
            if task_id not in self.results:
                if proc is not None and receipt['status'] in ('failed', 'interrupted'):
                    try:
                        _, cleanup_cancelled = await finish_cleanup(stop_worker(proc))
                        if cleanup_cancelled:
                            cancelled = True
                            receipt.update(status='interrupted', last_error='dispatcher interrupted during cleanup')
                    except Exception as exc:
                        receipt['cleanup_error'] = str(exc)
                    receipt['exit_code'] = proc.returncode
                if err is not None:
                    try:
                        err.close()
                    except OSError as exc:
                        receipt.update(status='failed', last_error=f'closing worker log: {exc}')
                receipt['end_utc'] = stamp()
                try:
                    # Setup/read failures still get their own attempt, not an old receipt overwrite.
                    if attempt is None:
                        attempt = next_attempt(directory)
                        receipt['attempt'] = attempt
                    if result_path is not None and receipt['status'] in ('failed', 'interrupted'):
                        (directory / 'result.json').unlink(missing_ok=True)
                    self.save_receipt(directory, receipt)
                except Exception as exc:
                    receipt.update(status='failed', receipt_error=str(exc))
                self.results[task_id] = receipt
                self.running.pop(task_id, None)
                try:
                    self.record(event='worker_finished', task_id=task_id, status=receipt['status'])
                except Exception as exc:
                    receipt['log_error'] = str(exc)
            if cancelled:
                raise asyncio.CancelledError

    @staticmethod
    def save_receipt(directory, receipt):
        write_json(directory / f'receipt-{receipt["attempt"]:02d}.json', receipt)
        write_json(directory / 'receipt.json', receipt)

    async def run(self):
        # Reuse needs no subprocess or memory headroom. Keep it ahead of new work.
        pending = deque((j, False) for j in sorted(self.jobs, key=lambda j: not j.get('_reuse', False)))
        delayed = []
        sequence = 0
        state = 'interrupted'
        try:
            while pending or self.inflight or delayed:
                now = time.monotonic()
                while delayed and delayed[0][0] <= now:
                    _, _, job = heapq.heappop(delayed)
                    pending.append((job, True))
                elapsed = time.monotonic() - self.started
                capacity = min(self.args.concurrency, 4 if elapsed < self.args.ramp_seconds else
                               12 if elapsed < 2*self.args.ramp_seconds else self.args.concurrency)
                self.waiting_for_memory = False
                while pending and len(self.inflight) < capacity:
                    job, force = pending[0]
                    if not job.get('_reuse') and available_kib() < 1536*1024:
                        self.waiting_for_memory = True
                        break
                    pending.popleft()
                    self.queued.discard(job['id'])
                    self.retrying.pop(job['id'], None)
                    self.inflight[asyncio.create_task(self.worker(job, force))] = job
                self.publish()
                wait = max(.01, self.status_interval - (time.monotonic() - self.last_publish))
                if delayed:
                    wait = min(wait, max(.01, delayed[0][0] - time.monotonic()))
                if self.inflight:
                    done, _ = await asyncio.wait(self.inflight, timeout=wait, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        job = self.inflight.pop(task)
                        task.result()
                        task_id = job['id']
                        receipt = self.results[task_id]
                        if (task_id not in self.reused and receipt['status'] == 'failed'
                                and receipt.get('retryable') and not receipt.get('cleanup_error')
                                and not receipt.get('receipt_error')
                                and (job.get('retry_safe', False) or not receipt.get('tools_observed'))
                                and self.retries[task_id] < getattr(self.args, 'max_retries', 2)):
                            self.retries[task_id] += 1
                            delay = min(120, getattr(self.args, 'retry_backoff_seconds', 30)
                                        * 2 ** (self.retries[task_id] - 1) * random.uniform(.8, 1.2))
                            ready = time.monotonic() + delay
                            self.retrying[task_id] = ready
                            sequence += 1
                            heapq.heappush(delayed, (ready, sequence, job))
                            self.record(event='retry_scheduled', task_id=task_id,
                                        retry=self.retries[task_id], delay_seconds=round(delay, 2))
                elif pending or delayed:
                    await asyncio.sleep(wait)
            state = 'complete'
        finally:
            for task in self.inflight:
                task.cancel()
            _, cleanup_cancelled = await finish_cleanup(asyncio.gather(*self.inflight, return_exceptions=True))
            if cleanup_cancelled:
                state = 'interrupted'
            self.inflight.clear()
            self.retrying.clear()
            self.queued = {j['id'] for j in self.jobs if j['id'] not in self.results}
            final = self.save_summary(state)
            self.publish(state, summary_path=final['summary_path'], report_path=final['report_path'],
                         attention_count=final['attention_count'], ok=final['ok'])
            # A closed output pipe must not turn a saved batch result into a failure.
            try:
                print(format_report(self.root, self.jobs, final), file=sys.stderr, flush=True)
            except (OSError, ValueError):
                pass


def prepare_jobs(root, args):
    jobs = load_jobs(root, args)
    common = (root / 'COMMON.md').read_text()
    # Also detect workers left by a pre-lock dispatcher (including other manifests).
    for path in (root / 'workers').glob('*/receipt.json'):
        try:
            old = read_receipt(path)
        except (OSError, ValueError):
            continue  # A damaged task receipt is handled inside its worker boundary.
        if old and has_live_worker(old, path.parent):
            raise ConfigurationError(f'task still has a live worker: {path.parent.name}')
    for job in jobs:
        directory = root / 'workers' / job['id']
        # Share COMMON in memory instead of storing a full copy for every queued job.
        job['_common'] = common
        job['_suffix'] = '\n\n' + job['prompt'] + '\n\nYour assigned writable directory: ' + str(directory) + '\n'
        job['_spec'] = task_spec(job, common + job['_suffix'], args.channel)
        job['_fingerprint'] = fingerprint(job['_spec'])
        try:
            old = read_receipt(directory / 'receipt.json')
        except (OSError, ValueError):
            continue
        if old:
            previous = old.get('input_fingerprint') or legacy_fingerprint(old)
            if previous != job['_fingerprint']:
                raise ConfigurationError(f'task {job["id"]}: inputs/configuration changed or old inputs cannot be verified; '
                                         'use a new task ID or run directory (no tasks launched)')
            job['_reuse'] = old['status'] == 'complete' or (old['status'] != 'running' and not args.retry_failed)
    return jobs


async def main():
    ap = argparse.ArgumentParser(description='Run a batch of independent Codex tasks.')
    ap.add_argument('--run-dir', required=True)
    action = ap.add_mutually_exclusive_group()
    action.add_argument('--init', action='store_true')
    action.add_argument('--status', action='store_true', help='Read compact current status without scanning workers')
    action.add_argument('--report', action='store_true', help='Read the bounded final report, or current status if running')
    action.add_argument('--results', action='store_true', help='Read a page of final task results')
    ap.add_argument('--attention', action='store_true', help='With --results, show only tasks needing attention')
    ap.add_argument('--offset', type=int, default=0)
    ap.add_argument('--limit', type=int, default=20, help='Result page size (1–100)')
    ap.add_argument('--channel', choices=('sub', 'api'), default='sub')
    ap.add_argument('--manifest', default='tasks.json')
    ap.add_argument('--concurrency', type=int, default=8)
    ap.add_argument('--ramp-seconds', type=int, default=20)
    ap.add_argument('--timeout-seconds', type=int, default=1200)
    ap.add_argument('--only', nargs='*')
    ap.add_argument('--retry-failed', action='store_true')
    ap.add_argument('--max-retries', type=int, default=2, help='Automatic retries per task for transient failures (0 disables)')
    ap.add_argument('--retry-backoff-seconds', type=float, default=30, help='Initial automatic retry delay; exponential jitter, capped at 120s')
    ap.add_argument('--status-interval-seconds', type=float, default=2, help='Status/resource sampling interval (at least 0.1s)')
    ap.add_argument('--progress', action='store_true', help='Also emit periodic status JSON to stdout (default: start/end only)')
    ap.add_argument('--no-dashboard', action='store_true', help='Do not launch the optional terminal workflow monitor')
    args = ap.parse_args()
    if args.concurrency < 1 or args.timeout_seconds < 1 or args.ramp_seconds < 0:
        ap.error('Concurrency and timeout must be positive; ramp must be nonnegative')
    if (not 0 <= args.max_retries <= 10 or not 0 <= args.retry_backoff_seconds <= 120
            or not .1 <= args.status_interval_seconds <= 60):
        ap.error('retries must be 0–10, backoff 0–120s, status interval 0.1–60s')
    if args.offset < 0 or not 1 <= args.limit <= 100 or (args.attention and not args.results):
        ap.error('--results accepts --offset >= 0, --limit 1–100, and optional --attention')
    root = Path(args.run_dir).expanduser().resolve()
    if args.init:
        root.mkdir(parents=True, exist_ok=False)
        (root / 'COMMON.md').write_text('Shared context and task boundaries.\n')
        (root / 'tasks.json').write_text('[]\n')
        print(root)
        return
    if args.status or args.report or args.results:
        status = json.loads((root / ('status-' + Path(args.manifest).stem + '.json')).read_text())
        if args.status or (args.report and status.get('state') == 'running'):
            result = compact_status(status)
            result['scheduler_live'] = has_live_worker(status, 'dispatch.py')
        else:
            if status.get('state') == 'running':
                raise ConfigurationError('this invocation is still running; use --status or --report')
            if not status.get('report_path'):
                raise ConfigurationError('this older run has no compact report; see its summary_path or rerun to index saved results')
            report = json.loads(Path(status['report_path']).read_text())
            result = report
            if args.results:
                total = report['attention_count'] if args.attention else report['total']
                path = report['attention_path'] if args.attention else report['results_path']
                items = []
                with Path(path).open() as stream:
                    for index, line in enumerate(stream):
                        if index < args.offset:
                            continue
                        if len(items) >= args.limit:
                            break
                        items.append(json.loads(line))
                next_offset = args.offset + len(items)
                result = dict(total=total, offset=args.offset, limit=args.limit, items=items,
                              next_offset=next_offset if next_offset < total else None, results_path=path)
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
        remember_run(root, args.manifest)
        if not args.no_dashboard:
            launch_dashboard(root, args.manifest)
        loop = asyncio.get_running_loop()
        main_task = asyncio.current_task()
        loop.add_signal_handler(signal.SIGTERM, main_task.cancel)
        try:
            await batch.run()
        finally:
            loop.remove_signal_handler(signal.SIGTERM)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (ConfigurationError, OSError, ValueError) as exc:
        sys.exit(f'dispatch: {exc}')
