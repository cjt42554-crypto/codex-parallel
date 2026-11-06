#!/usr/bin/env python3
"""Independent read-only terminal workflow monitor for Codex EXEC."""
from __future__ import annotations
import argparse
from collections import Counter, OrderedDict
from contextlib import contextmanager
import curses
from datetime import datetime
import fcntl
import hashlib
import json
import locale
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import unicodedata

def read_json(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def live(pid, marker):
    try:
        stat = Path(f'/proc/{int(pid)}/stat').read_text().rsplit(')', 1)[1].split()
        return stat[0] != 'Z' and marker.encode() in Path(f'/proc/{int(pid)}/cmdline').read_bytes()
    except (OSError, ValueError, TypeError, IndexError):
        return False


def file_version(path):
    try:
        stat = path.stat()
    except OSError:
        return None
    return stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


class FileCache:
    """Reload changed files, including atomic replacements and missing files."""

    def __init__(self, limit=None):
        self.entries = OrderedDict()
        self.limit = limit

    def get(self, path, reader=None, *, attempt=None):
        key = path if attempt is None else (path, attempt)
        version = file_version(path)
        if key in self.entries:
            saved_version, value = self.entries.pop(key)
            if saved_version == version:
                self.entries[key] = saved_version, value
                return value
        value = (reader or read_json)(path) if version is not None else None
        self.entries[key] = version, value
        if self.limit is not None and len(self.entries) > self.limit:
            self.entries.popitem(last=False)
        return value


def read_text(path):
    try:
        return path.read_text()
    except (OSError, ValueError):
        return None


def as_dict(value):
    return value if isinstance(value, dict) else {}


def id_set(value):
    return {item for item in value if isinstance(item, str)} if isinstance(value, list) else set()


def aggregate_path(root, manifest):
    return root / f'status-{Path(manifest).stem}.json'


def aggregate(cache, root, manifest, reader=None):
    data = as_dict(cache.get(aggregate_path(root, manifest), reader))
    if data.get('manifest') == manifest and data.get('state') in ('running', 'complete', 'interrupted'):
        return data
    return None


def manifest_jobs(value):
    defaults = {}
    if isinstance(value, dict):
        defaults = {k: v for k, v in as_dict(value.get('defaults')).items()
                    if k in ('model', 'effort', 'web_search', 'write_dirs', 'retry_safe')}
        value = value.get('tasks')
    if not isinstance(value, list):
        raise ValueError('任务清单暂不可读，稍后自动重试')
    if any(not isinstance(job, dict) or 'id' not in job for job in value):
        raise ValueError('任务清单包含无效任务')
    return [{**defaults, **job} for job in value]


def attempt_number(value):
    return value if type(value) is int and value > 0 else 0


RESULT_FACTS = ('attempt_status', 'execution', 'reported_status', 'result_observation',
                'recovery_state', 'reason')
RESULT_FIELDS = ('title', 'status', 'attempt', 'receipt_path', 'result_path', 'issues',
                 'needs_attention', *RESULT_FACTS)
RESULT_STATUSES = ('complete', 'partial', 'blocked')


def diagnostics(evidence, receipt, extra=()):
    facts = {k: evidence.get(k, receipt.get(k)) for k in RESULT_FACTS}
    issues = []
    for source in (evidence, receipt):
        if isinstance(source.get('issues'), list):
            issues.extend(v for v in source['issues'] if isinstance(v, str) and v)
    issues.extend(str(receipt[k]) for k in ('last_error', 'receipt_error', 'cleanup_error', 'log_error', 'projection_error')
                  if receipt.get(k))
    issues.extend(extra)
    if facts['result_observation'] in ('unavailable', 'invalid'):
        issues.append('结果证据缺失' if facts['result_observation'] == 'unavailable' else '结果证据无效')
    elif facts['execution'] == 'succeeded' and facts['reported_status'] == 'unknown':
        issues.append('执行已成功，结果状态待核实')
    if facts['reason'] and not issues:
        issues.append(str(facts['reason']))
    return dict(**facts, issues=list(dict.fromkeys(issues)),
                needs_attention=bool(issues or evidence.get('needs_attention') or receipt.get('needs_attention')))


class ResultIndex:
    """Keep the metadata from completed rows, reading only new lines on refresh."""

    def __init__(self):
        self.path = self.version = None
        self.position = 0
        self.rows = {}

    def refresh(self, run, terminal):
        path = run / 'result-events.jsonl'
        version = file_version(path)
        if version is None and terminal:
            path = run / 'results.jsonl'
            version = file_version(path)
        if (path, version) == (self.path, self.version):
            return self.rows
        previous = self.version
        if (path != self.path or version is None or previous is None or
                version[0] != previous[0] or version[1] <= previous[1] or version[1] < self.position):
            self.rows.clear()
            self.position = 0
        self.path, self.version = path, version
        if version is None:
            return self.rows
        try:
            with path.open('rb') as stream:
                stream.seek(self.position)
                while line := stream.readline():
                    if path.name == 'result-events.jsonl' and not line.endswith(b'\n'):
                        break  # A concurrently appended line may still be incomplete.
                    self.position = stream.tell()
                    try:
                        row = as_dict(json.loads(line))
                    except ValueError:
                        continue
                    task_id = row.get('id')
                    if isinstance(task_id, str) and isinstance(row.get('status'), str):
                        self.rows[task_id] = {k: row[k] for k in RESULT_FIELDS if k in row}
        except OSError:
            self.version = None  # Retry on a transient read failure.
        return self.rows


def binding_files(directory):
    try:
        # The dispatcher replaces each binding atomically, changing the directory version.
        return {p.stem for p in directory.iterdir() if p.suffix == '.json'}
    except OSError:
        return None


class BatchView:
    def __init__(self, root, manifest):
        self.root, self.manifest = root.resolve(), manifest
        self.files = FileCache(limit=4)
        self.receipts = FileCache()
        self.details = FileCache(limit=8)
        self.bindings = FileCache()
        self.index = ResultIndex()
        self.manifest_source = None
        self.jobs = {}
        self.legacy = None
        self.next_legacy = 0
        self.binding_run = None

    def task_sources(self, *, allow_missing=False):
        source = self.files.get(self.root / self.manifest)
        if source is None or self.manifest_source is not source:
            try:
                self.jobs = {str(job['id']): job for job in manifest_jobs(source)}
            except ValueError:
                if not allow_missing:
                    raise
                self.jobs = {}
            self.manifest_source = source
            self.receipts.entries.clear()
        return self.jobs

    def legacy_config(self):
        if time.monotonic() >= self.next_legacy:
            self.legacy = None
            for path in sorted((self.root / 'runs').glob('*/config.json'), reverse=True):
                config = as_dict(read_json(path))
                if config and config.get('manifest', 'tasks.json') == self.manifest:
                    self.legacy = config, path.parent
                    break
            self.next_legacy = time.monotonic() + 3
        return self.legacy or ({}, None)

    def invocation(self, data):
        run_id = data.get('run_id')
        batch_id = data.get('batch_id')
        if not isinstance(run_id, str) or not run_id.isascii() or not run_id.isdecimal():
            if batch_id is not None:
                raise ValueError('批次快照缺少有效的运行编号')
            return None
        path = self.root / 'runs' / run_id
        if batch_id is not None and batch_id != str(path):
            raise ValueError('批次快照与运行目录不匹配')
        if batch_id is not None or path.is_dir():
            return path
        return None

    def attempt_file(self, run, directory, task_id, attempt, reference, kind):
        """Accept only evidence files in this invocation or this numbered attempt."""
        stored = run / ('receipts' if kind == 'receipt' else 'legacy-results') / f'{task_id}.json'
        numbered = directory / f'{kind}-{attempt:02d}.json'
        if reference == str(stored) and not stored.parent.is_symlink() and not stored.is_symlink():
            return stored
        if (attempt and reference in (None, str(numbered)) and not directory.is_symlink() and
                not numbered.is_symlink()):
            return numbered
        return None

    def attempt_receipt(self, run, directory, task_id, attempt, source, cache):
        path = self.attempt_file(run, directory, task_id, attempt, source.get('receipt_path'), 'receipt')
        receipt = as_dict(cache.get(path)) if path is not None else {}
        if attempt_number(receipt.get('attempt')) == attempt:
            return receipt
        return {}

    def rows(self, *, use_aggregate=True):
        current = aggregate(self.files, self.root, self.manifest) if use_aggregate else None
        config, run = {}, self.invocation(current or {})
        if current is None:
            config, candidate = self.legacy_config()
            if candidate is not None:
                status = as_dict(self.files.get(candidate / 'status.json'))
                if status.get('state') in ('running', 'complete', 'interrupted'):
                    current, run = status, candidate
                elif (self.invocation(config) is not None or
                      any((candidate / name).is_file() for name in ('summary.json', 'results.jsonl', 'result-events.jsonl'))):
                    run = candidate
        if run is not None and (run.parent.is_symlink() or run.is_symlink()):
            raise ValueError('批次目录不能是符号链接')
        if run is not None and not config:
            config = as_dict(self.files.get(run / 'config.json'))
        if self.binding_run != run:
            self.bindings.entries.clear()
            self.receipts.entries.clear()
            self.binding_run = run
        state = (current or {}).get('state')
        scheduler_live = (state in (None, 'running') and live((current or config).get('pid'), 'dispatch.py'))
        queued = id_set((current or {}).get('queued_ids'))
        retrying = id_set((current or {}).get('retrying_ids'))
        running = id_set((current or {}).get('running_ids'))
        workers_running = as_dict((current or {}).get('running_workers'))
        selected = (current or {}).get('selected_ids')
        if not isinstance(selected, list):
            selected = config.get('jobs')
        jobs = (as_dict(config.get('job_metadata')) if run is not None else
                self.task_sources(allow_missing=isinstance(selected, list)))
        ids = dict.fromkeys(i for i in selected if isinstance(i, str)) if isinstance(selected, list) else jobs
        final, summary, binding_ids = {}, {}, None
        common_path = None
        if run is not None:
            terminal = state in ('complete', 'interrupted') or (current is None and not scheduler_live)
            final = self.index.refresh(run, terminal)
            if terminal and (not ids or any(i not in final for i in ids)):
                summary = as_dict(self.files.get(run / 'summary.json'))
            if not isinstance(selected, list) and not ids:
                ids = dict.fromkeys((*final, *as_dict(summary.get('results')), *id_set(summary.get('unstarted'))))
            if any(i not in final for i in ids):
                bindings_dir = run / 'bindings'
                if not bindings_dir.is_symlink():
                    binding_ids = self.files.get(bindings_dir, binding_files)
            if config.get('common_prompt_file') == str(run / 'common.md'):
                common_path = run / 'common.md'
        saved_receipts = as_dict(summary.get('results'))
        saved_unstarted = id_set(summary.get('unstarted'))

        rows = []
        workers = self.root / 'workers'
        if workers.is_symlink():
            return scheduler_live, rows
        for task_id in ids:
            if task_id in ('', '.', '..') or Path(task_id).name != task_id:
                continue
            job = as_dict(jobs.get(task_id))
            directory = workers / task_id
            evidence = final.get(task_id, {})
            receipt, binding = {}, {}
            execution = None
            notes = []
            pending = ('retrying' if task_id in retrying else 'queued' if task_id in queued else None)
            if evidence:
                status = evidence['status']
                attempt = 0 if status == 'not_started' else attempt_number(evidence.get('attempt'))
            elif task_id in saved_receipts:
                receipt = as_dict(saved_receipts[task_id])
                status, attempt = receipt.get('status'), attempt_number(receipt.get('attempt'))
            elif task_id in saved_unstarted:
                status, attempt = 'not_started', 0
            elif run is not None:
                worker = as_dict(workers_running.get(task_id))
                if binding_ids is not None and task_id in binding_ids:
                    path = run / 'bindings' / f'{task_id}.json'
                    saved = as_dict(self.bindings.get(path)) if not path.is_symlink() else {}
                    if (saved.get('task_id') == task_id and saved.get('run_id', run.name) == run.name and
                            saved.get('batch_id', str(run)) == str(run) and
                            (attempt_number(saved.get('attempt')) or saved.get('source') == 'reuse')):
                        binding = saved
                    else:
                        notes.append('尝试绑定不可读取')
                attempt = attempt_number((binding or worker).get('attempt'))
                if binding or attempt:
                    candidate = self.attempt_receipt(run, directory, task_id, attempt, binding or worker, self.receipts)
                    if (binding.get('source') == 'reuse' or
                            (candidate.get('run_id', run.name) == run.name and
                             candidate.get('batch_id', str(run)) == str(run))):
                        receipt = candidate
                status = receipt.get('status')
                execution = receipt.get('execution', binding.get('execution'))
                if binding or worker or task_id in running or notes:
                    if (scheduler_live and pending == 'retrying' and
                            (execution in ('failed', 'interrupted') or status in ('failed', 'interrupted'))):
                        status = 'retrying'
                    elif not status:
                        status = ('starting' if scheduler_live and not notes and binding.get('source') != 'reuse'
                                  else 'unknown')
                        if binding:
                            notes.append('本次尝试的回执缺失或无法读取')
                    if not scheduler_live:
                        notes.append('本次运行未写完任务结果索引；显示的是已保存的尝试证据')
                elif scheduler_live:
                    status = pending or ('queued' if current is None else 'unknown')
                elif terminal or binding_ids is None or config.get('attempt_journal_version') != 1:
                    status = 'unknown'
                    notes.append('缺少本次运行的任务记录，无法确认是否已启动')
                else:
                    status = 'not_started'
            else:
                # Only pre-invocation layouts have no safer place to read compatibility receipts.
                if directory.is_symlink():
                    continue
                if scheduler_live and pending:
                    status, attempt = pending, 0
                else:
                    receipt = as_dict(self.receipts.get(directory / 'receipt.json'))
                    status, attempt = receipt.get('status'), attempt_number(receipt.get('attempt'))
                    if not status and scheduler_live and current is None and isinstance(selected, list):
                        status = 'queued'
            if status == 'running':
                if execution == 'prepared' and run is not None:
                    status = 'starting' if scheduler_live else 'unknown'
                elif not live(receipt.get('pid'), str(directory)):
                    status = 'unknown' if run is not None and not scheduler_live else 'interrupted'
                elif not receipt.get('turn_started_utc'):
                    status = 'starting'
            if not isinstance(status, str) or not status:
                status = 'not_started'
            rows.append(dict(id=task_id, title=evidence.get('title', receipt.get('title', job.get('title', task_id))),
                             status=status, model=receipt.get('model_requested', job.get('model')),
                             effort=receipt.get('reasoning_effort_requested', job.get('effort')),
                             attempt=attempt, start=receipt.get('start_utc'),
                             end=receipt.get('end_utc'), prompt=None, prompt_sent=False, answer=None,
                             **diagnostics(evidence or {**binding, **receipt}, receipt, notes),
                             _directory=directory, _receipt=receipt, _job=job, _run=run, _evidence=evidence,
                             _common_path=common_path))
        return scheduler_live, rows

    def final_receipt(self, row):
        if row['_receipt'] or not row['_evidence'] or row['status'] == 'not_started':
            return row['_receipt']
        return self.attempt_receipt(row['_run'], row['_directory'], row['id'], row['attempt'],
                                    row['_evidence'], self.details)

    def detail(self, row, *, include_prompts=True):
        receipt, directory, run = self.final_receipt(row), row['_directory'], row['_run']
        attempt, status = row['attempt'], row['status']
        detail = dict(model=receipt.get('model_requested', row['model']),
                      effort=receipt.get('reasoning_effort_requested', row['effort']),
                      start=receipt.get('start_utc'), end=receipt.get('end_utc'))
        notes = []
        if run is not None and attempt and row['_evidence'] and not receipt:
            notes.append('本次尝试的回执缺失或无法读取')
        # Final index facts win over a receipt; otherwise use the bound receipt to fill in absent facts.
        facts = {k: row[k] for k in RESULT_FIELDS if k in row and row[k] is not None}
        if not include_prompts:
            return dict(**detail, **diagnostics(facts, receipt, notes))
        path = directory / f'prompt-{attempt:02d}.txt'
        prompt = (self.details.get(path, read_text)
                  if attempt and not directory.is_symlink() and not path.is_symlink() else None)
        sent = prompt is not None and row['execution'] != 'prepared'
        if prompt is None:
            path = self.root / 'COMMON.md' if run is None else row['_common_path']
            common = self.details.get(path, read_text) if path is not None and not path.is_symlink() else None
            task_prompt = row['_job'].get('prompt')
            if run is None:
                common, task_prompt = common or '', task_prompt or ''
            else:
                common = common if common is not None else '〔本次运行未保存共享提示词〕'
                task_prompt = task_prompt if isinstance(task_prompt, str) else '〔本次运行未保存任务提示词〕'
            prompt = f'{common}\n\n{task_prompt}\n\nYour assigned writable directory: {directory}\n'
        answer = None
        if status in RESULT_STATUSES:
            result_file = receipt.get('result_file', 'result.json')
            path = None
            if run is None:
                if isinstance(result_file, str) and Path(result_file).name == result_file:
                    path = directory / result_file
            else:
                reference = row['_evidence'].get('result_path')
                stored = run / 'legacy-results' / f'{row["id"]}.json'
                if reference is None and result_file == 'result.json' and not stored.parent.is_symlink() and stored.is_file():
                    reference = str(stored)
                if reference is not None or result_file in ('result.json', f'result-{attempt:02d}.json'):
                    path = self.attempt_file(run, directory, row['id'], attempt, reference, 'result')
            result = (as_dict(self.details.get(path, attempt=attempt))
                      if path is not None and not path.is_symlink() else {})
            if run is not None and (result.get('status', status) != status or
                                    not isinstance(result.get('summary'), str)):
                result = {}
            if run is not None and not result:
                notes.append('本次尝试的结果文件缺失或无法验证')
            if run is not None and isinstance(result.get('issues'), list):
                notes.extend(issue for issue in result['issues'] if isinstance(issue, str) and issue)
            if run is None and status == 'partial' and result.get('status') == 'blocked':
                status = 'blocked'
            if (status == 'complete' or row['_evidence'].get('result_path') or
                    (receipt.get('exit_code') == 0 and receipt.get('turn_completed_utc'))):
                paragraphs = [result.get('summary')]
                if run is None and isinstance(result.get('issues'), list):
                    paragraphs.extend(result['issues'])
                answer = '\n\n'.join(p.strip() for p in paragraphs if isinstance(p, str) and p.strip()) or None
        return dict(**detail, **diagnostics(facts, receipt, notes), status=status, prompt=prompt,
                    prompt_sent=sent, answer=answer)


def snapshot(root, manifest, include_prompts=True):
    view = BatchView(root, manifest)
    scheduler_live, rows = view.rows()
    for row in rows:
        row.update(view.detail(row, include_prompts=include_prompts))
        for key in ('_directory', '_receipt', '_job', '_run', '_evidence', '_common_path'):
            row.pop(key)
    return dict(root=str(root), manifest=manifest, scheduler_live=scheduler_live,
                updated=time.time(), tasks=rows)


LABELS = {'retrying':'等待重试', 'queued':'排队中', 'not_started':'未启动', 'starting':'启动中',
          'running':'运行中', 'complete':'已完成', 'partial':'部分完成',
          'blocked':'阻塞', 'failed':'失败', 'interrupted':'已中断', 'unknown':'状态待核实'}
ICONS = {'retrying':'↻', 'queued':'○','not_started':'○','starting':'◐','running':'●',
         'complete':'✓','partial':'◑','blocked':'!','failed':'×','interrupted':'−','unknown':'?'}


def clean(text):
    return ''.join(c if c in '\n\t' or not unicodedata.category(c).startswith('C') else '�'
                   for c in str(text)).expandtabs(4)


def width(c):
    return 0 if unicodedata.combining(c) else 2 if unicodedata.east_asian_width(c) in ('W','F') else 1


def clip(text, columns):
    out=[]; size=0
    for c in clean(text).replace('\n',' '):
        n=width(c)
        if size+n>columns: break
        out.append(c);size+=n
    return ''.join(out)


def wrap(text, columns):
    lines=[]
    for line in clean(text).split('\n'):
        buf='';size=0
        for c in line:
            n=width(c)
            if size+n>columns and buf:
                lines.append(buf);buf='';size=0
            buf+=c;size+=n
        lines.append(buf)
    return lines


def elapsed(task):
    if not task.get('start'):
        return '尚未开始' if task['status'] in ('not_started', 'queued') else '启动时间未知'
    if task['status'] in ('interrupted', 'unknown') and not task.get('end'): return '结束时间未知'
    try:
        end=datetime.fromisoformat(task['end']).timestamp() if task.get('end') else time.time()
        seconds=max(0,int(end-datetime.fromisoformat(task['start']).timestamp()))
        return f'{seconds//60:02d}:{seconds%60:02d}'
    except (ValueError,TypeError): return '时间未知'


def ui(screen, root, manifest):
    try: curses.curs_set(0)
    except curses.error: pass
    screen.timeout(200)
    screen.keypad(True)
    color=False
    if curses.has_colors():
        try:
            curses.start_color();curses.use_default_colors()
            for n,c in enumerate((curses.COLOR_CYAN,curses.COLOR_GREEN,curses.COLOR_YELLOW,curses.COLOR_RED),1):
                curses.init_pair(n,c,-1)
            color=True
        except curses.error: pass
    view=BatchView(root,manifest)
    tasks=[]; scheduler_live=False; error=''; selected=0; offset=0; next_read=0
    detail_row=None; detail={}; content=''; layout=None; lines=[]; counts=Counter()
    def put(y,x,text,attr=0,limit=None):
        h,w=screen.getmaxyx()
        if y<0 or y>=h or x<0 or x>=w: return
        try: screen.addstr(y,x,clip(text,min(w-x-1,limit if limit is not None else w)),attr)
        except curses.error: pass
    def tint(status, attention):
        n=4 if status in ('failed','interrupted') else 3 if attention or status in ('blocked','partial') else 2 if status=='complete' else 1
        return curses.color_pair(n) if color else 0
    while True:
        if time.monotonic()>=next_read:
            try:
                scheduler_live,tasks=view.rows()
                counts=Counter(task['status'] for task in tasks)
                error=''
            except Exception as exc: error='读取失败，自动重试：'+str(exc)
            next_read=time.monotonic()+1
        screen.erase();h,w=screen.getmaxyx()
        if h<12 or w<45:
            put(0,0,'请放大终端（至少 45 列 × 12 行）；Esc 返回批次列表')
            screen.refresh()
            key=screen.getch()
            if key==27:return 'back'
            if key==ord('q'):return
            continue
        selected=max(0,min(selected,len(tasks)-1))
        if tasks and tasks[selected] is not detail_row:
            detail_row=tasks[selected]
            detail=view.detail(detail_row)
            content=detail['prompt']
            if detail['answer']:
                content+='\n\n回答：\n'+detail['answer']
            if detail['needs_attention']:
                facts = '执行已成功。' if detail['execution'] == 'succeeded' else ''
                warnings = '\n'.join(detail['issues']) or '此任务有待核实的信息。'
                content=f'告警：{facts}\n{warnings}\n\n{content}'
        put(0,2,'CODEX  /  EXEC WORKFLOW',curses.A_BOLD | (curses.color_pair(1) if color else 0))
        put(1,2,'  ·  '.join(f'{LABELS.get(s,s)} {n}' for s,n in counts.items()) or '等待任务清单')
        put(2,2,str(root),curses.A_DIM)
        put(3,2,error or ('● 调度器运行中 · 每秒刷新' if scheduler_live else '○ 调度器未运行'),curses.A_DIM)
        wide=w>=100; split=min(48,w//3) if wide else w
        list_top=6; list_height=h-8 if wide else min(6,max(2,(h-10)//3))
        put(5,2,'任务 / AGENTS',curses.A_BOLD)
        start=max(0,selected-list_height+1)
        for i,t in enumerate(tasks[start:start+list_height],start):
            active=i==selected
            marker='›' if active else ' '
            shown=detail if active else t
            status=shown['status']; attention=shown['needs_attention']
            icon='!' if attention else ICONS.get(status,'?')
            label=LABELS.get(status,status) + (' · 告警' if attention else '')
            text=f'{marker} {icon} {label}  {t["title"]}'
            put(list_top+i-start,1,text,tint(status,attention) | (curses.A_REVERSE if active else 0),split-2)
        if not tasks:put(list_top,2,'没有任务',curses.A_DIM)
        px=split+2 if wide else 2; py=5 if wide else list_top+list_height+1
        if wide:
            for y in range(5,h-2):put(y,split,'│',curses.A_DIM)
        if tasks:
            t=tasks[selected]
            put(py,px,t['title'],curses.A_BOLD)
            put(py+1,px,f'{detail["model"] or "未记录"} / {detail["effort"] or "未记录"} · 尝试 {t["attempt"]} · {elapsed(detail)}',curses.A_DIM)
            columns=max(1,w-px-2)
            if layout != (columns,content):
                layout=(columns,content)
                lines=wrap(content,columns)
            visible=max(1,h-py-5)
            offset=max(0,min(offset,max(0,len(lines)-visible)))
            for row,line in enumerate(lines[offset:offset+visible]):put(py+2+row,px,line)
            put(h-3,px,f'{offset+1}–{min(offset+visible,len(lines))} / {len(lines)} 行',curses.A_DIM)
        put(h-1,1,'↑↓/jk 切换任务  PgUp/PgDn 翻页  Esc 批次列表  r 刷新  q 退出（任务继续）',curses.A_DIM)
        screen.refresh();key=screen.getch()
        if key==ord('q'):return
        if key==27:return 'back'
        if key in (curses.KEY_DOWN,ord('j')):selected=min(len(tasks)-1,selected+1);offset=0
        elif key in (curses.KEY_UP,ord('k')):selected=max(0,selected-1);offset=0
        elif key==curses.KEY_NPAGE:offset+=max(1,h//3)
        elif key==curses.KEY_PPAGE:offset=max(0,offset-max(1,h//3))
        elif key==curses.KEY_HOME:offset=0
        elif key==ord('r'):next_read=0


def registry_directory():
    from monitor_registry import registry_directory as get_registry_directory
    return get_registry_directory()


def registry_entry(path):
    entry = as_dict(read_json(path))
    manifest = entry.get('manifest', 'tasks.json')
    if not isinstance(entry.get('run_dir'), str) or not isinstance(manifest, str):
        return None
    try:
        root = Path(entry['run_dir']).expanduser().resolve()
    except (OSError, RuntimeError, ValueError):
        return None
    try:
        registered = float(entry.get('registered_at', 0))
    except (ValueError, TypeError):
        registered = 0
    return dict(root=root, manifest=manifest, registered_at=registered, key=(str(root), manifest))


def count(value):
    return value if type(value) is int and value >= 0 else 0


def overview_status(path):
    data = as_dict(read_json(path))
    # The overview never needs the potentially large lists of task IDs.
    counts = {k: count(v) for k, v in as_dict(data.get('counts')).items()}
    if 'running' not in counts and 'starting' not in counts:
        counts['running'] = count(data.get('running'))
    if 'retrying' not in counts:
        retrying = data.get('retrying_ids')
        counts['retrying'] = len(retrying) if isinstance(retrying, list) else 0
    if 'queued' not in counts:
        counts['queued'] = max(0, count(data.get('pending')) - counts['retrying'])
    return dict(manifest=data.get('manifest'), state=data.get('state'), pid=data.get('pid'),
                counts=counts, total=count(data.get('total', sum(counts.values()))),
                waiting_for_memory=bool(data.get('waiting_for_memory')),
                waiting_for_service=bool(data.get('waiting_for_service')),
                quiet_workers=len(id_set(data.get('stalled_ids'))))


class Overview:
    def __init__(self):
        self.files = FileCache()
        self.legacy_views = {}
        self.legacy_stopped = {}

    def legacy_row(self, entry):
        root, manifest, key = entry['root'], entry['manifest'], entry['key']
        signature = entry['registered_at'], file_version(root / manifest), file_version(root / 'runs')
        saved = self.legacy_stopped.get(key)
        if saved and saved[0] == signature and time.monotonic() < saved[1]:
            return saved[2]
        view = self.legacy_views.get(key) or BatchView(root, manifest)
        counts = {}
        try:
            active, tasks = view.rows(use_aggregate=False)
            for task in tasks:
                state = task['status']
                counts[state] = counts.get(state, 0) + 1
            active = active or bool(counts.get('running', 0) + counts.get('starting', 0))
        except (OSError, ValueError) as exc:
            return dict(counts={}, total=0, complete=0, active=False, status='不可读取', error=str(exc))
        total = sum(counts.values())
        complete = counts.get('complete', 0)
        status = '运行中' if active else '已完成' if total and complete == total else '待处理/已停止'
        row = dict(counts=counts, total=total, complete=complete, active=active, status=status, error='')
        if active:
            self.legacy_views[key] = view
            self.legacy_stopped.pop(key, None)
        else:
            # Avoid rescanning old receipts; periodically recheck unregistered legacy changes.
            self.legacy_stopped[key] = signature, time.monotonic() + 60, row
            self.legacy_views.pop(key, None)
        return row

    def rows(self):
        state = registry_directory()
        paths = [state / 'latest.json', *(state / 'batches').glob('*.json')]
        entries = {}
        for path in paths:
            entry = self.files.get(path, registry_entry)
            if entry and (entry['key'] not in entries or
                          entry['registered_at'] >= entries[entry['key']]['registered_at']):
                entries[entry['key']] = entry
        rows = []
        legacy_keys = set()
        for key, entry in entries.items():
            root, manifest = entry['root'], entry['manifest']
            paths.append(aggregate_path(root, manifest))
            data = aggregate(self.files, root, manifest, overview_status)
            if data is None:
                legacy_keys.add(key)
                row = self.legacy_row(entry)
            else:
                active = data['state'] == 'running' and live(data['pid'], 'dispatch.py')
                status = '运行中' if active else '已结束' if data['state'] == 'complete' else '已中断'
                counts = data['counts']
                error = ''
                if active:
                    if data['waiting_for_memory']:
                        status = '等待内存'
                    elif data['waiting_for_service'] and not counts.get('running'):
                        status = '等待重试'
                    elif data['quiet_workers']:
                        status = f'运行中 · {data["quiet_workers"]} 项暂未有新事件'
                elif data['state'] == 'running':
                    status = '调度器失联'
                    error = '当前计数为最后保存的快照；请按批次标识检查任务存活状态或执行取消。'
                row = dict(counts=counts, total=data['total'], complete=counts.get('complete', 0),
                           active=active, status=status, error=error)
            rows.append(dict(**entry, **row))
        for cache, keep in ((self.files.entries, set(paths)), (self.legacy_views, legacy_keys),
                            (self.legacy_stopped, legacy_keys)):
            for key in cache.keys() - keep:
                del cache[key]
        return sorted(rows, key=lambda row: (not row['active'], -row['registered_at'], row['key']))


def batch_ui(screen):
    try:curses.curs_set(0)
    except curses.error:pass
    screen.timeout(200);screen.keypad(True)
    overview=Overview()
    rows=[];selected_key=None;index=0;next_read=0;error=''
    def put(y,x,text,attr=0):
        h,w=screen.getmaxyx()
        if 0<=y<h and x<w-1:
            try:screen.addstr(y,x,clip(text,w-x-1),attr)
            except curses.error:pass
    while True:
        if time.monotonic()>=next_read:
            try:rows=overview.rows();error=''
            except Exception as exc:error='批次登记暂不可读：'+str(exc)
            index=next((i for i,r in enumerate(rows) if r['key']==selected_key),min(index,max(0,len(rows)-1)))
            if rows:selected_key=rows[index]['key']
            next_read=time.monotonic()+2
        screen.erase();h,w=screen.getmaxyx()
        put(0,2,'CODEX  /  BATCHES',curses.A_BOLD)
        put(1,2,f'批次总览 · {len(rows)} 批 · {sum(r["active"] for r in rows)} 批运行中 · 每 2 秒刷新')
        put(2,2,error or '运行中优先；同一运行目录与任务清单合为一个批次。',curses.A_DIM)
        visible=max(1,(h-10)//2);start=max(0,index-visible+1)
        for i,row in enumerate(rows[start:start+visible],start):
            c=row['counts'];attr=curses.A_REVERSE if i==index else 0
            put(4+2*(i-start),1,f'{"›" if i==index else " "} {row["status"]}  {row["root"].name} · {row["manifest"]}',attr)
            put(5+2*(i-start),4,f'完成 {row["complete"]}/{row["total"]}  执行 {c.get("running",0)+c.get("starting",0)}  排队 {c.get("queued",0)}  重试 {c.get("retrying",0)}  失败 {c.get("failed",0)}  阻塞 {c.get("blocked",0)}',curses.A_DIM)
        if not rows:put(4,2,'暂无登记批次；运行调度器后将自动出现。')
        if rows:
            row=rows[index]
            put(h-4,2,str(row['root']),curses.A_DIM)
            put(h-3,2,row['error'] or 'Enter 查看 EXEC 列表及提示词；在任务面板按 Esc 返回。',curses.A_DIM)
        put(h-1,1,'↑↓/jk 选择批次  Enter 进入  r 刷新  Esc/q 退出（所有任务继续）',curses.A_DIM)
        screen.refresh();key=screen.getch()
        if key in (ord('q'),27):return
        if key in (curses.KEY_DOWN,ord('j')):index=min(len(rows)-1,index+1)
        elif key in (curses.KEY_UP,ord('k')):index=max(0,index-1)
        elif key in (10,13,curses.KEY_ENTER) and rows:
            row=rows[index]
            if ui(screen,row['root'],row['manifest'])!='back':return
            next_read=0
        elif key==ord('r'):next_read=0
        if rows:selected_key=rows[index]['key']


def application(screen,root,manifest):
    if root is None or ui(screen,root,manifest)=='back':batch_ui(screen)


@contextmanager
def try_lock(path, *, grace=0):
    # Never unlink these files: a concurrent opener must see the same inode.
    with path.open('a') as handle:
        deadline = time.monotonic() + grace
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    yield False
                    return
                time.sleep(0.01)
            else:
                yield True
                return


def graphical_session():
    # Wayland and XWayland can share a session. Prefer its login ID when available.
    if session := os.environ.get('XDG_SESSION_ID'):
        identity = 'session:' + session
    elif wayland := os.environ.get('WAYLAND_DISPLAY'):
        identity = 'wayland:' + Path(wayland).name
    else:
        display = os.environ.get('DISPLAY', '')
        host, sep, screen = display.rpartition(':')
        identity = 'x11:' + host + sep + screen.split('.')[0] if display else 'default'
    return hashlib.sha256(identity.encode()).hexdigest()[:20]


def automatic_locks(session):
    state = registry_directory()
    return state / f'launch-{session}.lock', state / f'overview-{session}.lock'


def monitor_is_open(lock):
    with try_lock(lock) as acquired:
        return not acquired


STARTUP_GRACE = 2.0


def await_terminal(proc, monitor_lock):
    """Return an early exit code, or zero once startup has been handed off."""
    deadline = time.monotonic() + STARTUP_GRACE
    detached = False
    while True:
        if monitor_lock is not None and monitor_is_open(monitor_lock):
            return 0
        code = proc.poll()
        if code:
            return code
        now = time.monotonic()
        if code == 0 and not detached:
            # Terminal servers may exit successfully before starting the command.
            detached = True
            deadline = now + STARTUP_GRACE
        if monitor_lock is None and code == 0:
            return 0
        if now >= deadline and (detached or monitor_lock is None):
            return 0
        # With an automatic launch, keep the reservation while a live launcher
        # is starting. Once the UI takes its lock, the terminal may outlive us.
        time.sleep(0.05)


def launch_in_terminal(command, monitor_lock=None):
    errors = []
    for launcher, options in (
        ('xdg-terminal-exec', ['--title=Codex EXEC · 工作流', '--']),
        ('x-terminal-emulator', ['-e']),
    ):
        executable = shutil.which(launcher)
        if not executable:
            continue
        try:
            proc = subprocess.Popen([executable, *options, *command], stdin=subprocess.DEVNULL,
                                    start_new_session=True, close_fds=True)
        except OSError as exc:
            errors.append(f'{launcher}: {exc}')
            continue
        code = await_terminal(proc, monitor_lock)
        if code == 0:
            return
        errors.append(f'{launcher}: exit {code}')
    if errors:
        raise RuntimeError('终端启动失败：' + '; '.join(errors))
    raise RuntimeError('没有可用的系统默认终端启动器；请在已有终端输入 parallel')


def launch_monitor(root, manifest, allow_multiple=False):
    command = [sys.executable, str(Path(__file__).resolve())]
    if allow_multiple:
        if root is not None:
            command += ['--run-dir', str(root), '--manifest', manifest]
        launch_in_terminal([*command, '--allow-multiple'])
        return

    # The dispatcher still supplies a batch path. All automatic launches open
    # the same overview instead, and the registry discovers newly added batches.
    session = graphical_session()
    startup_lock, monitor_lock = automatic_locks(session)
    with try_lock(startup_lock) as acquired:
        if acquired and not monitor_is_open(monitor_lock):
            # Pass the session identity through terminal servers that change the environment.
            launch_in_terminal([*command, '--session-monitor', session], monitor_lock)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir',help='Open a specific batch; omitted shows the batch selector')
    parser.add_argument('--manifest',default='tasks.json')
    parser.add_argument('--launch',action='store_true',help='Open the session overview in the default terminal')
    parser.add_argument('--allow-multiple',action='store_true',help='Open another view even if a monitor is already running')
    parser.add_argument('--session-monitor', help=argparse.SUPPRESS)
    args=parser.parse_args(argv);root=Path(args.run_dir).expanduser().resolve() if args.run_dir else None
    lock = None
    if args.session_monitor is not None:
        session = args.session_monitor
        if (len(session) != 20 or any(c not in '0123456789abcdef' for c in session) or
                root is not None or args.launch or args.allow_multiple):
            parser.error('invalid automatic monitor arguments')
        _, lock = automatic_locks(session)
    if args.launch:
        launch_monitor(root, args.manifest, args.allow_multiple)
        return
    if root is not None and not args.allow_multiple:
        key = hashlib.sha256(args.manifest.encode()).hexdigest()[:12]
        lock = root / f'terminal-monitor-{key}.lock'

    def run_ui():
        locale.setlocale(locale.LC_ALL, '')
        curses.wrapper(application, root, args.manifest)

    if lock is None:
        run_ui()
    else:
        # An automatic child can arrive during the launcher's brief lock probe.
        with try_lock(lock, grace=0.15 if args.session_monitor else 0) as acquired:
            if acquired:
                run_ui()


if __name__=='__main__':
    main()
