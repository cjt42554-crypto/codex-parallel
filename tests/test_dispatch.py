"""Regression tests use a fake EXEC process; no model calls or user task mutations."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'scripts'
if not (SOURCE / 'dispatch.py').exists():
    SOURCE = Path(__file__).resolve().parent
sys.path.insert(0, str(SOURCE))

FAKE = r'''#!/usr/bin/env python3
import json,sys,time
from pathlib import Path
args=sys.argv
worker=Path(args[args.index('-C')+1]);root=worker.parent.parent
mode=json.loads((root/'behavior.json').read_text()).get(worker.name,'complete')
attempt=int(Path(args[args.index('-o')+1]).stem.rsplit('-',1)[1])
if isinstance(mode,list):mode=mode[min(attempt-1,len(mode)-1)]
(worker/'started').write_text('yes')
sys.stdin.read()
print(json.dumps({'type':'turn.started'}),flush=True)
if mode=='hang':time.sleep(30)
if mode=='slow':time.sleep(3)
if mode=='brief':time.sleep(.3)
if mode in ('transient','transient_tool','quota'):
 if mode=='transient_tool':print(json.dumps({'type':'item.started','item':{'type':'command_execution'}}),flush=True)
 print(json.dumps({'type':'turn.failed','error':{'code':'insufficient_quota' if mode=='quota' else 'server_is_overloaded'}}),flush=True)
 sys.exit(1)
result={'status':mode if mode in ('partial','blocked') else 'complete','answer':'body'}
if mode in ('research','action','simple_partial','simple_blocked'):
 result={'status':mode.removeprefix('simple_') if mode in ('simple_partial','simple_blocked') else 'complete',
         'answer':'研究已完成，仅两场历史；未完成真实成交验证。报告：[结果](/not/a/real/artifact.txt)'}
 if mode=='action':result.update(status='partial',answer='成果待整合，请主会话完成整合。')
if mode=='bad_schema':result['answer']=['wrong']
if mode!='missing':Path(args[args.index('-o')+1]).write_text(json.dumps(result))
print('null',flush=True)
print(json.dumps({'type':'turn.failed' if mode=='failed_turn' else 'turn.completed'}),flush=True)
if mode=='both':print(json.dumps({'type':'turn.failed'}),flush=True)
sys.exit(3 if mode=='nonzero' else 0)
'''


class DispatchFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.scripts = self.base / 'scripts';self.scripts.mkdir()
        shutil.copy2(SOURCE / 'dispatch.py', self.scripts / 'dispatch.py')
        for helper in ('batch_control.py', 'task_control.py', 'process_ownership.py', 'result_contract.py'):
            if (SOURCE / helper).exists():
                shutil.copy2(SOURCE / helper, self.scripts / helper)
        (self.scripts / 'monitor_registry.py').write_text('from pathlib import Path\ndef registry_directory():\n p=Path('+repr(str(self.base/'registry'))+');p.mkdir(exist_ok=True);return p\n')
        (self.scripts / 'codexexec').write_text(FAKE);(self.scripts / 'codexexec').chmod(0o755)
        self.procs=[]
        self.root=self.make_root('run')

    def tearDown(self):
        for proc in self.procs:
            if proc.poll() is None:proc.terminate()
            try:proc.wait(timeout=12)
            except subprocess.TimeoutExpired:proc.kill();proc.wait()
        self.temp.cleanup()

    def make_root(self,name):
        root=self.base/name;root.mkdir()
        (root/'COMMON.md').write_text('shared')
        self.jobs(root,['a']);self.behavior(root,{})
        return root

    def jobs(self,root,ids):
        (root/'tasks.json').write_text(json.dumps([dict(id=i,title=i,model='fake',effort='low',prompt='test') for i in ids]))

    def behavior(self,root,value):
        (root/'behavior.json').write_text(json.dumps(value))

    def command(self,root=None,*extra):
        return [sys.executable,str(self.scripts/'dispatch.py'),'--run-dir',str(root or self.root),'--no-dashboard',*extra]

    def run_cli(self,*extra,root=None):
        return subprocess.run(self.command(root,*extra),capture_output=True,text=True,timeout=20)

    def receipt(self,name='a'):
        directory = self.root / 'workers' / name
        path = max(directory.glob('receipt-*.json'), key=lambda p: int(p.stem.removeprefix('receipt-')), default=None)
        if path is None:
            raise FileNotFoundError(directory)
        return json.loads(path.read_text())

    def start(self,root=None,*extra):
        p=subprocess.Popen(self.command(root,*extra),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        self.procs.append(p);return p

    def wait_started(self,root=None):
        path=(root or self.root)/'workers/a/started'
        until=time.monotonic()+8
        while not path.exists() and time.monotonic()<until:time.sleep(.03)
        self.assertTrue(path.exists())


class DispatchTests(DispatchFixture):
    def test_validation_precedes_launch(self):
        for mutate in ('duplicate','missing','path','web','dirs'):
            self.jobs(self.root,['a','b']);jobs=json.loads((self.root/'tasks.json').read_text())
            if mutate=='duplicate':jobs[1]['id']='a'
            if mutate=='missing':del jobs[1]['model']
            if mutate=='path':jobs[1]['id']='../escape'
            if mutate=='web':jobs[1]['web_search']='invalid'
            if mutate=='dirs':jobs[1]['write_dirs']=['relative']
            (self.root/'tasks.json').write_text(json.dumps(jobs))
            with self.subTest(mutate=mutate):
                result=self.run_cli();self.assertNotEqual(result.returncode,0);self.assertFalse((self.root/'workers').exists())
        self.jobs(self.root,['a']);self.assertNotEqual(self.run_cli('--only','unknown').returncode,0)

    def test_multi_agent_validation_precedes_launch(self):
        defaults = dict(model='fake', effort='ultra', multi_agent=True,
                        subagent_model='child', subagent_effort='high')
        for field, value in (('multi_agent', 'true'), ('max_subagents', True), ('max_subagents', 0),
                             ('max_subagents', -1), ('max_subagents', 2.5), ('max_subagents', 2**63),
                             ('subagent_model', None), ('subagent_effort', None), ('subagent_effort', ' ')):
            with self.subTest(field=field, value=value):
                jobs = [dict(id='a', prompt='valid task'), dict(id='b', prompt='bad task', **{field: value})]
                (self.root/'tasks.json').write_text(json.dumps(dict(defaults=defaults, tasks=jobs)))
                result = self.run_cli()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f'task b: {field}', result.stderr)
                self.assertFalse((self.root/'workers').exists())
        for field in ('subagent_model', 'subagent_effort'):
            with self.subTest(missing=field):
                incomplete = {k: v for k, v in defaults.items() if k != field}
                (self.root/'tasks.json').write_text(json.dumps(dict(defaults=incomplete, tasks=[dict(id='a', prompt='test')])))
                self.assertIn(f'task a: {field}', self.run_cli('--retry-failed').stderr)
                self.assertFalse((self.root/'workers').exists())

    def test_multi_agent_worker_defaults_overrides_and_explicit_retry(self):
        defaults = dict(model='fake', effort='ultra', multi_agent=True,
                        subagent_model='child', subagent_effort='high')
        jobs = [dict(id='a', prompt='use inherited settings'),
                dict(id='b', prompt='use overrides', max_subagents=2, subagent_model='child-2', subagent_effort='ultra'),
                dict(id='c', prompt='opt out', multi_agent=False)]
        (self.root/'tasks.json').write_text(json.dumps(dict(defaults=defaults, tasks=jobs)))
        self.behavior(self.root, {'b': 'partial'})
        result = self.run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        for name, enabled, limit, model, effort in [('a', True, 3, 'child', 'high'),
                                                    ('b', True, 2, 'child-2', 'ultra'),
                                                    ('c', False, None, None, None)]:
            with self.subTest(name=name):
                receipt = self.receipt(name)
                cmd = receipt['command']
                cfg = {v.split('=', 1)[0]: v.split('=', 1)[1] for v in cmd if v.startswith('agents.')}
                self.assertEqual(cmd[cmd.index('multi_agent') - 1], '--enable' if enabled else '--disable')
                self.assertEqual(cfg, {'agents.enabled': 'true',
                                       'agents.max_concurrent_threads_per_session': str(limit),
                                       'agents.default_subagent_model': json.dumps(model),
                                       'agents.default_subagent_reasoning_effort': json.dumps(effort)} if enabled else {})
                self.assertIn('--ignore-user-config', cmd)
                self.assertIn('--sub', cmd)
                self.assertNotIn('--add-dir', cmd)
                self.assertEqual(cmd[cmd.index('-s') + 1], 'workspace-write')
                for feature in ('apps', 'memories', 'browser_use', 'computer_use'):
                    self.assertEqual(cmd[cmd.index(feature) - 1], '--disable')
                self.assertEqual(receipt['task_timeout_seconds'], 0)
        partial = self.receipt('b')
        self.assertEqual(partial['status'], 'partial')
        self.behavior(self.root, {})
        result = self.run_cli('--only', 'b', '--retry-failed')
        self.assertEqual(result.returncode, 0, result.stderr)
        retried = self.receipt('b')
        self.assertEqual((retried['attempt'], retried['status']), (2, 'complete'))
        self.assertEqual(retried['input_fingerprint'], partial['input_fingerprint'])
        self.assertEqual(retried['command'][:-2], partial['command'][:-2])  # only the numbered output changes

    def test_multi_agent_fingerprint_rejects_changes_but_preserves_opt_out(self):
        self.assertEqual(self.run_cli().returncode, 0)
        directory = self.root/'workers/a'
        original = json.loads((self.root/'tasks.json').read_text())[0]
        disabled = (directory/'receipt-01.json').read_bytes()
        agent_config = dict(max_subagents=1, subagent_model='child', subagent_effort='high')
        (self.root/'tasks.json').write_text(json.dumps([{**original, **agent_config, 'multi_agent': False}]))
        result = self.run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((directory/'receipt-01.json').read_bytes(), disabled)

        enabled = {**original, **agent_config, 'multi_agent': True}
        (self.root/'tasks.json').write_text(json.dumps([enabled]))
        self.assertIn('inputs/configuration changed', self.run_cli('--retry-failed').stderr)
        self.assertEqual((directory/'receipt-01.json').read_bytes(), disabled)

        root = self.make_root('enabled')
        (root/'tasks.json').write_text(json.dumps([enabled]))
        self.assertEqual(self.run_cli(root=root).returncode, 0)
        active_directory = root/'workers/a'
        active = (active_directory/'receipt-01.json').read_bytes()
        for field, value in [('multi_agent', False), ('max_subagents', 2), ('subagent_model', 'other'), ('subagent_effort', 'ultra')]:
            with self.subTest(field=field):
                (root/'tasks.json').write_text(json.dumps([{**enabled, field: value}]))
                self.assertIn('inputs/configuration changed', self.run_cli('--retry-failed', root=root).stderr)
                self.assertEqual((active_directory/'receipt-01.json').read_bytes(), active)
        unverified = json.loads(active)
        unverified.pop('input_fingerprint')
        (active_directory/'receipt-01.json').write_text(json.dumps(unverified))
        (root/'tasks.json').write_text(json.dumps([original]))
        self.assertIn('cannot be verified', self.run_cli(root=root).stderr)

    def test_lock_same_root_other_manifest_and_separate_roots(self):
        self.behavior(self.root,{'a':'slow'});first=self.start();self.wait_started()
        (self.root/'other.json').write_text((self.root/'tasks.json').read_text())
        blocked=self.run_cli('--manifest','other.json')
        self.assertIn('active dispatcher',blocked.stderr)
        other=self.make_root('other');self.assertEqual(self.run_cli(root=other).returncode,0)
        self.assertEqual(first.wait(timeout=10),0)
        self.assertEqual(self.run_cli().returncode,0);self.assertEqual(self.receipt()['attempt'],1)

    def test_lock_survives_killed_dispatcher_until_worker_exits(self):
        self.behavior(self.root,{'a':'slow'});first=self.start();self.wait_started()
        first.kill();first.wait()
        self.assertIn('active dispatcher',self.run_cli().stderr)
        time.sleep(3.2)
        self.behavior(self.root,{})
        self.assertEqual(self.run_cli().returncode,0)
        self.assertEqual(self.receipt()['attempt'],1)
        report=json.loads(self.run_cli('--report').stdout)
        self.assertFalse(report['ok'])
        self.assertEqual(report['counts'],{'interrupted':1})
        self.assertEqual(self.run_cli('--retry-failed').returncode,0)
        self.assertEqual(self.receipt()['status'],'complete')
        self.assertEqual(self.receipt()['attempt'],2)

    def test_corrupt_receipt_and_unwritable_task_do_not_stop_peers(self):
        self.jobs(self.root,['bad','a','file'])
        w=self.root/'workers';(w/'bad').mkdir(parents=True)
        (w/'bad/receipt-01.json').write_text('{corrupt')
        (w/'file').write_text('not a directory')
        self.assertEqual(self.run_cli().returncode,0)
        self.assertEqual(self.receipt()['status'],'complete')
        self.assertEqual((w/'bad/receipt-01.json').read_text(),'{corrupt')
        self.assertFalse((w/'bad/started').exists())
        rows={r['id']:r for r in json.loads(self.run_cli('--results').stdout)['items']}
        self.assertEqual(rows['bad']['status'],'interrupted')
        self.assertEqual(rows['bad']['recovery_state'],'unknown')
        self.assertEqual(rows['file']['status'],'failed')
        self.assertIn('receipt_error',json.loads(Path(rows['file']['receipt_path']).read_text()))

    def test_spawn_failure_is_contained(self):
        # Load the dispatcher and inject one launch error at the actual subprocess boundary.
        spec=importlib.util.spec_from_file_location('isolated_dispatch',self.scripts/'dispatch.py')
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
        import asyncio,argparse,fcntl
        from unittest.mock import patch
        self.jobs(self.root,['bad','a'])
        args=argparse.Namespace(manifest='tasks.json',only=None,channel='sub',retry_failed=False,timeout_seconds=5,concurrency=2,ramp_seconds=0)
        jobs=mod.prepare_jobs(self.root,args)
        original=asyncio.create_subprocess_exec
        async def spawn(*cmd,**kw):
            if Path(cmd[cmd.index('-C')+1]).name=='bad':raise OSError('injected launch failure')
            return await original(*cmd,**kw)
        async def run():
            with (self.root/'.dispatch.lock').open('a') as lock:
                fcntl.flock(lock,fcntl.LOCK_EX)
                b=mod.Batch(self.root,args,jobs,lock.fileno())
                with patch.object(mod.asyncio,'create_subprocess_exec',side_effect=spawn):await b.run()
        asyncio.run(run())
        self.assertEqual(self.receipt('bad')['status'],'failed');self.assertEqual(self.receipt()['status'],'complete')

    def test_status_events_schema_and_timeout(self):
        ids=['a','partial','blocked','failed_turn','both','missing','bad_schema','nonzero','hang']
        self.jobs(self.root,ids);self.behavior(self.root,{i:i for i in ids if i!='a'})
        self.assertEqual(self.run_cli('--concurrency','9','--ramp-seconds','0','--task-timeout-seconds','1').returncode,0)
        for i in ids:
            expected=i if i in ('partial','blocked') else 'complete' if i=='a' else 'blocked' if i in ('missing','bad_schema') else 'failed'
            with self.subTest(i=i):self.assertEqual(self.receipt(i)['status'],expected)
        for i in ('missing','bad_schema'):
            self.assertEqual(self.receipt(i)['execution'],'succeeded')
            self.assertEqual(self.receipt(i)['reported_status'],'unknown')
        r=self.receipt('failed_turn');self.assertIsNone(r['turn_completed_utc']);self.assertTrue(r['turn_failed_utc'])
        self.assertEqual(self.receipt('hang')['last_error'],'worker deadline exceeded')

    def test_retry_result_isolation_and_skipping(self):
        self.behavior(self.root,{'a':'partial'});self.assertEqual(self.run_cli().returncode,0)
        w=self.root/'workers/a';original=(w/'result-01.json').read_bytes()
        self.assertEqual(self.run_cli().returncode,0);self.assertEqual(self.receipt()['attempt'],1)
        self.behavior(self.root,{'a':'nonzero'})
        self.assertEqual(self.run_cli('--retry-failed').returncode,0)
        self.assertEqual(self.receipt()['status'],'failed');self.assertEqual(self.receipt()['result_file'],'result-02.json')
        self.assertEqual((w/'result-01.json').read_bytes(),original)
        self.behavior(self.root,{})
        self.assertEqual(self.run_cli('--retry-failed').returncode,0)
        self.assertEqual(self.receipt()['attempt'],3);self.assertEqual(self.receipt()['status'],'complete')
        self.assertEqual(json.loads((w/'receipt-01.json').read_text())['status'],'partial')
        self.assertEqual(json.loads((w/'receipt-02.json').read_text())['status'],'failed')
        self.assertFalse((w/'receipt.json').exists())
        self.assertFalse((w/'result.json').exists())

    def test_configuration_changes_refused_without_overwrite(self):
        self.assertEqual(self.run_cli().returncode,0)
        before=(self.root/'workers/a/receipt-01.json').read_bytes()
        original=(self.root/'tasks.json').read_text()
        for field,value in [('prompt','changed'),('model','another'),('effort','high'),('web_search','live'),('write_dirs',[str(self.root)])]:
            jobs=json.loads(original);jobs[0][field]=value;(self.root/'tasks.json').write_text(json.dumps(jobs))
            result=self.run_cli('--retry-failed')
            with self.subTest(field=field):self.assertIn('inputs/configuration changed',result.stderr)
            self.assertEqual((self.root/'workers/a/receipt-01.json').read_bytes(),before)
        (self.root/'tasks.json').write_text(original)
        self.assertIn('inputs/configuration changed',self.run_cli('--channel','api').stderr)
        (self.root/'COMMON.md').write_text('changed')
        self.assertIn('inputs/configuration changed',self.run_cli().stderr)


    def test_dashboard_reads_attempt_result_and_summary_failure(self):
        self.jobs(self.root,['a','blocked','file'])
        self.behavior(self.root,{'blocked':'blocked'})
        w=self.root/'workers';w.mkdir();(w/'file').write_text('not a directory')
        self.assertEqual(self.run_cli().returncode,0)
        spec=importlib.util.spec_from_file_location('monitor',SOURCE/'dashboard.py')
        monitor=importlib.util.module_from_spec(spec);spec.loader.exec_module(monitor)
        rows={t['id']:t for t in monitor.snapshot(self.root,'tasks.json')['tasks']}
        self.assertEqual(rows['a']['answer'],'body')
        self.assertEqual(rows['blocked']['status'],'blocked')
        self.assertEqual(rows['blocked']['answer'],'body')
        self.assertEqual(rows['file']['status'],'failed')
        self.assertIsNone(rows['file']['answer'])

    def test_sigterm_cleans_worker_and_releases_lock(self):
        self.behavior(self.root,{'a':'hang'});first=self.start();self.wait_started()
        first.terminate();first.wait(timeout=12)
        self.assertEqual(self.receipt()['status'],'interrupted')
        self.behavior(self.root,{})
        self.assertEqual(self.run_cli('--retry-failed').returncode,0)
        self.assertEqual(self.receipt()['status'],'complete')

    def test_final_report_names_each_task_and_distinguishes_history(self):
        self.jobs(self.root,['a','blocked','nonzero'])
        self.behavior(self.root,{'blocked':'blocked','nonzero':'nonzero'})
        result=self.run_cli()
        self.assertEqual(result.returncode,0)
        self.assertIn('批次结束：',result.stderr)
        self.assertIn('已完成 1',result.stderr)
        self.assertIn('阻塞 1',result.stderr)
        self.assertIn('失败 1',result.stderr)
        for name in ('a','blocked','nonzero'):
            row=next(line for line in result.stderr.splitlines() if line.startswith(name+' '))
            self.assertIn(str(self.receipt(name)['pid']),row)
        blocked=next(line for line in result.stderr.splitlines() if line.startswith('blocked '))
        self.assertIn('阻塞',blocked);self.assertIn('body',blocked)
        self.assertIn('worker did not complete successfully (exit 3)',result.stderr)
        # Existing stdout consumers can continue parsing every line as JSON.
        for line in result.stdout.splitlines():self.assertIsInstance(json.loads(line),dict)
        again=self.run_cli()
        self.assertIn('历史',again.stderr);self.assertIn('本轮未重跑',again.stderr)
        rows=json.loads(self.run_cli('--results').stdout)['items']
        self.assertEqual(sorted(r['id'] for r in rows if r['reused']),['a','blocked','nonzero'])

    def test_report_missing_pid_unstarted_and_multiline_errors(self):
        spec=importlib.util.spec_from_file_location('report_dispatch',self.scripts/'dispatch.py')
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
        jobs=[dict(id='failed',title='启动失败'),dict(id='pending',title='尚未启动')]
        final=dict(results={'failed': {}}, counts={'failed': 1, 'not_started': 1},
                   report_path='report.json', results_path='results.jsonl')
        rows={'failed': dict(status='failed', issues=['spawn error\nsecond line\x1b']),
              'pending': dict(status='not_started')}
        report=mod.format_report(jobs,final,rows)
        self.assertIn('启动失败 [failed]',report);self.assertIn('尚未启动 [pending]',report)
        self.assertIn('未启动 1',report);self.assertIn('spawn error second line',report)
        self.assertNotIn('\x1b',report);self.assertIn('—',report)

    def test_report_uses_published_answers_without_reopening_result_files(self):
        from unittest.mock import patch

        spec=importlib.util.spec_from_file_location('published_report_dispatch',self.scripts/'dispatch.py')
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
        jobs=[dict(id='a',title='a'),dict(id='b',title='b')]
        final=dict(results={'a': dict(pid=12), 'b': dict(pid=34)}, counts={'complete': 1, 'blocked': 1},
                   report_path='report.json', results_path='results.jsonl')
        rows={'a': dict(status='complete', answer='routine answer', issues=[]),
              'b': dict(status='blocked', answer='published answer', issues=['published issue'])}
        with patch.object(mod,'read_result',side_effect=AssertionError('the report must not reopen results')):
            report=mod.format_report(jobs,final,rows)
        self.assertIn('已完成 1，阻塞 1',report)
        self.assertIn('published answer；published issue',report)
        self.assertNotIn('routine answer',report)

if __name__=='__main__':unittest.main()
