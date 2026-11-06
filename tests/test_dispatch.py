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
result={'status':mode if mode in ('partial','blocked') else 'complete','summary':'body','artifacts':[],'issues':[]}
if mode=='issues':result['issues']=['needs integration review']
if mode=='bad_schema':result['artifacts']='wrong'
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
        for helper in ('batch_control.py', 'task_control.py', 'process_ownership.py'):
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
        return json.loads((self.root/'workers'/name/'receipt.json').read_text())

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
        (w/'bad/receipt.json').write_text('{corrupt')
        (w/'file').write_text('not a directory')
        self.assertEqual(self.run_cli().returncode,0)
        self.assertEqual(self.receipt()['status'],'complete')
        self.assertEqual((w/'bad/receipt.json').read_text(),'{corrupt')
        self.assertFalse((w/'bad/started').exists())
        status=json.loads((self.root/'status-tasks.json').read_text())
        final=json.loads(Path(status['summary_path']).read_text())
        self.assertEqual(final['results']['bad']['status'],'interrupted')
        self.assertEqual(final['results']['bad']['recovery_state'],'unknown')
        self.assertEqual(final['results']['file']['status'],'failed')
        self.assertIn('receipt_error',final['results']['file'])

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
        self.assertEqual(self.run_cli('--concurrency','9','--ramp-seconds','0','--timeout-seconds','1').returncode,0)
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
        self.assertEqual((w/'result-01.json').read_bytes(),original);self.assertFalse((w/'result.json').exists())
        self.behavior(self.root,{})
        self.assertEqual(self.run_cli('--retry-failed').returncode,0)
        self.assertEqual(self.receipt()['attempt'],3);self.assertEqual(self.receipt()['status'],'complete')
        self.assertEqual(json.loads((w/'receipt-01.json').read_text())['status'],'partial')
        self.assertEqual(json.loads((w/'receipt-02.json').read_text())['status'],'failed')

    def test_configuration_changes_refused_without_overwrite(self):
        self.assertEqual(self.run_cli().returncode,0)
        before=(self.root/'workers/a/receipt.json').read_bytes()
        original=(self.root/'tasks.json').read_text()
        for field,value in [('prompt','changed'),('model','another'),('effort','high'),('web_search','live'),('write_dirs',[str(self.root)])]:
            jobs=json.loads(original);jobs[0][field]=value;(self.root/'tasks.json').write_text(json.dumps(jobs))
            result=self.run_cli('--retry-failed')
            with self.subTest(field=field):self.assertIn('inputs/configuration changed',result.stderr)
            self.assertEqual((self.root/'workers/a/receipt.json').read_bytes(),before)
        (self.root/'tasks.json').write_text(original)
        self.assertIn('inputs/configuration changed',self.run_cli('--channel','api').stderr)
        (self.root/'COMMON.md').write_text('changed')
        self.assertIn('inputs/configuration changed',self.run_cli().stderr)

    def test_legacy_receipt_verification(self):
        self.assertEqual(self.run_cli().returncode,0)
        r=self.receipt();r.pop('input_fingerprint');r.pop('result_file')
        (self.root/'workers/a/receipt-01.json').unlink()
        (self.root/'workers/a/receipt.json').write_text(json.dumps(r))
        self.assertEqual(self.run_cli().returncode,0);self.assertEqual(self.receipt()['attempt'],1)
        r.pop('command');(self.root/'workers/a/receipt.json').write_text(json.dumps(r))
        self.assertIn('cannot be verified',self.run_cli().stderr)

    def test_dashboard_reads_attempt_result_and_summary_failure(self):
        self.jobs(self.root,['a','blocked','file'])
        self.behavior(self.root,{'blocked':'blocked'})
        w=self.root/'workers';w.mkdir();(w/'file').write_text('not a directory')
        self.assertEqual(self.run_cli().returncode,0)
        # Compatibility file content must not override the result bound to this receipt.
        (w/'a/result.json').write_text(json.dumps(dict(status='complete',summary='STALE',artifacts=[],issues=[])))
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
        status=json.loads((self.root/'status-tasks.json').read_text())
        final=json.loads(Path(status['summary_path']).read_text())
        self.assertEqual(final['reused'],['a','blocked','nonzero'])

    def test_report_missing_pid_unstarted_and_multiline_errors(self):
        spec=importlib.util.spec_from_file_location('report_dispatch',self.scripts/'dispatch.py')
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
        jobs=[dict(id='failed',title='启动失败'),dict(id='pending',title='尚未启动')]
        final=dict(results={'failed':dict(status='failed',last_error='spawn error\nsecond line\x1b')},
                   summary_path='summary.json')
        report=mod.format_report(self.root,jobs,final)
        self.assertIn('启动失败 [failed]',report);self.assertIn('尚未启动 [pending]',report)
        self.assertIn('未启动 1',report);self.assertIn('spawn error second line',report)
        self.assertNotIn('\x1b',report);self.assertIn('—',report)

if __name__=='__main__':unittest.main()
