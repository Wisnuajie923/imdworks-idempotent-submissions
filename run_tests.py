import concurrent.futures as cf
import hashlib
import http.client
import json
import os
from pathlib import Path
import random
import select
import sqlite3
import subprocess
import sys
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parent
TRACE = []
LOCK = threading.Lock()
RESULTS = {}

def record(row):
    with LOCK:
        row['sequence'] = len(TRACE)
        TRACE.append(row)

class Service:
    def __init__(self, name, mode='defended'):
        self.db = ROOT / 'artifacts' / (name + '.sqlite')
        self.mode = mode
        self.p = None
    def start(self):
        assert (ROOT / 'service.py').exists(), 'HTTP submission service not implemented'
        self.p = subprocess.Popen([sys.executable, str(ROOT / 'service.py'), str(self.db), self.mode], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=ROOT)
        ready, _, _ = select.select([self.p.stdout], [], [], 10)
        assert ready, 'service readiness timeout'
        line = self.p.stdout.readline()
        assert line, 'service failed: ' + self.p.stderr.read()
        self.port = json.loads(line)['port']
        record({'event':'start', 'db': self.db.name, 'pid':self.p.pid, 'port':self.port})
        return self
    def stop(self, kill=False):
        if self.p is not None:
            if self.p.poll() is None:
                self.p.kill() if kill else self.p.terminate()
            self.p.wait(timeout=10)
            record({'event':'kill' if kill else 'stop', 'pid':self.p.pid, 'exit_code':self.p.returncode, 'db':self.db.name})
            self.p.stdout.close(); self.p.stderr.close()
            self.p = None
    def call(self, opid, payload, key=None, fault=None):
        row = {'operation_id':opid, 'wallet':payload['wallet'], 'request':payload, 'key':key or opid, 'fault':fault, 'db':self.db.name, 'started_ns':time.monotonic_ns()}
        con = http.client.HTTPConnection('127.0.0.1', self.port, timeout=30)
        headers = {'Content-Type':'application/json', 'Idempotency-Key':key or opid}
        if fault: headers['X-Test-Fault'] = fault
        try:
            con.request('POST', '/submission', json.dumps(payload), headers)
            response = con.getresponse()
            body = json.loads(response.read())
            result = (response.status, body)
            row.update(status=response.status, response=body, committed_version=body.get('version'))
            return result
        except (OSError, http.client.HTTPException) as e:
            row.update(status=None, response=None, transport_error=type(e).__name__)
            return None
        finally:
            row['completed_ns'] = time.monotonic_ns()
            con.close(); record(row)
    def raw_call(self, opid, body, headers, path='/submission'):
        con = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        row = {'operation_id':opid, 'request_raw':repr(body) if isinstance(body,bytes) else body, 'headers':headers, 'path':path, 'db':self.db.name}
        try:
            con.request('POST', path, body, headers)
            response = con.getresponse()
            result = (response.status, json.loads(response.read()))
            row.update(status=result[0], response=result[1])
            return result
        except (OSError, http.client.HTTPException) as e:
            row.update(status=None, transport_error=type(e).__name__)
            return None
        finally:
            con.close(); record(row)

    def rows(self, table='submissions'):
        with sqlite3.connect(self.db) as c:
            c.row_factory = sqlite3.Row
            order = 'bounty,wallet' if table == 'submissions' else 'key'
            return [dict(r) for r in c.execute('SELECT * FROM ' + table + ' ORDER BY ' + order)]

def request(wallet='synthetic-00', action='create', version=None, content='initial'):
    p = dict(bounty='fixture-bounty', wallet=wallet, action=action, content=content)
    if version is not None: p['version'] = version
    return p

class Tests(unittest.TestCase):
    def setUp(self):
        name = self._testMethodName
        folder = ROOT / 'artifacts'; folder.mkdir(exist_ok=True)
        for p in folder.glob(name + '.sqlite*'): p.unlink()
        self.s = Service(name)
        self.addCleanup(self.s.stop)
    def test_01_unique(self):
        self.s.start()
        a = self.s.call('unique-a', request())
        b = self.s.call('unique-b', request())
        self.assertEqual(a[0], 201)
        self.assertEqual(b[0], 409)
        self.assertEqual(len(self.s.rows()), 1)
        with sqlite3.connect(self.s.db) as c:
            with self.assertRaises(sqlite3.IntegrityError):
                c.execute("INSERT INTO submissions(bounty,wallet,content,state,version) VALUES('fixture-bounty','synthetic-00','evil','active',1)")
        RESULTS['unique'] = {'rows':1,'direct_duplicate_insert':'IntegrityError'}

    def test_02_idempotency(self):
        self.s.start()
        p = request()
        first = self.s.call('idem-first', p, 'shared-key')
        self.assertEqual(self.s.call('idem-retry', p, 'shared-key'), first)
        conflict = self.s.call('idem-conflict', request(content='different'), 'shared-key')
        self.assertEqual(conflict, (409, {'error':'key_conflict'}))
        self.assertEqual(len(self.s.rows('replies')), 1)
        self.s.stop(); self.s.start()
        self.assertEqual(self.s.call('idem-restart', p, 'shared-key'), first)
        RESULTS['idempotency'] = {'stable_after_restart':True,'key_conflict':409,'reply_rows':1}

    def test_03_versions(self):
        self.s.start()
        self.s.call('v-create', request())
        updated = self.s.call('v-update', request(action='update', version=1, content='new'))
        self.assertEqual(updated[0], 200)
        self.assertEqual(updated[1]['version'], 2)
        stale = self.s.call('v-stale', request(action='update', version=1, content='stale'))
        self.assertEqual(stale, (409, {'error':'stale_version','version':2}))
        self.s.call('v-advance', request(action='update', version=2, content='newest'))
        self.assertEqual(self.s.call('v-stale-retry', request(action='update', version=1, content='stale'), 'v-stale'), stale)
        self.assertEqual(self.s.rows()[0]['content'], 'newest')
        RESULTS['versions'] = {'final_version':3,'stale_reply_stable_after_advance':True}

    def test_04_review_and_stress(self):
        self.s.start()
        jobs = [(f'stress-{i:03d}', request(wallet=f'synthetic-{i%10:02d}'), f'create-{i%10}') for i in range(120)]
        random.Random(13).shuffle(jobs)
        def run_batch(batch):
            barrier = threading.Barrier(len(batch))
            def go(job):
                barrier.wait(timeout=20)
                return self.s.call(*job)
            with cf.ThreadPoolExecutor(max_workers=len(batch)) as pool:
                return list(pool.map(go, batch))
        responses = run_batch(jobs)
        self.assertTrue(all(r and r[0] == 201 for r in responses))
        self.assertEqual(len(self.s.rows()), 10)
        self.assertEqual(len(self.s.rows('replies')), 10)
        pairs = []
        for i in range(10):
            w = f'synthetic-{i:02d}'
            pairs.extend([(f'review-{i}', request(w,'review',1), f'review-{i}'), (f'update-{i}',request(w,'update',1,'edited'),f'update-{i}')])
        raced = run_batch(pairs)
        for i in range(10):
            winners = raced[i*2:i*2+2]
            self.assertEqual(sorted(r[0] for r in winners), [200,409])
            w = f'synthetic-{i:02d}'
            row = self.s.rows()[i]
            if row['state'] == 'active':
                self.assertEqual(self.s.call(f'finish-review-{i}', request(w,'review',2))[0],200)
            before = self.s.rows()[i]
            denied = self.s.call(f'overwrite-{i}',request(w,'update',before['version'],'forbidden'))
            self.assertEqual(denied,(409,{'error':'reviewed','version':before['version']}))
            self.assertEqual(self.s.rows()[i],before)
        with sqlite3.connect(self.s.db) as c:
            with self.assertRaises(sqlite3.IntegrityError): c.execute("UPDATE submissions SET content='bypass' WHERE id=1")
            with self.assertRaises(sqlite3.IntegrityError): c.execute('DELETE FROM submissions WHERE id=1')
        measured = [r for r in TRACE if r.get('operation_id','').startswith('stress-')]
        events = sorted([(r['started_ns'],1) for r in measured]+[(r['completed_ns'],-1) for r in measured])
        active = peak = 0
        for _, delta in events:
            active += delta; peak = max(peak,active)
        self.assertGreaterEqual(peak,100,'at least 100 measured overlapping HTTP client operations')
        RESULTS['stress'] = {'measured_peak_inflight_client_operations':peak, 'barrier_clients':120,'create_http_requests':120,'race_http_requests':20,'synthetic_wallets':10,'final_rows':self.s.rows(),'response_order':[r['operation_id'] for r in TRACE if r.get('operation_id','').startswith('stress-')],'race_winners_depend_on_scheduling':True}

    def test_05_lost_response(self):
        self.s.start()
        p = request()
        lost = self.s.call('drop-first',p,'drop-key','drop_after_commit')
        self.assertIsNone(lost, 'must actually lose HTTP response after commit')
        persisted = self.s.rows('replies')[0]
        self.assertEqual(len(self.s.rows()),1)
        expected = (persisted['status'],json.loads(persisted['body']))
        self.assertEqual(self.s.call('drop-retry',p,'drop-key'),expected)
        self.s.stop(kill=True); self.s.start()
        self.assertEqual(self.s.call('drop-restart-retry',p,'drop-key'),expected)
        RESULTS['lost_response'] = {'transport_lost':True,'commit_rows':1,'reply_recovered_after_SIGKILL':True}

    def test_06_process_interruption(self):
        self.s.start()
        outcomes = []
        for phase, wallet, count in [('pause_before_commit','synthetic-pre',0),('pause_after_commit','synthetic-post',2)]:
            marker = Path(str(self.s.db)+'.fault.json')
            if marker.exists(): marker.unlink()
            p = request(wallet)
            with cf.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(self.s.call, phase+'-first',p,phase,phase)
                deadline = time.monotonic()+5
                while not marker.exists() and not pending.done() and time.monotonic()<deadline: time.sleep(.01)
                if not marker.exists():
                    self.s.stop(kill=True)
                    self.fail('missing actual transaction fault checkpoint: '+phase)
                checkpoint = json.loads(marker.read_text())
                record({'event':'fault_checkpoint','wallet':wallet,'operation_id':phase+'-first',**checkpoint})
                self.s.stop(kill=True)
                self.assertIsNone(pending.result(timeout=5))
            self.s.start()
            self.assertEqual(len(self.s.rows()),count)
            self.assertEqual(len(self.s.rows('replies')),count)
            reply = self.s.call(phase+'-retry',p,phase)
            self.assertEqual(reply,(checkpoint['status'],checkpoint['response']))
            self.assertEqual(len(self.s.rows()),1 if count==0 else 2)
            outcomes.append({'phase':phase,'rows_before_retry':count,'stable_reply':True,'kill_signal':9})
        RESULTS['process_interruption'] = outcomes

    def test_07_naive_failure(self):
        self.s.mode = 'naive'
        self.s.start()
        barrier = threading.Barrier(2)
        def go(i):
            barrier.wait(timeout=5)
            return self.s.call(f'naive-{i}',request('synthetic-naive'),'naive-shared')
        with cf.ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(go,range(2)))
        self.assertEqual([r[0] for r in responses],[201,201])
        rows = self.s.rows()
        self.assertEqual(len(rows),2,'naive read/read/insert/insert must violate logical uniqueness')
        try:
            self.assertEqual(len(rows),1,'one logical row invariant')
        except AssertionError as e:
            evidence = {'expected_invariant_failure':str(e),'logical_rows':len(rows),'responses':responses,'rows':rows}
            (ROOT/'artifacts'/'naive_failure.json').write_text(json.dumps(evidence,indent=2,sort_keys=True)+'\n')
            RESULTS['naive'] = evidence
        else:
            self.fail('naive unexpectedly preserved invariant')

    def test_08_request_schema(self):
        self.s.start()
        self.assertEqual(self.s.call('schema-seed', request())[0], 201)
        cases = [('unknown_action', request(action='delete')),
                 ('bool_version', request(action='update', version=True)),
                 ('review_bool', request(action='review', version=True)),
                 ('missing_version', request(action='update')),
                 ('zero_version', request(action='update', version=0)),
                 ('float_version', request(action='update', version=1.0)),
                 ('negative_version', request(action='review', version=-1)),
                 ('create_version', request(version=1)),
                 ('extra_field', dict(request(), extra=1)),
                 ('missing_content', {k:v for k,v in request().items() if k != 'content'}),
                 ('array', []), ('null', None)]
        for field, bound in [('bounty',128), ('wallet',128), ('content',4096)]:
            for label, value in [('empty',''), ('whitespace','   '), ('type',1), ('bool',True), ('long','x'*(bound+1))]:
                cases.append((field+'_'+label, dict(request(), **{field:value})))
        checked = []
        for label, payload in cases:
            with self.subTest(case=label):
                result = self.s.raw_call(label, json.dumps(payload), {'Idempotency-Key':label})
                self.assertEqual(result, (400, {'error':'invalid_request'}))
                checked.append(label)
        self.assertEqual(len(self.s.rows()),1)
        self.assertEqual(self.s.rows()[0]['version'],1)
        self.assertEqual(len(self.s.rows('replies')),1)
        boundary = request(wallet='w'*128, content='x'*4096)
        boundary['bounty'] = 'b'*128
        self.assertEqual(self.s.call('schema-boundary',boundary)[0],201)
        RESULTS['request_schema'] = {'rejected_cases':checked, 'invalid_requests_not_persisted':True, 'maximum_string_lengths_accepted':True}

    def test_09_http_envelope(self):
        self.s.start()
        good = json.dumps(request())
        cases = [('wrong_route',good,{'Idempotency-Key':'route'},'/other',404,'not_found'),
                 ('query_route',good,{'Idempotency-Key':'query'},'/submission?x=1',404,'not_found'),
                 ('slash_route',good,{'Idempotency-Key':'slash'},'/submission/',404,'not_found'),
                 ('malformed_json','{',{'Idempotency-Key':'json'},'/submission',400,'invalid_request'),
                 ('invalid_utf8',b'\xff',{'Idempotency-Key':'utf8'},'/submission',400,'invalid_request'),
                 ('missing_key',good,{},'/submission',400,'invalid_key'),
                 ('empty_key',good,{'Idempotency-Key':''},'/submission',400,'invalid_key'),
                 ('blank_key',good,{'Idempotency-Key':'   '},'/submission',400,'invalid_key'),
                 ('long_key',good,{'Idempotency-Key':'k'*129},'/submission',400,'invalid_key'),
                 ('bad_length','',{'Idempotency-Key':'length','Content-Length':'bad'},'/submission',400,'invalid_length'),
                 ('negative_length','',{'Idempotency-Key':'negative','Content-Length':'-1'},'/submission',400,'invalid_length'),
                 ('missing_length',None,{'Idempotency-Key':'missing-length','Transfer-Encoding':'identity'},'/submission',400,'invalid_length'),
                 ('chunked','',{'Idempotency-Key':'chunked','Transfer-Encoding':'chunked'},'/submission',400,'invalid_length'),
                 ('zero_length','',{'Idempotency-Key':'zero','Content-Length':'0'},'/submission',400,'invalid_length'),
                 ('oversize','x'*32769,{'Idempotency-Key':'big'},'/submission',413,'too_large')]
        checked = []
        for label, body, headers, path, status, error in cases:
            with self.subTest(case=label):
                self.assertEqual(self.s.raw_call(label,body,headers,path),(status,{'error':error}))
                checked.append(label)
        self.assertEqual(self.s.rows(),[])
        self.assertEqual(self.s.rows('replies'),[])
        self.assertEqual(self.s.raw_call('key-boundary',good,{'Idempotency-Key':'k'*128})[0],201)
        RESULTS['http_envelope'] = {'rejected_cases':checked, 'invalid_requests_not_persisted':True, 'maximum_key_length_accepted':True}

if __name__ == '__main__':
    selected = sys.argv[1:]
    suite = unittest.defaultTestLoader.loadTestsFromNames(['__main__.Tests.' + n for n in selected]) if selected else unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
    started = time.monotonic()
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    elapsed = time.monotonic()-started
    (ROOT / 'trace.jsonl').write_text(''.join(json.dumps(r, sort_keys=True) + '\n' for r in TRACE))
    report = {'exit_code':0 if result.wasSuccessful() else 1, 'elapsed_seconds':elapsed,'http_client_operations':sum('operation_id' in r and ('request' in r or 'request_raw' in r) for r in TRACE), 'success':result.wasSuccessful(), 'tests_run':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'results':RESULTS,'trace_rows':len(TRACE),'ordering':'Trace sequence is observed completion/event order, not deterministic scheduling.'}
    (ROOT / 'report.json').write_text(json.dumps(report, indent=2, sort_keys=True)+'\n')
    sys.exit(0 if result.wasSuccessful() else 1)
