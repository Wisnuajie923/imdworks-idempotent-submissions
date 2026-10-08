"""Local-only SQLite submission demonstration. Not a production API."""
import hashlib
import json
import sqlite3
import sys
import threading
from pathlib import Path
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DB = sys.argv[1]
MODE = sys.argv[2]

def connect():
    c = sqlite3.connect(DB, timeout=30, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA foreign_keys=ON')
    c.execute('PRAGMA synchronous=FULL')
    return c

with connect() as c:
    c.execute('PRAGMA journal_mode=WAL')
    unique = ', UNIQUE(bounty,wallet)' if MODE == 'defended' else ''
    c.execute("CREATE TABLE IF NOT EXISTS submissions(id INTEGER PRIMARY KEY, bounty TEXT NOT NULL, wallet TEXT NOT NULL, content TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('active','reviewed')), version INTEGER NOT NULL CHECK(version>0)" + unique + ')')

    c.execute("CREATE TRIGGER IF NOT EXISTS immutable_reviewed_update BEFORE UPDATE ON submissions WHEN OLD.state='reviewed' BEGIN SELECT RAISE(ABORT,'reviewed immutable'); END")
    c.execute("CREATE TRIGGER IF NOT EXISTS immutable_reviewed_delete BEFORE DELETE ON submissions WHEN OLD.state='reviewed' BEGIN SELECT RAISE(ABORT,'reviewed immutable'); END")
    c.execute('CREATE TABLE IF NOT EXISTS replies(key TEXT PRIMARY KEY, request_hash TEXT NOT NULL, status INTEGER NOT NULL, body TEXT NOT NULL)')

def checkpoint(fault, phase, status, body):
    if fault == phase:
        path = Path(DB+'.fault.json')
        temp = Path(str(path)+'.tmp')
        temp.write_text(json.dumps({'phase':phase,'status':status,'response':body,'committed_version':body['version'] if phase == 'pause_after_commit' else None}))
        temp.replace(path)
        threading.Event().wait()

NAIVE_BARRIER = threading.Barrier(2)

def transact(p, key, fault=None):
    if MODE == 'naive':
        with connect() as c:
            row = c.execute('SELECT * FROM submissions WHERE bounty=? AND wallet=?',(p['bounty'],p['wallet'])).fetchone()
            NAIVE_BARRIER.wait(timeout=10)  # Force both real requests to read absence before either inserts.
            if not row:
                c.execute("INSERT INTO submissions(bounty,wallet,content,state,version) VALUES(?,?,?,'active',1)",(p['bounty'],p['wallet'],p['content']))
            return 201, {'bounty':p['bounty'],'wallet':p['wallet'],'content':p['content'],'state':'active','version':1}
    digest = hashlib.sha256(json.dumps(p, sort_keys=True, separators=(',',':')).encode()).hexdigest()
    with connect() as c:
        c.execute('BEGIN IMMEDIATE')
        prior = c.execute('SELECT * FROM replies WHERE key=?', (key,)).fetchone()
        if prior:
            c.commit()
            return (prior['status'], json.loads(prior['body'])) if prior['request_hash'] == digest else (409, {'error':'key_conflict'})
        row = c.execute('SELECT * FROM submissions WHERE bounty=? AND wallet=?', (p['bounty'],p['wallet'])).fetchone()
        if p['action'] in ('update','review'):
            if not row:
                status, body = 404, {'error':'not_found'}
            elif p.get('version') != row['version']:
                status, body = 409, {'error':'stale_version','version':row['version']}
            elif row['state'] == 'reviewed':
                status, body = 409, {'error':'reviewed','version':row['version']}
            else:
                state = 'reviewed' if p['action'] == 'review' else 'active'
                content = row['content'] if p['action'] == 'review' else p['content']
                c.execute('UPDATE submissions SET content=?,state=?,version=version+1 WHERE id=? AND version=?', (content,state,row['id'],p['version']))
                status, body = 200, {'bounty':p['bounty'],'wallet':p['wallet'],'content':content,'state':state,'version':row['version']+1}
        elif row:
            status, body = 409, {'error':'exists', 'version':row['version']}
        else:
            c.execute("INSERT INTO submissions(bounty,wallet,content,state,version) VALUES(?,?,?,'active',1)", (p['bounty'],p['wallet'],p['content']))
            status, body = 201, {'bounty':p['bounty'],'wallet':p['wallet'],'content':p['content'],'state':'active','version':1}
        c.execute('INSERT INTO replies VALUES(?,?,?,?)', (key,digest,status,json.dumps(body,sort_keys=True)))
        checkpoint(fault,'pause_before_commit',status,body)
        c.commit()
        checkpoint(fault,'pause_after_commit',status,body)
        return status, body

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        if self.path != '/submission':
            self.respond(404, {'error':'not_found'})
            return
        lengths = self.headers.get_all('Content-Length', [])
        if self.headers.get('Transfer-Encoding') is not None or len(lengths) != 1:
            self.respond(400, {'error':'invalid_length'})
            return
        length = lengths[0]
        if not length or len(length) > 10 or not length.isascii() or not length.isdecimal() or int(length) <= 0:
            self.respond(400, {'error':'invalid_length'})
            return
        length = int(length)
        if length > 32768:
            self.respond(413, {'error':'too_large'})
            return
        keys = self.headers.get_all('Idempotency-Key', [])
        if len(keys) != 1 or not keys[0].strip() or len(keys[0]) > 128:
            self.respond(400, {'error':'invalid_key'})
            return
        try:
            self.connection.settimeout(5)
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError('incomplete body')
            p = json.loads(raw.decode('utf-8'))
        except (ValueError, RecursionError, OSError):
            self.respond(400, {'error':'invalid_request'})
            return
        fields = {'bounty','wallet','action','content'}
        valid = isinstance(p, dict) and isinstance(p.get('action'), str) and p['action'] in ('create','update','review')
        if valid:
            mutation = p['action'] != 'create'
            valid = set(p) == (fields | {'version'} if mutation else fields)
            valid = valid and all(isinstance(p.get(k),str) and p[k].strip() and len(p[k]) <= n for k,n in [('bounty',128),('wallet',128),('content',4096)])
            valid = valid and (not mutation or (type(p.get('version')) is int and 0 < p['version'] <= 9223372036854775807))
        if not valid:
            self.respond(400, {'error':'invalid_request'})
            return
        status, body = transact(p, self.headers['Idempotency-Key'], self.headers.get('X-Test-Fault'))
        if self.headers.get('X-Test-Fault') == 'drop_after_commit':
            self.close_connection = True
            self.connection.shutdown(2)
            self.connection.close()
            return
        self.respond(status, body)

    def respond(self, status, body):
        raw = json.dumps(body, sort_keys=True).encode()
        self.send_response(status); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(raw))); self.end_headers()
        self.wfile.write(raw)

class LocalServer(ThreadingHTTPServer):
    request_queue_size = 256

server = LocalServer(('127.0.0.1',0), Handler)
print(json.dumps({'port':server.server_port}), flush=True)
server.serve_forever()
