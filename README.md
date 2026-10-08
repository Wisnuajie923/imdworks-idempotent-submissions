# Idempotent submissions under concurrency — bounty #13

Contributor: **enueex**, https://x.com/AjaPawang.

A minimal Python standard-library HTTP service and SQLite workload, using only local synthetic fixtures. This is a concurrency and recovery demonstration, **not a production API**. No packages, accounts, blockchain access, or real wallets are needed.

## One-command replay

From this directory, run:

```sh
python3 run_tests.py
```

Python 3 with SQLite support is required. The harness launches loopback subprocesses, waits for readiness, runs nine tests (including the original seven), and terminates every subprocess. Each test clears only its own generated `artifacts/test_*.sqlite*` fixture before starting. It writes `report.json`, `trace.jsonl`, and `artifacts/naive_failure.json`. Runtime databases are ignored by Git; evidence logs are not. Replay replaces the latest trace/report, not the original TDD logs. Successful replay exits 0; an unexpected assertion or error exits 1.

The stress test releases **120 client threads across 10 synthetic wallets** using a barrier. It measures overlapping client operation intervals and requires at least 100 overlapping operations. This is measured client concurrency, not 120 simultaneous SQLite writers. It also races 20 review/update operations, then finishes any remaining reviews and tests that reviewed records cannot be overwritten or deleted.

## Local endpoint and request contract

To launch manually (parent directory for the database must already exist):

```sh
python3 service.py artifacts/manual.sqlite defended
```

The first stdout line is JSON containing an ephemeral port. Binding is hard-coded to `127.0.0.1`. The only supported API operation is **POST `/submission`**. Other POST paths, including query strings and trailing slashes, return JSON 404 `not_found`; other HTTP methods use the standard library's unsupported-method handling.

Example body with header `Idempotency-Key: example-create-1`:

```json
{"bounty":"fixture-bounty","wallet":"synthetic-00","action":"create","content":"initial"}
```

Send UTF-8 JSON with exactly these fields:

| Field | Contract |
| --- | --- |
| `bounty` | Nonblank string, 1–128 characters; opaque fixture identifier |
| `wallet` | Nonblank string, 1–128 characters; opaque synthetic label, not an authenticated wallet |
| `action` | Exactly `create`, `update`, or `review` |
| `content` | Nonblank string, 1–4096 characters; required for all actions |
| `version` | Required only for `update`/`review`: positive JSON integer, at most 9223372036854775807; booleans/floats are rejected. Forbidden for `create`. |

Extra fields, missing fields, arrays, null, wrong types, unknown actions, and invalid string lengths return 400 `invalid_request`. Nonblank means `strip()` is nonempty; accepted strings are stored as supplied, without trimming or identity normalization. `review` requires `content` for the common request contract but preserves the stored content. This service does not fetch or validate submission URLs: content is opaque text. Exact route validation prevents arbitrary URL paths from silently acting as submission requests.

One `Idempotency-Key` header is mandatory: nonblank and at most 128 characters. It is an opaque global key in this database, not scoped by wallet. Use a fresh key per distinct logical operation and reuse that key for retries. Missing/empty/oversized or duplicate keys return 400 `invalid_key`.

One positive decimal `Content-Length` is required; unsupported transfer encoding, missing/duplicate/nondecimal/nonpositive lengths return 400 `invalid_length`. Declared lengths above 32768 bytes return 413 `too_large` (numeric header representations longer than 10 digits are invalid). JSON is explicitly decoded as UTF-8. Malformed JSON, malformed UTF-8, incomplete bodies, or a body read timeout return 400 `invalid_request` when the client remains able to receive a response. Body reads have a five-second timeout. `Content-Type: application/json` is recommended but not required/enforced. Responses include JSON content type and byte content length.

## State and error semantics

- `create`: absence yields 201, active state, version 1. An existing `(bounty,wallet)` yields 409 `exists` with current version, even after review.
- `update`: absence yields 404 `not_found`; matching version on an active record changes content and increments version, returning 200.
- `review`: matching version on an active record preserves content, changes state to reviewed, increments version, and returns 200.
- Both mutations check existence, then version, then reviewed state. Thus stale input yields 409 `stale_version` with current version; a matching version against reviewed state yields 409 `reviewed`. This precedence is deliberate and deterministic.
- Validation errors happen before a transaction and do not reserve keys or persist replies.
- Valid defended requests persist their status/body, including business failures (404/409). A same-key, same-request retry returns the original status/body, even after later state changes or a restart. A changed request under an existing key returns 409 `key_conflict`, without replacing the saved reply.

Successful replies contain `bounty`, `wallet`, `content`, `state`, and `version`. Error bodies contain `error`, with `version` for version/existence/review conflicts. Stable replies refer to application status and JSON content, not volatile HTTP Date headers.

## Schema and transaction rationale

`submissions` stores an integer primary key, nonnull bounty/wallet/content, state checked to `active`/`reviewed`, and a positive version. Defended mode adds **UNIQUE(bounty,wallet)**. This is stronger than only one active row: review retains the sole logical row permanently. SQLite triggers `immutable_reviewed_update` and `immutable_reviewed_delete` abort all updates/deletes to reviewed rows, including direct SQL bypass attempts. HTTP type/length validation complements, rather than replaces, these database constraints.

`replies` stores `key TEXT PRIMARY KEY`, request hash, status, and serialized body. The SHA-256 digest is over `json.dumps(payload, sort_keys=True, separators=(',',':'))` encoded to UTF-8. JSON key order/insignificant input whitespace do not affect the digest; supplied field values do. The whole validated request is included, including review's otherwise unused content. JSON property names duplicated in a raw request follow the standard `json.loads` behavior: the last occurrence wins; the resulting object is then validated and hashed. This digest is not authentication.

The database uses **WAL** and each connection sets **synchronous=FULL**, with foreign keys enabled and a 30-second SQLite busy timeout. Each defended operation uses **BEGIN IMMEDIATE** before reading replies or submission state, serializing writers before decisions. Mutation and reply insertion commit in the **same transaction**. A rollback loses both; a completed commit retains both. A duplicate request waits and then reads its persisted reply. The version predicate also guards the update. Schema constraints are the final uniqueness/immutability defense; a Python precheck alone is insufficient.

## Actual fault reproduction

The full replay includes these real HTTP/subprocess fault tests; run separately with:

```sh
python3 run_tests.py test_05_lost_response test_06_process_interruption
```

**Local test hooks only:** `X-Test-Fault: drop_after_commit` closes the socket after the defended transaction commits, deliberately losing the response. The client records an actual transport failure, reads the persisted reply for its oracle, retries the same key, then SIGKILLs/restarts the service against the same database and verifies that reply again.

`X-Test-Fault: pause_before_commit` and `pause_after_commit` atomically publish a `.sqlite.fault.json` checkpoint and block the handler. The harness waits for the checkpoint, sends actual SIGKILL, waits for process exit, and restarts. Before-commit interruption leaves neither mutation nor reply; after-commit interruption preserves both. Retrying returns the checkpoint's expected status/body and does not duplicate state. Markers and trace events identify the phase, status, expected response, PID, and kill outcome. These hooks intentionally permit disruptive local actions and must never be exposed remotely.

## Naive negative control and evidence

`naive` mode deliberately omits the uniqueness constraint and transactional reply protocol. An internal two-request barrier forces both real HTTP requests to read absence before either inserts. Both return 201 and create **two rows for one logical pair**. The test explicitly checks the one-row invariant, captures its actual assertion failure (`2 != 1 : one logical row invariant`) in `artifacts/naive_failure.json`, and passes only when that expected defect is observed. This is not an unexplained failing overall test suite.

- `report.json`: timings, exit code, suite outcomes, stress overlap, final rows, fault outcomes, and schema/envelope rejection cases.
- `trace.jsonl`: observed completion/event order, requests, keys, statuses, response content, versions, starts/stops, and checkpoints; new validation cases are included as raw HTTP request records.
- `tdd_logs/full-replay-final.log`: latest real complete replay output and exit code; the earlier `full-replay.log` is also retained.
- `TDD.md`: original and added RED/GREEN log index and provenance caveat.
- `SIGNOFF.md`: contributor and local verification scope.

## Limitations

This is deliberately bounded, not production-hardened. There is **no real wallet authentication, signature verification, authorization, TLS, or global auth requirement**; any local caller can assert any wallet label and review it. Local fault headers are trusted. Do not rebind to a public interface. Review is a synthetic transition, not a permissioned adjudication system. Keys/replies have no expiry and SQLite has one serialized writer. No migrations, distributed consensus, production capacity claims, or hostile-client load guarantees are provided. Storage/OS power-loss guarantees depend on the filesystem and hardware; SIGKILL recovery is measured, power loss is not.

The fixed workload seed determines job input order, **not thread scheduling**. Completion order, race winners, ports, PIDs, elapsed time, and measured concurrency vary per run. Invariants and saved replies are deterministic; an identical trace is not promised. No blockchain transactions, wallet reads, network services, commit, push, or bounty submission are part of this artifact.
