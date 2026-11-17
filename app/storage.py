"""SQLite storage: main line, branches, merge state machine, subscriptions.

The database is the single source of truth. A merge moves through

    prepared -> resolving -> committing -> committed
                      \\-> aborted

and every main-line write is recorded in main_events *and* fanned out into
per-subscription outbox rows inside the same transaction, so a notification
that later goes missing can always be re-delivered.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

from . import merge as m

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Main line: current live rows ------------------------------------------------
CREATE TABLE IF NOT EXISTS main_rows (
    key      TEXT PRIMARY KEY,
    payload  TEXT NOT NULL,
    version  INTEGER NOT NULL
);

-- Main line: append-only change log (the stream downstream subscribes to) ----
CREATE TABLE IF NOT EXISTS main_events (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    op         TEXT NOT NULL CHECK (op IN ('upsert','delete','merge_switch')),
    key        TEXT,
    payload    TEXT,
    origin     TEXT NOT NULL,                  -- 'main' | 'branch:<id>' | 'merge:<id>'
    merge_id   INTEGER,
    created_at REAL NOT NULL
);

-- Branches --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS branches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    status      TEXT NOT NULL DEFAULT 'open'
                CHECK (status IN ('open','merging','merged','aborted')),
    base_seq    INTEGER NOT NULL,              -- main-line seq at creation
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS branch_baseline (
    branch_id INTEGER NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    key       TEXT NOT NULL,
    state     TEXT NOT NULL,                   -- {"kind":...,"payload":...}
    PRIMARY KEY (branch_id, key)
);

CREATE TABLE IF NOT EXISTS branch_rows (
    branch_id INTEGER NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
    key       TEXT NOT NULL,
    state     TEXT NOT NULL,
    last_seq  INTEGER,                         -- branch_op seq that last touched it
    PRIMARY KEY (branch_id, key)
);

CREATE TABLE IF NOT EXISTS branch_ops (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id  INTEGER NOT NULL,
    op         TEXT NOT NULL CHECK (op IN ('upsert','delete')),
    key        TEXT NOT NULL,
    payload    TEXT,
    created_at REAL NOT NULL
);

-- Merges ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS merges (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_id   INTEGER NOT NULL REFERENCES branches(id),
    status      TEXT NOT NULL
                CHECK (status IN ('prepared','resolving','committing','committed','aborted')),
    mode        TEXT NOT NULL DEFAULT 'replay' CHECK (mode IN ('replay','snapshot')),
    plan        TEXT NOT NULL,                 -- JSON list of KeyDecision dicts
    prepared_at REAL NOT NULL,
    committed_at REAL
);

-- Subscriptions + durable outbox ----------------------------------------------
CREATE TABLE IF NOT EXISTS subscriptions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    event_seq       INTEGER NOT NULL REFERENCES main_events(seq),
    delivered       INTEGER NOT NULL DEFAULT 0,
    delivered_at    REAL,
    UNIQUE (subscription_id, event_seq)
);

-- Fault injection knobs --------------------------------------------------------
CREATE TABLE IF NOT EXISTS faults (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True)


def _loads(s: str | None) -> Any:
    return json.loads(s) if s is not None else None


def now() -> float:
    return time.time()


class Store:
    """Thread-safe wrapper around a single SQLite file."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------------------------------------------------------------- meta
    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return _loads(row["value"]) if row else default

    def set_meta(self, key: str, value: Any) -> None:
        with self.tx() as c:
            c.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                      "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                      (key, _dumps(value)))

    # --------------------------------------------------------------- faults
    def set_fault(self, key: str, value: Any) -> None:
        with self.tx() as c:
            c.execute("INSERT INTO faults(key,value) VALUES(?,?) "
                      "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                      (key, _dumps(value)))

    def get_fault(self, key: str, default: Any = None) -> Any:
        row = self._conn.execute(
            "SELECT value FROM faults WHERE key=?", (key,)).fetchone()
        return _loads(row["value"]) if row else default

    # ------------------------------------------------------------ main line
    def main_head_seq(self) -> int:
        row = self._conn.execute(
            "SELECT MAX(seq) AS s FROM main_events").fetchone()
        return row["s"] or 0

    def main_state_map(self) -> dict[str, dict]:
        return {
            r["key"]: m.value(_loads(r["payload"]))
            for r in self._conn.execute("SELECT key,payload FROM main_rows")
        }

    def list_main_rows(self) -> list[dict]:
        return [
            {"key": r["key"], "payload": _loads(r["payload"]),
             "version": r["version"]}
            for r in self._conn.execute(
                "SELECT key,payload,version FROM main_rows ORDER BY key")
        ]

    def list_main_events(self, after_seq: int = 0, limit: int | None = None) -> list[dict]:
        q = ("SELECT seq,op,key,payload,origin,merge_id,created_at "
             "FROM main_events WHERE seq>? ORDER BY seq")
        if limit:
            q += f" LIMIT {int(limit)}"
        return [self._event_row(r) for r in self._conn.execute(q, (after_seq,))]

    @staticmethod
    def _event_row(r: sqlite3.Row) -> dict:
        return {
            "seq": r["seq"], "op": r["op"], "key": r["key"],
            "payload": _loads(r["payload"]), "origin": r["origin"],
            "merge_id": r["merge_id"], "created_at": r["created_at"],
        }

    def _emit_main_event(self, c: sqlite3.Connection, op: str, key: str | None,
                         payload: Any, origin: str,
                         merge_id: int | None) -> int:
        cur = c.execute(
            "INSERT INTO main_events(op,key,payload,origin,merge_id,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (op, key, _dumps(payload) if payload is not None else None,
             origin, merge_id, now()))
        seq = cur.lastrowid
        # Fan out: one durable outbox row per subscription, same transaction.
        c.execute("INSERT INTO outbox(subscription_id,event_seq) "
                  "SELECT id,? FROM subscriptions", (seq,))
        return seq

    def main_upsert(self, key: str, payload: Any,
                    origin: str = "main", merge_id: int | None = None) -> int:
        with self.tx() as c:
            row = c.execute("SELECT version FROM main_rows WHERE key=?",
                            (key,)).fetchone()
            version = (row["version"] + 1) if row else 1
            c.execute("INSERT INTO main_rows(key,payload,version) VALUES(?,?,?) "
                      "ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,"
                      "version=excluded.version",
                      (key, _dumps(payload), version))
            return self._emit_main_event(c, "upsert", key, payload,
                                         origin, merge_id)

    def main_delete(self, key: str, origin: str = "main",
                    merge_id: int | None = None) -> int | None:
        with self.tx() as c:
            row = c.execute("SELECT 1 FROM main_rows WHERE key=?",
                            (key,)).fetchone()
            c.execute("DELETE FROM main_rows WHERE key=?", (key,))
            if row is None:
                return None  # already absent: no event, nothing to notify
            return self._emit_main_event(c, "delete", key, None,
                                         origin, merge_id)

    # -------------------------------------------------------------- branches
    def create_branch(self, name: str) -> dict:
        with self.tx() as c:
            base_seq = self.main_head_seq()
            cur = c.execute(
                "INSERT INTO branches(name,base_seq,created_at) VALUES(?,?,?)",
                (name, base_seq, now()))
            branch_id = cur.lastrowid
            for r in c.execute("SELECT key,payload FROM main_rows"):
                c.execute("INSERT INTO branch_baseline(branch_id,key,state) "
                          "VALUES(?,?,?)",
                          (branch_id, r["key"], _dumps(m.value(_loads(r["payload"])))))
                c.execute("INSERT INTO branch_rows(branch_id,key,state) "
                          "VALUES(?,?,?)",
                          (branch_id, r["key"], r["payload"] and _dumps(m.value(_loads(r["payload"])))))
        return self.get_branch(branch_id)

    def get_branch(self, branch_id: int) -> dict:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM branches WHERE id=?", (branch_id,)).fetchone()
            if not r:
                raise KeyError(f"branch {branch_id} not found")
            return dict(r)

    def find_branch(self, name: str) -> dict | None:
        r = self._conn.execute(
            "SELECT * FROM branches WHERE name=?", (name,)).fetchone()
        return dict(r) if r else None

    def list_branches(self) -> list[dict]:
        return [dict(r) for r in self._conn.execute(
            "SELECT * FROM branches ORDER BY id")]

    def branch_state_map(self, branch_id: int) -> dict[str, dict]:
        return {
            r["key"]: _loads(r["state"])
            for r in self._conn.execute(
                "SELECT key,state FROM branch_rows WHERE branch_id=?",
                (branch_id,))
        }

    def branch_baseline_map(self, branch_id: int) -> dict[str, dict]:
        return {
            r["key"]: _loads(r["state"])
            for r in self._conn.execute(
                "SELECT key,state FROM branch_baseline WHERE branch_id=?",
                (branch_id,))
        }

    def list_branch_rows(self, branch_id: int) -> list[dict]:
        return [
            {"key": r["key"], "state": _loads(r["state"]),
             "last_seq": r["last_seq"]}
            for r in self._conn.execute(
                "SELECT key,state,last_seq FROM branch_rows "
                "WHERE branch_id=? ORDER BY key", (branch_id,))
        ]

    def list_branch_ops(self, branch_id: int) -> list[dict]:
        return [dict(r) for r in self._conn.execute(
            "SELECT seq,op,key,payload,created_at FROM branch_ops "
            "WHERE branch_id=? ORDER BY seq", (branch_id,))]

    def _branch_write(self, branch_id: int, op: str, key: str,
                      payload: Any) -> None:
        branch = self.get_branch(branch_id)
        if branch["status"] != "open":
            raise RuntimeError(
                f"branch {branch['name']} is {branch['status']}, not writable")
        state = m.value(payload) if op == "upsert" else m.tombstone()
        with self.tx() as c:
            cur = c.execute(
                "INSERT INTO branch_ops(branch_id,op,key,payload,created_at) "
                "VALUES(?,?,?,?,?)",
                (branch_id, op, key,
                 _dumps(payload) if payload is not None else None, now()))
            op_seq = cur.lastrowid
            c.execute("INSERT INTO branch_rows(branch_id,key,state,last_seq) "
                      "VALUES(?,?,?,?) ON CONFLICT(branch_id,key) DO UPDATE "
                      "SET state=excluded.state,last_seq=excluded.last_seq",
                      (branch_id, key, _dumps(state), op_seq))

    def branch_upsert(self, branch_id: int, key: str, payload: Any) -> None:
        self._branch_write(branch_id, "upsert", key, payload)

    def branch_delete(self, branch_id: int, key: str) -> None:
        self._branch_write(branch_id, "delete", key, None)

    # ---------------------------------------------------------------- merges
    def prepare_merge(self, branch_id: int, mode: str = "replay") -> dict:
        branch = self.get_branch(branch_id)
        if branch["status"] != "open":
            raise RuntimeError(
                f"branch {branch['name']} is {branch['status']}; "
                "only open branches can be merged")
        plan = m.build_plan(
            self.branch_baseline_map(branch_id),
            self.branch_state_map(branch_id),
            self.main_state_map(),
            {r["key"]: r["last_seq"]
             for r in self._conn.execute(
                 "SELECT key,last_seq FROM branch_rows WHERE branch_id=?",
                 (branch_id,)) if r["last_seq"] is not None},
        )
        with self.tx() as c:
            cur = c.execute(
                "INSERT INTO merges(branch_id,status,mode,plan,prepared_at) "
                "VALUES(?, 'prepared', ?, ?, ?)",
                (branch_id, mode, _dumps([d.to_dict() for d in plan]), now()))
            merge_id = cur.lastrowid
            c.execute("UPDATE branches SET status='merging' WHERE id=?",
                      (branch_id,))
        return self.get_merge(merge_id)

    def get_merge(self, merge_id: int) -> dict:
        r = self._conn.execute("SELECT * FROM merges WHERE id=?",
                               (merge_id,)).fetchone()
        if not r:
            raise KeyError(f"merge {merge_id} not found")
        d = dict(r)
        d["plan"] = _loads(d["plan"])
        return d

    def list_merges(self) -> list[dict]:
        out = []
        for r in self._conn.execute("SELECT * FROM merges ORDER BY id"):
            d = dict(r)
            d["plan"] = _loads(d["plan"])
            out.append(d)
        return out

    def _save_plan(self, c: sqlite3.Connection, merge_id: int,
                   plan: list[dict]) -> None:
        c.execute("UPDATE merges SET plan=? WHERE id=?",
                  (_dumps(plan), merge_id))

    def set_merge_mode(self, merge_id: int, mode: str) -> dict:
        if mode not in ("replay", "snapshot"):
            raise ValueError("mode must be replay or snapshot")
        merge = self.get_merge(merge_id)
        if merge["status"] not in ("prepared", "resolving"):
            raise RuntimeError(f"merge is {merge['status']}, mode locked")
        with self.tx() as c:
            c.execute("UPDATE merges SET mode=? WHERE id=?", (mode, merge_id))
        return self.get_merge(merge_id)

    def begin_resolution(self, merge_id: int) -> dict:
        merge = self.get_merge(merge_id)
        if merge["status"] != "prepared":
            raise RuntimeError(f"merge is {merge['status']}")
        with self.tx() as c:
            c.execute("UPDATE merges SET status='resolving' WHERE id=?",
                      (merge_id,))
        return self.get_merge(merge_id)

    def resolve_key(self, merge_id: int, key: str, choice: str,
                    custom_payload: Any = None) -> dict:
        merge = self.get_merge(merge_id)
        if merge["status"] != "resolving":
            raise RuntimeError(f"merge is {merge['status']}")
        plan = merge["plan"]
        target = next((d for d in plan if d["key"] == key), None)
        if target is None:
            raise KeyError(key)
        if target["status"] != m.CONFLICT:
            raise RuntimeError(f"{key} is not a conflict")
        # Rebuild a typed decision and run the same pure function as prepare.
        d = m.KeyDecision(
            key=target["key"], base=target["base"], branch=target["branch"],
            main=target["main"], branch_changed=target["branch_changed"],
            main_changed=target["main_changed"], status=m.CONFLICT,
            branch_seq=target.get("branch_seq"))
        m.apply_resolution(d, choice, custom_payload)
        target.update(d.to_dict())
        with self.tx() as c:
            self._save_plan(c, merge_id, plan)
        return self.get_merge(merge_id)

    def abort_merge(self, merge_id: int) -> dict:
        merge = self.get_merge(merge_id)
        if merge["status"] in ("committed", "aborted"):
            raise RuntimeError(f"merge is {merge['status']}, cannot abort")
        with self.tx() as c:
            c.execute("UPDATE merges SET status='aborted' WHERE id=?",
                      (merge_id,))
            c.execute("UPDATE branches SET status='open' WHERE id=?",
                      (merge["branch_id"],))
        return self.get_merge(merge_id)

    # --------------------------------------------------------- commit worker
    def commit_merge(self, merge_id: int) -> dict:
        """Apply the adjudicated plan onto the main line.

        Re-validates the plan against the *current* main line inside one
        transaction: a main-line change landing after prepare would invalidate
        a stale verdict, so we re-run the three-way check and refuse to commit
        if a new conflict appeared.
        """
        merge = self.get_merge(merge_id)
        if merge["status"] not in ("prepared", "resolving", "committing"):
            raise RuntimeError(f"merge is {merge['status']}")
        branch_id = merge["branch_id"]
        branch = self.get_branch(branch_id)
        main_states = self.main_state_map()

        # Re-validation against fresh main-line state.
        fresh = {d.key: d for d in m.build_plan(
            self.branch_baseline_map(branch_id),
            self.branch_state_map(branch_id),
            main_states)}
        for item in merge["plan"]:
            d = fresh[item["key"]]
            if d.status == m.CONFLICT and item.get("resolution") is None:
                raise RuntimeError(
                    f"new unresolved conflict on {item['key']}; "
                    "re-adjudicate before commit")
            if d.status == m.CLEAN and item["status"] == m.CONFLICT:
                # Main converged with branch while we were resolving:
                # the manual verdict is moot, take the clean decision.
                item.update(d.to_dict())

        unresolved = [d for d in merge["plan"]
                      if d["status"] == m.CONFLICT and not d.get("resolution")]
        if unresolved:
            raise RuntimeError("unresolved conflicts remain")

        crash = self.get_fault("crash_in_committing", False)
        with self.tx() as c:
            c.execute("UPDATE merges SET status='committing' WHERE id=?",
                      (merge_id,))
            c.execute("UPDATE branches SET status='merging' WHERE id=?",
                      (branch_id,))

            if crash:
                # Simulated worker death mid-commit: the transaction rolls
                # back leaving the merge parked in 'committing'. No main-line
                # data was applied; recover() resumes the exact same plan.
                self.set_fault("crash_in_committing", False)  # one-shot fault
                raise RuntimeError(
                    "SIMULATED CRASH: worker died while merge was committing")

            origin = f"branch:{branch['name']}"
            mode = merge["mode"]
            if mode == "replay":
                self._commit_replay(c, merge, branch_id, origin)
            else:
                self._commit_snapshot(c, merge, origin)

            c.execute("INSERT INTO main_events(op,key,payload,origin,merge_id,"
                      "created_at) VALUES('merge_switch',NULL,NULL,?,?,?)",
                    (f"merge:{merge_id}", merge_id, now()))
            switch_seq = c.execute(
                "SELECT MAX(seq) AS s FROM main_events").fetchone()["s"]
            c.execute("INSERT INTO outbox(subscription_id,event_seq) "
                      "SELECT id,? FROM subscriptions", (switch_seq,))

            c.execute("UPDATE merges SET status='committed', committed_at=? "
                      "WHERE id=?", (now(), merge_id))
            c.execute("UPDATE branches SET status='merged' WHERE id=?",
                      (branch_id,))
        return self.get_merge(merge_id)

    def resume_committing(self) -> list[int]:
        """Re-enter commit for merges parked in 'committing' after a crash."""
        ids = [r["id"] for r in self._conn.execute(
            "SELECT id FROM merges WHERE status='committing'")]
        for merge_id in ids:
            self.commit_merge(merge_id)
        return ids

    def _commit_replay(self, c: sqlite3.Connection, merge: dict,
                       branch_id: int, origin: str) -> None:
        """Replay branch changes in original branch-op order.

        Clean branch changes go first (ordered), adjudicated conflicts follow
        keyed order. Consumers see individual row changes — the same shape as
        ordinary main-line traffic — then a merge_switch marker.
        """
        plan = {d["key"]: d for d in merge["plan"]}
        applied_keys: set[str] = set()
        ops = c.execute(
            "SELECT op,key,payload FROM branch_ops WHERE branch_id=? ORDER BY seq",
            (branch_id,)).fetchall()
        for op_row in ops:
            d = plan.get(op_row["key"])
            if d is None or d["status"] == m.CONFLICT:
                continue  # conflicts handled below with their verdict
            if d["action"] == m.NOOP:
                continue
            applied_keys.add(d["key"])
            self._apply_action(c, d, origin, merge["id"])
        for d in merge["plan"]:
            if d["key"] in applied_keys or d["action"] == m.NOOP:
                continue
            self._apply_action(c, d, origin, merge["id"])

    def _commit_snapshot(self, c: sqlite3.Connection, merge: dict,
                         origin: str) -> None:
        """Emit the post-merge row set as one snapshot wave.

        Every key the branch touched gets an explicit final state, so a
        consumer can treat the wave as a complete materialization rather
        than ordering individual replays.
        """
        for d in merge["plan"]:
            if d["action"] == m.NOOP:
                continue
            self._apply_action(c, d, origin, merge["id"])

    def _apply_action(self, c: sqlite3.Connection, decision: dict,
                      origin: str, merge_id: int) -> None:
        key = decision["key"]
        if decision["action"] == m.UPSERT:
            row = c.execute("SELECT version FROM main_rows WHERE key=?",
                            (key,)).fetchone()
            version = (row["version"] + 1) if row else 1
            c.execute("INSERT INTO main_rows(key,payload,version) VALUES(?,?,?) "
                      "ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,"
                      "version=excluded.version",
                      (key, _dumps(decision["payload"]), version))
            self._emit_main_event(c, "upsert", key, decision["payload"],
                                  origin, merge_id)
        elif decision["action"] == m.DELETE:
            c.execute("DELETE FROM main_rows WHERE key=?", (key,))
            self._emit_main_event(c, "delete", key, None, origin, merge_id)

    def recover(self) -> list[str]:
        """Startup recovery for crashes inside the commit window."""
        notes: list[str] = []
        resumed = self.resume_committing()
        for merge_id in resumed:
            notes.append(f"merge {merge_id}: resumed parked 'committing' "
                         "work and completed the commit")
        with self.tx() as c:
            pending = c.execute(
                "SELECT COUNT(*) AS n FROM outbox WHERE delivered=0").fetchone()
            if pending["n"]:
                notes.append(
                    f"{pending['n']} undelivered outbox rows will be re-pushed "
                    "to subscribers on reconnect/startup")
        return notes

    # --------------------------------------------------------- subscriptions
    def create_subscription(self, name: str) -> dict:
        with self.tx() as c:
            cur = c.execute(
                "INSERT INTO subscriptions(name,created_at) VALUES(?,?)",
                (name, now()))
            sub_id = cur.lastrowid
            # Backfill: a new subscriber receives the whole durable history.
            c.execute("INSERT INTO outbox(subscription_id,event_seq) "
                      "SELECT ?,seq FROM main_events", (sub_id,))
        return self.get_subscription(sub_id)

    def get_subscription(self, sub_id: int) -> dict:
        r = self._conn.execute("SELECT * FROM subscriptions WHERE id=?",
                               (sub_id,)).fetchone()
        if not r:
            raise KeyError(f"subscription {sub_id} not found")
        return dict(r)

    def list_subscriptions(self) -> list[dict]:
        subs = [dict(r) for r in self._conn.execute(
            "SELECT * FROM subscriptions ORDER BY id")]
        for s in subs:
            pending = self._conn.execute(
                "SELECT COUNT(*) AS n FROM outbox WHERE subscription_id=? "
                "AND delivered=0", (s["id"],)).fetchone()
            s["pending"] = pending["n"]
        return subs

    def outbox_pending(self, sub_id: int) -> list[dict]:
        rows = self._conn.execute(
            "SELECT o.event_seq AS seq, e.op, e.key, e.payload, e.origin, "
            "e.merge_id, e.created_at FROM outbox o JOIN main_events e "
            "ON e.seq=o.event_seq WHERE o.subscription_id=? AND o.delivered=0 "
            "ORDER BY o.event_seq", (sub_id,)).fetchall()
        return [self._event_row(r) for r in rows]

    def mark_delivered(self, sub_id: int, event_seq: int) -> None:
        with self.tx() as c:
            c.execute("UPDATE outbox SET delivered=1, delivered_at=? "
                      "WHERE subscription_id=? AND event_seq=?",
                      (now(), sub_id, event_seq))

    def last_delivered_seq(self, sub_id: int) -> int:
        r = self._conn.execute(
            "SELECT MAX(event_seq) AS s FROM outbox WHERE subscription_id=? "
            "AND delivered=1", (sub_id,)).fetchone()
        return r["s"] or 0
