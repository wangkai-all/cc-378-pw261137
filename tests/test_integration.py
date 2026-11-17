"""End-to-end tests through the storage layer and HTTP API."""
import os
import sys
import tempfile

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(monkeypatch):
    tmp = tempfile.mkdtemp()
    db = os.path.join(tmp, "test.sqlite3")
    monkeypatch.setenv("APP_DB", db)
    # Drop cached modules so app.api (and its module-level Store) are rebuilt
    # against the temp DB and route decorators re-bind the new functions.
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    import app.api as api
    with TestClient(api.app) as c:
        yield c, api, db


def _upsert(client, key, payload):
    return client.post("/api/main/rows", json={"key": key, "payload": payload})


def test_stale_branch_copy_never_overwrites_main(client):
    c, _, _ = client
    _upsert(c, "k4", {"v": 1})
    c.post("/api/branches", json={"name": "b"})
    # main moves on; branch never edits k4
    _upsert(c, "k4", {"v": 9})
    merge = c.post("/api/branches/1/merge", json={"mode": "replay"}).json()
    k4 = next(d for d in merge["plan"] if d["key"] == "k4")
    assert k4["action"] == "noop"
    c.post("/api/merges/1/commit")
    rows = {r["key"]: r["payload"] for r in c.get("/api/state").json()["main"]["rows"]}
    assert rows["k4"] == {"v": 9}                        # main value preserved


def test_full_conflict_lifecycle_with_adjudication(client):
    c, _, _ = client
    _upsert(c, "k1", {"v": 1})
    _upsert(c, "k2", {"v": 1})
    _upsert(c, "k3", {"v": 1})
    c.post("/api/branches", json={"name": "b"})

    # concurrent main traffic
    _upsert(c, "k2", {"v": 3})
    c.delete("/api/main/rows/k3")

    # branch traffic
    c.post("/api/branches/1/rows", json={"key": "k1", "payload": {"v": 2}})
    c.post("/api/branches/1/rows", json={"key": "k2", "payload": {"v": 2}})
    c.post("/api/branches/1/rows", json={"key": "k3", "payload": {"v": 2}})

    merge = c.post("/api/branches/1/merge",
                   json={"mode": "replay"}).json()
    conflicts = {d["key"]: d for d in merge["plan"] if d["status"] == "conflict"}
    assert set(conflicts) == {"k2", "k3"}               # edit/edit + edit/delete
    k1 = next(d for d in merge["plan"] if d["key"] == "k1")
    assert k1["action"] == "upsert"                     # clean fast-forward

    # cannot commit before adjudication
    r = c.post("/api/merges/1/commit")
    assert r.status_code == 409

    c.post("/api/merges/1/begin")
    c.post("/api/merges/1/resolve/k2",
           json={"choice": "custom", "custom_payload": {"v": 42}})
    c.post("/api/merges/1/resolve/k3", json={"choice": "main"})
    c.post("/api/merges/1/commit")

    state = c.get("/api/state").json()
    rows = {r["key"]: r["payload"] for r in state["main"]["rows"]}
    assert rows["k1"] == {"v": 2}
    assert rows["k2"] == {"v": 42}
    assert "k3" not in rows                             # main's delete won
    merge_state = state["merges"][0]
    assert merge_state["status"] == "committed"
    assert state["branches"][0]["status"] == "merged"

    # merge_switch marker exists in the event stream
    ops = [e["op"] for e in c.get("/api/main/events").json()]
    assert "merge_switch" in ops


def test_abort_reopens_branch_and_leaves_main_untouched(client):
    c, _, _ = client
    _upsert(c, "k1", {"v": 1})
    c.post("/api/branches", json={"name": "b"})
    c.post("/api/branches/1/rows", json={"key": "k1", "payload": {"v": 2}})
    c.post("/api/branches/1/merge", json={"mode": "snapshot"})
    c.post("/api/merges/1/abort")
    state = c.get("/api/state").json()
    assert state["merges"][0]["status"] == "aborted"
    assert state["branches"][0]["status"] == "open"
    rows = {r["key"]: r["payload"] for r in state["main"]["rows"]}
    assert rows["k1"] == {"v": 1}                       # nothing applied


def test_snapshot_mode_emits_explicit_final_states(client):
    c, _, _ = client
    _upsert(c, "k1", {"v": 1})
    c.post("/api/branches", json={"name": "b"})
    c.post("/api/branches/1/rows", json={"key": "k1", "payload": {"v": 2}})
    c.post("/api/branches/1/rows", json={"key": "k2", "payload": {"v": 7}})
    c.post("/api/branches/1/merge", json={"mode": "snapshot"})
    c.post("/api/merges/1/commit")
    origins = {e["key"]: e["origin"]
               for e in c.get("/api/main/events").json() if e["key"]}
    assert origins["k1"].startswith("branch:")
    rows = {r["key"]: r["payload"]
            for r in c.get("/api/state").json()["main"]["rows"]}
    assert rows == {"k1": {"v": 2}, "k2": {"v": 7}}


def test_commit_blocks_while_branch_not_open(client):
    c, _, _ = client
    c.post("/api/branches", json={"name": "b"})
    c.post("/api/branches/1/merge", json={"mode": "replay"})
    r = c.post("/api/branches/1/merge", json={"mode": "replay"})
    assert r.status_code == 409


def test_commit_succeeds_but_notification_lost_then_replay_redelivers(client):
    c, api, db = client
    _upsert(c, "k1", {"v": 1})
    c.post("/api/branches", json={"name": "b"})
    c.post("/api/branches/1/rows", json={"key": "k1", "payload": {"v": 2}})
    c.post("/api/branches/1/merge", json={"mode": "replay"})
    c.post("/api/subscriptions", json={"name": "warehouse"})  # backfilled

    # Arm the fault: live push after commit is swallowed.
    c.post("/api/faults", json={
        "crash_in_committing": False,
        "drop_notification_after_commit": True})

    r = c.post("/api/merges/1/commit")
    assert r.status_code == 200
    # Data landed durably...
    rows = {x["key"]: x["payload"]
            for x in c.get("/api/state").json()["main"]["rows"]}
    assert rows["k1"] == {"v": 2}
    # ...but the outbox shows the merge events as undelivered.
    sub_id = c.get("/api/state").json()["subscriptions"][0]["id"]
    sub = c.get(f"/api/subscriptions/{sub_id}").json()
    assert len(sub["pending"]) >= 2                      # upsert + switch marker

    # Replay endpoint redelivers and reconciles.
    replay = c.post(f"/api/subscriptions/{sub_id}/replay").json()
    assert replay["redelivered"] >= 2
    assert c.get(f"/api/subscriptions/{sub_id}").json()["pending"] == []


def test_simulated_crash_in_committing_is_recoverable(client):
    c, api, db = client
    _upsert(c, "k1", {"v": 1})
    c.post("/api/branches", json={"name": "b"})
    c.post("/api/branches/1/rows", json={"key": "k1", "payload": {"v": 2}})
    c.post("/api/branches/1/merge", json={"mode": "replay"})
    c.post("/api/faults", json={
        "crash_in_committing": True,
        "drop_notification_after_commit": False})

    r = c.post("/api/merges/1/commit")
    assert r.status_code == 202                         # parked, nothing applied
    assert "SIMULATED CRASH" in r.json()["warning"]
    assert c.get("/api/merges/1").json()["status"] == "committing"

    # Simulate process restart: a brand-new Store on the same file.
    fresh = api.Store(db)
    notes = fresh.recover()
    assert any("resumed" in n for n in notes)
    merge = fresh.get_merge(1)
    assert merge["status"] == "committed"
    rows = {r["key"]: r["payload"] for r in fresh.list_main_rows()}
    assert rows["k1"] == {"v": 2}                       # resume applied the plan
    # The resume path also emitted the switch marker exactly once.
    switches = [e for e in fresh.list_main_events()
                if e["op"] == "merge_switch"]
    assert len(switches) == 1


def test_sse_backlog_resumes_after_last_seq(client):
    c, api, _ = client
    _upsert(c, "k1", {"v": 1})
    _upsert(c, "k2", {"v": 1})
    sub_id = c.post("/api/subscriptions",
                    json={"name": "s2"}).json()["id"]   # backfilled
    with c.stream("GET",
                  f"/api/subscriptions/{sub_id}/stream?last_seq=1") as resp:
        assert resp.status_code == 200
        chunks: list[str] = []
        for chunk in resp.iter_text():
            chunks.append(chunk)
            if "data:" in "".join(chunks):
                break
        body = "".join(chunks)
        assert '"seq": 2' in body


def test_mode_switch_before_commit(client):
    c, _, _ = client
    c.post("/api/branches", json={"name": "b"})
    c.post("/api/branches/1/merge", json={"mode": "replay"})
    r = c.post("/api/merges/1/mode", json={"mode": "snapshot"})
    assert r.json()["mode"] == "snapshot"
