"""HTTP API and SSE wiring."""
from __future__ import annotations

import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .hub import SubscriptionHub
from .storage import Store

DB_PATH = os.environ.get("APP_DB", "/workspace/data/app.sqlite3")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "web")

app = FastAPI(title="Pointwise Data Branching & Merge")
store = Store(DB_PATH)
hub = SubscriptionHub(store)


@app.on_event("startup")
def _bind_event_loop() -> None:
    import asyncio
    hub.bind_loop(asyncio.get_event_loop())


for note in store.recover():
    print("[recover]", note)


# ------------------------------------------------------------------ models
class RowIn(BaseModel):
    key: str
    payload: dict | list | str | int | float | bool | None


class BranchIn(BaseModel):
    name: str


class MergeModeIn(BaseModel):
    mode: str = Field(pattern="^(replay|snapshot)$")


class ResolutionIn(BaseModel):
    choice: str = Field(pattern="^(branch|main|custom|delete)$")
    custom_payload: dict | list | str | int | float | bool | None = None


class FaultIn(BaseModel):
    crash_in_committing: bool = False
    drop_notification_after_commit: bool = False


class SubIn(BaseModel):
    name: str


# ------------------------------------------------------------- error helper
def error(status: int, detail: str) -> HTTPException:
    return HTTPException(status_code=status, detail=detail)


def publish_tail(origin_seq: int) -> None:
    """Push everything at-or-after origin_seq to live subscribers."""
    for event in store.list_main_events(after_seq=origin_seq - 1):
        hub.publish_all(event)


# ------------------------------------------------------------- system/state
@app.get("/api/state")
def full_state():
    return {
        "main": {
            "head_seq": store.main_head_seq(),
            "rows": store.list_main_rows(),
        },
        "branches": store.list_branches(),
        "merges": store.list_merges(),
        "subscriptions": store.list_subscriptions(),
        "faults": {
            "crash_in_committing": store.get_fault("crash_in_committing", False),
            "drop_notification_after_commit":
                store.get_fault("drop_notification_after_commit", False),
        },
    }


@app.get("/api/main/events")
def main_events(after_seq: int = 0, limit: int | None = None):
    return store.list_main_events(after_seq=after_seq, limit=limit)


# ----------------------------------------------------------------- main row
@app.post("/api/main/rows")
def main_upsert(body: RowIn):
    seq = store.main_upsert(body.key, body.payload)
    publish_tail(seq)
    return {"ok": True, "seq": seq}


@app.delete("/api/main/rows/{key}")
def main_delete(key: str):
    seq = store.main_delete(key)
    if seq is not None:
        publish_tail(seq)
    return {"ok": True, "seq": seq}


# ----------------------------------------------------------------- branches
@app.post("/api/branches")
def create_branch(body: BranchIn):
    if store.find_branch(body.name):
        raise error(409, f"branch {body.name!r} already exists")
    return store.create_branch(body.name)


@app.get("/api/branches/{branch_id}")
def get_branch(branch_id: int):
    try:
        branch = store.get_branch(branch_id)
    except KeyError:
        raise error(404, "branch not found")
    branch["baseline"] = store.branch_baseline_map(branch_id)
    branch["rows"] = store.list_branch_rows(branch_id)
    branch["ops"] = store.list_branch_ops(branch_id)
    return branch


@app.post("/api/branches/{branch_id}/rows")
def branch_upsert(branch_id: int, body: RowIn):
    try:
        store.branch_upsert(branch_id, body.key, body.payload)
    except KeyError:
        raise error(404, "branch not found")
    except RuntimeError as e:
        raise error(409, str(e))
    return {"ok": True}


@app.delete("/api/branches/{branch_id}/rows/{key}")
def branch_delete(branch_id: int, key: str):
    try:
        store.branch_delete(branch_id, key)
    except KeyError:
        raise error(404, "branch not found")
    except RuntimeError as e:
        raise error(409, str(e))
    return {"ok": True}


# ------------------------------------------------------------------- merges
@app.post("/api/branches/{branch_id}/merge")
def prepare_merge(branch_id: int, body: MergeModeIn | None = None):
    mode = body.mode if body else "replay"
    try:
        return store.prepare_merge(branch_id, mode)
    except KeyError:
        raise error(404, "branch not found")
    except RuntimeError as e:
        raise error(409, str(e))


@app.get("/api/merges/{merge_id}")
def get_merge(merge_id: int):
    try:
        return store.get_merge(merge_id)
    except KeyError:
        raise error(404, "merge not found")


@app.post("/api/merges/{merge_id}/mode")
def set_merge_mode(merge_id: int, body: MergeModeIn):
    try:
        return store.set_merge_mode(merge_id, body.mode)
    except KeyError:
        raise error(404, "merge not found")
    except RuntimeError as e:
        raise error(409, str(e))


@app.post("/api/merges/{merge_id}/begin")
def begin_resolution(merge_id: int):
    try:
        return store.begin_resolution(merge_id)
    except KeyError:
        raise error(404, "merge not found")
    except RuntimeError as e:
        raise error(409, str(e))


@app.post("/api/merges/{merge_id}/resolve/{key}")
def resolve_key_with_key(merge_id: int, key: str, body: ResolutionIn):
    try:
        return store.resolve_key(merge_id, key, body.choice,
                                 body.custom_payload)
    except KeyError:
        raise error(404, "key or merge not found")
    except RuntimeError as e:
        raise error(409, str(e))


@app.post("/api/merges/{merge_id}/commit")
def commit_merge(merge_id: int):
    head_before = store.main_head_seq()
    try:
        merge = store.commit_merge(merge_id)
    except KeyError:
        raise error(404, "merge not found")
    except RuntimeError as e:
        if str(e).startswith("SIMULATED CRASH"):
            # Worker died in the 'committing' window: no data applied yet,
            # merge is parked for recovery. Re-open the app/process (or call
            # /api/recover) to resume.
            return JSONResponse(status_code=202, content={
                "ok": False,
                "warning": str(e),
                "merge": store.get_merge(merge_id),
                "recovery": "merge parked in 'committing'; /api/recover or a "
                            "process restart resumes it from the durable plan",
            })
        raise error(409, str(e))
    publish_tail(head_before + 1)
    return {"ok": True, "merge": merge}


@app.post("/api/merges/{merge_id}/abort")
def abort_merge(merge_id: int):
    try:
        return store.abort_merge(merge_id)
    except KeyError:
        raise error(404, "merge not found")
    except RuntimeError as e:
        raise error(409, str(e))


# ------------------------------------------------------------- subscriptions
@app.post("/api/subscriptions")
def create_subscription(body: SubIn):
    return store.create_subscription(body.name)


@app.get("/api/subscriptions")
def list_subscriptions():
    return store.list_subscriptions()


@app.get("/api/subscriptions/{sub_id}/stream")
async def sse_stream(sub_id: int, last_seq: int = 0):
    try:
        store.get_subscription(sub_id)
    except KeyError:
        raise error(404, "subscription not found")
    return StreamingResponse(
        hub.stream(sub_id, last_seq),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache",
                 "X-Accel-Buffering": "no"})


@app.get("/api/subscriptions/{sub_id}")
def get_subscription(sub_id: int):
    try:
        sub = store.get_subscription(sub_id)
    except KeyError:
        raise error(404, "subscription not found")
    sub["last_delivered_seq"] = store.last_delivered_seq(sub_id)
    sub["pending"] = [e["seq"] for e in store.outbox_pending(sub_id)]
    return sub


@app.post("/api/subscriptions/{sub_id}/replay")
def replay_subscription(sub_id: int):
    """Manual redelivery: return every undelivered event and mark them sent.

    This is the operational escape hatch after a lost notification (or after
    the simulated crash) — same data the SSE backlog path would send.
    """
    try:
        events = store.outbox_pending(sub_id)
    except Exception:
        raise error(404, "subscription not found")
    for e in events:
        hub.publish(sub_id, e)
        store.mark_delivered(sub_id, e["seq"])
    return {"redelivered": len(events), "events": events}


# -------------------------------------------------------------------- faults
@app.post("/api/recover")
def recover():
    return {"notes": store.recover()}


@app.post("/api/faults")
def set_faults(body: FaultIn):
    store.set_fault("crash_in_committing", body.crash_in_committing)
    store.set_fault("drop_notification_after_commit",
                    body.drop_notification_after_commit)
    return {"ok": True}


# --------------------------------------------------------------------- reset
@app.post("/api/reset")
def reset():
    """Wipe and re-seed the demo scenario in one click."""
    global store
    store.close()
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(DB_PATH + suffix)
        except FileNotFoundError:
            pass
    # Rebuild the Store and re-point the hub at it; module globals keep their
    # identity for request handlers while the underlying connection is fresh.
    from app.storage import Store as _Store
    store = _Store(DB_PATH)
    hub.store = store
    from scripts.seed import populate
    populate(store)
    return {"ok": True, "seeded": True}


# ---------------------------------------------------------------- static UI
@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
