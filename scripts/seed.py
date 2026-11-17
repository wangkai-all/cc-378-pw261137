"""Seed a realistic branch-vs-main divergence scenario.

Main-line timeline:
  t0  insert k1..k5
  t1  create branch "analyst-q3" (common baseline = state at t0)
  t2  concurrent main-line traffic while analysts work on the branch:
        - main updates k2            (branch will also edit k2  -> edit/edit conflict)
        - main deletes k3            (branch will edit k3        -> edit/delete conflict)
        - main updates k4            (branch never touches k4    -> stale-copy guard)
        - main inserts k6            (branch never sees k6       -> main-only insert)
  t3  branch work:
        - branch edits k1            (main silent on k1          -> clean fast-forward)
        - branch edits k2            (conflict)
        - branch edits k3            (conflict)
        - branch deletes k5          (main silent on k5          -> clean delete)
        - branch inserts k7          (clean insert)
        - k4, k6 untouched           (the merge must NOT resurrect stale k4)
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.storage import Store  # noqa: E402


def populate(s: Store) -> Store:
    # t0: baseline rows
    s.main_upsert("k1", {"name": "Alpha", "amount": 100, "region": "north"})
    s.main_upsert("k2", {"name": "Bravo", "amount": 200, "region": "north"})
    s.main_upsert("k3", {"name": "Charlie", "amount": 300, "region": "south"})
    s.main_upsert("k4", {"name": "Delta", "amount": 400, "region": "south"})
    s.main_upsert("k5", {"name": "Echo", "amount": 500, "region": "east"})

    # t1: analyst branch
    branch = s.create_branch("analyst-q3")
    bid = branch["id"]

    # t2: main-line keeps moving
    s.main_upsert("k2", {"name": "Bravo", "amount": 260, "region": "north",
                         "note": "mainline price correction"})
    s.main_delete("k3")
    s.main_upsert("k4", {"name": "Delta", "amount": 440, "region": "west",
                         "note": "mainline region fix"})
    s.main_upsert("k6", {"name": "Foxtrot", "amount": 600, "region": "east"})

    # t3: analyst edits on the branch
    s.branch_upsert(bid, "k1", {"name": "Alpha", "amount": 120,
                                "region": "north", "reviewed": True})
    s.branch_upsert(bid, "k2", {"name": "Bravo", "amount": 210,
                                "region": "north-central", "reviewed": True})
    s.branch_upsert(bid, "k3", {"name": "Charlie", "amount": 330,
                                "region": "south", "reviewed": True})
    s.branch_delete(bid, "k5")
    s.branch_upsert(bid, "k7", {"name": "Golf", "amount": 700,
                                "region": "west", "reviewed": True})

    # A downstream consumer subscribed from the very beginning.
    s.create_subscription("warehouse-sync")
    return s


def seed(db_path: str) -> Store:
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(db_path + suffix)
        except FileNotFoundError:
            pass
    return populate(Store(db_path))


if __name__ == "__main__":
    db = os.environ.get("APP_DB", "/workspace/data/app.sqlite3")
    os.makedirs(os.path.dirname(db), exist_ok=True)
    store = seed(db)
    print(f"seeded {db}")
    print("main rows :", [r["key"] for r in store.list_main_rows()])
    print("branches  :", [(b["id"], b["name"], b["status"])
                          for b in store.list_branches()])
    print("subs      :", store.list_subscriptions())
