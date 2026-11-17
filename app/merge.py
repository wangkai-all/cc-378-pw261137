"""Pure three-way merge logic.

No I/O lives here so the conflict rules can be unit-tested in isolation.

Every row key is described by a *state*:

    {"kind": "absent"}                 key does not exist
    {"kind": "value", "payload": ...} live row with a JSON payload
    {"kind": "tombstone"}             key existed in the baseline but was deleted

A merge is decided per key against the *common baseline* (the main-line
snapshot taken when the branch was created):

    base   - state at branch creation (common ancestor)
    branch - state on the branch now
    main   - state on the main line now

The golden rule this module enforces: a branch row that the branch never
touched can never overwrite a newer main-line value.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

ABSENT = "absent"
VALUE = "value"
TOMBSTONE = "tombstone"

CLEAN = "clean"
CONFLICT = "conflict"

NOOP = "noop"
UPSERT = "upsert"
DELETE = "delete"


def absent() -> dict:
    return {"kind": ABSENT}


def value(payload: Any) -> dict:
    return {"kind": VALUE, "payload": payload}


def tombstone() -> dict:
    return {"kind": TOMBSTONE}


def states_equal(a: dict, b: dict) -> bool:
    """Structural equality (payloads are parsed JSON values, not raw text)."""
    if a["kind"] != b["kind"]:
        return False
    if a["kind"] == VALUE:
        return a["payload"] == b["payload"]
    return True


def action_for(state: dict) -> dict:
    """Translate a desired end-state into a main-line action."""
    if state["kind"] == VALUE:
        return {"action": UPSERT, "payload": state["payload"]}
    if state["kind"] == TOMBSTONE:
        return {"action": DELETE}
    raise ValueError(f"cannot turn state {state!r} into an action")


@dataclass
class KeyDecision:
    """Three-way verdict for one key."""

    key: str
    base: dict
    branch: dict
    main: dict
    branch_changed: bool
    main_changed: bool
    status: str                       # CLEAN | CONFLICT
    action: str = NOOP                # NOOP | UPSERT | DELETE (clean rows only)
    payload: Any = None
    branch_seq: int | None = None
    resolution: dict | None = None   # user verdict when conflicted

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "base": self.base,
            "branch": self.branch,
            "main": self.main,
            "branch_changed": self.branch_changed,
            "main_changed": self.main_changed,
            "status": self.status,
            "action": self.action,
            "payload": self.payload,
            "branch_seq": self.branch_seq,
            "resolution": self.resolution,
        }


def decide_one(key: str, base: dict, branch: dict, main: dict,
               branch_seq: int | None = None) -> KeyDecision:
    """Run the three-way merge table for a single key."""
    branch_changed = not states_equal(branch, base)
    main_changed = not states_equal(main, base)
    d = KeyDecision(
        key=key, base=base, branch=branch, main=main,
        branch_changed=branch_changed, main_changed=main_changed,
        status=CLEAN, branch_seq=branch_seq,
    )

    # Branch never touched this key: main always wins, even if main deleted it.
    # This is the rule that stops stale branch copies clobbering new main data.
    if not branch_changed:
        d.action = NOOP
        return d

    # Branch changed it, main did not: fast-forward the branch change.
    if not main_changed:
        applied = action_for(branch)
        d.action = applied["action"]
        d.payload = applied.get("payload")
        return d

    # Both sides changed. Convergent edits are not a conflict.
    if states_equal(branch, main):
        d.action = NOOP
        return d

    # Genuine divergence: a human (or policy) must adjudicate.
    d.status = CONFLICT
    return d


def apply_resolution(decision: KeyDecision, choice: str,
                     custom_payload: Any = None) -> dict:
    """Return the effective action for an adjudicated conflict.

    choice:
      branch - keep the branch value (replay branch change)
      main   - keep the main value (drop the branch change) -> NOOP
      custom - write an explicit merged snapshot payload
      delete - tombstone the key
    """
    if choice == "branch":
        target = decision.branch
    elif choice == "main":
        target = decision.main
    elif choice == "custom":
        target = value(custom_payload)
    elif choice == "delete":
        target = tombstone()
    else:
        raise ValueError(f"unknown resolution choice {choice!r}")

    resolution = {"choice": choice}
    if target["kind"] == VALUE:
        resolution["payload"] = target["payload"]
    decision.resolution = resolution

    if target["kind"] == ABSENT or states_equal(target, decision.main):
        decision.action = NOOP
        decision.payload = None
        return decision
    applied = action_for(target)
    decision.action = applied["action"]
    decision.payload = applied.get("payload")
    return decision


def build_plan(baseline: dict[str, dict],
               branch_states: dict[str, dict],
               main_states: dict[str, dict],
               branch_seqs: dict[str, int] | None = None) -> list[KeyDecision]:
    """Enumerate a decision for every key visible to any of the three sides."""
    branch_seqs = branch_seqs or {}
    keys = set(baseline) | set(branch_states) | set(main_states)
    plan = []
    for key in sorted(keys):
        plan.append(decide_one(
            key,
            baseline.get(key, absent()),
            branch_states.get(key, absent()),
            main_states.get(key, absent()),
            branch_seqs.get(key),
        ))
    return plan


def unresolved(plan: list[KeyDecision]) -> list[KeyDecision]:
    return [d for d in plan if d.status == CONFLICT and d.resolution is None]
