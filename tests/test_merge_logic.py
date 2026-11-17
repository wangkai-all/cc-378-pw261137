"""Tests for the pure three-way merge table."""
from app import merge as m


def test_branch_silent_means_main_wins_even_when_main_changed():
    # Stale-copy guard: branch still holds the old k4 copy, main moved on.
    d = m.decide_one("k4",
                     base=m.value({"v": 1}),
                     branch=m.value({"v": 1}),          # untouched on branch
                     main=m.value({"v": 9}))            # main updated
    assert d.status == m.CLEAN
    assert d.action == m.NOOP


def test_branch_silent_and_main_deleted_keeps_tombstone():
    d = m.decide_one("k3",
                     base=m.value({"v": 1}),
                     branch=m.value({"v": 1}),
                     main=m.tombstone())
    assert d.action == m.NOOP                           # main's delete stands


def test_branch_only_change_fast_forwards_upsert():
    d = m.decide_one("k1",
                     base=m.value({"v": 1}),
                     branch=m.value({"v": 2}),
                     main=m.value({"v": 1}))
    assert d.status == m.CLEAN
    assert d.action == m.UPSERT
    assert d.payload == {"v": 2}


def test_branch_only_delete_fast_forwards_delete():
    d = m.decide_one("k5",
                     base=m.value({"v": 1}),
                     branch=m.tombstone(),
                     main=m.value({"v": 1}))
    assert d.action == m.DELETE


def test_branch_insert_where_main_silent_is_clean():
    d = m.decide_one("k7", base=m.absent(),
                     branch=m.value({"v": 1}), main=m.absent())
    assert d.status == m.CLEAN and d.action == m.UPSERT


def test_main_only_insert_branch_absent_is_noop():
    d = m.decide_one("k6", base=m.absent(),
                     branch=m.absent(), main=m.value({"v": 1}))
    assert d.action == m.NOOP


def test_edit_edit_conflict():
    d = m.decide_one("k2",
                     base=m.value({"v": 1}),
                     branch=m.value({"v": 2}),
                     main=m.value({"v": 3}))
    assert d.status == m.CONFLICT
    assert d.resolution is None


def test_edit_delete_conflict():
    d = m.decide_one("k3",
                     base=m.value({"v": 1}),
                     branch=m.value({"v": 2}),
                     main=m.tombstone())
    assert d.status == m.CONFLICT


def test_both_changed_to_same_value_converges():
    d = m.decide_one("k", base=m.value({"v": 1}),
                     branch=m.value({"v": 5}), main=m.value({"v": 5}))
    assert d.status == m.CLEAN and d.action == m.NOOP


def test_resolution_branch_replays_branch_value():
    d = m.decide_one("k2", base=m.value({"v": 1}),
                     branch=m.value({"v": 2}), main=m.value({"v": 3}))
    m.apply_resolution(d, "branch")
    assert d.action == m.UPSERT and d.payload == {"v": 2}
    assert d.resolution == {"choice": "branch", "payload": {"v": 2}}


def test_resolution_main_drops_branch_change():
    d = m.decide_one("k2", base=m.value({"v": 1}),
                     branch=m.value({"v": 2}), main=m.value({"v": 3}))
    m.apply_resolution(d, "main")
    assert d.action == m.NOOP


def test_resolution_custom_snapshot():
    d = m.decide_one("k2", base=m.value({"v": 1}),
                     branch=m.value({"v": 2}), main=m.value({"v": 3}))
    m.apply_resolution(d, "custom", {"v": 99})
    assert d.action == m.UPSERT and d.payload == {"v": 99}


def test_resolution_branch_on_edit_delete_tombstones():
    d = m.decide_one("k3", base=m.value({"v": 1}),
                     branch=m.value({"v": 2}), main=m.tombstone())
    m.apply_resolution(d, "branch")
    assert d.action == m.UPSERT                        # branch revives the row
    assert d.payload == {"v": 2}


def test_resolution_delete_explicitly_tombstones():
    d = m.decide_one("k2", base=m.value({"v": 1}),
                     branch=m.value({"v": 2}), main=m.value({"v": 3}))
    m.apply_resolution(d, "delete")
    assert d.action == m.DELETE


def test_resolution_main_on_edit_delete_is_noop():
    d = m.decide_one("k3", base=m.value({"v": 1}),
                     branch=m.value({"v": 2}), main=m.tombstone())
    m.apply_resolution(d, "main")
    assert d.action == m.NOOP


def test_build_plan_covers_all_keys():
    plan = m.build_plan(
        baseline={"a": m.value(1)},
        branch_states={"a": m.value(1), "b": m.value(2)},
        main_states={"a": m.value(1), "c": m.value(3)})
    keys = {d.key for d in plan}
    assert keys == {"a", "b", "c"}


def test_payloads_compared_structurally_not_by_text():
    # JSON key order must not matter: state equality is structural.
    assert m.states_equal(m.value({"a": 1, "b": 2}),
                          m.value({"b": 2, "a": 1}))
