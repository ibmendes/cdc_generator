import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "generator"))

from generate_cdc_batch import build_batch  # noqa: E402


def test_first_batch_is_all_inserts():
    state = {"next_sequence": 1, "customers": {}}
    events = build_batch(state, batch_size=10, insert_ratio=1.0)

    assert len(events) == 10
    assert all(e["operation_type"] == "INSERT" for e in events)
    assert len(state["customers"]) == 10
    assert state["next_sequence"] == 11


def test_second_batch_can_update_or_delete_existing_customers():
    state = {"next_sequence": 1, "customers": {}}
    build_batch(state, batch_size=20, insert_ratio=1.0)  # seed customers
    before = set(state["customers"].keys())

    events = build_batch(state, batch_size=10, insert_ratio=0.3)
    op_types = {e["operation_type"] for e in events}
    inserted_this_batch = {e["customer_id"] for e in events if e["operation_type"] == "INSERT"}

    assert op_types <= {"INSERT", "UPDATE", "DELETE"}
    # every UPDATE/DELETE should reference a customer_id known by that point
    # (created before this batch, or inserted earlier within this same batch)
    for e in events:
        if e["operation_type"] in ("UPDATE", "DELETE"):
            assert e["customer_id"] in before | inserted_this_batch


def test_sequence_num_is_monotonic():
    state = {"next_sequence": 1, "customers": {}}
    events = build_batch(state, batch_size=15, insert_ratio=0.6)
    sequences = [e["sequence_num"] for e in events]

    assert sequences == sorted(sequences)
    assert len(set(sequences)) == len(sequences)
