"""
Fake CDC event generator.

Simulates a source transactional system emitting a change-data-capture feed
for a `customers` table. Each run of this script represents "one more batch
of changes arriving" and is meant to be run as a Lakeflow Job task, on a
schedule, right before the Lakeflow Declarative Pipeline refreshes.

State (the current view of "what customers exist today") is kept as a JSON
file inside the same UC Volume, under a `_control/` folder, so consecutive
runs produce consistent UPDATE/DELETE events against real existing keys
instead of random noise.

Runs on serverless compute. UC Volumes are exposed as a normal POSIX path
(/Volumes/<catalog>/<schema>/<volume>/...) on Databricks compute, so plain
Python file I/O is enough — no Spark session required here.
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker

fake = Faker()

COUNTRIES = ["BR", "US", "PT", "DE", "FR", "AR", "MX", "CA"]
SEGMENTS = ["retail", "wholesale", "enterprise"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a fake CDC batch for the customers table")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--volume", required=True)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument(
        "--insert-ratio", type=float, default=0.5,
        help="Fraction of the batch that will be new INSERTs (rest split between UPDATE/DELETE)",
    )
    return parser.parse_args()


def volume_root(catalog: str, schema: str, volume: str) -> str:
    return f"/Volumes/{catalog}/{schema}/{volume}"


def load_state(control_dir: str) -> dict:
    state_path = os.path.join(control_dir, "customers_state.json")
    if not os.path.exists(state_path):
        return {"next_sequence": 1, "customers": {}}
    with open(state_path, "r") as f:
        return json.load(f)


def save_state(control_dir: str, state: dict) -> None:
    os.makedirs(control_dir, exist_ok=True)
    state_path = os.path.join(control_dir, "customers_state.json")
    tmp_path = state_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f)
    os.replace(tmp_path, state_path)


def new_customer_payload() -> dict:
    return {
        "full_name": fake.name(),
        "email": fake.unique.email(),
        "country_code": random.choice(COUNTRIES),
        "segment": random.choice(SEGMENTS),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
    }


def build_batch(state: dict, batch_size: int, insert_ratio: float) -> list[dict]:
    events = []
    sequence = state["next_sequence"]
    customers = state["customers"]

    n_inserts = max(1, int(batch_size * insert_ratio))
    n_changes = batch_size - n_inserts

    # INSERTs: brand-new customer_ids
    for _ in range(n_inserts):
        customer_id = str(uuid.uuid4())
        payload = new_customer_payload()
        event = {
            "customer_id": customer_id,
            "operation_type": "INSERT",
            "sequence_num": sequence,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        events.append(event)
        customers[customer_id] = payload
        sequence += 1

    # UPDATE / DELETE: pick from existing customers, if any exist yet
    existing_ids = list(customers.keys())
    for _ in range(n_changes):
        if not existing_ids:
            break
        customer_id = random.choice(existing_ids)

        if random.random() < 0.25:
            # DELETE — carry the last known payload so the target can be dropped/tombstoned
            event = {
                "customer_id": customer_id,
                "operation_type": "DELETE",
                "sequence_num": sequence,
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                **customers[customer_id],
            }
            del customers[customer_id]
            existing_ids.remove(customer_id)
        else:
            # UPDATE — mutate one or two fields to trigger a new SCD2 version
            payload = dict(customers[customer_id])
            payload["segment"] = random.choice(SEGMENTS)
            payload["email"] = fake.unique.email()
            customers[customer_id] = payload
            event = {
                "customer_id": customer_id,
                "operation_type": "UPDATE",
                "sequence_num": sequence,
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                **payload,
            }

        events.append(event)
        sequence += 1

    state["next_sequence"] = sequence
    return events


def write_batch(landing_dir: str, events: list[dict]) -> str:
    os.makedirs(landing_dir, exist_ok=True)
    filename = f"batch_{int(time.time() * 1000)}.json"
    batch_path = os.path.join(landing_dir, filename)
    with open(batch_path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    return batch_path


def main() -> None:
    args = parse_args()
    root = volume_root(args.catalog, args.schema, args.volume)
    control_dir = os.path.join(root, "_control")
    landing_dir = os.path.join(root, "landing")

    state = load_state(control_dir)
    events = build_batch(state, args.batch_size, args.insert_ratio)
    batch_path = write_batch(landing_dir, events)
    save_state(control_dir, state)

    counts = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
    for e in events:
        counts[e["operation_type"]] += 1

    print(f"Wrote {len(events)} CDC events to {batch_path}")
    print(f"Breakdown: {counts}")
    print(f"Known customers after this batch: {len(state['customers'])}")


if __name__ == "__main__":
    main()
