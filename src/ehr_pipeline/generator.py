from __future__ import annotations

import copy
import random
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Iterator


EVENT_TYPES = ("vital", "lab", "medication", "admission")
VITALS = (
    ("heart_rate", "bpm", 55, 145),
    ("systolic_bp", "mmHg", 88, 185),
    ("diastolic_bp", "mmHg", 48, 110),
    ("temperature_c", "C", 36.0, 40.5),
    ("spo2", "%", 88, 100),
)
LABS = (
    ("wbc", "10^9/L", 3.5, 18.0),
    ("lactate", "mmol/L", 0.6, 4.2),
    ("creatinine", "mg/dL", 0.4, 2.8),
)
MEDICATIONS = ("vancomycin", "ceftriaxone", "norepinephrine", "acetaminophen")
DEPARTMENTS = ("ED", "ICU", "medicine", "surgery", "cardiology")


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def synthetic_events(
    count: int,
    patient_count: int,
    invalid_rate: float,
    duplicate_rate: float,
    out_of_order_rate: float,
    late_rate: float,
    allowed_lateness_sec: int,
    seed: int,
) -> Iterator[dict[str, Any]]:
    rng = random.Random(seed)
    base_time = datetime.now(UTC).replace(microsecond=0)
    previous: list[dict[str, Any]] = []

    for index in range(count):
        if previous and rng.random() < duplicate_rate:
            duplicate = copy.deepcopy(rng.choice(previous[-200:]))
            duplicate["source_emit_time"] = iso_z(datetime.now(UTC))
            duplicate["injected_issue"] = "duplicate"
            yield duplicate
            continue

        patient_id = f"P{rng.randrange(patient_count):06d}"
        event_time = base_time + timedelta(seconds=index // max(1, patient_count // 20))

        injected_issue = "none"
        if rng.random() < late_rate:
            event_time -= timedelta(seconds=allowed_lateness_sec + rng.randint(20, 180))
            injected_issue = "late"
        elif rng.random() < out_of_order_rate:
            event_time -= timedelta(seconds=rng.randint(1, allowed_lateness_sec))
            injected_issue = "out_of_order"

        event_type = rng.choice(EVENT_TYPES)
        event = {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "patient_id": patient_id,
            "event_type": event_type,
            "event_time": iso_z(event_time),
            "source_emit_time": iso_z(datetime.now(UTC)),
            "source": rng.choice(("bedside-monitor", "lab-system", "ehr-hl7", "patient-portal")),
            "schema_version": "ehr.v1",
            "payload": _payload_for(event_type, rng),
            "injected_issue": injected_issue,
        }

        if rng.random() < invalid_rate:
            _corrupt(event, rng)
            event["injected_issue"] = "invalid"

        previous.append(copy.deepcopy(event))
        yield event


def _payload_for(event_type: str, rng: random.Random) -> dict[str, Any]:
    if event_type == "vital":
        name, unit, low, high = rng.choice(VITALS)
        return {"name": name, "value": round(rng.uniform(low, high), 1), "unit": unit}
    if event_type == "lab":
        name, unit, low, high = rng.choice(LABS)
        return {"test": name, "result": round(rng.uniform(low, high), 2), "unit": unit}
    if event_type == "medication":
        return {
            "medication": rng.choice(MEDICATIONS),
            "dose": round(rng.uniform(1, 500), 1),
            "unit": "mg",
            "route": rng.choice(("IV", "PO", "IM")),
        }
    return {"department": rng.choice(DEPARTMENTS), "acuity": rng.randint(1, 5)}


def _corrupt(event: dict[str, Any], rng: random.Random) -> None:
    choices = ("drop_patient", "bad_type", "bad_time", "bad_range", "phi")
    issue = rng.choice(choices)
    if issue == "drop_patient":
        event.pop("patient_id", None)
    elif issue == "bad_type":
        event["event_type"] = "fax"
    elif issue == "bad_time":
        event["event_time"] = "not-a-timestamp"
    elif issue == "bad_range":
        if event.get("event_type") == "vital":
            event["payload"] = {"name": "heart_rate", "value": 999, "unit": "bpm"}
        elif event.get("event_type") == "lab":
            event["payload"] = {"test": "lactate", "result": -2, "unit": "mmol/L"}
        else:
            event["payload"] = {"dose": -10}
    else:
        event["patient_name"] = "Jane Example"

