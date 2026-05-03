from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


REQUIRED_FIELDS = {
    "event_id",
    "patient_id",
    "event_type",
    "event_time",
    "source",
    "schema_version",
    "payload",
}
EVENT_TYPES = {"vital", "lab", "medication", "admission"}
PHI_FIELDS = {"patient_name", "ssn", "address", "phone", "email"}


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def validate_event(event: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing = sorted(field for field in REQUIRED_FIELDS if field not in event)
    if missing:
        errors.append("missing_" + "_".join(missing))
        return errors

    if PHI_FIELDS.intersection(event):
        errors.append("contains_direct_phi")
    if event["event_type"] not in EVENT_TYPES:
        errors.append("unknown_event_type")
    if not isinstance(event["payload"], dict):
        errors.append("payload_not_object")
    if not str(event["patient_id"]).startswith("P"):
        errors.append("bad_patient_id")
    try:
        parse_time(event["event_time"])
    except ValueError:
        errors.append("bad_event_time")

    payload_errors = _validate_payload(event.get("event_type"), event.get("payload", {}))
    errors.extend(payload_errors)
    return errors


def _validate_payload(event_type: str | None, payload: dict[str, Any]) -> list[str]:
    if event_type == "vital":
        name = payload.get("name")
        value = payload.get("value")
        if name not in {"heart_rate", "systolic_bp", "diastolic_bp", "temperature_c", "spo2"}:
            return ["bad_vital_name"]
        if not isinstance(value, (int, float)):
            return ["bad_vital_value"]
        ranges = {
            "heart_rate": (20, 250),
            "systolic_bp": (40, 260),
            "diastolic_bp": (20, 160),
            "temperature_c": (25, 45),
            "spo2": (50, 100),
        }
        low, high = ranges[name]
        return [] if low <= value <= high else ["vital_out_of_range"]

    if event_type == "lab":
        test = payload.get("test")
        result = payload.get("result")
        if test not in {"wbc", "lactate", "creatinine"}:
            return ["bad_lab_test"]
        if not isinstance(result, (int, float)):
            return ["bad_lab_result"]
        ranges = {"wbc": (0, 80), "lactate": (0, 20), "creatinine": (0, 20)}
        low, high = ranges[test]
        return [] if low <= result <= high else ["lab_out_of_range"]

    if event_type == "medication":
        if payload.get("medication") is None:
            return ["missing_medication"]
        dose = payload.get("dose")
        if not isinstance(dose, (int, float)) or dose <= 0:
            return ["bad_medication_dose"]
        return []

    if event_type == "admission":
        if payload.get("department") is None:
            return ["missing_department"]
        acuity = payload.get("acuity")
        if not isinstance(acuity, int) or not 1 <= acuity <= 5:
            return ["bad_acuity"]
    return []

