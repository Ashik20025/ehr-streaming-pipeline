from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ehr_pipeline.validation import validate_event


def valid_event() -> dict:
    return {
        "event_id": "evt-1",
        "patient_id": "P000001",
        "event_type": "vital",
        "event_time": "2026-01-24T12:00:00Z",
        "source_emit_time": "2026-01-24T12:00:00Z",
        "source": "bedside-monitor",
        "schema_version": "ehr.v1",
        "payload": {"name": "heart_rate", "value": 88, "unit": "bpm"},
    }


class ValidationTests(unittest.TestCase):
    def test_valid_event_has_no_errors(self) -> None:
        self.assertEqual(validate_event(valid_event()), [])

    def test_direct_phi_is_rejected(self) -> None:
        event = valid_event()
        event["patient_name"] = "Jane Example"
        self.assertIn("contains_direct_phi", validate_event(event))

    def test_out_of_range_vital_is_rejected(self) -> None:
        event = valid_event()
        event["payload"]["value"] = 999
        self.assertIn("vital_out_of_range", validate_event(event))


if __name__ == "__main__":
    unittest.main()
