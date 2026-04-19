from __future__ import annotations

import time
from collections import defaultdict
from datetime import timedelta
from typing import Any, Iterable

from .metrics import Metrics
from .storage import Storage
from .validation import parse_time, validate_event


class EHRIngestionPipeline:
    def __init__(self, storage: Storage, allowed_lateness_sec: int = 30) -> None:
        self.storage = storage
        self.allowed_lateness = timedelta(seconds=allowed_lateness_sec)
        self.metrics = Metrics()
        self.seen_event_ids: set[str] = set()
        self.max_event_time = None
        self.patient_state: dict[str, dict[str, tuple[float, str]]] = defaultdict(dict)
        self.last_alert_time: dict[str, Any] = {}

    def run(self, events: Iterable[dict[str, Any]]) -> dict[str, Any]:
        for event in events:
            self.process(event)
        metrics = self.metrics.finish()
        self.storage.save_metrics(metrics)
        return metrics

    def process(self, event: dict[str, Any]) -> None:
        self.metrics.observe_event()
        start = time.perf_counter()

        errors = validate_event(event)
        if errors:
            self._quarantine(event, "|".join(errors))
            return

        event_time = parse_time(event["event_time"])
        if self.max_event_time is None or event_time > self.max_event_time:
            self.max_event_time = event_time
        watermark = self.max_event_time - self.allowed_lateness
        was_out_of_order = event_time < self.max_event_time

        if event["event_id"] in self.seen_event_ids:
            self.metrics.duplicates += 1
            self._quarantine(event, "duplicate_event_id")
            return

        if event_time < watermark:
            self.metrics.late += 1
            self._quarantine(event, "late_event")
            return

        inserted = self.storage.insert_event(event, was_out_of_order)
        if not inserted:
            self.metrics.duplicates += 1
            self._quarantine(event, "duplicate_event_id")
            return

        self.seen_event_ids.add(event["event_id"])
        self.metrics.accepted += 1
        if was_out_of_order:
            self.metrics.out_of_order += 1
        self._update_cep_state(event)
        self.metrics.observe_latency((time.perf_counter() - start) * 1000)

    def _quarantine(self, event: dict[str, Any], reason: str) -> None:
        self.metrics.quarantined += 1
        self.metrics.reasons[reason] += 1
        self.storage.quarantine(event, reason)

    def _update_cep_state(self, event: dict[str, Any]) -> None:
        patient_id = event["patient_id"]
        event_time = event["event_time"]
        payload = event["payload"]
        state = self.patient_state[patient_id]

        if event["event_type"] == "vital":
            state[payload["name"]] = (float(payload["value"]), event_time)
        elif event["event_type"] == "lab":
            state[payload["test"]] = (float(payload["result"]), event_time)

        hr = state.get("heart_rate", (0, ""))[0]
        temp = state.get("temperature_c", (0, ""))[0]
        wbc = state.get("wbc", (0, ""))[0]
        lactate = state.get("lactate", (0, ""))[0]
        deterioration = hr >= 110 and temp >= 38.0 and (wbc >= 12.0 or lactate >= 2.2)
        if not deterioration:
            return

        parsed_time = parse_time(event_time)
        last_time = self.last_alert_time.get(patient_id)
        if last_time and parsed_time - last_time < timedelta(minutes=10):
            return

        details = {
            "heart_rate": hr,
            "temperature_c": temp,
            "wbc": wbc,
            "lactate": lactate,
            "rule": "HR >= 110 and temp >= 38 and (WBC >= 12 or lactate >= 2.2)",
        }
        self.storage.insert_alert(patient_id, event_time, details)
        self.last_alert_time[patient_id] = parsed_time
        self.metrics.alerts += 1

