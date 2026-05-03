from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class Storage:
    def __init__(self, db_path: Path, quarantine_path: Path, reset: bool = True) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        if reset:
            db_path.unlink(missing_ok=True)
            quarantine_path.unlink(missing_ok=True)
        self.db_path = db_path
        self.quarantine_path = quarantine_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("pragma journal_mode=wal")
        self.conn.execute("pragma synchronous=normal")
        self._setup()

    def _setup(self) -> None:
        self.conn.executescript(
            """
            create table if not exists events (
                event_id text primary key,
                patient_id text not null,
                event_type text not null,
                event_time text not null,
                source text not null,
                payload_json text not null,
                inserted_at text default current_timestamp,
                was_out_of_order integer not null default 0
            );

            create table if not exists quarantine (
                id integer primary key autoincrement,
                reason text not null,
                event_json text not null,
                quarantined_at text default current_timestamp
            );

            create table if not exists alerts (
                id integer primary key autoincrement,
                patient_id text not null,
                alert_type text not null,
                event_time text not null,
                details_json text not null,
                created_at text default current_timestamp
            );

            create table if not exists run_metrics (
                id integer primary key check (id = 1),
                metrics_json text not null
            );
            """
        )
        self.conn.commit()

    def insert_event(self, event: dict[str, Any], was_out_of_order: bool) -> bool:
        try:
            self.conn.execute(
                """
                insert into events (
                    event_id, patient_id, event_type, event_time, source,
                    payload_json, was_out_of_order
                ) values (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["event_id"],
                    event["patient_id"],
                    event["event_type"],
                    event["event_time"],
                    event["source"],
                    json.dumps(event["payload"], sort_keys=True),
                    int(was_out_of_order),
                ),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def quarantine(self, event: dict[str, Any], reason: str) -> None:
        event_json = json.dumps(event, sort_keys=True)
        self.conn.execute(
            "insert into quarantine (reason, event_json) values (?, ?)",
            (reason, event_json),
        )
        with self.quarantine_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"reason": reason, "event": event}, sort_keys=True) + "\n")

    def insert_alert(self, patient_id: str, event_time: str, details: dict[str, Any]) -> None:
        self.conn.execute(
            """
            insert into alerts (patient_id, alert_type, event_time, details_json)
            values (?, ?, ?, ?)
            """,
            (patient_id, "possible_sepsis", event_time, json.dumps(details, sort_keys=True)),
        )

    def save_metrics(self, metrics: dict[str, Any]) -> None:
        self.conn.execute(
            "insert or replace into run_metrics (id, metrics_json) values (1, ?)",
            (json.dumps(metrics, sort_keys=True),),
        )
        self.conn.commit()

    def fetch_summary(self) -> dict[str, Any]:
        cur = self.conn.cursor()
        event_types = dict(cur.execute(
            "select event_type, count(*) from events group by event_type order by event_type"
        ).fetchall())
        reasons = dict(cur.execute(
            "select reason, count(*) from quarantine group by reason order by reason"
        ).fetchall())
        samples = cur.execute(
            "select reason, substr(event_json, 1, 220) from quarantine order by id limit 5"
        ).fetchall()
        alert_count = cur.execute("select count(*) from alerts").fetchone()[0]
        metrics_row = cur.execute("select metrics_json from run_metrics where id = 1").fetchone()
        metrics = json.loads(metrics_row[0]) if metrics_row else {}
        return {
            "event_types": event_types,
            "reasons": reasons,
            "samples": samples,
            "alert_count": alert_count,
            "metrics": metrics,
        }

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

