create keyspace if not exists ehr
with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use ehr;

create table if not exists events_by_id (
  event_id text primary key,
  patient_id text,
  event_type text,
  event_time timestamp,
  source text,
  payload text,
  patient_metadata text,
  ingestion_latency_ms double,
  raw_event text,
  processed_at timestamp
);

create table if not exists events_by_type (
  event_type text,
  event_id text,
  patient_id text,
  event_time timestamp,
  primary key ((event_type), event_id)
);

create table if not exists quarantine_by_reason (
  reason text,
  observed_at timestamp,
  event_id text,
  errors list<text>,
  raw_event text,
  primary key ((reason), observed_at, event_id)
) with clustering order by (observed_at desc, event_id asc);

create table if not exists alerts_by_patient (
  patient_id text,
  alert_time timestamp,
  alert_type text,
  details text,
  primary key ((patient_id), alert_time)
) with clustering order by (alert_time desc);

create table if not exists alerts_by_type (
  alert_type text,
  alert_time timestamp,
  patient_id text,
  details text,
  primary key ((alert_type), alert_time, patient_id)
) with clustering order by (alert_time desc, patient_id asc);

