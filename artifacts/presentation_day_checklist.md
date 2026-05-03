# Presentation Day Checklist

## Before class

1. Open Docker Desktop if you want the live dashboard and Flink UI.
2. Open the slide deck:
   - `artifacts/EECS247_Final_Presentation_Hanuman_Akshintala.pptx`
3. Keep these artifacts ready:
   - `artifacts/experiments/experiment_summary.html`
   - `artifacts/distributed_report.html`
   - `artifacts/presentation_talk_track.md`

## Live demo path

From the project root:

```bash
./scripts/start_presentation_demo.sh
```

If Docker is running, this script opens:

- React dashboard: `http://localhost:5173`
- Flink UI: `http://localhost:8081`

If Docker is not running, it automatically opens the saved final report and the
experiment summary instead.

## Suggested presentation flow

1. Problem and motivation
2. Paper connection
3. Architecture
4. Validation and quarantine logic
5. Experiment setup
6. Final results and tradeoffs
7. Dashboard/report output
8. Limitations and future work

## Main numbers to remember

- Total experiment matrix events: `30,000`
- Best throughput: `33,783 events/sec`
- Lowest p95 latency: `783 ms`
- Latest 10,000-event run: `9,425 valid`, `575 quarantined`
- Accepted percentage: `94.25%`

## If someone asks why quarantine exists

The generator intentionally injects bad records such as duplicates, missing
fields, malformed timestamps, out-of-range values, and PHI-like fields. The
quarantine path shows that the pipeline does not silently accept bad data.
