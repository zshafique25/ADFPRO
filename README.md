# ADFPRO

## CDC Ingestion Pipeline — Azure Data Factory
 
Incremental (CDC-style) load of SQL tables into ADLS Gen2 Parquet, orchestrated by two ADF pipelines and backed by a Logic App for failure alerts.
 
## Architecture
 
```
Scheduled (daily trigger)
  └─ Execute Pipeline1 ──▶ Ingestion
        ├─ On Success: done
        └─ On Failure: Alerts (Web) ──▶ Logic App ──▶ Email
```
 
**Components**
- ADLS Gen2 storage account — `source` container, `monitor` directory (holds `cdc.json` checkpoint and the throwaway `empty.json`)
- Azure SQL DB — schema `source`, table `Orders` (parameterized, so other schema/table pairs work too)
- `cdc.json` — seeded at `1900-01-01` so the first run picks up the entire table
- Logic App (Consumption) — HTTP-triggered, sends the failure email
---
 
## Pipeline: `Ingestion`
 
Does the actual incremental load for one schema/table.
 
| Parameter | Default | Purpose |
|---|---|---|
| `schema` | `source` | target schema |
| `table` | `Orders` | target table |
| `backdate` | `2025-09-24` | manual override date; blank = trust checkpoint |
<img width="1285" height="507" alt="Ingestion pipeline" src="https://github.com/user-attachments/assets/3e0fd401-63ba-4b61-bc0a-3521f33b8aaf" />

<img width="995" height="222" alt="If new rows" src="https://github.com/user-attachments/assets/553485da-5a3b-4ed5-a7c7-eba100a0791c" />

### Activities
 
1. **`last_cdc`** (Lookup, `ls-datalake`)
   Reads `cdc.json` to get the last-processed timestamp.
2. **`Totalresults`** (Script, `ls-database`)
   Counts rows changed since the checkpoint (or since `backdate` if supplied):
```sql
   SELECT count(*) as total_count
   FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table}
   WHERE last_updated > '@{if(empty(pipeline().parameters.backdate),
                               activity('last_cdc').output.value[0].cdc_timestamp,
                               pipeline().parameters.backdate)}'
```
   The `if(empty(...))` expression is the key logic: blank `backdate` → trust `cdc.json`; supplied `backdate` → force a re-scan from that date without touching the checkpoint.
 
3. **`If New Rows`** (If Condition)
   `@greater(activity('Totalresults').output.resultSets[0].rows[0].total_count, 0)`
   - **False** → no-op (nothing changed since last run)
   - **True** →
     - **`Load`** (Copy) — same dynamic `WHERE last_updated > ...` query, Azure SQL → Parquet in ADLS Gen2
     - **`maxCDC`** (Script) — `SELECT MAX(last_updated) as cdc_timestamp FROM @{schema}.@{table}`, computes the new high-water mark
     - **`change_cdc`** (Copy) — reads a throwaway `empty.json`, adds an Additional Column `cdc_timestamp = @activity('maxCDC').output.resultSets[0].rows[0].cdc_timestamp`, and sinks into `cdc.json` — this overwrite is the checkpoint update for the next run
**Tested:** insert rows → confirm True branch fires and loads only the delta → re-run with no new rows → confirm False branch (no-op).
 
---
 
## Pipeline: `Scheduled`
 
Wraps `Ingestion` with a daily trigger and failure alerting.
 
| Parameter | Default | Purpose |
|---|---|---|
| `schema` | `source` | forwarded to `Ingestion` |
| `table` | `Orders` | forwarded to `Ingestion` |
| `backdate` | *(empty string)* | forwarded to `Ingestion`; left blank so scheduled runs always trust the checkpoint |
| `alertsUrl` | *(empty string)* | Logic App trigger URL used by the `Alerts` Web activity; supplied at trigger/deployment time, never checked in with a real value |
<img width="686" height="190" alt="Scheduled pipeline" src="https://github.com/user-attachments/assets/580c6cae-6ee4-488f-aec7-f9de3cf36279" />

### Activities
 
1. **`Execute Pipeline1`** (Execute Pipeline → `Ingestion`, wait on completion) — forwards `schema` / `table` / `backdate` straight through.
2. **`Alerts`** (Web activity, runs only when `Execute Pipeline1` **Failed**) — `POST`s to the Logic App trigger URL, now read from the `alertsUrl` pipeline parameter (`secureInput: true` so the resolved URL is masked in activity run history):
```json
   { "pipeline name": "@{pipeline().Pipeline}", "pipeline runid": "@{pipeline().RunId}" }
```
3. **Trigger** — daily recurrence. `backdate` is intentionally left blank for scheduled runs; it's only meant to be overridden manually for recovery/backfill runs.
 
---
 
## Logic App: Failure Alert
 
Consumption-tier workflow:
 
```
When an HTTP request is received  ──▶  Send an email (V2)
```
<img width="365" height="349" alt="Logic app" src="https://github.com/user-attachments/assets/3e7cf297-eb0f-454f-9d05-72c3e3fdb617" />

 
- **Trigger**: `When an HTTP request is received`, with a Request Body JSON Schema expecting `pipeline name` and `pipeline runid` (strings). Its callback URL is what's configured as the `Alerts` Web activity URL in `Scheduled`.
- **Action**: `Send an email (V2)` (Outlook connector)
  - Subject: `Pipeline Failed, Wake UP`
  - Body: includes the `pipeline name` and `pipeline runid` dynamic tokens from the trigger
 
---
 
## Recovery / Backfill Runs
 
To re-process data from an arbitrary date without disturbing the checkpoint, trigger `Scheduled` (or `Ingestion` directly) manually with `backdate` set to the desired date (e.g. `2025-01-01`). `cdc.json` is only updated by the `change_cdc` activity, so backfills never overwrite the running checkpoint.
 
