# ADFPRO

# CDC Ingestion Pipeline — Azure Data Factory
 
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
 

# REST API Ingestion — PokeAPI, Two Pagination Strategies
 
Two ADF pipelines that pull the full Pokémon list from the public [PokeAPI](https://pokeapi.co/api/v2/pokemon) into ADLS Gen2 as JSON. Both do the same job end-to-end but paginate through the API differently — one follows the API's own "next page" link, the other computes offsets itself. Which one to use depends entirely on whether the API you're integrating with hands you a next-page URL or not.
 
## Common setup
- **Linked service**: `ls_rest` — base URL set to the API root, authentication: Anonymous
- **Sink**: `ds_API` — Azure Data Lake Gen2, JSON, file path `sink/API/pokemon.json`
- **API shape**: `GET /api/v2/pokemon` returns a paged JSON body containing a `count` (total items — 1300 for Pokémon), a `next` URL (or `null`/absent when there's no more data), and a `results` array of 20 items per page
- At 20 items per call and ~1300 total items, a full pull takes ~65 API calls
Both pipelines follow the same two-activity shape:
 
```
Endpoint (Web)  ──▶  CopyAllData (Copy)
```
<img width="625" height="162" alt="REST API absoluteurl" src="https://github.com/user-attachments/assets/a25ae9d0-f63d-4566-860f-f6c38d1db5d4" />
 
1. **`Endpoint`** (Web Activity) — `GET https://pokeapi.co/api/v2/pokemon`. Used purely to probe the API once (e.g. to read `count`) before the paginated Copy activity runs.
2. **`CopyAllData`** (Copy Activity) — `RestSource` → `JsonSink`, with a **pagination rule** that tells ADF how to fetch every subsequent page automatically. This rule is the only thing that differs between the two pipelines.
---
 
## Pipeline: `RestAPI` — AbsoluteUrl pagination
 
Use this approach whenever the API response includes a direct link to the next page (the common, well-behaved case).
 
**Source dataset**: `ds_Rest_source` (base URL only — no relative path/query needed, since ADF gets the URL to call next straight from the response)
 
**Pagination rule**:
| Name | Value |
|---|---|
| `AbsoluteUrl` | `$.next` |
 
**How it works**: after each call, ADF reads the `next` field from the JSON response body and calls that exact URL for the following page. It keeps going until `next` is empty/null, at which point pagination stops automatically — no manual end condition needed.
 
**Setup steps**
1. Create pipeline `RestAPI`.
2. Add Web activity `Endpoint` → method `GET` → URL `https://pokeapi.co/api/v2/pokemon` → Debug to confirm it pulls data.
3. Add Copy activity `CopyAllData`, connected from `Endpoint`'s green (Success) output.
4. Source: `+ New` → REST → name `ds_Rest_source` → linked service `ls_rest` (base URL set, Anonymous auth) → Create.
5. Sink: `+ New` → Azure Data Lake Gen2 → JSON → name `ds_API` → file path `sink/API/pokemon.json`.
6. Debug — confirms only the first 20 records land initially (no pagination rule yet).
7. In `CopyAllData` → Source → Pagination rules → delete any existing rule → Add New → Name: `AbsoluteUrl`, Value: `$.next`.
8. Debug again — now the pipeline follows `next` across every page until it's empty, pulling all ~1300 records.
---
 
## Pipeline: `RestAPI_offset` — Query Parameter Offset pagination
 
Use this approach when the API does **not** return a next-page URL, so you have to compute the next page yourself.
 
**Source dataset**: `ds_rest_offset` — base URL already set on the linked service; relative URL: `?offset={offset}`
 
**Pagination rule**:
| Name | Value |
|---|---|
| Query Parameter `offset` | `RANGE:0:@{activity('Endpoint').output.count}:20` |
| End Condition `$.next` | `Empty` |
 
**How it works**:
- `RANGE:start:end:step` drives a loop over the `offset` query parameter: starting at `0`, going up to the total item count (read dynamically from `Endpoint`'s response `count` field), in steps of `20` (the page size) — i.e. `offset=0`, `offset=20`, `offset=40`, … up to ~1300.
- The **End Condition** (`$.next` = `Empty`) is a safety net: even though the range is computed from `count`, pagination also stops early if a response's `next` field comes back empty — covering cases where the total count changes mid-run or the API stops returning data sooner than expected.
**Setup steps**
1. Clone the `RestAPI` pipeline (or build fresh) and rename to `RestAPI_offset`.
2. Keep `Endpoint` (Web, `GET https://pokeapi.co/api/v2/pokemon`) — its `count` field feeds the range below.
3. In `CopyAllData` → Source → Pagination rules → delete the existing `AbsoluteUrl` rule.
4. Add rule: **Query parameter** `offset` → value:
```
   RANGE:0:@{activity('Endpoint').output.count}:20
```
5. Add rule: **End condition** `$.next` → `Empty`.
6. Source dataset `ds_rest_offset` — base URL from the linked service, relative URL `?offset={offset}`.
7. Debug — pipeline pulls all pages by incrementing `offset` in steps of 20 until the range is exhausted or `next` comes back empty, storing everything in `sink/API/pokemon.json`.
---
 
## Which one to use
 
| Scenario | Use |
|---|---|
| API response includes a usable "next page" URL | `RestAPI` (AbsoluteUrl) — simpler, no need to know total count upfront |
| API does **not** expose a next-page URL, but supports an `offset`/`limit`-style query parameter | `RestAPI_offset` (Query Parameter Offset + Range) |
 
## Notes / Caveats
- The offset approach depends on knowing the total item count (`count`) up front from a single probe call (`Endpoint`) — if the API doesn't expose a total count either, the range would need a hardcoded or otherwise-derived upper bound.
- This is a manual/on-demand pull (not scheduled) and pulls the full dataset from scratch each run — it does **not** implement incremental/CDC-style loading. For very large or frequently-changing datasets (millions of rows), a real-time/incremental design is more appropriate; this pattern is meant for pulling a full reference/lookup dataset such as a paginated public API listing.
- Pagination behavior is API-specific — always check the target API's docs to see whether it returns a next-page link (`AbsoluteUrl`) or expects the caller to compute paging (`offset`/`limit`, `page`, cursor tokens, etc.) before picking a rule.
- 
