# Azure Data Factory - Pipelines

# 1. CDC Ingestion Pipeline — Azure Data Factory
 
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

---

# 2. REST API Ingestion — PokeAPI, Two Pagination Strategies
 
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
---

# 3. Router Pipeline — Route Files to Destination Folders by Filename
 
Watches for a small "flag" file, then scans a source folder and routes every file it finds into the correct destination folder based on the file's name (e.g. `customers.csv` → `sink/customers/`, `drivers.csv` → `sink/drivers/`, `trips.csv` → `sink/trips/`). Built as a lightweight alternative to a Storage Event trigger.
 
## Architecture
 
```
Validate ──▶ Get Metadata ──▶ ForEachFile ──▶ Delete
                                  └─ SwitchFile (per item)
                                       ├─ customers ─▶ Copy Customers
                                       ├─ drivers   ─▶ Copy Drivers
                                       └─ trips     ─▶ Copy Trips
```
 
**Why this shape**: Storage Event triggers aren't available on every subscription tier and add cost, so this pipeline polls instead — it waits for a flag file to appear, then does the routing work, then deletes the flag file so the next run starts clean.
<img width="1291" height="347" alt="Router" src="https://github.com/user-attachments/assets/29f4e4ea-975d-45ac-95a1-d3bad6b3330b" />
<img width="257" height="610" alt="Router1" src="https://github.com/user-attachments/assets/d74d25ba-0917-482d-8e28-0168d45bfb93" />

---
 
## Step 1: `Validate` (Validation Activity)
 
Polls for a specific flag file before anything else runs.
 
- **Dataset**: `ds_csv_dynamic` → `p_container = source`, `p_folder = trigger`, `p_file = locations.csv`
- **Timeout**: `0.12:00:00` (12 hours)
- **Sleep**: `10` (seconds between existence checks)
While debugging, this activity keeps re-checking the `trigger` folder for `locations.csv`. Once that file is uploaded, the activity succeeds and the pipeline moves forward. This is effectively a manual, polling-based stand-in for a Storage Event trigger — the timeout/sleep can also be tuned or scheduled for a specific window (e.g. 12 PM–4 PM) so the pipeline only fires once per upload during business hours.
 
---
 
## Step 2: `Get Metadata`
 
Lists every file sitting in the folder that needs routing.
 
- **Dataset**: `ds_meta` (Azure Data Lake Gen2, CSV) → file path set to `source/files`, "First row as header" checked
- **Field list**: `childItems`
- **Depends on**: `Validate` (Succeeded)
`childItems` comes back as an array of objects, one per file:
```json
[
  { "name": "customers.csv", "type": "File" },
  { "name": "drivers.csv",   "type": "File" },
  { "name": "trips.csv",     "type": "File" }
]
```
 
---
 
## Step 3: `ForEachFile` (ForEach) → `SwitchFile` (Switch)
 
- **Items**: `@activity('Get Metadata').output.childItems` — iterates once per file found above
- Inside the loop, a **Switch** activity (`SwitchFile`) decides which Copy activity to run:
  - **On**: `@split(item().name,'.')[0]` — strips the extension off the current file's name (e.g. `customers.csv` → `customers`)
  - **Cases**:
| Case value | Activity | Sink folder |
|---|---|---|
| `customers` | `Copy Customers` | `sink/customers/` |
| `drivers` | `Copy Drivers` | `sink/drivers/` |
| `trips` | `Copy Trips` | `sink/trips/` |
| *(Default)* | — (empty) | file is silently skipped |
 
Each Copy activity uses the same **dynamic dataset** (`ds_csv_dynamic`) on both source and sink, driven by three dataset parameters — `p_container`, `p_folder`, `p_file` — instead of needing a separate hardcoded dataset per file type:
 
- **Source**: `p_container = source`, `p_folder = files`, `p_file = @item().name`
- **Sink**: `p_container = sink`, `p_folder = customers` / `drivers` / `trips` (matching the case), `p_file = @item().name`
This is what lets one dataset + one Copy-activity pattern serve all three file types — only the sink's `p_folder` value changes per case.
 
---
 
## Step 4: `Delete`
 
- **Depends on**: `ForEachFile` (Succeeded)
- **Dataset**: `ds_csv_dynamic` → `p_container = source`, `p_folder = trigger`, `p_file = locations.csv`
- **Enable logging**: unchecked (off)
Removes the flag file from the `trigger` folder once routing is complete, so the pipeline is ready to detect the *next* upload rather than re-triggering on the same file.
 
---
 
## End-to-end flow
 
1. Someone uploads `customers.csv`, `drivers.csv`, `trips.csv` into `source/files/`.
2. Someone (or an upstream process) uploads `locations.csv` into `source/trigger/` — this is the signal that files are ready.
3. `Validate` detects `locations.csv` and lets the pipeline proceed.
4. `Get Metadata` lists everything in `source/files/`.
5. `ForEachFile` + `SwitchFile` routes each file to its matching destination folder based on filename.
6. `Delete` removes `locations.csv` from `trigger/`, resetting for the next run.
---
 
## Notes / Things to double-check
 
- **Case values are case- and spelling-sensitive.** `SwitchFile` matches on the exact string produced by `split(item().name,'.')[0]`. If a source file is actually named `customer.csv` (singular) but the switch case is `customers` (plural), it won't match — it silently falls into the empty **Default** case and never gets copied, with no error or alert raised. Worth confirming the real file-naming convention in `source/files/` matches the case values exactly (`customers`, `drivers`, `trips`).
- **Unmatched files are silent.** Since Default has no activities, any file that doesn't match one of the three cases is simply skipped with no logging or notification. Consider adding a Default action (e.g. logging the unmatched filename, or a failure/alert branch) if unexpected files showing up in `source/files/` should be visible.
- **This is a polling design, not an event-driven one.** The `Validate` activity's `sleep`/`timeout` control how promptly and how long it waits for the flag file — tune these (or run the pipeline on a schedule/window) based on how quickly routing needs to happen after upload.
- **Storage Event triggers were intentionally avoided** here due to cost and subscription-tier availability; if that changes, replacing `Validate` with a native Storage Event trigger on `source/trigger/` would remove the polling overhead entirely.
---

# 4. Schema Pipeline — Dynamic Column Mapping in ADF
 
Copies every file from a source folder to a destination folder using **one** Copy activity, while applying a **different column mapping (translator) per file** — `customers.csv` gets the customers mapping, `drivers.csv` gets the drivers mapping, `trips.csv` gets the trips mapping — instead of needing a separate Copy activity per file type.
 
## Why this exists
A single dynamic dataset (`ds_csv_dynamic` + a `ForEach` over the source folder) already lets one Copy activity move any file by filename. The remaining problem is **schema**: each file has different columns, so the mapping ("translator") can't be hardcoded — it has to be picked dynamically based on which file is currently being copied. This pipeline solves that by storing each file's mapping as a **pipeline parameter** and picking the right one at runtime with an `if/equals` expression.
 
## Architecture
 
```
Get Metadata ──▶ ForEachFile
                     └─ Data Load (Copy, translator picked per-file)
```
 
- **`Get Metadata`** — dataset `ds_meta`, field list `childItems`, lists every file in `source/files/`.
- **`ForEachFile`** — iterates `@activity('Get Metadata').output.childItems`; inside, a single Copy activity (`Data Load`) handles every file.
<img width="640" height="365" alt="Schema" src="https://github.com/user-attachments/assets/1a7419ce-3aaa-4127-87ae-f9bb9bb27343" />

---
 
## Pipeline parameters — one schema per file
 
Three parameters hold the actual column mappings, each pasted in as a full `TabularTranslator` JSON object:
 
| Parameter | Type | Holds |
|---|---|---|
| `customers_schema` | object | Mapping for `customers.csv` (`customer_id`, `first_name`, `last_name`, `email`, `phone_number`, `city`, `signup_date`, `last_updated_timestamp`) |
| `drivers_schema` | object | Mapping for `drivers.csv` (`driver_id`, `first_name`, `last_name`, `phone_number`, `vehicle_id`, `driver_rating`, `city`, `last_updated_timestamp`) |
| `trips_schema` | object | Mapping for `trips.csv` (`trip_id`, `driver_id`, `customer_id`, `vehicle_id`, `trip_start_time`, `trip_end_time`, `start_location`, `end_location`, `distance_km`, `fare_amount`, `payment_method`, `trip_status`, `last_updated_timestamp`) |
 
Each default value is a full source→sink column mapping (kept 1:1, same names/types — the point here is dynamic *selection* of mappings, not transformation).
 
## `Data Load` (Copy Activity)
 
**Source** (`ds_csv_dynamic`): `p_container = source`, `p_folder = files`, `p_file = @item().name`
**Sink** (`ds_csv_dynamic`): `p_container = sink`, `p_folder = schema`, `p_file = @item().name`
 
**Translator** — instead of a static mapping, this is an expression that switches on the current file's name:
```
@if(equals(item().name,'customers.csv'), pipeline().parameters.customers_schema,
    if(equals(item().name,'drivers.csv'), pipeline().parameters.drivers_schema,
        pipeline().parameters.trips_schema))
```
For each file the loop hits, this resolves to exactly one of the three parameter objects and ADF applies that mapping for the copy. Since `trips_schema` is the final `else`, any file that isn't `customers.csv` or `drivers.csv` falls through to the trips mapping by default.
 
---
 
## How this was built (repeatable process)
 
1. **Get one mapping working manually first.** Build a throwaway Copy activity with source = `customers.csv` (hardcoded `p_file`), sink = `schema/customers.csv`, then in **Mapping → Import schemas** let ADF infer the column list — this is the fastest way to get a correct mapping without typing it by hand.
2. **Grab the generated code.** Open the Copy activity's **Code** view and copy everything from `"translator"` onward (the mapping JSON ADF just built for you).
3. **Delete the throwaway Copy activity** — it was only there to generate the mapping code.
4. **Repeat step 1–2 for each file** (`drivers.csv`, `trips.csv`) to get all three mapping blocks.
5. **Create the pipeline parameters.** In the pipeline's Parameters tab, add `customers_schema`, `drivers_schema`, `trips_schema` (all type `object`), and paste each file's generated mapping JSON in as that parameter's default value.
6. **Build the real dynamic pipeline**: `Get Metadata` (dataset `ds_meta`, field list `childItems`) → `ForEachFile` (Items = child items) → inside, one Copy activity (`Data Load`) using the dynamic dataset for source/sink.
7. **Set the translator as an expression** (not a static mapping) using the `if(equals(...))` chain above, referencing the three parameters.
8. Debug, confirm all three files land in `sink/schema/` with the correct columns, then publish.
---
 
## Scaling beyond 3 file types
 
Hardcoding one pipeline parameter per file type works fine for a handful of schemas, but doesn't scale to, say, 100 file types. For that case, consider instead:
- Storing all mappings as one JSON array/config file (e.g. `{ "customers.csv": {...}, "drivers.csv": {...}, ... }`) and looking up the right mapping by filename via a `Lookup` activity, or
- Storing the mapping definitions in a SQL table (one row per file type) and driving the translator lookup from a query, rather than a hardcoded `if/equals` chain per file.
Both avoid needing a new pipeline parameter and a new `if(equals(...))` branch every time a new file type is added.
 
## Notes / Things to double-check
- The `if/equals` chain matches on **exact filename**, including extension (`'customers.csv'`, `'drivers.csv'`) — case-sensitive and extension-sensitive. Any filename that doesn't match either of the first two conditions silently falls through to `trips_schema`, so an unexpected file (e.g. a typo'd name or a 4th file type accidentally dropped into `source/files/`) would get copied using the wrong mapping rather than failing loudly.
- All three schemas currently keep source and sink column names identical (straight pass-through mapping with type conversion allowed and truncation permitted) — if any target table needs renamed or reordered columns, that's where the mapping objects would need actual source→sink differences rather than 1:1 copies.
---

# 5. Deltalake Pipeline — Upsert into Delta Lake (Silver Layer)
 
Reads a CSV, applies a couple of light transformations, and **upserts** (insert new + update existing) the result into a Delta Lake table — keyed on `location_id`. Built with a Mapping Data Flow rather than a plain Copy activity, since Delta merge/upsert logic needs the data flow engine (Copy activities can only append/overwrite, not merge on a key).
 
## Why Data Flow instead of Copy Data
 
| | Use for |
|---|---|
| **Copy Data** | Moving data from source into the **bronze** layer — straight land-as-is copies |
| **Data Flow** | Moving data from **bronze into silver/gold** — anything needing transformations, upserts, or Delta merge logic |
 
Upserts specifically require Data Flow: it's low-code/no-code on the surface, but runs as generated Spark under the hood, which is what makes key-based merge (update-if-matched, insert-if-not) possible.
 
## Architecture
 
```
Deltalake (pipeline)
  └─ Data flow (ExecuteDataFlow) ──▶ deltaflow (Mapping Data Flow)
                                       source → UpperState → selectCols → alterRow → sinkDelta
```
 
---
 
## Pipeline: `Deltalake`
 
One activity: **`Data flow`** (Execute Data Flow), pointing at the `deltaflow` Mapping Data Flow.

<img width="315" height="171" alt="Deltalake" src="https://github.com/user-attachments/assets/5ee86afd-02cc-4ca6-9916-41b9f9b95804" />
 
**Dataset parameters passed to the data flow's `source`:**
| Parameter | Value |
|---|---|
| `p_container` | `source` |
| `p_folder` | `Deltasource` |
| `p_file` | `locations.csv` |
 
**Compute**: `coreCount: 8`, `computeType: General` (per notes: use **Small** compute size for this workload)
**Trace level**: `None` (matches "Logging level: None" from notes)
**Cache sinks**: `firstRowOnly: true`

---
 
## Data Flow: `deltaflow`

<img width="1741" height="197" alt="Deltaflow" src="https://github.com/user-attachments/assets/14391b2d-0905-45a8-b564-b88c5cea6746" />

### 1. `source`
Reads `ds_csv_dynamic` with an explicit projection:
 
| Column | Type |
|---|---|
| `location_id` | short |
| `city` | string |
| `state` | string |
| `country` | string |
| `latitude` | double |
| `longitude` | double |
| `last_updated_timestamp` | timestamp |
 
Settings: `allowSchemaDrift: true` (lets the CSV's schema evolve over time without breaking the flow), `validateSchema: false` (only worth turning on if the schema is guaranteed never to change), `ignoreNoFilesFound: false`.
 
### 2. `UpperState` (Derived Column)
```
state = upper(state)
```
Normalizes the `state` column to uppercase.
 
### 3. `selectCols` (Select)
Maps forward only: `location_id`, `city`, `state`, `country`, `last_updated_timestamp` — **`latitude` and `longitude` are dropped here** and don't reach the sink.
 
### 4. `alterRow` (Alter Row)
```
upsertIf(1==1)
```
Marks every row as an upsert candidate (the `1==1` condition is always true, so this applies to 100% of incoming rows — no rows are filtered out at this stage).
 
### 5. `sinkDelta` (Sink)
- **Type**: Inline dataset, format `delta`
- **Linked service**: `ls_datalake`
- **Path**: `sink/deltasink`
- **Row policies**: `insertable: true`, `upsertable: true`, `updateable: false`, `deletable: false` — new rows are inserted and existing rows matching the merge key are upserted; a separate/plain "update" and "delete" are not enabled
- **Merge key**: `location_id` — this is what determines whether an incoming row is treated as a new insert or an update to an existing row
- **Other settings**: `mergeSchema: false`, `autoCompact: false`, `optimizedWrite: false`, `vacuum: 0`, no compression
---
 
## Upsert logic, visually
 
```
Existing table:        Incoming write:        Result:
  1, 2, 3        +        1, 2, 4, 5      =    1 (updated), 2 (updated), 3 (untouched), 4 (inserted), 5 (inserted)
```
Rows whose `location_id` already exists get updated in place; rows with a new `location_id` get inserted; rows not present in the incoming batch are left alone.
 
---
 
## How this was built (repeatable process)
 
1. Create folder `source/Deltasource` and upload `locations.csv`.
2. Create pipeline `Deltalake`, drag in a **Data flow** activity, settings → `+New` → build/name the data flow `deltaflow`.
3. In the data flow: **Add Source** → name `source` → source type: dataset → `ds_csv_dynamic`.
4. Turn on **Data Flow Debug** (spins up a live cluster) to preview data as you build.
5. In source **debug settings → parameters**, set `p_container = source`, `p_folder = Deltasource`, `p_file = locations.csv` so previews reflect the real file.
6. Review source options:
   - **Allow schema drift** — lets future columns show up without breaking the flow
   - **Validate schema** — only if the schema is guaranteed stable
   - **Skip line count** — to skip header/junk rows if needed
   - **Sampling** — pull a small sample for faster iteration while building/debugging
7. Add a **Derived Column** transformation named `UpperState`: column `state`, expression `upper(state)`. Refresh Data Preview to confirm.
8. Add a **Select** transformation named `selectCols`: remove `latitude` and `longitude`. Refresh Data Preview.
9. Add an **Alter Row** transformation named `alterRow`: condition `Upsert if` → `1==1` (passes all rows through as upsert candidates).
10. Add a **Sink** named `sinkDelta`:
    - Sink type: Inline, dataset type: Delta
    - Linked service: `ls_datalake`
    - File path: `sink/deltasink`
    - No compression, vacuum: 0
    - Enable **Allow upsert**, set merge **key column**: `location_id`
11. Refresh Data Preview once more to confirm the end-to-end result, then **turn off the data flow debug cluster** and save (leaving debug on burns compute unnecessarily).
12. Back in the `Deltalake` pipeline, set the Execute Data Flow activity's parameter values, **Compute size: Small**, **Logging level: None**, then **Debug → Use activity runtime**.
13. Once it runs successfully, save, open a Pull Request, and merge into the `master` branch.
14. On the `master` branch in ADF, the pipeline is already present from the merge — click **Publish**.
## Notes / Things to double-check
- **Update and delete are off at the sink** (`updateable: false`, `deletable: false`) even though `alterRow` marks every row as an upsert candidate. This is expected for a pure insert-or-update-on-match upsert pattern (upsert covers both the "new row" and "matching row changed" cases together), but if a genuine separate "update-only" or "delete" scenario is ever needed, those flags would need to be enabled explicitly.
- **`mergeSchema: false`** means if the source CSV picks up new columns in the future (allowed via schema drift on the source), those new columns won't automatically propagate into the Delta table structure — they'd need `mergeSchema: true` or a manual schema update to land in the sink.
- **`vacuum: 0`** means no automatic cleanup of old Delta file versions is happening — worth revisiting if storage/versioning cost becomes a concern over time.
- The merge key is `location_id` alone — make sure this is genuinely unique per real-world location in the source data; a non-unique key here would cause updates to apply non-deterministically across matching rows.
  
---
