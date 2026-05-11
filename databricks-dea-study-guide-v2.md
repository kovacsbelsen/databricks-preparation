# Databricks Certified Data Engineer Associate — Complete Study Guide (v2, May 2026 exam)

> **Read this first.** This is built against the **current 7-domain exam guide** (Databricks Intelligence Platform / Ingestion / Transformation / Lakeflow Jobs / CI/CD / Troubleshooting / Governance). The product family is now called **Lakeflow** — what used to be Workflows/Jobs is **Lakeflow Jobs**, what used to be Delta Live Tables (DLT) is **Lakeflow Spark Declarative Pipelines**, and **Lakeflow Connect** is a new ingestion product. Most underlying mechanics (Delta, Spark, Auto Loader, UC) are unchanged — but the names, the CI/CD domain, and the troubleshooting domain are new and you must learn them.
>
> When this guide says "DLT", "Lakeflow Pipelines", or "Lakeflow Spark Declarative Pipelines", they refer to the same thing. The Python decorators (`@dlt.table`, etc.) still use the `dlt` module. Older docs and questions may still call it DLT — treat the terms as interchangeable for exam purposes.

---

## Table of Contents

1. [Exam Overview & 2-Week Study Plan](#1-exam-overview--2-week-study-plan)
2. [Domain 1 — Databricks Intelligence Platform](#2-domain-1--databricks-intelligence-platform)
3. [Domain 2 — Data Ingestion and Loading](#3-domain-2--data-ingestion-and-loading)
4. [Domain 3 — Data Transformation and Modeling](#4-domain-3--data-transformation-and-modeling)
5. [Domain 4 — Working with Lakeflow Jobs](#5-domain-4--working-with-lakeflow-jobs)
6. [Domain 5 — Implementing CI/CD](#6-domain-5--implementing-cicd)
7. [Domain 6 — Troubleshooting, Monitoring, and Optimization](#7-domain-6--troubleshooting-monitoring-and-optimization)
8. [Domain 7 — Governance and Security](#8-domain-7--governance-and-security)
9. [Syntax Cheat Sheet](#9-syntax-cheat-sheet)
10. [Most-Tested Gotchas](#10-most-tested-gotchas)
11. [Final-Week Checklist](#11-final-week-checklist)

---

## 1. Exam Overview & 2-Week Study Plan

### The exam
- **45 multiple-choice questions, 90 minutes** (~2 minutes per question)
- **Passing score: 70%** — you can miss ~13 questions
- No penalty for guessing — never leave a question blank
- Closed book, online or test center

### Approximate domain weighting
The official guide doesn't publish exact weights post-refresh, but based on the subdomain count and recent exam reports:

| Domain | Likely weight |
|---|---|
| 1. Databricks Intelligence Platform | ~10% |
| 2. Data Ingestion and Loading | ~20% |
| 3. Data Transformation and Modeling | ~22% |
| 4. Working with Lakeflow Jobs | ~12% |
| 5. Implementing CI/CD | ~13% |
| 6. Troubleshooting, Monitoring, Optimization | ~13% |
| 7. Governance and Security | ~10% |

### Question patterns to recognize

1. **"Which statement is true?"** — three plausible distractors + one correct. The discriminator is usually a single word (managed vs external, append vs overwrite, view vs streaming table, GRANT vs DENY).
2. **"A junior engineer wrote this code… what's wrong?"** — code debug. Look at object type, mode, and output sink.
3. **"Which approach satisfies these requirements?"** — match the feature to the use case: COPY INTO vs Auto Loader vs Lakeflow Connect; views vs materialized views vs streaming tables; row filter vs ABAC.
4. **"Fill in the blank in this command"** — syntax recall.
5. **NEW pattern — Spark UI screenshots / descriptions** — given a stage description, identify the bottleneck (skew, shuffle, spill).
6. **NEW pattern — YAML / CLI snippets** — given a `databricks.yml` fragment or a `databricks bundle ...` command, identify what it does.

### 2-week plan

**Week 1 — foundation + biggest domains**
- **Day 1:** Sections 1–2 (Overview + Platform). Drill ~60 questions on platform/compute.
- **Day 2:** Section 3 (Ingestion). Drill ~80 questions.
- **Day 3:** Section 4 first half (Transformation: SQL/PySpark basics, joins, cleaning). Drill ~80.
- **Day 4:** Section 4 second half (Gold-layer objects, Delta, MERGE). Drill ~80.
- **Day 5:** Section 5 (Lakeflow Jobs). Drill ~60.
- **Day 6:** Section 6 (CI/CD: Repos, Bundles, CLI). Drill ~60. *Brand-new domain — give it real time.*
- **Day 7:** Re-drill weak areas. Review §10 (Gotchas).

**Week 2 — production + governance + practice**
- **Day 8:** Section 7 (Troubleshooting/Monitoring/Optimization). Drill ~60. *Also new — give it real time.*
- **Day 9:** Section 8 (Governance/Security). Drill ~50.
- **Day 10:** Timed mock exam (45 questions). Review every miss.
- **Day 11:** Re-drill anything below 80% accuracy.
- **Day 12:** Second timed mock. Aim for 85%+.
- **Day 13:** Read §9 (Cheat Sheet) and §10 (Gotchas) carefully. Light review only.
- **Day 14:** Rest. Exam.

---

## 2. Domain 1 — Databricks Intelligence Platform

### 2.1 What "Data Intelligence Platform" means

The Databricks Data Intelligence Platform = **Lakehouse architecture** + **AI-aware metadata and optimization layers**. For the exam, treat the marketing term as shorthand for: open-format storage (Delta) + unified governance (Unity Catalog) + Lakeflow tooling for ingestion/transformation/orchestration + AI capabilities layered on top.

A **lakehouse** combines data-lake economics (cheap object storage, open formats) with data-warehouse guarantees (ACID, schema enforcement, fast queries). Delta Lake is the storage layer that makes this work.

Lakehouse properties:
- ACID transactions on object storage
- Schema enforcement + opt-in schema evolution
- Time travel (query a table as of version N or timestamp T)
- Unified batch and streaming
- Open format (Parquet under the hood)

### 2.2 Architecture: control plane vs data plane

- **Control plane** — managed by Databricks: web UI, notebooks (definitions), job/pipeline metadata, cluster manager. **Customer data does not flow through the control plane.**
- **Data plane** — in **your** cloud account (AWS/Azure/GCP): the actual cluster VMs and the data in object storage.

Exam question pattern: "Where is the data physically stored?" → **the customer's cloud object storage**, not Databricks.

### 2.3 Delta Lake — the storage layer

A Delta table is a directory containing:
```
/path/orders/
├── _delta_log/
│   ├── 00000000000000000000.json    # commit 0
│   ├── 00000000000000000001.json    # commit 1
│   └── 00000000000000000010.checkpoint.parquet
├── part-00000-...snappy.parquet
└── part-00001-...snappy.parquet
```

The `_delta_log` holds an ordered log of JSON commits. Each commit records files added, files removed, and schema changes. Data files are **never modified in place** — updates write new files and the log marks old ones as removed. Every 10 commits, a `.checkpoint.parquet` summarizes state so readers don't replay the whole log.

This is what gives Delta:
- **ACID** — the log entry is atomic
- **Time travel** — replay log up to version N
- **Concurrent writes** — optimistic concurrency on the log
- **Open format** — anyone with a Parquet reader can read the data files

### 2.4 Unity Catalog (deep coverage in Domain 7)

Unity Catalog is the unified governance layer for data and AI. Three-level namespace: `catalog.schema.table`. Replaces the legacy `hive_metastore` (which had only two levels). Centralizes access control, lineage, audit, and discovery.

### 2.5 Compute services — what runs your code

This is heavily tested. There are **three compute types** plus **one serverless variant** for each.

| Compute | Purpose | Lifecycle | Cost profile |
|---|---|---|---|
| **All-purpose cluster** | Interactive notebooks, dev, ad-hoc | Manual start/stop; auto-terminate after idle | Highest DBU rate; pay for idle time until termination |
| **Job cluster** | Runs one job, then dies | Created when job starts, terminated when job finishes | Lowest DBU rate; no idle time |
| **SQL warehouse** (formerly SQL endpoint) | Databricks SQL — dashboards, BI, ad-hoc SQL | Auto-start on query, auto-stop after idle | Separate DBU rate per tier |

**Serverless variants** exist for all three (Serverless SQL Warehouse, Serverless Jobs, Serverless Notebooks): compute runs in **Databricks' account** instead of yours, starts in seconds instead of minutes, billed by the second. More expensive per second of compute but often cheaper overall thanks to fast scaling and no idle time.

**SQL warehouse tiers:**
- **Classic** — compute in your cloud account; basic features
- **Pro** — adds Photon, query federation, geospatial, predictive I/O
- **Serverless** — compute in Databricks account; fastest startup

**Cluster modes:**
- **Single node** — driver only, no workers. Use for small data, ML on one machine, library testing.
- **Multi-node (Standard)** — 1 driver + N workers. Production default.

**Access modes** (Unity Catalog era):
- **Single user** — one named principal; supports all languages including Scala and ML runtimes
- **Shared** — multiple users simultaneously; enforces UC permissions per user; Python and SQL only
- **No isolation shared** — legacy; no UC enforcement
- **Custom** — for clusters predating these modes

> ⚠️ "High Concurrency" mode is **removed**. The replacement is **Shared** access mode under UC. If a question mentions High Concurrency, it's testing whether you know it's legacy.

**Runtime versions:**
- **Standard** — base Spark + Delta
- **ML** — adds TensorFlow, PyTorch, scikit-learn, MLflow pre-installed
- **Photon** — vectorized C++ execution engine, transparent 2–4× speedup for most SQL/DataFrame ops (not all — Python UDFs don't get Photon)
- **LTS (Long Term Support)** — ~2 years of support; use for production
- **Standard releases** — newer features, ~6 months of support

**Cost rule examiners love:** scheduled production work → **job clusters** (cheapest DBU rate). Interactive development → all-purpose. SQL/BI → SQL warehouses. Running production jobs on an all-purpose cluster is the textbook anti-pattern.

### 2.6 Notebooks

- **Default language** is set per notebook
- **Magic commands** override cell behavior:

| Magic | Purpose |
|---|---|
| `%python` / `%sql` / `%scala` / `%r` | Switch cell language |
| `%md` | Render as Markdown |
| `%sh` | Shell command (driver node) |
| `%fs` | Shortcut for `dbutils.fs` |
| `%run /path/to/notebook` | Inline-import another notebook (shares variables) |
| `%pip install pkg` | Notebook-scoped Python install |

> 📌 **`%run` shares state; `dbutils.notebook.run()` does not.** `%run` is like an import. `dbutils.notebook.run("path", timeout, args)` launches another notebook in a separate context and returns a string. This distinction shows up almost every exam.

`dbutils.notebook.exit("value")` ends the current notebook and returns a value to the caller.

### 2.7 Databricks Repos (Git folders)

- Sync notebook folders with GitHub / GitLab / Azure DevOps / Bitbucket
- Operations from the UI: clone, pull, commit, push, branch, switch, merge, PR
- Notebooks stored as source files (`.py`, `.sql`, `.ipynb`) so they version cleanly
- Foundation for the CI/CD domain (§6)

### 2.8 DBFS and Volumes

- **DBFS root** — default workspace storage. Avoid for production data; visible to all workspace users.
- **DBFS mounts** (`/mnt/...`) — pointers to external object storage. **Deprecated in favor of Unity Catalog external locations and Volumes.**
- **Volumes** (UC) — governed file storage for non-tabular files (uploads, ML models, archives). Path: `/Volumes/<catalog>/<schema>/<volume>/...`. Access governed by `READ VOLUME` / `WRITE VOLUME` grants.

---

## 3. Domain 2 — Data Ingestion and Loading

The Associate exam now tests **four ingestion mechanisms** and your ability to pick the right one. Memorize this decision table.

### 3.1 The ingestion decision table

| Method | When to use | Notes |
|---|---|---|
| **Auto Loader** (`cloudFiles`) | Continuous or scheduled ingest of **files** from cloud storage; large volumes; schema may evolve | Streaming, scales to billions of files, schema inference & evolution |
| **COPY INTO** | One-shot or periodic batch ingest of **files**; small/medium file counts | Idempotent SQL command, simpler than streaming |
| **Lakeflow Connect — managed connectors** | Ingest from **enterprise SaaS sources** (Salesforce, ServiceNow, Workday, SQL Server, etc.) | Fully managed by Databricks; point-and-click; lands in UC |
| **Lakeflow Connect — standard connectors** | File-based ingest with built-in Databricks tooling | Replaces older partner connectors for many cases |
| **JDBC/ODBC/REST in notebooks** | One-off, custom, low-volume, or proprietary sources | You write the code; you own retry/error handling |
| **Partner connectors** (Fivetran, Stitch, …) | Third-party-managed pipelines | Outside Databricks; integrates with UC |

### 3.2 COPY INTO

```sql
COPY INTO target_table
FROM '/Volumes/main/landing/orders/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

- File formats: `CSV`, `JSON`, `PARQUET`, `ORC`, `AVRO`, `TEXT`, `BINARYFILE`
- **Idempotent** — re-running with the same files is a no-op (it tracks loaded files)
- Pattern matching: `FROM '/path/' FILES = ('a.json', 'b.json')` or `PATTERN = '*.json'`
- Best for **thousands** of files; Auto Loader is better at **millions**
- Lands data into UC-governed tables when target is a UC table

### 3.3 Auto Loader (`cloudFiles`)

Streaming source that incrementally processes new files. Tracks which files it has seen via a checkpoint (and optionally a cloud notification queue).

```python
df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/main/chk/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/main/landing/events"))

(df.writeStream
   .format("delta")
   .option("checkpointLocation", "/Volumes/main/chk/bronze_events")
   .outputMode("append")
   .trigger(availableNow=True)          # process all new, then stop
   .toTable("main.bronze.events"))
```

Key options:
- `cloudFiles.format` — `json`, `csv`, `parquet`, `avro`, `orc`, `text`, `binaryFile`
- `cloudFiles.schemaLocation` — required for schema inference/evolution
- `cloudFiles.schemaEvolutionMode`:
  - `addNewColumns` (default) — stream restarts with new column added
  - `rescue` — unexpected data captured in a `_rescued_data` column, no restart
  - `failOnNewColumns` — fail the stream
  - `none` — ignore new columns silently
- `cloudFiles.useNotifications` — switch from directory listing to cloud event queue (cheaper at huge file counts)

**Auto Loader detection modes:**
- **Directory listing** (default) — lists files in the source path each trigger. Simple, scales to ~millions of files.
- **File notification** — Auto Loader subscribes to S3/ADLS/GCS events. Required for very high file counts; needs cloud permissions to create notification resources.

**"Batch mode" with Auto Loader** — use `.trigger(availableNow=True)` to get streaming semantics (checkpoints, exactly-once) with batch economics (cluster spins up, processes everything new, spins down). This is the standard pattern for scheduled ingest of files.

### 3.4 Lakeflow Connect

Lakeflow Connect is Databricks' native ingestion product. Two flavors on the exam:

- **Standard connectors** — built-in file-based ingestion paths (Auto Loader and COPY INTO are essentially standard-connector tooling). Use for files in object storage.
- **Managed connectors** — fully managed SaaS-source pipelines. Databricks operates the connector for you. Available for sources like Salesforce, ServiceNow, Workday, SQL Server (CDC), and others. Configured in the workspace UI; lands data directly into UC-governed Delta tables; handles incremental CDC, retries, schema evolution.

When to choose Lakeflow Connect managed over alternatives:
- You need data from a SaaS/enterprise source that has a managed connector
- You want Databricks (not you) to operate the pipeline
- You need governance (UC) end-to-end without gluing third-party tools

When to stick with Auto Loader / COPY INTO:
- Source is files in object storage, not a SaaS API
- You already have an upstream system landing files

### 3.5 JDBC/ODBC/REST in notebooks

For sources without a managed connector (legacy databases, custom APIs):

```python
# JDBC read
jdbc_df = (spark.read
             .format("jdbc")
             .option("url", "jdbc:postgresql://host:5432/db")
             .option("dbtable", "public.orders")
             .option("user", dbutils.secrets.get("scope", "user"))
             .option("password", dbutils.secrets.get("scope", "pwd"))
             .load())

# REST — use Python requests inside a notebook
import requests
resp = requests.get("https://api.example.com/orders",
                    headers={"Authorization": f"Bearer {token}"})
data = resp.json()
df = spark.createDataFrame(data)
```

Usually wrapped in a Lakeflow Job task on a schedule. Never paste credentials into notebooks — use **secret scopes**: `dbutils.secrets.get("scope", "key")`.

### 3.6 Semi-structured and unstructured ingest

Auto Loader, COPY INTO, and Lakeflow Connect all handle JSON, including nested structures. For querying nested data, use the colon/dot path syntax or `from_json` (covered in §4).

```python
# Land raw JSON, parse later
raw = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", "/chk/schema")
         .load("/Volumes/main/landing/events"))
```

For **binary files** (images, PDFs, audio): use `cloudFiles.format = binaryFile` — each file becomes a row with `path`, `modificationTime`, `length`, `content` (bytes).

### 3.7 Schema enforcement vs schema evolution

- **Enforcement** (default) — writes fail if the DataFrame's schema doesn't match the table's
- **Evolution** (opt-in) — new columns from the write are added to the table

```python
(df.write
   .option("mergeSchema", "true")    # opt in to evolution
   .mode("append")
   .saveAsTable("main.bronze.orders"))
```

```sql
-- Session-wide
SET spark.databricks.delta.schema.autoMerge.enabled = true;

-- Or manual
ALTER TABLE main.bronze.orders ADD COLUMNS (region STRING);
```

For overwrite-and-change-type, use `overwriteSchema = true`. To rename or drop columns you need **column mapping**:
```sql
ALTER TABLE t SET TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE t RENAME COLUMN old TO new;
```

---

## 4. Domain 3 — Data Transformation and Modeling

The largest domain — most questions are SQL/PySpark syntax recall.

### 4.1 Databases (schemas) and tables

```sql
CREATE SCHEMA IF NOT EXISTS main.sales;
USE CATALOG main;
USE SCHEMA sales;
SHOW SCHEMAS;
DESCRIBE SCHEMA EXTENDED main.sales;
DROP SCHEMA main.sales CASCADE;    -- removes tables too
```

### 4.2 Managed vs external tables

| | **Managed** | **External** |
|---|---|---|
| `LOCATION` clause | Not specified | Specified |
| Data lives | UC metastore-managed location | Wherever `LOCATION` points |
| `DROP TABLE` | **Deletes data** | Drops metadata, files remain |
| Predictive optimization eligible | **Yes** | No |
| Best for | Default — UC manages lifecycle | Data must outlive table; shared with external systems |

```sql
CREATE TABLE main.sales.customers (id INT, name STRING);                   -- managed Delta
CREATE TABLE main.sales.customers (id INT, name STRING)
  LOCATION 's3://bucket/customers';                                        -- external Delta
```

**Converting between managed and external:**
- External → managed: `ALTER TABLE ... SET TBLPROPERTIES ('delta.managed' = 'true')` is **not** how it's done. The supported approach is to deep-clone to a new managed table.
- The reverse uses similar patterns. Operationally, recreate via CTAS into the target managed/external form.

### 4.3 CTAS

```sql
CREATE TABLE silver.orders AS
SELECT * FROM bronze.orders WHERE status = 'completed';

CREATE OR REPLACE TABLE silver.orders AS ...;   -- atomic replace, preserves history
```

CTAS doesn't carry constraints, comments, or generated columns — declare those on a plain `CREATE TABLE`, then `INSERT`.

### 4.4 Generated columns and CHECK constraints

```sql
CREATE TABLE events (
  event_time TIMESTAMP,
  event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
);

ALTER TABLE events ADD CONSTRAINT valid_date
  CHECK (event_date >= '2020-01-01');
```

CHECK constraints fail the **whole transaction** on violation. Compare to DLT/Lakeflow expectations (§5.10), which can drop/quarantine rows instead.

### 4.5 Data cleaning: bronze → silver

A canonical silver-layer build looks like:

```python
from pyspark.sql import functions as F

bronze = spark.read.table("main.bronze.orders")

silver = (bronze
            # 1. Drop irrelevant rows
            .filter(F.col("status").isin("completed", "shipped"))
            # 2. Handle nulls
            .fillna({"region": "unknown", "discount": 0.0})
            .dropna(subset=["order_id", "customer_id"])
            # 3. Standardize types
            .withColumn("order_date", F.to_date("order_date_str", "yyyy-MM-dd"))
            .withColumn("amount",     F.col("amount").cast("decimal(10,2)"))
            # 4. Deduplicate
            .dropDuplicates(["order_id"])
            # 5. Rename for downstream
            .withColumnRenamed("cust_id", "customer_id"))

(silver.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable("main.silver.orders"))
```

Equivalent SQL:
```sql
CREATE OR REPLACE TABLE main.silver.orders AS
SELECT DISTINCT
  order_id,
  cust_id AS customer_id,
  COALESCE(region, 'unknown') AS region,
  COALESCE(discount, 0.0)     AS discount,
  TO_DATE(order_date_str, 'yyyy-MM-dd') AS order_date,
  CAST(amount AS DECIMAL(10,2))         AS amount
FROM main.bronze.orders
WHERE status IN ('completed', 'shipped')
  AND order_id IS NOT NULL
  AND cust_id  IS NOT NULL;
```

### 4.6 Joins

```sql
-- Standard joins
SELECT a.*, b.region FROM a INNER JOIN b ON a.id = b.id;
SELECT a.*, b.region FROM a LEFT  JOIN b ON a.id = b.id;
SELECT a.*, b.region FROM a RIGHT JOIN b ON a.id = b.id;
SELECT a.*, b.region FROM a FULL  JOIN b ON a.id = b.id;
SELECT a.*, b.* FROM a CROSS JOIN b;                  -- cartesian
SELECT a.*       FROM a SEMI  JOIN b ON a.id = b.id;  -- a rows that have a match (no b cols)
SELECT a.*       FROM a ANTI  JOIN b ON a.id = b.id;  -- a rows with NO match

-- Multiple keys
SELECT * FROM a INNER JOIN b ON a.id = b.id AND a.region = b.region;

-- Broadcast join hint (force broadcasting small side)
SELECT /*+ BROADCAST(b) */ * FROM a JOIN b ON a.id = b.id;
```

```python
# PySpark
a.join(b, on="id", how="left")
a.join(b, on=["id", "region"], how="inner")
a.join(F.broadcast(b), on="id", how="inner")    # broadcast hint
```

**Broadcast join** — copies the small side to every executor, avoiding shuffle. Auto-applied when smaller side < `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). Manually hint with `/*+ BROADCAST(t) */` or `F.broadcast(df)`.

### 4.7 Set operations

```sql
SELECT id FROM a UNION     SELECT id FROM b;   -- dedup
SELECT id FROM a UNION ALL SELECT id FROM b;   -- keep duplicates
SELECT id FROM a INTERSECT SELECT id FROM b;
SELECT id FROM a EXCEPT    SELECT id FROM b;   -- a MINUS b
```

`UNION` dedups; `UNION ALL` does not. `INTERSECT` and `EXCEPT` dedup by default.

### 4.8 Column / row / structure manipulation

```python
# Add / rename / drop
df.withColumn("price_eur", F.col("price") * F.lit(0.93))
df.withColumnRenamed("old", "new")
df.drop("a", "b")

# Split string into multiple columns
df.withColumn("first", F.split("full_name", " ").getItem(0)) \
  .withColumn("last",  F.split("full_name", " ").getItem(1))

# Filter
df.filter(F.col("price") > 100)
df.where("price > 100 AND region = 'US'")

# Explode arrays into rows
df.select("id", F.explode("items").alias("item"))
df.select("id", F.posexplode("items").alias("pos", "item"))  # with position

# Complex type access
df.select(F.col("address.city"))               # struct field
df.select(F.col("items")[0])                   # array index
df.select(F.col("properties")["color"])        # map key
```

### 4.9 Deduplication and aggregation

```python
df.dropDuplicates()                            # exact-row dedup
df.dropDuplicates(["order_id"])                # by key columns
df.distinct()                                  # alias for full-row dedup

(df.groupBy("region")
   .agg(F.count("*").alias("n_orders"),
        F.approx_count_distinct("customer_id").alias("uniq_customers"),
        F.mean("amount").alias("avg_amount"),
        F.sum("amount").alias("total"),
        F.min("amount").alias("min_amt"),
        F.max("amount").alias("max_amt")))

df.describe()         # count, mean, stddev, min, max for numeric cols
df.summary()          # describe + percentiles
df.summary("count", "min", "25%", "50%", "75%", "max")
```

`approx_count_distinct` uses HyperLogLog — much faster than `countDistinct` on large data, with a small (~2%) accuracy tradeoff.

### 4.10 Gold layer objects — UC table types

This is a **major exam topic** in the new guide.

| Object | What it is | Refresh | Storage | When to use |
|---|---|---|---|---|
| **Table** (managed/external) | Materialized data, you control writes | Whenever you write | On disk | Default for facts/dims; arbitrary writes |
| **View** | Stored SQL query; computed every read | None — always live | None | Lightweight transforms; security filters |
| **Materialized view** | Pre-computed query result | Scheduled or on-demand `REFRESH`; incremental where possible | On disk | Expensive aggregations queried often |
| **Streaming table** | Incrementally appended from a streaming source | Continuously or on each pipeline run | On disk | Append-only ingest at the bronze/silver boundary |

```sql
-- Regular view (recomputes each query)
CREATE OR REPLACE VIEW gold.active_customers AS
SELECT * FROM silver.customers WHERE status = 'active';

-- Materialized view (stored; refresh manages it)
CREATE OR REPLACE MATERIALIZED VIEW gold.daily_revenue AS
SELECT order_date, region, SUM(amount) AS revenue
FROM silver.orders
GROUP BY order_date, region;

-- Refresh on demand
REFRESH MATERIALIZED VIEW gold.daily_revenue;

-- Streaming table (typically inside a Lakeflow Pipeline)
CREATE OR REFRESH STREAMING TABLE bronze.events
AS SELECT * FROM STREAM read_files('/Volumes/main/landing/events', format => 'json');
```

**Decision rule for the exam:**
- Need always-fresh, cheap, on-the-fly logic → **view**
- Aggregation queried often, source changes infrequently → **materialized view**
- Append-only ingest of changing files / streams → **streaming table**
- Anything that doesn't fit above (full overwrites, MERGE-based silvers) → **table**

### 4.11 Complex types and JSON

```sql
-- Colon/dot path on JSON-string columns
SELECT raw:order_id, raw:customer.name, raw:items[0].sku FROM events;

-- Convert JSON string to struct
SELECT from_json(raw, 'order_id STRING, customer STRUCT<name: STRING>') AS parsed
FROM events;

-- Serialize back
SELECT to_json(struct_col) FROM ...;
```

### 4.12 Higher-order functions on arrays

```sql
SELECT FILTER(scores, x -> x >= 60)        AS passing FROM students;
SELECT TRANSFORM(prices, p -> p * 1.1)     AS with_tax FROM products;
SELECT EXISTS(roles, r -> r = 'admin')     AS is_admin FROM users;
SELECT REDUCE(nums, 0, (acc, x) -> acc + x) AS total FROM data;
```

Memorize the `x -> ...` lambda arrow.

### 4.13 Window functions

```sql
SELECT
  customer_id, order_id, amount,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn,
  RANK()       OVER (PARTITION BY customer_id ORDER BY amount DESC)     AS rk,
  DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC)     AS drk,
  LAG(amount)  OVER (PARTITION BY customer_id ORDER BY order_date)      AS prev,
  SUM(amount)  OVER (PARTITION BY customer_id ORDER BY order_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS running
FROM orders;
```

- `ROW_NUMBER` — always unique
- `RANK` — ties get same rank, **skips** next (1, 1, 3)
- `DENSE_RANK` — ties get same rank, **doesn't skip** (1, 1, 2)

### 4.14 MERGE INTO

```sql
MERGE INTO main.silver.customers t
USING staged_updates s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.is_deleted = true THEN DELETE
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *;
```

- One atomic operation does INSERT + UPDATE + DELETE
- Multiple `WHEN MATCHED` clauses — first match wins. Put DELETE conditions before UPDATE.
- `WHEN NOT MATCHED BY SOURCE` — handles target rows missing from source (full-sync pattern)

### 4.15 Data quality checks

Three places to validate data, in increasing strength:

1. **Programmatic checks** (read, count, assert) — flexible but ad-hoc
   ```python
   assert silver.filter("amount < 0").count() == 0, "negative amounts found"
   ```
2. **Delta CHECK constraints** — fail the whole transaction on any violation
   ```sql
   ALTER TABLE silver.orders ADD CONSTRAINT positive_amount CHECK (amount > 0);
   ```
3. **Lakeflow Pipelines (DLT) expectations** — drop, fail, or just record violations
   ```sql
   CREATE OR REFRESH STREAMING TABLE silver.orders (
     CONSTRAINT positive_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
   ) AS SELECT * FROM STREAM(LIVE.bronze_orders);
   ```

Decision rule:
- One-off / ad-hoc → programmatic
- Hard schema rule, never accept bad data → CHECK constraint
- Quality monitoring with quarantine / drop / failure tiers → DLT expectations

### 4.16 Spark tuning parameters

Listed explicitly in the exam guide — know what each one does.

| Parameter | Default | Effect |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | Number of partitions after shuffles (joins, aggregations). Too low → big partitions + spill. Too high → small partitions + overhead. |
| `spark.default.parallelism` | depends on cluster | Default partition count for raw RDDs and `parallelize` |
| `spark.executor.memory` | per-cluster | RAM per executor JVM |
| `spark.driver.memory` | per-cluster | RAM for the driver (the process running your notebook/job) |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Tables smaller than this get auto-broadcast in joins. `-1` disables. |

Set them in cluster config (Spark config field) or per-session:
```python
spark.conf.set("spark.sql.shuffle.partitions", 400)
```

**Tuning heuristic** (for the exam):
- Disk spill in stages → increase `shuffle.partitions` (more, smaller tasks) or increase executor memory
- Slow join with a small lookup table → bump `autoBroadcastJoinThreshold` or hint `BROADCAST`
- Driver OOM during `.collect()` → increase driver memory or avoid the collect

### 4.17 Delta time travel and maintenance

```sql
SELECT * FROM orders VERSION AS OF 5;
SELECT * FROM orders TIMESTAMP AS OF '2026-05-01';
RESTORE TABLE orders TO VERSION AS OF 5;

DESCRIBE HISTORY orders;
DESCRIBE DETAIL  orders;

OPTIMIZE orders;
OPTIMIZE orders ZORDER BY (customer_id);            -- ZORDER is being superseded by Liquid Clustering — see §7.4
OPTIMIZE orders WHERE order_date >= '2026-01-01';

VACUUM orders;                                       -- default 7 days (168h)
VACUUM orders RETAIN 168 HOURS;
```

> ⚠️ Default VACUUM retention is **7 days (168 hours)**. After VACUUM, time travel to versions whose files were deleted is no longer possible. Going below 168 hours requires disabling a safety check.

---

## 5. Domain 4 — Working with Lakeflow Jobs

### 5.1 What Lakeflow Jobs is

Lakeflow Jobs (formerly Databricks Jobs / Workflows) is the orchestration product. A **job** has one or more **tasks**, arranged as a DAG. Tasks run on clusters (job clusters by default).

### 5.2 Task types

A single job can chain heterogeneous tasks:
- **Notebook**
- **Python script** (file)
- **Python wheel**
- **JAR**
- **Spark Submit**
- **SQL** (query / dashboard refresh / alert / file)
- **Pipeline** (run a Lakeflow Spark Declarative Pipeline)
- **dbt**
- **Run job** (call another job)
- **If/else condition** (control flow)
- **For each** (looping)

### 5.3 Task dependencies and the DAG

Each task can depend on one or more upstream tasks. The DAG is visible in the UI. Common patterns:
- Linear chain: A → B → C
- Fan-out: A → {B, C, D}
- Fan-in: {A, B} → C
- Diamond: A → {B, C} → D

A downstream task can be configured to run **only if** all upstreams succeed (default), or also on failure, or always.

### 5.4 Control flow

The exam guide explicitly lists **retries** and **conditional tasks (branching and looping)**.

**Retries** (per-task):
- Max retries
- Min retry interval
- Retry on timeout
- Configured in the task UI or in the job JSON / bundle YAML

**If/else condition task** — evaluates a boolean expression (often against a previous task's value via `dbutils.jobs.taskValues`). Branches the DAG based on the result.

**For each task** — iterates over an input (list of values) and runs a sub-task for each, potentially in parallel. Used for partitioned backfills, multi-tenant runs, etc.

### 5.5 Sharing values between tasks

```python
# In task A
dbutils.jobs.taskValues.set("row_count", str(df.count()))

# In task B
value = dbutils.jobs.taskValues.get(taskKey="task_a", key="row_count")
```

`taskValues` are how an If/else condition gets the boolean it needs.

### 5.6 Triggers

The exam tests three trigger types — memorize all three.

| Trigger | When it fires |
|---|---|
| **Scheduled** (time-based) | Cron expression or simple UI schedule (every N min, daily, weekly) |
| **File arrival** (data-driven) | Files land in a watched cloud-storage path |
| **Table update** (data-driven) | A watched Delta table gets a new commit |
| **Continuous** | Job stays running, restarts on completion |
| **Manual** | "Run Now" button / API |

**Time-based vs data-driven — decision rule:**
- Source data has predictable cadence → **scheduled**
- Source data arrival is sporadic or bursty → **file arrival** or **table update**
- Source is many small frequent events → **continuous** (or `availableNow` streaming inside a scheduled job)

### 5.7 Notifications

- Email — on start / success / failure / duration threshold exceeded
- Webhook — POST to Slack, Teams, PagerDuty, etc.
- System destinations — admin-configured pre-set destinations

### 5.8 Cluster sharing across tasks

Tasks in the same job run can share a job cluster to save startup time. Tasks in **different** jobs can't share. For production, use job clusters with appropriate retry policies; use **pools** if cluster startup latency matters (warm VMs ready to attach).

---

## 6. Domain 5 — Implementing CI/CD

This is a **new domain**. Read this section carefully.

### 6.1 Databricks Repos workflow

Inside the workspace:
1. Open **Repos** → **Add Repo** → paste Git URL → clone
2. The repo appears as a folder with all standard Git operations available
3. Click the branch dropdown → **Create branch** for a feature branch
4. Edit notebooks/files; commit with a message
5. Push from the UI; create the pull request on GitHub/GitLab/Azure DevOps/Bitbucket
6. After PR merge, **Pull** the main branch back

Repos store notebooks as **source files** (`.py`, `.sql`, `.ipynb`), which is what makes them diffable and reviewable.

### 6.2 Declarative Automation Bundles (formerly Databricks Asset Bundles, "DABs")

> 📝 **Terminology note.** Databricks renamed Asset Bundles to **Declarative Automation Bundles**. The CLI commands and concepts (`databricks bundle ...`, `databricks.yml`) are unchanged. Older docs, examples, and exam questions use the old name. Both refer to the same thing.

A bundle is a **YAML-defined package** of Databricks resources — jobs, pipelines, ML experiments, dashboards — that you can validate, deploy, and run via the CLI. It's the Databricks-native way to do infrastructure-as-code for workspace assets.

**Typical project layout:**
```
my-project/
├── databricks.yml           # root config
├── resources/
│   ├── ingest_job.yml       # job definition
│   └── silver_pipeline.yml  # Lakeflow Pipeline definition
├── src/
│   ├── ingest.py
│   └── transform.sql
└── README.md
```

**Minimal `databricks.yml`:**
```yaml
bundle:
  name: orders_pipeline

variables:
  catalog:
    description: "Target catalog"
    default: dev_main
  notification_email:
    description: "Where alerts go"

include:
  - resources/*.yml

targets:
  dev:
    mode: development         # adds [dev <user>] prefix to all resource names
    default: true
    workspace:
      host: https://dev.cloud.databricks.com

  staging:
    mode: production
    workspace:
      host: https://staging.cloud.databricks.com
    variables:
      catalog: staging_main
      notification_email: data-eng-staging@co.com

  prod:
    mode: production
    workspace:
      host: https://prod.cloud.databricks.com
    variables:
      catalog: prod_main
      notification_email: data-eng-prod@co.com
```

**A job resource in `resources/ingest_job.yml`:**
```yaml
resources:
  jobs:
    ingest_orders:
      name: "Ingest orders [${bundle.target}]"
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ../src/ingest.py
            base_parameters:
              catalog: ${var.catalog}
          job_cluster_key: main
      job_clusters:
        - job_cluster_key: main
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
      email_notifications:
        on_failure:
          - ${var.notification_email}
```

**Substitutions you'll see:**
- `${var.<name>}` — variables defined in `variables:` or per-target
- `${bundle.target}` — the target name (dev/staging/prod)
- `${workspace.host}`, `${workspace.current_user.userName}`
- `${resources.jobs.<name>.id}` — reference another resource's runtime ID

**Targets** (formerly "environments") let you promote the **same codebase** across dev/staging/prod by changing variables — no code duplication.

**Modes:**
- `development` — adds a `[dev <username>]` prefix to all deployed resources, pauses schedules, uses single-user clusters, marks runs as dev. Safe for individual developer workspaces.
- `production` — no prefix, schedules active, must use a service principal in many configurations. Used for staging and prod targets.

### 6.3 Databricks CLI for bundles

```bash
# One-time setup
databricks auth login --host https://my.cloud.databricks.com

# In your bundle project
databricks bundle validate                  # check YAML, schema, references
databricks bundle deploy   -t dev           # deploy to dev target
databricks bundle deploy   -t prod
databricks bundle run      ingest_orders -t prod    # run a job in prod
databricks bundle destroy  -t dev           # remove resources from dev
```

`validate` is the dry run — always run it in CI before `deploy`.

**Other useful CLI commands** (general Databricks CLI, not just bundle):
```bash
databricks jobs list
databricks jobs run-now --job-id 123
databricks pipelines list
databricks fs ls /Volumes/main/landing/
databricks secrets list-scopes
databricks workspace export /Users/me/notebook
```

### 6.4 Typical CI/CD pipeline (GitHub Actions sketch)

```yaml
on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t staging

  deploy-staging:
    if: github.event_name == 'pull_request'
    needs: validate
    steps:
      - run: databricks bundle deploy -t staging
      - run: databricks bundle run integration_tests -t staging

  deploy-prod:
    if: github.ref == 'refs/heads/main'
    needs: validate
    steps:
      - run: databricks bundle deploy -t prod
```

The CI/CD pattern that questions test: **same code, different target, variables flip per environment.**

### 6.5 What CI/CD questions look like

- "What does `databricks bundle validate` do?" → Validates the YAML against the schema, resolves variables, checks references — without deploying.
- "How do you promote the same job across dev/test/prod without code changes?" → Bundle with multiple targets + variables.
- "What's the difference between `development` and `production` bundle mode?" → Dev adds username prefix, pauses schedules, single-user clusters; prod is the real thing.
- "Which file is the bundle root?" → `databricks.yml`
- "How do I run a job from CI?" → `databricks bundle run <job_name> -t <target>`

---

## 7. Domain 6 — Troubleshooting, Monitoring, and Optimization

Another **new domain** with big weight on the new exam.

### 7.1 Lakeflow Jobs run history

The Runs tab shows every execution of a job — duration, status, cluster used. Use it to:
- Spot run-time regressions (today's run 3× the median = something changed)
- Find the failure point in a DAG via the **task graph view** — failed tasks are red, upstream blockers are obvious
- Track failure rate over time

For pipeline tasks, drill into the **pipeline event log** for per-flow status and expectation results.

### 7.2 Spark UI diagnosis

Every cluster has a **Spark UI** accessible from the cluster page. The structure that matters for the exam:

- **Jobs** tab — one row per Spark job (an action like `count`, `save`, `show`)
- **Stages** tab — each job has stages (boundaries are shuffles)
- **Stage detail** — task list with metrics: duration, GC time, input size, shuffle read/write, spill (memory), spill (disk)
- **SQL/DataFrame** tab — query plans for SQL/DataFrame queries

### 7.3 Reading the three classic bottlenecks

**Data skew** — task time distribution is wildly uneven in one stage.
- *Symptom in stage UI:* max task time ≫ median task time (e.g., 10 min vs 30 s)
- *Cause:* a few partition keys have far more data than others (e.g., null keys, mega-customers)
- *Fix:* salt the join key (`key || rand_bucket`), use `BROADCAST` hint if small side fits, filter nulls separately, enable AQE skew handling

**Shuffle** — large data movement between stages.
- *Symptom:* large "Shuffle Read" / "Shuffle Write" bytes; many stages; long runtime in a join or aggregation
- *Cause:* wide transformations (joins, groupBy) on big data
- *Fix:* broadcast smaller side; reduce data before the shuffle (filter, project earlier); partition source data appropriately

**Disk spill** — tasks run out of memory and write intermediate state to disk.
- *Symptom:* nonzero "Spill (Memory)" and especially "Spill (Disk)" in stage metrics
- *Cause:* partitions too large to fit in executor memory; too few `shuffle.partitions`
- *Fix:* increase `spark.sql.shuffle.partitions` (more, smaller tasks); increase `spark.executor.memory`; avoid huge `collect()`s

### 7.4 Liquid Clustering

The modern alternative to partitioning **and** ZORDER.

```sql
-- At creation
CREATE TABLE main.silver.events (
  id BIGINT,
  event_time TIMESTAMP,
  region STRING,
  user_id BIGINT
) CLUSTER BY (region, event_time);

-- Change clustering keys later — no full rewrite required
ALTER TABLE main.silver.events CLUSTER BY (user_id);

-- Optimize applies clustering
OPTIMIZE main.silver.events;
```

**Why Liquid Clustering wins:**
- Replaces partitioning (which suffers from skewed partition sizes) and ZORDER (which requires rewrites to change)
- Clustering keys can be changed without rewriting the data
- Handles high-cardinality columns well (where partitioning would fail)
- Plays nicely with predictive optimization

**Exam-relevant rules:**
- Cannot mix partitioning and Liquid Clustering on the same table
- Up to 4 clustering keys recommended
- New writes are clustered automatically; `OPTIMIZE` reclusters existing data
- Available on UC managed Delta tables on recent runtimes

### 7.5 Predictive Optimization

Databricks-managed automatic optimization of UC managed Delta tables. Once enabled (at the metastore or catalog level), Databricks decides when to run:
- `OPTIMIZE` (file compaction)
- `VACUUM` (old file cleanup)
- `ANALYZE` (statistics for the query optimizer)

It uses query and write history to pick the right moments. **You stop scheduling these manually.**

Caveats:
- UC managed tables only — not external, not Hive-metastore
- Doesn't replace Liquid Clustering — they work together
- Runs as a Databricks-managed service; you pay for the compute it uses

### 7.6 Cluster startup, library, and OOM issues

**Cluster startup failures** — check the cluster's **Event Log** tab. Common causes:
- Cloud quota exceeded (too many VMs in the region) → request a quota increase or use a different instance type
- IAM/role permissions missing → fix the role/managed identity attached to the cluster
- Spot instance unavailability → switch to on-demand or another instance family
- Init script failure → check init-script logs

**Library conflicts** — symptoms include `NoSuchMethodError`, `ClassNotFoundException`, or "version X required, Y found".
- **Notebook-scoped libraries** (`%pip install`) — isolated to one notebook, no cluster restart needed, the safest fix
- **Cluster libraries** — installed on the whole cluster, affect everyone, require restart on change
- **Built-in libraries** — pre-installed in the runtime; check the runtime release notes for versions
- Conflict resolution: prefer notebook-scoped; pin versions; check if the runtime already has it

**Out of memory:**

| Where | Likely cause | Fix |
|---|---|---|
| **Driver OOM** | `collect()` of a big DataFrame; too many partitions sent to driver; very large broadcast | Increase `spark.driver.memory`; avoid `collect()`; reduce broadcast size |
| **Executor OOM** | Data skew; partitions too large; not enough `shuffle.partitions`; over-eager broadcast | Increase `spark.executor.memory`; bump `spark.sql.shuffle.partitions`; fix skew; disable auto-broadcast for problematic joins |

---

## 8. Domain 7 — Governance and Security

### 8.1 Managed vs external tables (revisited)

See §4.2. The Domain 7 angle is that UC manages permissions for both, but **only managed tables** are eligible for:
- Predictive optimization
- Some performance features tied to UC-managed storage credentials

### 8.2 The three-level namespace and the security hierarchy

```
Metastore → Catalog → Schema → Table / View / Function / Volume / Model
```

Permissions inherit downward, but can be overridden at any level.

```sql
USE CATALOG main;
USE SCHEMA  main.sales;
SELECT * FROM orders;                          -- resolves to main.sales.orders
SELECT * FROM analytics.marketing.campaigns;   -- fully qualified
```

### 8.3 GRANT, REVOKE, and DENY

```sql
GRANT  SELECT     ON TABLE   main.sales.orders TO `analysts`;
GRANT  MODIFY     ON TABLE   main.sales.orders TO `data_engineers`;
GRANT  USE CATALOG ON CATALOG main TO `analysts`;
GRANT  USE SCHEMA  ON SCHEMA  main.sales TO `analysts`;
GRANT  ALL PRIVILEGES ON SCHEMA main.sales TO `sales_admin`;

REVOKE SELECT     ON TABLE   main.sales.orders FROM `analysts`;

DENY   SELECT     ON TABLE   main.sales.salaries TO `interns`;

SHOW   GRANTS               ON TABLE main.sales.orders;
SHOW   GRANTS `user@co.com` ON TABLE main.sales.orders;
```

**Key rules:**
- To query `main.sales.orders` a user needs **all of**: `USE CATALOG` on `main`, `USE SCHEMA` on `main.sales`, **and** `SELECT` on the table. Missing one = access denied.
- **DENY overrides GRANT.** If a user is granted `SELECT` via a group but explicitly denied at the table level, the deny wins.
- `ALL PRIVILEGES` is a shorthand for every applicable privilege on the object.

**Privileges to know:**
- `SELECT` (read), `MODIFY` (INSERT/UPDATE/DELETE/MERGE)
- `CREATE TABLE` / `CREATE SCHEMA` / `CREATE FUNCTION` / `CREATE VOLUME`
- `USE CATALOG` / `USE SCHEMA`
- `EXECUTE` (call a function)
- `READ FILES` / `WRITE FILES` (external locations, volumes)
- `READ VOLUME` / `WRITE VOLUME`

**Principals:**
- **Users** — individual identities
- **Groups** — collections of users (and other groups); the recommended target of grants for manageability
- **Service principals** — non-human identities used by automation (CI/CD, jobs in production)

### 8.4 Column masking and row-level security

UC supports both as **native object-level** features (not just dynamic views).

**Column mask** — a function applied to a column at read time:
```sql
CREATE OR REPLACE FUNCTION main.sec.mask_ssn(ssn STRING)
RETURN CASE
  WHEN is_member('pii_readers') THEN ssn
  ELSE 'XXX-XX-' || RIGHT(ssn, 4)
END;

ALTER TABLE main.sales.customers
  ALTER COLUMN ssn SET MASK main.sec.mask_ssn;

-- Remove
ALTER TABLE main.sales.customers ALTER COLUMN ssn DROP MASK;
```

**Row filter** — a function evaluated per row; returns boolean:
```sql
CREATE OR REPLACE FUNCTION main.sec.region_filter(region STRING)
RETURN is_member('all_regions') OR region = current_user();

ALTER TABLE main.sales.orders
  SET ROW FILTER main.sec.region_filter ON (region);

-- Remove
ALTER TABLE main.sales.orders DROP ROW FILTER;
```

**Dynamic views** (the legacy approach) — encode masking/filtering inside the view definition:
```sql
CREATE OR REPLACE VIEW main.sales.customers_safe AS
SELECT
  id, name,
  CASE WHEN is_member('pii_readers') THEN ssn ELSE 'REDACTED' END AS ssn
FROM main.sales.customers
WHERE is_member('all_regions') OR region = current_user();
```

Useful built-ins: `current_user()`, `is_member('group_name')`, `is_account_group_member('group_name')`.

### 8.5 ABAC policies (Attribute-Based Access Control)

ABAC is the **central, scalable** way to apply masking and row filters. Instead of writing per-table `ALTER ... SET MASK` for hundreds of tables, you:
1. **Tag** columns/tables with attributes (e.g., `pii=true`, `region=EU`, `sensitivity=high`)
2. **Define a policy** that says: "for any column tagged `pii=true`, apply mask function X unless the user is in group Y"
3. UC applies the policy automatically wherever the tag is present

```sql
-- Tag a column
ALTER TABLE main.sales.customers ALTER COLUMN ssn SET TAGS ('pii' = 'true');

-- (Conceptual — exact policy syntax varies by region/release; check current docs)
-- Define an ABAC policy that masks any column tagged pii=true for non-pii_readers
```

**Why ABAC vs per-table masks:**
- Centralized — one policy covers thousands of columns
- Consistent — no risk of forgetting a table
- Audit-friendly — policies are reviewed and versioned centrally

For the Associate exam: know that ABAC exists, that it uses tags + policies, and that it's the centralized alternative to ALTER-per-table.

### 8.6 Volumes (UC-governed files)

```sql
CREATE VOLUME main.landing.raw_files;                              -- managed volume
CREATE EXTERNAL VOLUME main.landing.archive
  LOCATION 's3://bucket/archive';

-- Use in SQL
LIST '/Volumes/main/landing/raw_files/';
SELECT * FROM read_files('/Volumes/main/landing/raw_files/', format => 'json');

-- Grants
GRANT READ VOLUME  ON VOLUME main.landing.raw_files TO `data_engineers`;
GRANT WRITE VOLUME ON VOLUME main.landing.raw_files TO `ingest_service`;
```

Volumes replace DBFS mounts for governed file access.

---

## 9. Syntax Cheat Sheet

### SQL — create/write/read

```sql
-- Schema/table
CREATE SCHEMA IF NOT EXISTS sales;
CREATE TABLE t (id INT, name STRING);                            -- managed Delta
CREATE TABLE t (...) LOCATION 's3://b/t';                        -- external
CREATE TABLE t AS SELECT ...;                                    -- CTAS
CREATE OR REPLACE TABLE t AS SELECT ...;                         -- atomic replace
CREATE TABLE t (..., d DATE GENERATED ALWAYS AS (CAST(ts AS DATE)));
ALTER TABLE t ADD CONSTRAINT c CHECK (amount > 0);
CREATE TABLE t (...) CLUSTER BY (region, ts);                    -- Liquid Clustering

-- Materialized view & streaming table
CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT ...;
REFRESH MATERIALIZED VIEW mv;
CREATE OR REFRESH STREAMING TABLE st AS SELECT * FROM STREAM read_files(...);

-- Write
INSERT INTO t SELECT ...;
INSERT OVERWRITE t SELECT ...;
COPY INTO t FROM '/path/' FILEFORMAT = JSON COPY_OPTIONS ('mergeSchema' = 'true');

-- Read with time travel
SELECT * FROM t VERSION AS OF 5;
SELECT * FROM t TIMESTAMP AS OF '2026-05-01';

-- Inspect
DESCRIBE TABLE EXTENDED t;
DESCRIBE DETAIL t;
DESCRIBE HISTORY t;
SHOW TABLES IN schema_name;
SHOW CREATE TABLE t;
SHOW GRANTS ON TABLE t;

-- Modify
ALTER TABLE t ADD COLUMNS (region STRING);
ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE t ALTER COLUMN ssn SET MASK fn_mask;
ALTER TABLE t SET ROW FILTER fn_filter ON (region);
ALTER TABLE t CLUSTER BY (id);

-- Maintenance
OPTIMIZE t;
OPTIMIZE t ZORDER BY (col);             -- legacy; prefer CLUSTER BY
VACUUM t RETAIN 168 HOURS;
RESTORE TABLE t TO VERSION AS OF 5;

-- Permissions
GRANT  SELECT ON TABLE t TO `g`;
REVOKE SELECT ON TABLE t FROM `g`;
DENY   SELECT ON TABLE t TO `g`;
```

### PySpark — read/write/stream

```python
# Batch
df = spark.read.format("delta").load("/path")
df = spark.read.table("cat.sch.t")
df.write.format("delta").mode("append").saveAsTable("cat.sch.t")

# Stream — Auto Loader
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/chk/schema")
   .load("/Volumes/main/landing/x"))

# Stream — write
(stream.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", "/chk")
   .trigger(availableNow=True)
   .toTable("silver"))
```

### Lakeflow Spark Declarative Pipelines (DLT)

```python
import dlt

@dlt.table
def bronze():
    return spark.readStream.format("cloudFiles")...load(...)

@dlt.table
@dlt.expect_or_drop("ok", "amount > 0")
def silver():
    return dlt.read_stream("bronze").filter(...)
```

```sql
CREATE OR REFRESH STREAMING TABLE bronze
AS SELECT * FROM STREAM read_files('/Volumes/main/landing/', format => 'json');

CREATE OR REFRESH STREAMING TABLE silver (
  CONSTRAINT ok EXPECT (amount > 0) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM(LIVE.bronze);

-- CDC
APPLY CHANGES INTO LIVE.silver_customers
FROM STREAM(LIVE.bronze_changes)
KEYS (customer_id)
SEQUENCE BY change_ts
STORED AS SCD TYPE 1;
```

### Bundles & CLI

```bash
databricks bundle validate
databricks bundle deploy -t prod
databricks bundle run my_job -t prod
databricks bundle destroy -t dev
```

```yaml
# databricks.yml essentials
bundle: { name: my_project }
variables:
  catalog: { default: dev_main }
targets:
  dev:  { mode: development, default: true }
  prod: { mode: production, variables: { catalog: prod_main } }
```

---

## 10. Most-Tested Gotchas

Memorize these — they account for a disproportionate share of wrong answers.

1. **Managed vs external on DROP** — managed = data deleted; external = data remains. Presence of `LOCATION` makes it external.
2. **`%run` shares variables; `dbutils.notebook.run` does not.** First is import-like, second is orchestration.
3. **All-purpose vs job cluster** — production scheduled work goes on job clusters. Always.
4. **`Trigger.AvailableNow()` is the modern replacement for `Trigger.Once()`** and lets you do "scheduled streaming" — checkpoint semantics with batch cost.
5. **VACUUM default retention is 7 days (168 h).** Going below it requires disabling a safety check.
6. **CTAS doesn't carry constraints, comments, or generated columns.** Use plain CREATE + INSERT if you need them.
7. **`CREATE OR REPLACE TABLE` preserves history; DROP + CREATE wipes history.**
8. **Views recompute on read; materialized views are stored; streaming tables are incrementally appended.** The exam keeps asking which to use.
9. **Auto Loader needs a `schemaLocation`** for schema inference and evolution.
10. **Checkpoints are not interchangeable.** Two streams sharing a checkpoint location is corruption waiting to happen.
11. **DLT `expect` vs `expect_or_drop` vs `expect_or_fail`** — record / drop / kill. Practically guaranteed exam question.
12. **A user needs `USE CATALOG` + `USE SCHEMA` + `SELECT`** to query a UC table. Any missing = denied.
13. **DENY overrides GRANT** in Unity Catalog.
14. **`UNION` dedups, `UNION ALL` doesn't.** `INTERSECT` and `EXCEPT` dedup.
15. **ROW_NUMBER vs RANK vs DENSE_RANK** on ties: 1-2-3, 1-1-3, 1-1-2.
16. **MERGE's `WHEN MATCHED` clauses are evaluated top to bottom** — order matters.
17. **`COPY INTO` is idempotent** — re-running with same files is a no-op. That's why it's safe for retries.
18. **Photon doesn't accelerate Python UDFs.** Built-in SQL functions get the full benefit.
19. **`spark.sql.autoBroadcastJoinThreshold` default is 10 MB.** Set to `-1` to disable auto-broadcast.
20. **Disk spill → increase `shuffle.partitions` or executor memory.** Data skew → broadcast or salt the key.
21. **Driver OOM is almost always `.collect()` or oversized broadcast.** Executor OOM is usually skew or too few partitions.
22. **Bundle `mode: development`** adds a `[dev <user>]` prefix and pauses schedules. `mode: production` deploys as-is.
23. **`databricks bundle validate`** does not deploy — it checks YAML against the schema and resolves references.
24. **Targets, not environments** is the current bundle terminology. Variables differ per target; code does not.
25. **Predictive Optimization works only on UC managed tables.** External tables and Hive-metastore tables are not eligible.
26. **Liquid Clustering replaces partitioning AND ZORDER.** You can change clustering keys without rewriting the data.
27. **Lakeflow Connect managed connectors** are the answer when the source is a SaaS app (Salesforce, ServiceNow, etc.) and you don't want to operate the pipeline yourself.
28. **File arrival trigger vs scheduled trigger** — file arrival is data-driven (event-based), scheduled is time-based. Use file arrival when source cadence is unpredictable.
29. **ABAC = tags + policies.** It's the centralized alternative to per-table `ALTER ... SET MASK`.
30. **Service principals run production jobs**; users run interactive work. Production targets in bundles typically deploy under a service principal.

---

## 11. Final-Week Checklist

You should be able to do all of these from memory by exam day:

**Platform & compute**
- [ ] State the three cluster types and when each is right
- [ ] Explain control plane vs data plane and where data lives
- [ ] List the three access modes and what they restrict

**Ingestion**
- [ ] Pick between Auto Loader, COPY INTO, and Lakeflow Connect for a given scenario
- [ ] Write a complete Auto Loader read+write block with checkpointing
- [ ] Explain `availableNow` vs `processingTime` vs continuous triggers
- [ ] Choose `addNewColumns` vs `rescue` vs `failOnNewColumns` for schema evolution

**Transformation**
- [ ] Tell managed from external by sight and predict DROP behavior
- [ ] Write a MERGE with three WHEN clauses (delete + update + insert)
- [ ] Identify when to use view vs materialized view vs streaming table vs table
- [ ] Recall what each of the four Spark tuning params (`shuffle.partitions`, `default.parallelism`, `executor.memory`, `autoBroadcastJoinThreshold`) controls

**Lakeflow Jobs**
- [ ] Explain `%run` vs `dbutils.notebook.run`
- [ ] Configure retries, conditional branching, and for-each looping at the task level
- [ ] Pick between scheduled / file arrival / table update triggers

**CI/CD**
- [ ] Outline a bundle project layout (`databricks.yml`, `resources/`, `src/`)
- [ ] Explain `validate` / `deploy` / `run` / `destroy` CLI commands
- [ ] State the difference between bundle `development` and `production` modes
- [ ] Use variables and targets to promote code across dev/staging/prod

**Troubleshooting**
- [ ] Diagnose data skew, shuffle bottleneck, and disk spill from a Spark UI description
- [ ] Distinguish driver OOM from executor OOM by cause and fix
- [ ] Explain Liquid Clustering and what it replaces
- [ ] Explain Predictive Optimization and its eligibility rules

**Governance**
- [ ] Recall the three grants needed to query a UC table
- [ ] Apply DENY and know it overrides GRANT
- [ ] Write a column mask function and apply it with `ALTER TABLE ... SET MASK`
- [ ] Write a row filter function and apply it with `ALTER TABLE ... SET ROW FILTER`
- [ ] Explain ABAC at the level of "tags + policies, centralized"

If any item above is shaky, re-read the relevant section.

**Good luck — go pass it.**
