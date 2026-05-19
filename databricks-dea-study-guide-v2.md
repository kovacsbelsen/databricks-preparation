# Databricks Certified Data Engineer Associate — Expanded Complete Study Guide (v3, May 2026 exam)

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
12. [Expansion Pack — Missing Topics, Deeper Syntax, and Exam-Style Decision Rules](#12-expansion-pack--missing-topics-deeper-syntax-and-exam-style-decision-rules)

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

---

# 12. Expansion Pack — Missing Topics, Deeper Syntax, and Exam-Style Decision Rules

This section extends the original guide with the topics that are easy to miss in the May 2026 exam outline: Databricks Connect, Lakeflow Spark Declarative Pipelines, Lakeflow Jobs control flow, resource ownership, progress tracking, storage objects, Unity Catalog sharing/federation, and more complete SQL/PySpark syntax.

## 12.1 Official coverage matrix — what every exam bullet maps to

| Exam area | You must recognize | You must be able to choose / write |
|---|---|---|
| Platform architecture | Control plane, data plane, workspace, metastore, catalog, schema, table, volume | Which component owns metadata, compute, data files, governance, lineage, jobs |
| Compute | All-purpose, job cluster, SQL warehouse, serverless, Photon, cluster policies, pools | Correct compute for dev, scheduled ETL, BI, dashboarding, low-admin production |
| Ingestion | COPY INTO, Auto Loader, Lakeflow Connect, JDBC/ODBC/REST, partner connectors | Syntax for COPY INTO and Auto Loader; decision between batch, streaming, incremental, managed connector |
| Transformation | Bronze/silver/gold, joins, union/union all, cleaning, deduplication, aggregates, arrays/maps/structs | SQL and PySpark syntax for common transformations |
| Lakeflow Spark Declarative Pipelines | Streaming tables, materialized views, expectations, pipeline DAG, CDC/SCD patterns | Python decorators and SQL syntax; when to use declarative pipeline instead of notebook job |
| Lakeflow Jobs | Job, task, DAG, dependency, retry, repair, rerun, trigger, schedule, task values | Configure notebook/SQL/dashboard/pipeline tasks, branching, looping, file arrival/table update triggers |
| CI/CD | Repos, branches, commits, PRs, bundles, targets, variables, CLI | Validate/deploy/run bundles; promote same code to dev/test/prod |
| Monitoring | Run history, task graph, Spark UI, event logs, pipeline event logs, metrics | Diagnose skew, shuffle, spill, driver OOM, executor OOM, upstream blockers |
| Governance | Unity Catalog, privileges, storage credentials, external locations, volumes, lineage, audit logs | GRANT/REVOKE/DENY, row filters, column masks, Delta Sharing, Lakehouse Federation |

## 12.2 Databricks resource map — what each object is responsible for

| Resource | Responsibility | Exam clue |
|---|---|---|
| Workspace | UI container for notebooks, jobs, repos, queries, dashboards, clusters | “Where do users develop and schedule work?” |
| Metastore | Top-level Unity Catalog governance boundary | “Where are catalogs registered?” |
| Catalog | Business/domain-level namespace, e.g. `main`, `prod`, `finance` | First level of `catalog.schema.table` |
| Schema | Database-like namespace inside a catalog | Second level of `catalog.schema.table` |
| Table | Delta-backed tabular data | Managed/external, ACID, time travel |
| View | Stored query, no stored result | Recomputes every read |
| Materialized view | Stored query result | Refreshes on demand/schedule/pipeline |
| Streaming table | Incrementally maintained table | Append/streaming ingest use case |
| Volume | Governed file storage for non-tabular files | `/Volumes/catalog/schema/volume/path` |
| Storage credential | UC object that represents cloud IAM/managed identity | Used by external locations |
| External location | Governed cloud path + storage credential | Grants access to external object storage path |
| Share | Delta Sharing container | Add tables/views to share with recipients |
| Recipient | Delta Sharing consumer identity | External sharing target |
| Job | Orchestrated workflow definition | Contains one or more tasks |
| Task | One unit of work inside a job | Notebook, SQL, dashboard, pipeline, dbt, Python wheel, etc. |
| Pipeline | Declarative ETL graph | Lakeflow Spark Declarative Pipeline / DLT |
| Cluster | Spark compute for notebooks/jobs | Driver + workers |
| SQL warehouse | SQL compute for BI, dashboards, SQL editor | Not for PySpark notebooks |
| Repo / Git folder | Git-backed source control in workspace | Branch, commit, push, PR |
| Bundle | YAML package of Databricks resources | `databricks.yml`, targets, variables |

## 12.3 Compute, clusters, and cost — expanded decision table

### Compute decision tree

1. Is the workload BI/dashboard/SQL-only? Use a **SQL warehouse**.
2. Is the workload a scheduled production ETL job? Use a **job cluster** or **serverless jobs**.
3. Is the workload interactive development or exploration? Use an **all-purpose cluster**.
4. Is the workload a declarative pipeline? Use **Lakeflow Spark Declarative Pipelines** compute, often serverless where available.
5. Is startup time more important than lowest possible unit price? Consider **serverless** or **pools**.
6. Is governance required with multiple users? Use UC-compatible access modes, usually **shared** for multi-user SQL/Python and **single user** for one principal / ML / Scala-heavy use.

### All-purpose cluster

Use for:
- Notebook exploration.
- Debugging and developing PySpark/SQL transformations.
- Small ad-hoc analysis.
- Library testing.

Avoid for:
- Scheduled production jobs, because idle time and higher DBU rate make it expensive.
- BI dashboards, because SQL warehouses are designed for SQL concurrency.

### Job cluster

Use for:
- Production Lakeflow Job tasks.
- Scheduled ETL.
- One job run that should create compute, execute, then terminate.

Benefits:
- Lower DBU rate than all-purpose clusters.
- No accidental idle cost after completion.
- Isolated dependency/runtime environment per run.

### Shared job cluster inside a multi-task job

A job can define a `job_cluster_key` and reuse that job cluster across tasks in the same job run:

```yaml
resources:
  jobs:
    daily_etl:
      name: daily_etl
      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
      tasks:
        - task_key: bronze
          notebook_task: { notebook_path: ../src/bronze.py }
          job_cluster_key: etl_cluster
        - task_key: silver
          depends_on: [{ task_key: bronze }]
          notebook_task: { notebook_path: ../src/silver.py }
          job_cluster_key: etl_cluster
```

Use this when tasks are sequential and cluster startup time would dominate. Use separate job clusters when tasks need different runtimes, libraries, isolation, or scale.

### SQL warehouse

Use for:
- Databricks SQL editor.
- Dashboards.
- Alerts.
- BI tool connections through JDBC/ODBC.
- SQL query tasks in jobs.

Not for:
- PySpark notebook execution.
- Python wheel tasks.
- General Spark jobs.

### Serverless compute

Serverless means Databricks manages the compute plane for that workload. It is usually best when:
- You want hands-off compute management.
- You need fast startup.
- You do not want to tune clusters manually.
- You accept potentially higher unit price for lower ops overhead and less idle time.

Exam wording often says “hands-off, auto-optimized compute managed by Databricks” → choose **serverless**.

### Photon

Photon is Databricks’ vectorized execution engine for SQL/DataFrame workloads. It helps most with:
- SQL scans, filters, joins, aggregations.
- Delta table workloads.
- BI dashboards.

It helps less or not at all with:
- Python UDF-heavy logic.
- External non-Spark code.
- Workloads dominated by driver-side Python.

### Cluster policies

Cluster policies are admin-defined guardrails for compute configuration:
- Restrict instance types.
- Set max workers.
- Enforce auto-termination.
- Require specific runtimes or access modes.
- Control cost and governance.

Exam clue: “How can an admin prevent users from creating oversized clusters?” → **cluster policy**.

### Pools

Pools keep warm cloud instances ready so clusters start faster. Use pools when:
- Many job clusters start throughout the day.
- Startup latency matters.
- You still want job cluster lifecycle benefits.

Pools do not replace clusters; clusters attach to pools.

## 12.4 Databricks Connect — local development workflow

Databricks Connect lets you write code locally in an IDE while executing Spark commands on a Databricks cluster/serverless compute. It is useful when:
- You want local IDE features: debugger, linting, tests, project structure.
- You want code in a normal Python package instead of only notebooks.
- You still need Spark execution against Databricks data and compute.

Typical development pattern:

```python
# Local Python file, executed from your IDE
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

spark = DatabricksSession.builder.profile("dev").getOrCreate()

orders = spark.table("main.bronze.orders")
result = (orders
          .filter(F.col("amount") > 0)
          .groupBy("order_date")
          .agg(F.sum("amount").alias("revenue")))

result.show()
```

Key exam distinction:
- Local Python logic runs on your machine.
- Spark operations are executed remotely on Databricks compute.
- It is for development; production orchestration should still use Lakeflow Jobs / bundles.

## 12.5 Notebooks, widgets, and debugging

### Notebook widgets

Widgets parameterize notebooks, especially when the same notebook is used by multiple job tasks:

```python
dbutils.widgets.text("catalog", "dev_main")
dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"])

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
```

SQL access:

```sql
SELECT * FROM IDENTIFIER(:catalog || '.silver.orders');
```

### `dbutils.notebook.run` vs `%run`

```python
# Runs another notebook as a separate job-like execution context.
result = dbutils.notebook.run("/Repos/me/project/child", timeout_seconds=3600,
                              arguments={"date": "2026-05-19"})

# In child notebook
dbutils.notebook.exit("OK")
```

`%run` imports another notebook into the current notebook and shares variables:

```python
%run ./common_functions
```

Exam rule:
- `%run` = import-like, same context.
- `dbutils.notebook.run()` = separate context, parameterized execution, returns string.

### Built-in debugging places

| Problem | Where to look first |
|---|---|
| Notebook cell error | Cell output + stack trace |
| Spark query slow | Spark UI SQL/DataFrame and Stages tabs |
| Cluster fails to start | Cluster Event Log |
| Library/version issue | Libraries tab, notebook `%pip`, cluster logs |
| Job failed | Lakeflow Jobs run page + task output |
| Declarative pipeline failed | Pipeline event log + failed flow details |
| Permission denied | Unity Catalog grants, catalog/schema/table privileges |
| File path inaccessible | External location grants, volume grants, storage credential |

## 12.6 Ingestion syntax — expanded examples

### COPY INTO from CSV with schema and options

```sql
CREATE TABLE IF NOT EXISTS main.bronze.orders_raw (
  order_id STRING,
  customer_id STRING,
  order_ts TIMESTAMP,
  amount DOUBLE,
  _source_file STRING,
  _ingested_at TIMESTAMP
);

COPY INTO main.bronze.orders_raw
FROM (
  SELECT
    order_id,
    customer_id,
    to_timestamp(order_ts) AS order_ts,
    cast(amount AS DOUBLE) AS amount,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
  FROM '/Volumes/main/landing/orders/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'false',
  'delimiter' = ',',
  'mode' = 'PERMISSIVE'
)
COPY_OPTIONS (
  'mergeSchema' = 'false'
);
```

Important COPY INTO ideas:
- It is incremental/idempotent for files already loaded into the same target table.
- It is best when file arrival is batch-like and not huge-scale.
- It does not require writing streaming code.
- It can transform columns in the `FROM (SELECT ...)` part.

### COPY INTO with PATTERN / FILES

```sql
COPY INTO main.bronze.events
FROM '/Volumes/main/landing/events/'
FILEFORMAT = JSON
PATTERN = '.*2026-05-.*[.]json';
```

```sql
COPY INTO main.bronze.events
FROM '/Volumes/main/landing/events/'
FILEFORMAT = JSON
FILES = ('events_001.json', 'events_002.json');
```

### Auto Loader complete template

```python
from pyspark.sql import functions as F

source_path = "/Volumes/main/landing/events"
schema_path = "/Volumes/main/checkpoints/events_schema"
checkpoint_path = "/Volumes/main/checkpoints/bronze_events"

events_raw = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", schema_path)
         .option("cloudFiles.inferColumnTypes", "true")
         .option("cloudFiles.schemaEvolutionMode", "rescue")
         .option("cloudFiles.useNotifications", "false")
         .load(source_path)
         .withColumn("_source_file", F.col("_metadata.file_path"))
         .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    events_raw.writeStream
              .format("delta")
              .option("checkpointLocation", checkpoint_path)
              .outputMode("append")
              .trigger(availableNow=True)
              .toTable("main.bronze.events_raw")
)
```

Use `availableNow=True` when a scheduled job should process everything new and then stop.
Use `processingTime='5 minutes'` when the stream should keep running and process micro-batches periodically:

```python
(df.writeStream
   .trigger(processingTime="5 minutes")
   .option("checkpointLocation", checkpoint_path)
   .toTable("main.bronze.events"))
```

### Auto Loader schema evolution decision table

| Mode | Behavior | When to choose |
|---|---|---|
| `addNewColumns` | New columns are added; stream may fail once and require restart | You trust source changes and want the table schema to grow |
| `rescue` | Unexpected columns/data go into `_rescued_data` | You do not want schema drift to break ingestion |
| `failOnNewColumns` | Fails when new columns appear | Strict schema contracts |
| `none` | Ignores new columns | Rare; only when new fields are not needed |

### Streaming checkpoints

Checkpoint stores:
- Source progress: which files/offsets have been processed.
- Sink commit progress.
- State for stateful operations.

Rules:
- One streaming query = one checkpoint location.
- Do not share checkpoint locations across streams.
- Do not delete checkpoints unless you intentionally want to reprocess or reset state.
- Checkpoints are not the same as Auto Loader `schemaLocation`, though both are required in common patterns.

## 12.7 Lakeflow Spark Declarative Pipelines / LDP / DLT — complete exam guide

Lakeflow Spark Declarative Pipelines are the Databricks declarative ETL framework formerly known as Delta Live Tables. You describe **what tables/views should exist** and their dependencies; Databricks builds and runs the DAG.

Use declarative pipelines when:
- You want managed dependency resolution between bronze/silver/gold objects.
- You want built-in data quality expectations.
- You want pipeline event logs and lineage.
- You want simpler streaming table/materialized view definitions.
- You want CDC/SCD logic using declarative APIs.

Use notebook/jobs instead when:
- You need arbitrary procedural orchestration.
- You need custom external API calls in many steps.
- You need logic that does not fit declarative table/view definitions.

### Python pipeline syntax

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested from landing files"
)
def bronze_orders():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.schemaLocation", "/Volumes/main/checkpoints/dlt/orders_schema")
             .load("/Volumes/main/landing/orders")
             .withColumn("_ingested_at", F.current_timestamp())
    )

@dlt.table(
    name="silver_orders",
    comment="Cleaned valid orders"
)
@dlt.expect("valid_amount", "amount >= 0")
@dlt.expect_or_drop("order_id_present", "order_id IS NOT NULL")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
           .withColumn("amount", F.col("amount").cast("double"))
           .withColumn("order_date", F.to_date("order_ts"))
           .dropDuplicates(["order_id"])
    )

@dlt.table(name="gold_daily_revenue")
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")
           .groupBy("order_date")
           .agg(F.sum("amount").alias("revenue"),
                F.countDistinct("order_id").alias("orders"))
    )
```

### SQL pipeline syntax

```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw orders from files'
AS SELECT
  *,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/main/landing/orders',
  format => 'json',
  schemaLocation => '/Volumes/main/checkpoints/ldp/orders_schema'
);

CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_amount EXPECT (amount >= 0),
  CONSTRAINT order_id_present EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  order_id,
  customer_id,
  CAST(amount AS DOUBLE) AS amount,
  TO_DATE(order_ts) AS order_date,
  _ingested_at
FROM STREAM(LIVE.bronze_orders);

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
AS SELECT
  order_date,
  SUM(amount) AS revenue,
  COUNT(DISTINCT order_id) AS orders
FROM LIVE.silver_orders
GROUP BY order_date;
```

### `LIVE` and `STREAM`

- `LIVE.table_name` references another dataset in the same pipeline.
- `STREAM(LIVE.table_name)` reads it as a streaming input.
- `dlt.read("table")` reads a table in batch mode.
- `dlt.read_stream("table")` reads a table as a stream.

### Expectations

| Python | SQL | Behavior |
|---|---|---|
| `@dlt.expect("rule", "condition")` | `EXPECT (condition)` | Records metrics, keeps invalid rows |
| `@dlt.expect_or_drop("rule", "condition")` | `ON VIOLATION DROP ROW` | Drops invalid rows |
| `@dlt.expect_or_fail("rule", "condition")` | `ON VIOLATION FAIL UPDATE` | Fails the update |

Exam rule:
- Need monitoring only → expect.
- Need to keep pipeline running but remove bad rows → drop.
- Need strict data contract → fail.

### Pipeline modes

| Mode | Meaning | Use case |
|---|---|---|
| Triggered | Runs once, processes available data, then stops | Scheduled batch/incremental processing |
| Continuous | Keeps running and processes changes continuously | Low-latency streaming |
| Development | Debug-friendly, less strict, may allow faster iteration | Building/testing pipeline |
| Production | Stable operational mode | Production workloads |

### Pipeline event log

Use the pipeline event log to track:
- Dataset update status.
- Flow start/end/failure.
- Expectation metrics.
- Error messages.
- Runtime and throughput.

Typical troubleshooting path:
1. Open pipeline update.
2. Find failed dataset/flow.
3. Inspect event log error.
4. Check expectation failures or schema evolution issue.
5. Fix code/config and rerun.

### CDC and SCD with `APPLY CHANGES`

SCD Type 1 overwrites current values:

```sql
CREATE OR REFRESH STREAMING TABLE silver_customers;

APPLY CHANGES INTO LIVE.silver_customers
FROM STREAM(LIVE.bronze_customer_changes)
KEYS (customer_id)
SEQUENCE BY change_ts
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 1;
```

SCD Type 2 preserves history:

```sql
CREATE OR REFRESH STREAMING TABLE dim_customers;

APPLY CHANGES INTO LIVE.dim_customers
FROM STREAM(LIVE.bronze_customer_changes)
KEYS (customer_id)
SEQUENCE BY change_ts
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 2;
```

SCD Type 1 exam clue: “keep only latest value.”
SCD Type 2 exam clue: “preserve history / effective periods / previous values.”

## 12.8 Lakeflow Jobs — full orchestration guide

### Job vs task

A **job** is the orchestration container. A **task** is one executable unit inside the job.

A job can include:
- Notebook task.
- SQL query task.
- Dashboard refresh task.
- Pipeline task.
- Python script task.
- Python wheel task.
- JAR task.
- Spark submit task.
- dbt task.
- Run job task.
- If/else task.
- For each task.

### Dependencies

Task B can depend on Task A:

```yaml
tasks:
  - task_key: ingest
    notebook_task:
      notebook_path: ../src/ingest.py

  - task_key: transform
    depends_on:
      - task_key: ingest
    notebook_task:
      notebook_path: ../src/transform.py
```

If a dependency fails, downstream tasks are skipped unless configured to run on failure/always.

### Retries

Use retries for transient failures:
- Temporary cloud storage issue.
- API rate limit.
- Spot/preemptible instance interruption.
- Network blip.

```yaml
tasks:
  - task_key: ingest_api
    notebook_task:
      notebook_path: ../src/ingest_api.py
    max_retries: 3
    min_retry_interval_millis: 300000
    retry_on_timeout: true
```

Do not use retries to hide deterministic data quality failures; fix the data or code.

### Repair run and rerun

If a multi-task job fails after some tasks succeeded:
- **Rerun failed task** / **repair run** runs only failed and downstream dependent tasks.
- Full rerun starts the whole job again.

Exam clue: “A downstream task failed after upstream tasks succeeded; avoid recomputing successful tasks” → **repair run / rerun failed task**.

### Task values for passing state

```python
# producer task
row_count = spark.table("main.bronze.orders").count()
dbutils.jobs.taskValues.set(key="row_count", value=row_count)
```

```python
# consumer task
row_count = dbutils.jobs.taskValues.get(taskKey="ingest", key="row_count", default=0)
```

Use this for:
- If/else branching.
- Passing small metadata between tasks.
- Recording counts or flags.

Do not pass large datasets through task values. Write large data to tables/volumes.

### If/else branching

Use when later tasks depend on a condition:
- Row count > 0.
- Quality check passed.
- Environment is prod.
- File type equals X.

Example idea:
1. `check_new_data` task sets `has_rows=true`.
2. If/else task checks `{{tasks.check_new_data.values.has_rows}} == true`.
3. True branch runs transform; false branch ends or sends notification.

### For each looping

Use for repeated task execution over a list:
- Process many countries.
- Backfill many dates.
- Ingest many tenants/customers.
- Refresh many partitions.

Do not create 100 almost-identical tasks manually if a For each task can loop over the list.

### Trigger decision table

| Trigger | Use when | Example |
|---|---|---|
| Manual | Human-controlled run | Run now for test |
| Scheduled | Predictable cadence | Every day at 02:00 |
| File arrival | Source files arrive unpredictably | Trigger when file lands in `/landing/orders/` |
| Table update | Downstream job depends on upstream Delta table commits | Run silver after bronze table updates |
| Continuous | Always-on pipeline/job | Near-real-time processing |

### Monitoring job progress

In Lakeflow Jobs UI, track:
- Run status: queued, running, success, failed, canceled, skipped.
- Task graph: which task failed and which tasks were blocked.
- Duration by task: find bottlenecks.
- Historical run times: compare today to baseline.
- Retry attempts: transient vs persistent failure.
- Cluster used: runtime, node type, autoscaling, logs.
- Output/logs for each task.

### Notifications

Configure notifications for:
- Failure.
- Success.
- Start.
- Duration threshold exceeded.

Use email for simple alerts, webhooks/system destinations for Slack/Teams/PagerDuty-style workflows.

## 12.9 SQL and PySpark transformation syntax — expanded drill section

### Basic reads/writes

```python
# Read UC table
orders = spark.table("main.bronze.orders")
orders = spark.read.table("main.bronze.orders")

# Write managed table
(orders.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable("main.silver.orders"))

# Append
orders.write.mode("append").saveAsTable("main.silver.orders")
```

```sql
SELECT * FROM main.bronze.orders;

CREATE OR REPLACE TABLE main.silver.orders AS
SELECT * FROM main.bronze.orders;

INSERT INTO main.silver.orders
SELECT * FROM main.bronze.new_orders;
```

### Null handling

```python
from pyspark.sql import functions as F

df.fillna({"country": "unknown", "amount": 0})
df.dropna(subset=["order_id"])
df.withColumn("country", F.coalesce(F.col("country"), F.lit("unknown")))
```

```sql
SELECT
  COALESCE(country, 'unknown') AS country,
  IFNULL(amount, 0) AS amount
FROM orders
WHERE order_id IS NOT NULL;
```

### Type standardization

```python
df.withColumn("amount", F.col("amount").cast("decimal(10,2)")) \
  .withColumn("order_date", F.to_date("order_ts")) \
  .withColumn("order_ts", F.to_timestamp("order_ts_string", "yyyy-MM-dd HH:mm:ss"))
```

```sql
SELECT
  CAST(amount AS DECIMAL(10,2)) AS amount,
  TO_DATE(order_ts) AS order_date,
  TO_TIMESTAMP(order_ts_string, 'yyyy-MM-dd HH:mm:ss') AS order_ts
FROM orders;
```

### Filtering

```python
df.filter(F.col("amount") > 0)
df.where("amount > 0 AND status IN ('paid', 'shipped')")
df.filter(F.col("customer_id").isNotNull())
```

```sql
SELECT * FROM orders
WHERE amount > 0
  AND status IN ('paid', 'shipped')
  AND customer_id IS NOT NULL;
```

### Column add/drop/rename/split

```python
df.withColumn("gross_amount", F.col("amount") + F.col("tax"))
df.withColumnRenamed("cust_id", "customer_id")
df.drop("unused_col")
df.withColumn("email_domain", F.split(F.col("email"), "@").getItem(1))
```

```sql
SELECT
  amount + tax AS gross_amount,
  cust_id AS customer_id,
  SPLIT(email, '@')[1] AS email_domain
FROM orders;
```

### Joins

```python
# Inner join
orders.join(customers, on="customer_id", how="inner")

# Left join
orders.join(customers, on="customer_id", how="left")

# Multiple keys
orders.join(rates, on=["currency", "rate_date"], how="left")

# Cross join
orders.crossJoin(calendar)

# Broadcast join
orders.join(F.broadcast(small_customers), on="customer_id", how="left")

# Semi / anti joins
orders.join(customers, on="customer_id", how="left_semi")
orders.join(customers, on="customer_id", how="left_anti")
```

```sql
SELECT * FROM orders INNER JOIN customers USING (customer_id);
SELECT * FROM orders LEFT JOIN customers USING (customer_id);
SELECT * FROM orders o JOIN rates r ON o.currency = r.currency AND o.order_date = r.rate_date;
SELECT /*+ BROADCAST(c) */ * FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id;
SELECT o.* FROM orders o LEFT SEMI JOIN customers c ON o.customer_id = c.customer_id;
SELECT o.* FROM orders o LEFT ANTI JOIN customers c ON o.customer_id = c.customer_id;
```

### Union vs union all

```python
# PySpark union keeps duplicates, like SQL UNION ALL.
df_all = jan.unionByName(feb)

# For SQL UNION behavior, add distinct.
df_deduped = jan.unionByName(feb).distinct()

# Allow missing columns by name.
df = jan.unionByName(feb, allowMissingColumns=True)
```

```sql
SELECT * FROM jan
UNION ALL
SELECT * FROM feb;

SELECT * FROM jan
UNION
SELECT * FROM feb;
```

Exam gotcha: PySpark `union` / `unionByName` does **not** deduplicate; SQL `UNION` deduplicates, SQL `UNION ALL` does not.

### Deduplication

```python
# Full row dedup
df.distinct()
df.dropDuplicates()

# Key-based dedup
df.dropDuplicates(["order_id"])
```

Deterministic dedup using window:

```python
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
latest = (df.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1")
            .drop("rn"))
```

```sql
CREATE OR REPLACE TABLE silver.orders AS
SELECT * EXCEPT (rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
  FROM bronze.orders
)
WHERE rn = 1;
```

### Aggregations

```python
(df.groupBy("order_date", "country")
   .agg(F.count("*").alias("rows"),
        F.countDistinct("order_id").alias("orders"),
        F.approx_count_distinct("customer_id").alias("approx_customers"),
        F.sum("amount").alias("revenue"),
        F.mean("amount").alias("avg_amount"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount")))
```

```sql
SELECT
  order_date,
  country,
  COUNT(*) AS rows,
  COUNT(DISTINCT order_id) AS orders,
  APPROX_COUNT_DISTINCT(customer_id) AS approx_customers,
  SUM(amount) AS revenue,
  AVG(amount) AS avg_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
FROM orders
GROUP BY order_date, country;
```

### Window functions

```python
w = Window.partitionBy("customer_id").orderBy(F.col("order_ts").desc())

df.select(
    "customer_id", "order_id", "order_ts", "amount",
    F.row_number().over(w).alias("rn"),
    F.rank().over(w).alias("rank"),
    F.dense_rank().over(w).alias("dense_rank"),
    F.lag("amount").over(w).alias("previous_amount")
)
```

```sql
SELECT
  customer_id,
  order_id,
  amount,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC) AS rn,
  RANK()       OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk,
  DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS dense_rnk,
  LAG(amount)  OVER (PARTITION BY customer_id ORDER BY order_ts) AS previous_amount
FROM orders;
```

### MERGE patterns

Simple upsert:

```sql
MERGE INTO main.silver.customers AS t
USING main.staging.customer_updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Delete + update + insert:

```sql
MERGE INTO main.silver.customers AS t
USING main.staging.customer_updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  name = s.name,
  email = s.email,
  updated_at = s.updated_at
WHEN NOT MATCHED AND s.operation != 'DELETE' THEN INSERT (
  customer_id, name, email, updated_at
) VALUES (
  s.customer_id, s.name, s.email, s.updated_at
);
```

Full synchronization pattern:

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

## 12.10 Collections and complex data types

Spark “collections” usually means arrays, maps, and structs. The exam may phrase this as complex data, nested data, semi-structured data, or JSON handling.

### Structs

```python
df.select(F.col("customer.name"), F.col("customer.address.city"))
df.withColumn("customer_struct", F.struct("customer_id", "customer_name"))
```

```sql
SELECT customer.name, customer.address.city FROM events;
SELECT named_struct('id', customer_id, 'name', customer_name) AS customer FROM customers;
```

### Arrays

```python
df.select(F.col("items")[0].alias("first_item"))
df.select("order_id", F.explode("items").alias("item"))
df.select("order_id", F.posexplode("items").alias("position", "item"))
df.withColumn("item_count", F.size("items"))
df.withColumn("has_item", F.array_contains("items", "SKU123"))
```

```sql
SELECT items[0] AS first_item FROM orders;
SELECT order_id, EXPLODE(items) AS item FROM orders;
SELECT order_id, POSEXPLODE(items) AS (position, item) FROM orders;
SELECT SIZE(items) AS item_count FROM orders;
SELECT ARRAY_CONTAINS(items, 'SKU123') AS has_item FROM orders;
```

### Maps

```python
df.select(F.col("attributes")["color"].alias("color"))
df.select(F.map_keys("attributes"), F.map_values("attributes"))
```

```sql
SELECT attributes['color'] AS color FROM products;
SELECT map_keys(attributes), map_values(attributes) FROM products;
```

### JSON parsing

```python
schema = "order_id STRING, customer STRUCT<id: STRING, name: STRING>, items ARRAY<STRUCT<sku: STRING, qty: INT>>"
parsed = df.withColumn("json", F.from_json("raw_json", schema))
parsed.select("json.order_id", "json.customer.name", F.explode("json.items").alias("item"))
```

```sql
SELECT
  parsed.order_id,
  parsed.customer.name AS customer_name,
  item.sku,
  item.qty
FROM (
  SELECT from_json(raw_json, 'order_id STRING, customer STRUCT<id: STRING, name: STRING>, items ARRAY<STRUCT<sku: STRING, qty: INT>>') AS parsed
  FROM raw_events
)
LATERAL VIEW explode(parsed.items) exploded AS item;
```

### Higher-order functions

```sql
SELECT TRANSFORM(items, x -> x.sku) AS skus FROM orders;
SELECT FILTER(items, x -> x.qty > 1) AS multi_qty_items FROM orders;
SELECT EXISTS(items, x -> x.sku = 'SKU123') AS contains_sku FROM orders;
SELECT AGGREGATE(items, 0, (acc, x) -> acc + x.qty) AS total_qty FROM orders;
```

Use these when the question asks to transform/filter arrays without exploding them.

## 12.11 Delta Lake, storage, and table maintenance — deeper coverage

### Managed table vs external table

```sql
-- Managed table: no LOCATION clause
CREATE TABLE main.silver.orders (
  order_id STRING,
  amount DOUBLE
) USING DELTA;

-- External table: LOCATION clause points to external storage
CREATE TABLE main.silver.orders_ext (
  order_id STRING,
  amount DOUBLE
) USING DELTA
LOCATION 'abfss://container@account.dfs.core.windows.net/orders_ext/';
```

Drop behavior:
- Managed table: Databricks manages lifecycle; dropping table deletes underlying data after retention behavior.
- External table: dropping table removes metadata; files remain in the external location.

### Delta transaction log

The `_delta_log` contains commit JSON/checkpoint files. It enables:
- ACID transactions.
- Schema enforcement.
- Time travel.
- Concurrent reads/writes.
- `DESCRIBE HISTORY`.

### Table history and restore

```sql
DESCRIBE HISTORY main.silver.orders;

SELECT * FROM main.silver.orders VERSION AS OF 12;
SELECT * FROM main.silver.orders TIMESTAMP AS OF '2026-05-01T00:00:00Z';

RESTORE TABLE main.silver.orders TO VERSION AS OF 12;
```

### Optimize, liquid clustering, vacuum, analyze

```sql
OPTIMIZE main.silver.orders;

CREATE TABLE main.silver.events (
  event_id STRING,
  event_ts TIMESTAMP,
  customer_id STRING,
  region STRING
) USING DELTA
CLUSTER BY (region, event_ts);

ALTER TABLE main.silver.events CLUSTER BY (customer_id);
OPTIMIZE main.silver.events;

VACUUM main.silver.orders RETAIN 168 HOURS;

ANALYZE TABLE main.silver.orders COMPUTE STATISTICS;
```

Exam rules:
- `OPTIMIZE` compacts small files.
- Liquid clustering replaces static partition/ZORDER-style layout decisions in many modern UC Delta scenarios.
- `VACUUM` removes old files no longer needed by the transaction log retention window.
- `ANALYZE` collects statistics for query planning.
- Predictive Optimization can run optimize/vacuum/analyze automatically for eligible UC managed tables.

### Partitioning vs liquid clustering

| Feature | Partitioning | Liquid clustering |
|---|---|---|
| Physical layout | Directory partitions | Clustering managed by Delta |
| Good for | Low-cardinality columns used for pruning | High-cardinality or changing query patterns |
| Change keys later | Requires rewrite/recreate pattern | Keys can be changed with `ALTER TABLE ... CLUSTER BY` |
| Risk | Too many tiny partitions | Requires OPTIMIZE for existing data |

Do not partition by high-cardinality columns like user_id/order_id. That causes too many small directories/files.

## 12.12 Gold layer objects — exact use cases

| Object | Stored? | Updated how? | Use when |
|---|---:|---|---|
| Table | Yes | `INSERT`, `MERGE`, `CREATE OR REPLACE`, writes | General persistent dataset |
| View | No | Always recomputed on query | Simple abstraction, security filter, lightweight logic |
| Materialized view | Yes | Refresh/update mechanism | Expensive aggregation queried frequently |
| Streaming table | Yes | Incrementally from streaming source | Append/incremental stream ingestion or continuous updates |

Examples:

```sql
CREATE OR REPLACE VIEW main.gold.active_customers AS
SELECT * FROM main.silver.customers
WHERE status = 'active';
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW main.gold.daily_revenue AS
SELECT order_date, SUM(amount) AS revenue
FROM main.silver.orders
GROUP BY order_date;
```

```sql
CREATE OR REFRESH STREAMING TABLE main.bronze.events
AS SELECT * FROM STREAM read_files('/Volumes/main/landing/events', format => 'json');
```

```sql
CREATE OR REPLACE TABLE main.gold.customer_revenue AS
SELECT customer_id, SUM(amount) AS lifetime_revenue
FROM main.silver.orders
GROUP BY customer_id;
```

## 12.13 Data quality patterns

### Programmatic checks

```python
bad_rows = df.filter("amount < 0 OR order_id IS NULL").count()
if bad_rows > 0:
    raise ValueError(f"Data quality failed: {bad_rows} bad rows")
```

Flexible, but you must implement logging and actions yourself.

### Delta CHECK constraints

```sql
ALTER TABLE main.silver.orders
ADD CONSTRAINT positive_amount CHECK (amount >= 0);
```

Strict: invalid writes fail.

### Lakeflow expectations

```python
@dlt.expect_or_drop("valid_order", "order_id IS NOT NULL AND amount >= 0")
def silver_orders():
    return dlt.read_stream("bronze_orders")
```

Best when you want quality metrics integrated with the pipeline.

### Quarantine pattern

Good rows:

```sql
CREATE OR REPLACE TABLE main.silver.orders_clean AS
SELECT * FROM main.bronze.orders
WHERE order_id IS NOT NULL AND amount >= 0;
```

Bad rows:

```sql
CREATE OR REPLACE TABLE main.silver.orders_quarantine AS
SELECT *, current_timestamp() AS quarantined_at
FROM main.bronze.orders
WHERE order_id IS NULL OR amount < 0;
```

Use quarantine when data should not be lost but should not enter gold reporting.

## 12.14 Troubleshooting and optimization — exam diagnosis guide

### Spark action vs transformation

Transformations are lazy:
- `select`, `filter`, `withColumn`, `join`, `groupBy` definition, `drop`.

Actions trigger execution:
- `count`, `collect`, `show`, `display`, `write`, `saveAsTable`, `foreach`, `take`.

Exam clue: Spark UI shows a job only after an action runs.

### Narrow vs wide transformations

| Type | Examples | Shuffle? |
|---|---|---|
| Narrow | `select`, `filter`, `withColumn`, `map` | Usually no |
| Wide | `groupBy`, `join`, `distinct`, `orderBy`, `repartition` | Yes |

If a stage boundary appears, usually a shuffle happened.

### Bottleneck table

| Symptom | Likely issue | Fix |
|---|---|---|
| One or few tasks much slower than others | Data skew | Salt key, broadcast small side, filter null/mega-keys separately, AQE skew handling |
| High shuffle read/write | Expensive wide operation | Filter/project before join, broadcast, pre-aggregate, adjust partitioning |
| High disk spill | Partitions too large / not enough memory | Increase shuffle partitions, executor memory, reduce per-task data |
| Driver OOM | `collect()`/large result/broadcast to driver | Avoid collect, write to table, increase driver memory if needed |
| Executor OOM | Big partitions, skew, bad broadcast | More partitions, more memory, fix skew, disable/hint broadcast |
| Many tiny files | Too many small writes | OPTIMIZE, auto optimize/predictive optimization, coalesce carefully before write |
| Query reads too much data | Poor pruning/layout | Liquid clustering, partitioning when appropriate, file stats, filters |

### Spark UI tabs

| Tab | What it answers |
|---|---|
| Jobs | Which actions ran and whether they succeeded |
| Stages | Where time was spent and shuffle boundaries |
| Tasks | Task-level skew, spill, input size, duration |
| SQL/DataFrame | Logical/physical plan, join strategy, scan filters |
| Executors | Memory use, failed tasks, GC time, executor loss |
| Storage | Cached DataFrames/tables |
| Environment | Spark configs |

### Re-measurement loop

1. Baseline runtime and Spark UI metrics.
2. Change one thing: broadcast hint, shuffle partitions, cluster size, filter early, layout.
3. Rerun same workload.
4. Compare runtime, shuffle bytes, spill, task skew.
5. Keep change only if metrics improve.

Exam wording “re-measure performance” → do not guess; compare before/after metrics.

## 12.15 CI/CD and bundles — expanded syntax

### Bundle structure

```text
project/
  databricks.yml
  resources/
    jobs.yml
    pipelines.yml
  src/
    bronze.py
    silver.py
    pipeline.py
  tests/
    test_transforms.py
```

### Full bundle example

```yaml
bundle:
  name: dea_orders_project

variables:
  catalog:
    default: dev_main
  schema:
    default: orders
  warehouse_id:
    description: SQL warehouse for query tasks

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dev.cloud.databricks.com
    variables:
      catalog: dev_main

  prod:
    mode: production
    workspace:
      host: https://prod.cloud.databricks.com
    variables:
      catalog: prod_main
      schema: orders

resources:
  jobs:
    orders_daily:
      name: orders_daily_${bundle.target}
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./src/bronze.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
          job_cluster_key: main
        - task_key: transform
          depends_on:
            - task_key: ingest
          notebook_task:
            notebook_path: ./src/silver.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
          job_cluster_key: main
      job_clusters:
        - job_cluster_key: main
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
```

### CLI commands

```bash
databricks auth login --host https://dev.cloud.databricks.com

databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run orders_daily -t dev
databricks bundle deploy -t prod
databricks bundle destroy -t dev
```

Exam rules:
- `validate` checks bundle config; it does not deploy resources.
- `deploy` creates/updates workspace resources.
- `run` runs a deployed job/pipeline resource.
- `destroy` removes deployed resources for that target.
- Targets + variables avoid copying code for dev/test/prod.

### Repos workflow

1. Clone repo into Databricks Git folder.
2. Create or switch branch.
3. Edit notebooks/source files.
4. Commit changes.
5. Push branch.
6. Create PR in Git provider.
7. Merge after review.
8. Pull latest main branch in workspace or let CI/CD deploy bundle.

## 12.16 Unity Catalog governance, roles, audit, lineage, sharing, federation

### Privilege hierarchy

To query a table, a principal usually needs:

```sql
GRANT USE CATALOG ON CATALOG main TO `analysts`;
GRANT USE SCHEMA ON SCHEMA main.sales TO `analysts`;
GRANT SELECT ON TABLE main.sales.orders TO `analysts`;
```

For all current tables in a schema:

```sql
GRANT SELECT ON ALL TABLES IN SCHEMA main.sales TO `analysts`;
```

For future tables:

```sql
GRANT SELECT ON FUTURE TABLES IN SCHEMA main.sales TO `analysts`;
```

Common privileges:
- `USE CATALOG`
- `USE SCHEMA`
- `SELECT`
- `MODIFY`
- `CREATE TABLE`
- `CREATE VOLUME`
- `READ VOLUME`
- `WRITE VOLUME`
- `EXECUTE` on functions
- `MANAGE` for managing grants/object settings
- `CREATE CONNECTION`, `CREATE FOREIGN CATALOG` for federation-style scenarios

### DENY vs GRANT

```sql
GRANT SELECT ON TABLE main.hr.salaries TO `analysts`;
DENY SELECT ON TABLE main.hr.salaries TO `interns`;
```

If a user is in both groups, `DENY` wins.

### Ownership

Each UC securable has an owner. Owners can manage the object and its permissions. Production objects are often owned by groups or service principals rather than individual humans.

### Storage credentials and external locations

```sql
-- Conceptual syntax; cloud-specific identity details differ by cloud.
CREATE STORAGE CREDENTIAL my_cred
WITH AZURE_MANAGED_IDENTITY '...';

CREATE EXTERNAL LOCATION raw_landing
URL 'abfss://landing@storageaccount.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL my_cred);

GRANT READ FILES ON EXTERNAL LOCATION raw_landing TO `data_engineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION raw_landing TO `data_engineers`;
```

Use external locations instead of legacy DBFS mounts.

### Volumes

```sql
CREATE VOLUME main.raw.files;

GRANT READ VOLUME ON VOLUME main.raw.files TO `analysts`;
GRANT WRITE VOLUME ON VOLUME main.raw.files TO `data_engineers`;
```

Path:

```text
/Volumes/main/raw/files/my_file.csv
```

Use volumes for governed files that are not tables: CSV uploads, JSON files, PDFs, ML artifacts, libraries, checkpoints.

### Row filters

```sql
CREATE FUNCTION main.sec.region_filter(region STRING)
RETURN IF(is_account_group_member('eu_analysts'), region = 'EU', true);

ALTER TABLE main.sales.orders
SET ROW FILTER main.sec.region_filter ON (region);
```

Use row filters when different users should see different rows.

### Column masks

```sql
CREATE FUNCTION main.sec.mask_email(email STRING)
RETURN CASE
  WHEN is_account_group_member('pii_readers') THEN email
  ELSE '***MASKED***'
END;

ALTER TABLE main.sales.customers
ALTER COLUMN email SET MASK main.sec.mask_email;
```

Use column masks when the table is visible but sensitive columns should be hidden or transformed.

### ABAC

Attribute-based access control uses tags and policies to centralize rules. Exam-level definition:
- Tag data objects/columns with attributes such as `pii = true`.
- Apply policies based on those attributes.
- Prefer it when many tables/columns need consistent masking/filtering without hand-editing every object.

### Audit logs

Audit logs record security and workspace events such as:
- Login and access events.
- SQL queries / data access events.
- Permission changes.
- Object creation/deletion.
- Job and cluster actions.

Use audit logs for compliance and security investigations.

### Lineage

Unity Catalog lineage shows how data flows across:
- Tables.
- Views.
- Columns.
- Jobs.
- Notebooks/queries/dashboards where supported.

Use lineage to answer:
- What upstream tables feed this dashboard?
- What downstream reports break if I change this column?
- Which job produced this table?

### Delta Sharing

Delta Sharing lets you share data without copying it.

Provider side:

```sql
CREATE SHARE finance_share;
ALTER SHARE finance_share ADD TABLE main.gold.revenue;
CREATE RECIPIENT partner_org;
GRANT SELECT ON SHARE finance_share TO RECIPIENT partner_org;
```

Consumer side depends on Databricks-to-Databricks or open sharing recipient type.

Types:
- Databricks-to-Databricks sharing: recipient also uses Databricks; smoother UC integration.
- Open sharing / external recipients: recipient uses a sharing profile or compatible client.

Advantages:
- No duplicated files.
- Centralized provider control.
- Can share live Delta data.

Limitations/cost considerations:
- Cross-region/cross-cloud access can introduce egress/network costs.
- External recipients may have read-only access and fewer integrated governance features.
- Provider must manage what is added to the share and recipient permissions.

### Lakehouse Federation

Lakehouse Federation lets Databricks query external systems through governed connections and foreign catalogs.

Use when:
- Data should remain in an external database.
- You need exploratory queries or joins without first ingesting all data.
- You want UC governance over access to external sources.

Do not use federation as a replacement for ingestion when:
- You need high-performance repeated analytics at scale.
- You need Delta features like time travel/OPTIMIZE on the data.
- You want to transform and store curated bronze/silver/gold tables.

Decision rule:
- Query in place occasionally → federation.
- Build reliable lakehouse pipeline → ingest into Delta using Lakeflow Connect/Auto Loader/COPY INTO.

## 12.17 Exam-style scenario decision rules

| Scenario | Best answer |
|---|---|
| Millions of files arrive continuously in cloud storage | Auto Loader with file notification and checkpointing |
| A few CSV files arrive daily and must be loaded idempotently | COPY INTO |
| Salesforce/ServiceNow/enterprise SaaS source with managed support | Lakeflow Connect managed connector |
| Custom REST API with no connector | Notebook/script with REST client, secrets, scheduled by Lakeflow Job |
| Need strict schema rule that rejects whole write | Delta CHECK constraint |
| Need quality metrics and drop invalid rows without failing pipeline | Lakeflow expectation with DROP ROW |
| Need expensive daily aggregation for dashboards | Materialized view or gold table |
| Need always-live logical filter over table | View |
| Need append-only incremental ingest table | Streaming table |
| Need scheduled production ETL | Lakeflow Job on job cluster/serverless |
| Need SQL BI dashboard compute | SQL warehouse |
| Need local IDE development against Databricks Spark | Databricks Connect |
| Need deploy same jobs to dev/test/prod | Declarative Automation Bundle targets + variables |
| Need rerun only failed part of a multi-task job | Repair run / rerun failed task |
| Need see why query is slow | Spark UI SQL/stages/task metrics |
| One Spark task is much slower than others | Data skew |
| Large shuffle read/write | Expensive join/groupBy/sort shuffle |
| Spill to disk | Partitions too large or executor memory too low |
| User can’t query table despite SELECT | Missing USE CATALOG or USE SCHEMA |
| Share data externally without copying files | Delta Sharing |
| Query external database in place | Lakehouse Federation |

## 12.18 Mini mock questions — with answers

1. A job runs daily and processes all new files since the last run. It should stop when finished but keep exactly-once file tracking. Which trigger style is best inside Auto Loader?  
   **Answer:** `availableNow=True` with a unique checkpoint.

2. A BI team needs a precomputed revenue aggregate that refreshes periodically. Which gold object is most appropriate?  
   **Answer:** Materialized view, or a gold table if you need custom write logic.

3. A user has `SELECT` on `main.sales.orders` but cannot query it. What is the likely missing permission?  
   **Answer:** `USE CATALOG` on `main` or `USE SCHEMA` on `main.sales`.

4. A Spark stage has 199 tasks finishing in 20 seconds and 1 task running for 15 minutes. What is the likely problem?  
   **Answer:** Data skew.

5. Which command validates a Databricks bundle without deploying it?  
   **Answer:** `databricks bundle validate -t <target>`.

6. In PySpark, does `df1.union(df2)` remove duplicates?  
   **Answer:** No. PySpark union keeps duplicates; use `.distinct()` to remove them.

7. A source schema often adds unexpected fields but you do not want ingestion to stop. Which Auto Loader schema evolution mode should you prefer?  
   **Answer:** `rescue`.

8. A notebook must be parameterized by environment. Which feature is commonly used?  
   **Answer:** `dbutils.widgets` and job task base parameters.

9. A full customer sync should delete target rows missing from the source. Which MERGE clause handles that?  
   **Answer:** `WHEN NOT MATCHED BY SOURCE THEN DELETE`.

10. A production workload should not be owned by an individual user. What identity should usually run/deploy it?  
    **Answer:** A service principal or production group-owned setup.

## 12.19 Last 48-hour cram sheet

Memorize these code fragments:

```sql
GRANT USE CATALOG ON CATALOG main TO `group`;
GRANT USE SCHEMA ON SCHEMA main.sales TO `group`;
GRANT SELECT ON TABLE main.sales.orders TO `group`;
```

```sql
COPY INTO main.bronze.orders
FROM '/Volumes/main/landing/orders/'
FILEFORMAT = JSON;
```

```python
spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.schemaLocation", schema_path) \
  .load(source_path)
```

```python
df.writeStream.option("checkpointLocation", checkpoint_path) \
  .trigger(availableNow=True) \
  .toTable("main.bronze.events")
```

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

```bash
databricks bundle validate -t dev
databricks bundle deploy -t prod
databricks bundle run job_name -t prod
```

```python
dbutils.jobs.taskValues.set(key="row_count", value=123)
dbutils.jobs.taskValues.get(taskKey="task_a", key="row_count", default=0)
```

