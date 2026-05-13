Storage location natively supporting monitoring new files

Delta sharing tables

Delta Sharing is a protocol used for sharing governed data across different organizations or workspaces. It is not a supported source for monitoring file arrivals to trigger Databricks Jobs.

-----------------------------------------------------------------
Trigger jobs question answers

A continuous trigger is used for always-on streaming processing with low latency. It is not designed for daily batch reports that must execute at a specific time.

A scheduled (cron) trigger is a time-based trigger that ensures a job runs exactly at the specified time. Since the upstream data is guaranteed to be ready by 5:00 AM, scheduling the report job for 6:00 AM satisfies the SLA requirement perfectly.

A REST API trigger is typically used for external event-driven orchestration. Using it to call a job from a continuous streaming job would add unnecessary complexity and does not provide the precise time-based execution required by the SLA.

A file arrival trigger is data-driven and intended to start processing when new files land. It does not guarantee execution at a specific clock time. Additionally, monitoring Delta table underlying Parquet files directly is not recommended as it ignores the Delta transaction log.

Trigger.AvailableNow() is an execution mode for Spark Structured Streaming or Delta Live Tables that processes all available data in a batch-like fashion and then stops. It is not a job-level scheduling trigger used to launch a task at a specific time of day.
