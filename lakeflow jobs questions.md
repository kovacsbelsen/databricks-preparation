1
Which "Run If" condition should be configured on a downstream task to ensure it executes even if some of its upstream dependencies fail, as long as all upstream tasks have finished executing?

All Done

Correct. The 'All Done' condition triggers the downstream task as soon as all of its dependencies have finished executing, regardless of their final status (Succeeded, Failed, or Skipped). This is the standard way to implement a 'cleanup' or 'always-run' task.


2.
A data engineer configures a Lakeflow Job with a "Continuous" trigger to process streaming data 24/7. The job encounters a fatal error and fails. What is the default control flow behavior of the continuous job immediately after this failure?

C) The job automatically restarts using an exponential backoff mechanism.

Correct. For a job with a 'Continuous' trigger, the default behavior in Databricks after a failure is to automatically restart. To manage resources efficiently and avoid rapid-fire failures during outages, the system utilizes an exponential backoff mechanism between restart attempts.

3.
A Lakeflow Job with 10 sequential tasks fails at Task 7 due to a malformed record. The data engineer fixes the underlying data issue and wants to resume the job from Task 7 to complete the pipeline without re-running Tasks 1 through 6. Which feature should the engineer use?

A) Use the "Repair run" feature on the failed job run.

Correct. The "Repair run" feature in Databricks (Lakeflow Jobs) allows a user to rerun only the failed or canceled tasks and any downstream tasks that depend on them. This preserves the successful state of upstream tasks (1 through 6) and allows the pipeline to continue from the point of failure.


4.
A data engineer needs to execute a specific data validation notebook for five different regional datasets. They want to pass the region name dynamically to the notebook without manually creating five separate tasks in the Lakeflow Job. Which control flow feature should the engineer use to achieve this efficiently?

B) Use a "For each" task and pass a JSON array containing the five region names.

Correct. A "For each" task is specifically designed to iterate over a collection of items (such as a JSON array of strings or objects). This allows a data engineer to define a single task logic and run it multiple times, each time passing a different value from the array as a parameter.

5.
In a Databricks Lakeflow Job, how do you configure a "fan-out" architecture where multiple tasks run in parallel immediately after a single initialization task completes?

D) Set the "Depends on" field of all the parallel tasks to point to the initialization task.

This is the standard method for creating a fan-out architecture in Databricks. In a Lakeflow Job (DAG), you create parallelism by setting multiple downstream tasks to depend on the same parent task. Once the parent task completes successfully, all dependent tasks are triggered simultaneously and run in parallel.


6.
Task 1 runs a data quality check and outputs a task value named dq_status as either "PASS" or "FAIL". Task 2 is an "If/else" task that needs to route the control flow based on this value. What is the correct syntax to reference this task value in the condition of Task 2?

{{tasks.Task1.values.dq_status}} == "PASS"

Correct. In Lakeflow Jobs (Databricks Jobs), dynamic values from previous tasks are referenced in task expressions using the double-curly brace syntax: {{tasks.<task_name>.values.<key>}}. Since Task 1 emits the value dq_status, this syntax correctly retrieves it for comparison in the If/else condition.

7.
A specific task in a Lakeflow Job frequently fails due to a transient API timeout but usually succeeds on the second or third attempt. The engineer wants to automate this recovery without failing the entire job immediately. Which configuration is the most appropriate solution?

D) Configure the task's Retry policy with a maximum of 3 retries and an appropriate retry interval.

Configuring the Retry policy is the standard, built-in method in Databricks Jobs to handle transient errors. By setting a maximum number of retries and a retry interval, the job will automatically attempt to rerun the specific task without failing the entire job run unless all retry attempts are exhausted.


8.
Task A in a Lakeflow Job calculates a dynamic threshold date and needs to pass this specific date value to Task B, which runs immediately after Task A. How should the data engineer implement this value passing between tasks?

D) Task A uses dbutils.jobs.taskValues.set(), and Task B uses dbutils.jobs.taskValues.get().

Correct. The Databricks Task Values API (dbutils.jobs.taskValues) is the purpose-built feature for passing small pieces of metadata, such as strings or numbers, between dependent tasks in a workflow. Task A can set a value, and Task B can retrieve it by referencing Task A's name and the specific key.

9.
Task D depends on Task A, Task B, and Task C. The data engineer wants Task D to execute as long as any one of the upstream tasks (A, B, or C) completes successfully, even if the other two fail. Which "Run If" condition should be applied to Task D?

D) At Least One Succeeded

The 'At Least One Succeeded' condition ensures that Task D will execute as long as any one (or more) of the upstream tasks completes successfully, regardless of the failure status of other tasks. This perfectly matches the requirement.


10.
A Lakeflow Job has a strict SLA of 2 hours. It contains a machine learning task that sometimes hangs indefinitely. The engineer wants this specific task to fail if it takes longer than 45 minutes so a downstream alert task can trigger, but the overall job must not exceed the 2-hour SLA. How should the timeouts be configured?

C) Set the Task timeout to 45 minutes and the Job timeout to 120 minutes.

Correct. Setting the Task timeout to 45 minutes ensures that the specific machine learning task is killed and marked as failed if it hangs, which allows downstream tasks (like alerts) to trigger. The Job timeout of 120 minutes (2 hours) enforces the overall SLA for the entire workflow execution.


11.
A Lakeflow Job is configured with a Scheduled trigger to run every night at midnight. At 3:00 PM, a data engineer manually clicks the "Run now" button in the Jobs UI to test a recent code change. How does this manual execution affect the job's schedule?

B) The manual run executes independently and does not affect or alter the midnight scheduled run.

Correct. Manual runs (ad hoc executions) are completely independent of the job's configured schedule. Triggering a manual run does not change, delay, or cancel the next scheduled midnight run.

12.
A data engineering team needs to process data for five different geographic regions. The list of regions is generated dynamically by an upstream task and passed as a JSON array. The team wants to run the same processing notebook concurrently for each region in the array to minimize overall job duration. Which Lakeflow Job feature should they use?

 For Each task configured to iterate over the JSON array.

A For each task in Lakeflow Jobs is designed specifically to iterate over a collection (like a JSON array) and run a specified task for each item. This feature supports concurrent execution, allowing multiple instances of the processing notebook to run in parallel, which directly addresses the requirement to minimize overall job duration.

 
13.
An enterprise uses an external orchestration tool (Apache Airflow) to manage complex on-premises data pipelines. Once Airflow finishes uploading a massive dataset to cloud storage, it needs to immediately start a Databricks job to process the data. How should the Databricks job be triggered to ensure it runs exactly when Airflow finishes its tasks?

A) Have Airflow use the Databricks REST API (e.g., /api/2.1/jobs/run-now) to trigger the job.

Correct. Using the Databricks Jobs REST API (specifically the /api/2.1/jobs/run-now endpoint) allows Airflow to programmatically and immediately initiate a Databricks job. This creates a direct, event-driven dependency between the completion of the Airflow task and the start of the Databricks job.

14.
A Lakeflow Job uses an If/else task to route execution. If the condition is true, Task B runs; if false, Task C runs. Task D is configured downstream with dependencies on both Task B and Task C. The engineer wants Task D to execute as long as the chosen branch (B or C) completes successfully, but Task D is currently being skipped during every run. What is the most likely reason Task D is being skipped?

B) Task D's Run If  condition is set to All Succeeded , which requires both Task B and Task C to succeed, but one is always skipped by the branch.

Correct. In Databricks Workflows (Lakeflow Jobs), the default 'Run if' condition for a task is 'All Succeeded'. When an If/else branch is used, only one of the upstream tasks (Task B or Task C) will run, and the other will be marked as 'Skipped'. Because Task D's default condition requires 'All Succeeded', the presence of a 'Skipped' task causes the dependency criteria to fail, resulting in Task D being skipped.


15.
When configuring a File arrival trigger for a Databricks Job, which of the following storage locations is natively supported for monitoring new files?

D) External cloud object storage (e.g., Amazon S3, ADLS Gen2) or Unity Catalog external locations.

Correct. File arrival triggers natively support cloud object storage locations such as Amazon S3, ADLS Gen2, and GCS, as well as Unity Catalog external locations. These storage solutions allow Databricks to detect newly arrived files to initiate job execution.


16.
A financial institution has a strict Service Level Agreement (SLA) requiring that a daily summary report be generated and emailed to executives exactly at 6:00 AM every morning. The upstream data required for this report is guaranteed to be fully processed and available in a Delta table by 5:00 AM. Which trigger type should be used for the report generation job?

D) A Scheduled (cron) trigger set for 6:00 AM.

A scheduled (cron) trigger is a time-based trigger that ensures a job runs exactly at the specified time. Since the upstream data is guaranteed to be ready by 5:00 AM, scheduling the report job for 6:00 AM satisfies the SLA requirement perfectly.


17.
A data engineer wants to execute the following multi-line SQL statement as a task in a Databricks Job: MERGE INTO target_table t USING source_table s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *; The SQL statement is saved in a file named update_logic.sql in the Databricks Workspace. How can the engineer configure a task to run this specific file?

C) Add a 'SQL' task, select the 'File' option, choose the update_logic.sql file from the workspace, and assign a SQL warehouse.

Correct. Databricks Jobs include a 'SQL' task type specifically for running SQL queries. This task type allows you to select 'File' as the source, browse for a .sql file within the Workspace (or a remote Git provider), and execute it using a SQL Warehouse.

18.
A File Arrival trigger is configured to start a Databricks job when new files land in a Unity Catalog volume. The job is currently not running. If 10 new files arrive in the volume within a few seconds of each other, how will the File Arrival trigger behave?

A) It will fire a single run of the job to process the new state of the directory.

Correct. A File Arrival trigger is designed to fire a single run of the job when it detects new files in the volume. This behavior processes the new state of the directory (coalescing multiple files that arrive within a short time window) rather than issuing one run per file, ensuring efficient resource usage.

19.
A Databricks Job contains three tasks: Extract, Transform, and Send_Alert. The Send_Alert task is a notebook that sends an email to the on-call engineer if either the Extract or Transform task fails. How should the Send_Alert task be configured to achieve this?

E) Set Depends on Extract and Transform. Set the Run If condition to AT_LEAST_ONE_FAILED

Correct. AT_LEAST_ONE_FAILED is the appropriate Run If condition for this scenario. By setting Depends on to both tasks and choosing this condition, Databricks ensures that the Send_Alert task executes if one or more of its upstream dependencies fail.

20.
A Lakeflow Job is configured with a Table Update trigger monitoring the bronze_sensors Delta table. When the job is triggered, it runs a notebook that processes data and writes to a silver_sensors table. How does the notebook know which specific new rows triggered the run?

A) The trigger only starts the job; the notebook must use Structured Streaming or Delta Change Data Feed (CDF) to identify and read the new rows.

The Table Update trigger's role is strictly to signal that the source table has changed and start the job. To identify and process only the specific new or changed rows, the notebook logic must utilize tools like Structured Streaming (which uses checkpoints to track progress) or Delta Change Data Feed (CDF) to query specific versions or change types.
