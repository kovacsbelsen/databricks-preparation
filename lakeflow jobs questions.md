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


