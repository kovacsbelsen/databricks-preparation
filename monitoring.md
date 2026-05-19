1.
A data engineer attaches a new cloud IAM role (such as an AWS Instance Profile or Azure Managed Identity) to a Databricks cluster to grant it access to a secure storage bucket. When they attempt to start the cluster, it immediately fails to provision. What is the most likely cause of this cluster startup failure?

A) The Databricks compute nodes lack the necessary trust relationship or cloud permissions to assume the attached IAM role.

Correct. Immediate cluster provisioning failure when attaching a cloud identity usually indicates that the cloud provider's API cannot attach the identity to the underlying virtual machines. This is commonly caused by missing trust relationships (e.g., the role does not trust the EC2 service or the Azure VM service) or the Databricks control plane lacking the 'iam:PassRole' permission to assign that specific role to the compute nodes.

2.
A data engineer is analyzing a 100 GB dataset using PySpark. To utilize a specific third-party visualization library, they execute the command pandas_df = spark_df.toPandas(). The command runs for several minutes before the job fails with a java.lang.OutOfMemoryError. What is the primary reason for this out-of-memory issue?

