Questions and answers.

1
A data engineer is creating a Materialized View (MV) that aggregates daily sales. The underlying sales_raw table has a Unity Catalog row filter applied to it. When the Materialized View is refreshed, whose identity and group memberships are used to evaluate the row filter on the underlying sales_raw table?
A) The identity of the Materialized View creator/owner (the definer).

Correct. In Unity Catalog, Materialized Views are refreshed using the identity and group memberships of the Materialized View owner (the definer). This ensures the refresh honors security policies based on what the owner is permitted to see. The data persisted in the Materialized View reflects the results of the row filter as seen by the owner.

2
A data engineer needs to convert a managed table main.sales.leads into an external table main.sales.leads_ext located at abfss://container@account.dfs.core.windows.net/leads/. They decide to use a CREATE TABLE ... AS SELECT (CTAS) statement. What is a known consequence of using this method instead of DEEP CLONE?
E) The table history and time travel capabilities of the original table are not copied to the new external table.

Correct. A primary difference between CTAS and DEEP CLONE is that CTAS creates a brand-new table with a fresh transaction log (starting at version 0), meaning all history and time travel capabilities from the source table are lost. DEEP CLONE is designed to preserve the metadata and transaction history.

3
In Unity Catalog, which of the following database objects is used to define the underlying logic for a row-level filter or a column mask?
C) A User-Defined Function (UDF)

Correct. In Unity Catalog, the logic for row-level filters and column masks is implemented using SQL User-Defined Functions (UDFs). These functions allow for custom logic—such as checking group membership or evaluating user attributes—to be applied centrally to data objects.

4
A data engineer has a generic masking function named mask_string_data that redacts string values for non-privileged users. They need to apply this exact same masking function to both the email and phone_number columns of an existing users table. How can the engineer accomplish this?
ALTER TABLE users ALTER COLUMN phone_number SET MASK mask_string_data;

This is the correct approach. Column masking in Unity Catalog is applied by using the ALTER TABLE ... ALTER COLUMN ... SET MASK syntax for each specific column. The same masking function can be reused across multiple columns in the same or different tables, provided the data types are compatible.

5
A new workspace-level access control policy has been implemented, making an existing row filter on the transactions table obsolete. The data engineer needs to remove the row filter from the table without dropping the table itself. Which of the following commands successfully removes the row filter?
A) ALTER TABLE transactions DROP ROW FILTER;

Correct. In Databricks Unity Catalog, the proper syntax to remove an attached row filter from a table is to use the ALTER TABLE statement with the DROP ROW FILTER clause. This removes the row-level filtering policy without dropping the table data or the table object itself.

6
A data engineer recently created a managed table main.hr.salaries. They are moving to a different team and need to transfer the ownership of this table to the hr_admins group so that the group can manage the table's lifecycle and permissions. Which command accomplishes this?
ALTER TABLE main.hr.salaries OWNER TO hr_admins;

Correct. The ALTER TABLE ... OWNER TO ... command is the standard and officially documented way to transfer ownership of a table in Unity Catalog. Assigning ownership to a group like hr_admins is a best practice for managing the lifecycle of data assets, as it prevents access issues if an individual user leaves the organization. Note that the SET keyword is optional in this command; both ALTER TABLE ... OWNER TO ... and ALTER TABLE ... SET OWNER TO ... are valid syntaxes.


7
A data engineer accidentally executes the command DROP TABLE main.sales.transactions, where transactions is a managed table in Unity Catalog. What happens to the underlying data files associated with this table?
A) The data files are deleted from the managed storage location asynchronously, after a grace period of 8 days.

Correct. According to Databricks documentation, when a Unity Catalog managed table is dropped, its underlying data files are scheduled for deletion from the cloud storage location after an 8-day grace period. This allows for recovery using commands like UNDROP TABLE.


8
When designing a centralized access control strategy in Unity Catalog, can a data engineer apply both a row filter and a column mask to the same table?
C) Yes, a table can have both a single row filter and one or more column masks applied to it.

Correct. Unity Catalog supports applying both row-level filters and column masks to the same table. A table can have at most one row filter, but it can have multiple column masks (one for each column that requires masking). These controls are complementary and work together to implement fine-grained access control.


9
When a native Unity Catalog column mask is applied to a table, what is the expected behavior when a user without permission to see the unmasked data runs a SELECT * query on that table?
B) The query succeeds and returns the masked values for that specific column, while other columns remain visible.

Correct. Unity Catalog column masks are evaluated at query time. For users without specific permissions, the masking function transforms the values of that specific column (e.g., returning 'REDACTED' or partial strings). The query executes successfully, and all other columns where no mask is applied remain visible as normal.

