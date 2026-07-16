# Playbook: Table Metadata, Constraints, and Dynamic Data Masking Migration

This document serves as the reference guide for migrating table metadata (specifically non-nullable columns, primary/foreign key constraints, partition paths, and dynamic data masking) from Databricks Unity Catalog to Microsoft Fabric.

---

## 1. Schema Nullability & Datatype Override (Not Null)

### **The Problem**
Fabric SQL Endpoints require columns targeted by Primary Key constraints to be strictly declared as `NOT NULL`. However, when extracting tables from Databricks or writing Delta files locally via PySpark/DeltaLake, columns default to `nullable = True` in the Parquet metadata layout, causing PK constraints to fail on the SQL endpoint.

### **The Migration Solution**
During the local Delta write phase, enforce explicit column nullability using **PyArrow schema overrides**:

```python
import pyarrow as pa
import deltalake

# Define explicit PyArrow schema forcing nullable=False on the target Primary Key column
arrow_schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False), # Enforce NOT NULL
    pa.field("name", pa.string(), nullable=True),
    pa.field("secret_key", pa.string(), nullable=True),
    pa.field("region", pa.string(), nullable=True),
    pa.field("created_at", pa.timestamp('us'), nullable=True)
])

# Convert pandas DataFrame to Arrow table and serialize to local Delta folder
arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)
deltalake.write_deltalake(local_dir, arrow_table, partition_by=["region"])
```

> [!IMPORTANT]
> **Fabric Spark & SQL Endpoint Compatibility Fix (Standard Delta Protocol v1/v2):**
> Enforcing `nullable=False` directly inside a local Python `deltalake` write causes Spark in Microsoft Fabric to throw protocol metadata mismatches because the python writer defaults to writer protocol version 1 (which doesn't support constraints).
> 
> * **The Solution:** 
>   1. Define the table schema using PyArrow with `nullable=False` on key columns.
>   2. Serialize the table using the python `deltalake` writer.
>   3. Before uploading to OneLake, patch the generated `_delta_log/00000000000000000000.json` log file to set the protocol version explicitly to **Reader Version 1 and Writer Version 2** (with no custom table features lists):
>      ```json
>      {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
>      ```
>   *This allows Fabric Spark to load the table cleanly as a standard v1/v2 Delta table (no feature list checks), while exposing columns as `NOT NULL` to the SQL Endpoint database catalog for primary/foreign key creation.*

> [!WARNING]
> **Timestamp NTZ (No Time Zone) Column Warnings:**
> Microsoft Fabric SQL Endpoint does not support the `TIMESTAMP_NTZ` Delta datatype. If timezone-naive datetimes are serialized to Parquet (e.g. `pa.timestamp('us')` without a timezone), PyArrow writes them as `TIMESTAMP_NTZ`, causing SQL Endpoint to throw warnings like *"Columns of the specified data types are not supported for ColumnName: '[created_at] TIMESTAMP_NTZ'"* and hide those columns entirely from query results.
> 
> * **The Solution:** Always define timestamp schemas using UTC timezone-aware fields in PyArrow (`pa.timestamp('us', tz='UTC')`) and localize the datetime series in Pandas before serialization (`df[col] = pd.to_datetime(df[col]).dt.tz_localize('UTC')`). This forces Delta to write standard UTC timezone-aware timestamps which Fabric maps cleanly to T-SQL `datetime2` columns, avoiding NTZ warnings.
 
 ---

### **B. Column-Level Comments (Descriptions)**
To migrate column comments (descriptions) to Fabric OneLake, store them directly within the `metadata` dictionary of the PyArrow fields on serialization:

```python
# Define explicit PyArrow schema including column comments
arrow_schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False, metadata={"comment": "Unique identifier for the record"}),
    pa.field("name", pa.string(), nullable=True, metadata={"comment": "Name of the business entity"}),
    pa.field("secret_key", pa.string(), nullable=True, metadata={"comment": "Sensitive security token mapped to entities"}),
    pa.field("region", pa.string(), nullable=True, metadata={"comment": "Geographic region of execution"}),
    pa.field("created_at", pa.timestamp('us'), nullable=True, metadata={"comment": "Datetime indicating record insertion"})
])

# Serialize to Delta format
arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)
deltalake.write_deltalake(local_dir, arrow_table, partition_by=["region"])
```
*Note: In Microsoft Fabric, column comments are stored natively inside the Delta log schema and are visible when querying the table using PySpark (`DESCRIBE TABLE <table_name>`).*

---

## 2. Directory Structure & Partitioning in OneLake

To prevent the **"unidentified folder"** explorer issue and preserve partition layouts, upload all Delta files under schema-specific subdirectories:

1. **Remote Target Path:** `Tables/<target_schema>/<table_name>/`
2. **DFS Endpoint Upload:** Upload local Delta parquet files and `_delta_log/` commits directly using storage credentials (scope: `https://storage.azure.com/`).
3. **Partition folders:** Ensure partition directory structures (e.g. `region=US-East/`) are kept exactly as folders.

---

## 3. Re-creating Primary and Foreign Key Constraints

Once the tables have synchronized to the SQL Endpoint, execute the informational (non-enforced) key and reference constraints via the SQL database connection:

```sql
-- Recreate informational Primary Key constraint
ALTER TABLE <schema_name>.<table_name> 
ADD CONSTRAINT pk_<table_name> PRIMARY KEY NONCLUSTERED (<key_column>) NOT ENFORCED;

-- Recreate informational Foreign Key constraint (Parent-Child relationship)
ALTER TABLE <schema_name>.<child_table> 
ADD CONSTRAINT fk_<child_table>_<parent_table> FOREIGN KEY (<child_column>) REFERENCES <schema_name>.<parent_table>(<parent_column>) NOT ENFORCED;
```

> [!IMPORTANT]
> **Dynamic Connection Properties Retrieval:**
> Do not hardcode the server hostnames or database names when creating connection handles. Instead, query the Fabric REST Workspace Item API (`GET https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items`) to find the item of type `SQLEndpoint`, and then query the connection string sub-endpoint:
> `GET https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/connectionString`
> This avoids accidental cross-workspace connection leaks where queries are executed in old environments.
 
 ---

## 4. Re-creating Dynamic Data Masking (DDM)

Column masking rules are lost during Delta parquet transfers. If sensitive columns are identified (e.g. credentials, secrets, phone numbers), manually deploy dynamic database masking rules on the SQL Endpoint:

```sql
-- Recreate Dynamic Data Masking policy
ALTER TABLE <schema_name>.<table_name> 
ALTER COLUMN <sensitive_column> ADD MASKED WITH (FUNCTION = 'default()');
```

---

## 5. Re-creating Row-Level Security (RLS) Policies

Microsoft Fabric SQL Endpoint natively supports T-SQL Row-Level Security. While Databricks filters are catalog-level constructs, they must be manually re-established in Fabric using security predicate functions and policies:

```sql
-- 1. Create a dedicated security schema
CREATE SCHEMA security;

-- 2. Define the security predicate function checking user database roles or Entra ID groups
CREATE FUNCTION security.fn_salespredicate(@region AS sysname)
    RETURNS TABLE
WITH SCHEMABINDING
AS
    RETURN SELECT 1 AS fn_salespredicate_result
    WHERE 
        (@region = 'US' AND IS_MEMBER('US_Admins_Role') = 1)
        OR (@region = 'EU' AND IS_MEMBER('EU_Admins_Role') = 1);

-- 3. Bind the security policy to the table
CREATE SECURITY POLICY security.SalesFilter
ADD FILTER PREDICATE security.fn_salespredicate(region)
ON bronze.sales_security_test
WITH (STATE = ON);
```

---

## 6. Verification SQL Queries

Use these queries to verify column properties, key constraints, dynamic data masking, and RLS policies in Fabric:

### **A. Check Column Nullability and Masking Status**
```sql
SELECT 
    name AS [Column Name],
    TYPE_NAME(system_type_id) AS [Data Type],
    is_nullable AS [Is Nullable],
    is_masked AS [Is Masked],
    masking_function AS [Masking Function]
FROM sys.masked_columns
WHERE object_id = OBJECT_ID('<schema_name>.<table_name>');
```

### **B. Check Registered Key Constraints**
```sql
SELECT 
    name AS [Constraint Name], 
    type_desc AS [Type],
    OBJECT_NAME(parent_object_id) AS [Table Name]
FROM sys.key_constraints
WHERE parent_object_id = OBJECT_ID('<schema_name>.<table_name>');
```

### **C. Check Row-Level Security Policy Status**
```sql
SELECT 
    name AS [Policy Name],
    is_enabled AS [Is Enabled],
    OBJECT_SCHEMA_NAME(object_id) AS [Schema]
FROM sys.security_policies;

---

## 7. Notebook Lakehouse Attachment Metadata

When uploading notebooks via the Fabric REST API, they are not attached to any default Lakehouse context by default. If a notebook refers to schema paths (e.g. `silver.table_name`), it will fail unless the default Lakehouse context is attached.

### **The Migration Solution**
Before encoding and uploading the `.ipynb` JSON file payload, inject the default Lakehouse workspace connection properties directly inside the notebook's `"metadata"` configuration block:

```json
{
    "metadata": {
        "dependencies": {
            "lakehouse": {
                "default_lakehouse": "<target_lakehouse_guid>",
                "default_lakehouse_name": "<target_lakehouse_name>",
                "default_lakehouse_workspace_id": "<target_workspace_guid>",
                "defaultLakehouse": "<target_lakehouse_guid>",
                "defaultLakehouseName": "<target_lakehouse_name>",
                "defaultLakehouseWorkspaceId": "<target_workspace_guid>"
            }
        }
    }
}
```
> [!IMPORTANT]
> **Key Case-Sensitivity Warning:**
> Microsoft Fabric internally converts the `.ipynb` metadata block into `# META` comments in its python code representation. The parser expects **snake_case** keys (`default_lakehouse`, `default_lakehouse_name`, and `default_lakehouse_workspace_id`). Enforcing both camelCase and snake_case versions in the JSON payload guarantees cross-compatibility.
*This binds the Lakehouse as the default execution context, allowing direct delta table querying in code cells.*
```
