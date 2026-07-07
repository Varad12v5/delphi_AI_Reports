# Table Metadata Migration Playbook: Databricks to Microsoft Fabric

When transferring tables from Databricks Unity Catalog to Microsoft Fabric, some metadata components are stored physically in the Delta Lake storage files and migrate automatically, while other catalog-level configurations reside in the Databricks Unity Catalog metastore and are lost. 

This playbook provides a detailed analysis of what migrates, what is lost, and the exact step-by-step SQL scripts required to manually reconstruct the lost metadata in Fabric.

---

## 1. The Reference Experiment

To test metadata replication, the following DDL was executed in Databricks:

```sql
-- 1. Create a partitioned table with comments and custom table properties
CREATE TABLE delphi_dev.bronze.applications (
  id INT COMMENT 'Unique application ID',
  candidate_id INT,
  status STRING
) 
USING DELTA
PARTITIONED BY (status)
COMMENT 'Raw recruitment application records'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- 2. Add an informational Primary Key constraint
ALTER TABLE delphi_dev.bronze.applications ADD CONSTRAINT app_pk PRIMARY KEY(id);
```

---

## 2. Metadata Migration Analysis (Replicated vs. Lost)

Once this table’s underlying Delta files are migrated to Fabric OneLake (`/Tables/bronze/applications`), a comparative scan of the database catalogs reveals what migrated automatically and what was lost:

### A. Automatically Replicated Metadata (Zero Manual Effort)
These components are stored inside the physical Delta files (`_delta_log/` and `.parquet` format) and are resolved natively by the Fabric Lakehouse engine:

* **Table Structure:** Column names (`id`, `candidate_id`, `status`) and data types (`int`, `string`) map exactly.
* **Physical Partitioning:** The table remains partitioned by the `status` column in OneLake storage (saving data in `status=Proposal/`, `status=Interview/` folders).
* **Nullability Constraints:** `NOT NULL` constraints declared on table columns are written to the Delta schema log and enforced natively in Fabric.

### B. Lost Metadata (Requires Manual Re-creation)
These components exist purely within the Databricks Unity Catalog metastore layer. Since the metastore database is not copied during data transfer, these are lost in Fabric:

* **Table & Column Comments:** Descriptions (e.g., `'Unique application ID'` and `'Raw recruitment application records'`) are lost in the SQL Analytics Endpoint database schema.
* **Primary Key / Foreign Key Constraints:** The constraint `app_pk` is discarded. No indexes or key constraints are created on the SQL Endpoint.
* **Custom Table Properties (`TBLPROPERTIES`):** Properties like `'delta.autoOptimize.optimizeWrite'` are ignored by Fabric.

---

## 3. Step-by-Step Manual Re-creation Recipes in Fabric

To restore the lost metadata components on your migrated Fabric tables, connect to your workspace's **SQL Analytics Endpoint** (via SSMS, Azure Data Studio, or client scripts) and execute these T-SQL recipes:

### **Recipe 1: Re-creating Primary and Foreign Keys**
Microsoft Fabric Synapse SQL Endpoints support primary and foreign keys, but they **must be declared as non-enforced (informational)** constraints.
* Execute the following DDL on the SQL Endpoint:
  ```sql
  -- Add non-enforced primary key to the migrated table
  ALTER TABLE bronze.applications 
  ADD CONSTRAINT app_pk PRIMARY KEY (id) NONCLUSTERED NOT ENFORCED;
  ```
  *(Note: `NONCLUSTERED` and `NOT ENFORCED` are mandatory syntax requirements in Fabric SQL Endpoints).*

---

### **Recipe 2: Re-creating Column and Table Comments**
In traditional SQL Server engines, comments are stored as database Extended Properties. 
* While you cannot run Spark `COMMENT ON` commands directly in the SQL Endpoint, you can document columns using SQL extended properties:
  ```sql
  -- Add table description
  EXEC sp_addextendedproperty 
      @name = N'MS_Description', @value = N'Raw recruitment application records', 
      @level0type = N'SCHEMA',   @level0name = 'bronze', 
      @level1type = N'TABLE',    @level1name = 'applications';

  -- Add column description to the 'id' column
  EXEC sp_addextendedproperty 
      @name = N'MS_Description', @value = N'Unique application ID', 
      @level0type = N'SCHEMA',   @level0name = 'bronze', 
      @level1type = N'TABLE',    @level1name = 'applications', 
      @level2type = N'COLUMN',   @level2name = 'id';
  ```

---

### **Recipe 3: Handling Auto-Optimize and Performance Settings**
* In Databricks, developers write `TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')` to prevent small file problems.
* In Fabric, this property is obsolete. Fabric automatically manages optimizations natively behind the scenes:
  * **V-Order (Default):** Fabric automatically V-orders all parquet files on write, which optimizes sort, compression, and query speeds for Power BI.
  * **Serverless Compacting:** Fabric runs serverless background jobs to compact small files in your Lakehouses without requiring developer code setup.
* **Action Required:** Discard all Databricks-specific `TBLPROPERTIES` lines when writing target definitions.
