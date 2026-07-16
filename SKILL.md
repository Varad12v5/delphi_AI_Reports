---
name: fabric_migration
description: Playbook and instructions for migrating Databricks tables, notebooks, views, and catalogs to Microsoft Fabric.
---

# Databricks to Microsoft Fabric Migration Playbook — Skill Pack v1.0

This playbook governs the processes, code translations, security scanning, and reporting standards for migrating data objects (notebooks, tables, views) from Databricks Unity Catalog to Microsoft Fabric.

---

## 1. Establishing Connections

### A. Databricks Connection
Configure the local `.env` file with the following variables:
* `DATABRICKS_HOST`: Your Databricks workspace URL (e.g. `https://adb-xxxx.azuredatabricks.net/`).
* `DATABRICKS_TOKEN`: Your Personal Access Token (PAT).
* `DATABRICKS_WAREHOUSE_ID`: The SQL Warehouse ID used to run discovery queries.

### B. Microsoft Fabric Connection
Read authentication tokens dynamically from `C:\Users\VaradBrahmapurkar\.gemini\antigravity-ide\scratch\fabric_tokens.json`:
* `fabric_token`: Used for REST API requests (notebook uploads/deletions).
* `database_token`: Passed as an access token for T-SQL Endpoint database connections (`System.Data.SqlClient` or ODBC Attribute `1256`).

### C. Dynamic Target Workspace & Lakehouse Resolution

If the target names are not explicitly specified during migration, apply these dynamic naming resolution rules:

1. **Default Lakehouse Name Resolution:**
   * If the target Lakehouse name is not specified, resolve it based on the source **Databricks Catalog name** but **never** include environment suffixes like `_dev`, `_prod`, `_test`, etc.
   * *Example:* If the Databricks catalog is `delphi_dev` or `delphi_prod`, the target Lakehouse name must be formatted as either only **`delphi`** or **`delphi_lkh`** (stripping the environment suffix and optionally adding a `_lkh` suffix).
2. **Default Target Schema Resolution:**
   * If the target schema name is not specified, default to the source **Databricks Schema name** (e.g., copying from schema `bronze` results in a target schema named `bronze`).
3. **Auto-Provisioning Workspace:**
   * If workspace name is not specified, call the Fabric Workspace Creation API to provision a new workspace named `workspace_<timestamp>` or `<domain>_workspace` (e.g., `presales_workspace`).
   * Retrieve the active Fabric Capacity GUID from the cache configuration (e.g. `16e33d7d-eb1a-4088-89cf-2051b2226673`) and execute the `assignToCapacity` POST request to assign the capacity.
4. **Auto-Provisioning Lakehouse (Dynamic Creation):**
   * If a new Lakehouse must be created, ensure it is created as a Schema-Enabled Lakehouse with payload `{"displayName": "<resolved_catalog_name>", "creationPayload": { "enableSchemas": true }}`.
3. **Implementation Plan Documentation:**
   * Clearly state in the Implementation Plan under "User Review Required" the workspace and lakehouse creation steps, for example:
     > [!IMPORTANT]
     > *I will create a new workspace 'workspace_dev' (assigned to Capacity '16e33d7d...') and a schema-enabled Lakehouse 'lakehouse_dev' to migrate this asset.*
4. **Metadata Mapping Registry File:**
   * Maintain a local mapping file named `fabric_metadata_mapping.json` in the workspace directory to cache the generated Workspace name/ID and Lakehouse name/ID to reference dynamically during subsequent steps.
5. **Notebook Lakehouse Attachment:**
   * Analyze the source notebook cells. Identify the referenced schemas/tables.
   * In the Implementation Plan, explicitly document which Lakehouse will be attached to each notebook based on data schema requirements, or prompt the user for validation:
     > [!TIP]
     > *I will attach the 'delphi_dev' Lakehouse to notebook 'nb_silver_execution' based on its dependencies on silver schema tables.*

### D. Implementation Plan Structure & Artifact Copy Order

Every generated implementation plan must be highly detailed and explicitly structure the proposed changes using the following strict workflow execution sequence:

1. **Step 1: Workspace Provisioning**
   * Check and create the target Workspace if missing, then assign the active Fabric capacity GUID.
   * Propose and request user confirmation in the plan for:
     * **Workspace Meta:** Proposed Description, Domain tags (default to skip), and contact settings.
     * **Spark Pool Configuration:** Specify Starter Pool vs. Custom Pool (node size, node count limit, and High Concurrency mode settings). Propose custom pool sizes based on Databricks source cluster analysis, allowing the user to override parameters.
     * **Spark Environment Settings:** Proposed runtime version (e.g. Runtime 1.2 / Spark 3.4), default libraries, and public library installations.
   * Explicitly ask the user: *"Do you want to apply these custom configurations, or proceed with standard Microsoft Fabric defaults?"*
2. **Step 2: Lakehouse Provisioning** (Create a schema-enabled Lakehouse within the workspace to support schematized Delta namespaces).
3. **Step 3: Tables Migration** (Extract, convert to Delta, and upload all physical delta data files/logs before building code/logic on top of them).
4. **Step 4: Notebooks Conversion** (Translate cells utilizing regex patterns and upload notebooks:
   * **Default Lakehouse Link:** Enforce both snake_case and camelCase parameters in default lakehouse dependencies metadata configuration to ensure Fabric's internal parser registers the attached Lakehouse properly.
   * **Hierarchical Folders:** Preserve the Databricks notebook folder structure inside Fabric. Identify the path of each notebook (e.g. `/Shared/Delphi EDP/utils/nb_adls_to_bronze`) and replicate this folder structure within the Fabric Workspace, placing the notebook item inside the matching subfolder).
5. **Step 5: Pipelines Orchestration** (Create Fabric Data Pipelines with matching sequential TridentNotebook activities and dynamic exit value parameters:
   * **Schedule Replication:** Identify the schedule and cron-triggers configured for the corresponding Databricks Job and define matching schedule parameters on the Fabric Data Pipeline).
6. **Step 6: SQL Endpoint Views** (Deploy converted database T-SQL view definitions).
7. **Step 7: Database Constraints Recreation** (Deploy informational Primary Key and Foreign Key constraints on the SQL Endpoint. *Note: This must be executed only after all parent and child tables have fully synchronized to the SQL Endpoint catalog*).
8. **Step 8: Permissions & Security** (Deploy Dynamic Data Masking (DDM) rules on columns, followed by executing T-SQL database grants/denies, and mapping Workspace RBAC roles. *Note: Since permissions and masking rely on tables, views, and database roles existing in the catalog, this is executed as the final step*).

---

## 2. Table Migration Process

> [!IMPORTANT]
> **Schema-Enabled Lakehouse Requirement:**
> Standard Fabric lakehouses do not support custom nested subfolders under `Tables/` (e.g., `Tables/silver/table_name`). Uploading data to standard lakehouses using these paths will cause folders to register as **"unidentified folders"** in the Lakehouse explorer.
> To support custom target schemas (e.g., `config`, `log`, `bronze`, `silver`), when creating a Lakehouse programmatically via the Fabric REST API, you **MUST** specify `"creationPayload": { "enableSchemas": true }` in the payload body.
> Existing standard lakehouses cannot be converted to schema-enabled; they must be deleted and recreated.

Tables are migrated using the local machine as a temporary execution bridge:

1. **Extract Data:** Query Databricks SQL Warehouse to retrieve the table records into a local Pandas DataFrame.
2. **Convert to Delta Lake:** Convert the DataFrame locally to standard Delta Lake format (using the Python `deltalake` library) to write data files and transaction logs (`_delta_log/`).
3. **Upload to OneLake:** Upload the Delta files via ABFSS to the target path:
   `abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables/<target_schema>/<table_name>`
4. **Auto-Sync Wait:** Poll the Fabric SQL Endpoint using a loop (up to 25 attempts, sleeping 6 seconds between tries) to confirm that the table is registered and queryable:
   ```sql
   SELECT COUNT(*) FROM sys.tables WHERE name = 'table_name' AND SCHEMA_NAME(schema_id) = 'schema_name'
   ```

---

## 3. View Migration Process

Synapse SQL Endpoint does not support the Databricks Spark `CREATE OR REPLACE VIEW` or table/view namespace structures directly.

1. **DDL Extraction:** Run `SHOW CREATE TABLE <catalog>.<schema>.<view_name>` in Databricks to retrieve original source DDL.
2. **Translation & Compatibility:**
   * Strip catalog prefixes (e.g. `delphi_dev.`).
   * Translate Spark SQL functions to Synapse T-SQL equivalents:
     * **Date formatting:** `DATE_FORMAT(date_col, 'yyyy-MM')` must be translated to `FORMAT(CAST(date_col AS DATE), 'yyyy-MM')` because columns stored as `varchar` are invalid for the raw T-SQL `FORMAT` function.
     * **Year parsing:** Use `YEAR(CAST(date_col AS DATE))`.
3. **Deployment DDL:** Drop the existing view first if it exists, then deploy:
   ```sql
   IF EXISTS (SELECT * FROM sys.views WHERE name = 'view_name' AND SCHEMA_NAME(schema_id) = 'schema_name') 
       DROP VIEW schema_name.view_name;
   CREATE VIEW schema_name.view_name AS <translated_sql>;
   ```

---

## 4. Notebook Code Translation Rules

During notebook export and upload, apply these regex-based transformations to the raw notebook cells to match Fabric Spark runtime specs:

> [!IMPORTANT]
> **Complete Code Scanning Constraint:**
> Before migrating notebooks, you **MUST** scan all target notebook source code cells fully to discover any platform-specific API statements (like Databricks-specific widgets, secret vaults, taskValues, or filesystem endpoints). If new untranslated APIs or syntax layouts are found, you **MUST** update them as regex rules in both this playbook (`SKILL.md`) and the `translation_rules` engine inside [fabric_helper.py](file:///c:/Users/VaradBrahmapurkar/OneDrive%20-%20Delphi%20Consulting/Desktop/vv/ai_migration/.agents/skills/fabric/scripts/fabric_helper.py) before triggering the migration execution.

| Rule Category | Databricks Original Syntax | Fabric Equivalent Translation | Regex Pattern |
| :--- | :--- | :--- | :--- |
| **Run Orchestrations** | `%run "/Workspace/Shared/.../name"` | `%run name` (Workspace local) | `(r'%run\s+[\"\']\/Workspace\/[^\"]*\/([^\/\"\']+)[\"\']', r'%run \1')` |
| **Run Orchestrations (Unquoted)** | `%run /Workspace/Shared/.../name` | `%run name` (Workspace local) | `(r'%run\s+\/Workspace\/[^\s]*\/([^\/\s\n]+)', r'%run \1')` |
| **Parameter Widgets** | `dbutils.widgets.get("param_name")` | `mssparkutils.runtime.context.get("params", {}).get("param_name")` | `(r'dbutils\.widgets\.get\([\"\']([^\"\']+)[\"\']\)', r'mssparkutils.runtime.context.get("params", {}).get("\1")')` |
| **File System APIs** | `dbutils.fs.[ls/cp/mv/rm/put]` | `mssparkutils.fs.[ls/cp/mv/rm/put]` | `(r'dbutils\.fs\.', r'mssparkutils.fs.')` |
| **Notebook Run** | `dbutils.notebook.run(...)` | `mssparkutils.notebook.run(...)` | `(r'dbutils\.notebook\.run', r'mssparkutils.notebook.run')` |
| **Notebook Exit** | `dbutils.notebook.exit(...)` | `mssparkutils.notebook.exit(...)` | `(r'dbutils\.notebook\.exit', r'mssparkutils.notebook.exit')` |
| **Orchestrator taskValues Set** | `dbutils.jobs.taskValues.set(key="k", value=v)` | `mssparkutils.notebook.exit(v)` | `(r'dbutils\.jobs\.taskValues\.set\(.*key.*value.*\)', ...)` |
| **Orchestrator taskValues Get** | `dbutils.jobs.taskValues.get(taskKey="t", key="k")` | `mssparkutils.runtime.context.get("params", {}).get("param_k")` | `(r'dbutils\.jobs\.taskValues\.get\(.*taskKey.*key.*\)', ...)` |
| **Catalog Translation** | `delphi_dev.` (Catalog reference) | `delphi_lkh.` (Target Lakehouse) | `(r'\bdelphi_dev\.', f'{lk_name}.')` |

---

## 4.5. Notebook Workspace Upload & In-place Updates

When re-running a notebook migration or overwriting existing workspace files, do not delete the notebook first. 

* **The Problem:** Fabric runs soft-delete procedures asynchronously in the background. Deleting an item and trying to recreate it immediately with the same name triggers a `409 Conflict` (error code: `ItemDisplayNameNotAvailableYet`) because the display name is locked during deletion.
* **The Solution:** Use the **Update Notebook Definition** REST API endpoint to overwrite the item's code and metadata in-place:
  * Check if the notebook display name exists in the workspace.
  * If it exists, send a `POST` request to `https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/updateDefinition` carrying the base64-encoded IPYNB payload.
  * This preserves the item GUID and avoids name availability locks.

### 4.6. Workspace Folders Creation and Notebook Placement API

To organize workspace items programmatically, follow these parent folder rules:
1. **Asset Root Folders**:
   * Create three root folders at the workspace level: `lakehouses`, `pipelines`, and `notebooks`.
2. **Assign Items**:
   * **Lakehouses**: Create under the `lakehouses` folder by providing the `"folderId"` during item creation.
   * **Pipelines**: Create under the `pipelines` folder by providing the `"folderId"` during item creation.
   * **Notebooks**: Identify the notebook path (e.g. `/Shared/Delphi EDP/silver/nb_presales`). Prepend the `notebooks` parent folder to the path, so the segment hierarchy becomes `/notebooks/Shared/Delphi EDP/silver/`.
3. **Replicate Folders**:
   * Query existing workspace folders: `GET https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders`
   * For each directory segment in the hierarchy:
     * Check if it exists. If not, create it via `POST https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders`:
       ```json
       {
         "displayName": "FolderName",
         "parentFolderId": "optional-parent-guid"
       }
       ```
     * Cache and use the returned folder GUID as the `parentFolderId` for the next nested subdirectory segment.
3. **Link Notebook to Folder during Creation:**
   * When creating the notebook via `POST https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks`, include the `folderId` property pointing to the target leaf folder GUID:
     ```json
     {
       "displayName": "NotebookName",
       "folderId": "target-leaf-folder-guid",
       "definition": {
         "format": "ipynb",
         "parts": [
           {
             "path": "notebook-content.ipynb",
             "payload": "base64-payload",
             "payloadType": "InlineBase64"
           }
         ]
       }
     }
     ```

---

### 4.7. Pipeline Scheduler trigger API

To automate pipeline schedules corresponding to Databricks Jobs triggers:
1. **Retrieve Job Triggers:** Query Databricks Job Settings to fetch the cron expression and timezone.
2. **Apply Pipeline Schedule:**
   * Send a `POST` request to `https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/Pipeline/schedules`
   * Request Payload Example (Cron/Daily Execution):
     ```json
     {
       "enabled": true,
       "configuration": {
         "startDateTime": "2026-07-16T00:00:00Z",
         "localTimeZoneId": "UTC",
         "type": "Cron",
         "interval": "0 9 * * *"
       }
     }
     ```

---

## 5. Security & Masking Recommender Heuristics

Every implementation plan must check for sensitive columns and suggest Dynamic Data Masking (DDM) on the Fabric SQL Endpoint if they are not already masked in Databricks.

### A. Scanning Keywords & Functions mapping
* **Financials:** `salary`, `base_salary`, `bonus`, `compensation`, `hourly_rate` $\rightarrow$ `default()`
* **Emails:** `email`, `primary_email`, `secondary_email` $\rightarrow$ `email()`
* **Phone Numbers:** `phone`, `mobile`, `telephone` $\rightarrow$ `default()`
* **IDs:** `ssn`, `social_security`, `nid` $\rightarrow$ `default()`
* **Credit Cards:** `credit_card`, `card_number`, `pan` $\rightarrow$ `partial(0, "XXXX-XXXX-XXXX-", 4)`
* **Passports/Secrets:** `passport`, `drivers_license`, `password`, `secret`, `token` $\rightarrow$ `default()`

### B. Plan Notation Requirement
If any column matches, output the recommendations table in the Implementation Plan. If no column matches, explicitly state:
*"No columns found where masking can be applied."*

---

## 6. Standardization Reports Layout

Every completed migration task requires specific validation and test execution reports:

### **A. Tables and Views Reports**

#### **1. `validation_report.md` (and a copy under `migration_plans/` as `<asset>_validation_<yyyyMMdd_HHmm>.md`)**
* **Migration Summary:** Source/target paths, row count, target schemas, transfer status.
* **Reconciliation Report:** Table showing row counts, numeric aggregates (e.g. sums/averages), and distinct text metrics for *every* physical table in scope on both Databricks and Fabric SQL Endpoint, demonstrating zero variance.
* **Side-by-Side Comparison:** One-row sample value comparison for *every* physical table in scope (excluding views), ordered by key.
* **Table Schema & Translation Definitions:** Schema mapping table/block for *every* physical table showing Databricks source types vs Fabric target SQL types, along with PK constraints, view SQL, and masking settings.
* **Fabric Validation & Execution Logs:** Verification checklist and execution console output logs.

#### **2. `test_execution_report.md` (and a copy under `Reports/Test Execution Report/` as `<asset>_test_execution_report_<yyyyMMdd>.md`)**
* **10 Test Cases (TC-01 to TC-10):**
  * **TC-01 (Technical):** OneLake folder directory presence.
  * **TC-02 (Technical):** SQL Endpoint table registration/discovery.
  * **TC-03 (Technical):** SQL Endpoint View creation/metadata lookup.
  * **TC-04 (Technical):** Column schema matches expected layout.
  * **TC-05 (Technical):** Data types mapped correctly.
  * **TC-06 (Technical):** Row count reconciliation.
  * **TC-07 (Functional):** Key column uniqueness (duplicate count = 0).
  * **TC-08 (Functional):** Key column nullable check (null count = 0).
  * **TC-09 (Functional):** Sorted sample record matches exactly.
  * **TC-10 (Functional):** Endpoint queryable via T-SQL without compile/syntax errors.

---

### **B. Notebooks Reports**

#### **1. `notebook_conversion_report.md` (and local copies inside `Reports/Notebook Conversion Report/` as `<notebook_name>_conversion_report_<yyyyMMdd>.md`, and `migration_plans/` as `<notebook_name>_migration_<yyyyMMdd_HHmm>.md` and `<notebook_name>_validation_<yyyyMMdd_HHmm>.md`)**
* **Migration Summary:** Source/target paths, target workspace, language profile, import success state.
* **Translation & Syntax Conversion Details:** Mapping table showing before/after conversions (widgets, orchestration paths, filesystem commands, catalog prefixes).
* **Dependency Verification:** Check if all dependent/helper notebooks and config/log tables exist in the Fabric workspace.
* **Manual Post-Migration Steps / Code Adjustments:** Specific configuration updates (like pipeline parameters or mount configurations) or untranslated code blocks that did not convert automatically (such as multi-line `dbutils` expressions) must be explicitly listed here with review actions. Specifically, document warning blocks for any skipped multi-line `dbutils.jobs.taskValues.get(...)` statements (e.g. in `nb_silver_execution`, `nb_ingest_to_adls`, and `nb_adls_to_bronze`), detailing how they should be manually reconstructed to `mssparkutils.runtime.context.get("params", {}).get(...)`.

---

## 7. Data Pipeline & Job Migration Process

Multi-task workflow orchestrations (Jobs) in Databricks must be recreated as **Fabric Data Pipelines** using the following parameters and configurations:

1. **Activity Type:** Databricks notebook execution tasks must map to activities of type **`TridentNotebook`** in the Fabric pipeline schema, specifying the respective `notebookId` and `workspaceId`.
2. **Task Dependencies:** Sequential task execution maps to `dependsOn` arrays linking child activities to their parent:
   ```json
   "dependsOn": [
       {
           "activity": "Parent_Activity_Name",
           "dependencyConditions": ["Succeeded"]
       }
   ]
   ```
3. **Execution Outputs & Dynamic Parameters:**
   * Exit values returned by notebooks (e.g. `mssparkutils.notebook.exit(run_id)`) are bound to downstream activities using ADF expression language syntax:
     `@activity('Parent_Activity_Name').output.firstExitValue`
   * Bind this expression to the child activity parameter configurations to propagate execution IDs across the run hierarchy.
 4. **Trigger Schedules:** Configure daily/hourly cron schedule parameters directly within the pipeline configuration properties (matching original Databricks schedule definitions).
5. **Success & Failure Notifications:** Configure pipeline alert rules to send automated email reports on Failure or Success to the designated team contact list.

## 8. SQL View Translation Playbook

Microsoft Fabric SQL Endpoint is a T-SQL database analytics engine. Views defined in Databricks using Spark SQL dialect must be translated to standard T-SQL before being deployed.

### Common Spark to T-SQL View Mappings

| Feature | Databricks Spark SQL DDL | Microsoft Fabric T-SQL Equivalent |
| :--- | :--- | :--- |
| **Catalog Stripping** | `CREATE VIEW cat.db.vw AS SELECT * FROM cat.db.table` | `CREATE VIEW db.vw AS SELECT * FROM db.table` |
| **Spark View Metadata** | `WITH SCHEMA COMPENSATION` | *(Omit completely)* |
| **JSON Exploding** | `SELECT tx_id, explode(from_json(payload, 'items array<struct<name:string,price:double>>').items) AS item FROM raw_table` | `SELECT tx_id, JSON_VALUE(item.value, '$.name') AS name, CAST(JSON_VALUE(item.value, '$.price') AS FLOAT) AS price FROM raw_table CROSS APPLY OPENJSON(payload, '$.items') AS item` |
| **Regular Expressions** | `regexp_replace(col, '[0-9]', 'X')` | `TRANSLATE(col, '0123456789', 'XXXXXXXXXX')` or CLR/T-SQL functions |
| **String Splitting** | `split(col, ',')` | `SELECT value FROM STRING_SPLIT(col, ',')` |
| **Date Formatting** | `date_format(col, 'yyyy-MM-dd HH:mm:ss')` | `FORMAT(col, 'yyyy-MM-dd HH:mm:ss')` |

### JSON Array Explode Translation Pattern
When converting a Spark SQL nested JSON array expansion (`explode(from_json(col, schema))`) to Fabric SQL Endpoint:
1. Identify the JSON path containing the array (e.g. `$.items`).
2. Use **`CROSS APPLY OPENJSON(json_column, '$.path') AS alias`** to expand the array elements into individual rows.
3. Use **`JSON_VALUE(alias.value, '$.field_name')`** to retrieve individual attribute values, casting them to target SQL types (`CAST(... AS FLOAT)`, etc.) as required.

