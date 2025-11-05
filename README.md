# Lakeflow Jobs Meta

A metadata-driven framework for orchestrating Databricks Lakeflow Jobs. Package as a library and run as a single task in a Lakeflow Job to continuously monitor for metadata changes and automatically update jobs.

## Features

- ✅ **Dynamic Job Generation**: Automatically creates/updates Databricks jobs from metadata
- ✅ **Continuous Monitoring**: Automatically detects metadata changes and updates jobs
- ✅ **Multiple Task Types**: Support for Notebook, SQL Query, and SQL File tasks
- ✅ **Delta Table as Source of Truth**: Manage metadata directly in Delta tables
- ✅ **YAML Support**: YAML file ingestion for bulk updates
- ✅ **Dependency Management**: Handles execution order and task dependencies
- ✅ **Job Lifecycle**: Tracks and manages job IDs in Delta tables

## Architecture

```
┌─────────────────────────────────────────┐
│ Monitoring Job                          │
│  - Watches Delta Table                  │
│  - Watches Unity Catalog Volume (YAML)  │
│  - Auto-updates Jobs on Changes         │
└─────────────────────────────────────────┘
           │                    │
           ▼                    ▼
    Delta Control Table    YAML Files (UC Volume)
           │                    │
           └──────────┬─────────┘
                      ▼
              Job Generator
                      ▼
            Databricks Jobs
```

## Package Structure

```
.
├── lakeflow_jobs_meta/         # Main package
│   ├── __init__.py
│   ├── main.py                 # Entry point for monitoring task
│   ├── constants.py
│   ├── utils.py
│   ├── task_builders.py
│   ├── orchestrator.py
│   ├── metadata_manager.py
│   └── monitor.py
├── examples/                     # Example files
│   ├── orchestrator_example.ipynb  # Orchestrator example notebook
│   ├── notebook_task/          # Example notebook tasks
│   │   └── sample_ingestion_notebook.ipynb # Example ingestion notebook task
│   ├── sql_file_task/          # SQL file task examples
│   │   ├── 01_create_sample_data.sql
│   │   ├── 02_daily_aggregations.sql
│   │   ├── 03_bronze_to_silver_transformation.sql
│   │   ├── 04_data_freshness_check.sql
│   │   └── 05_incremental_load.sql
│   └── metadata_examples.yaml   # Example metadata configurations
├── docs/                        # Documentation
│   ├── PACKAGING_AND_DEPLOYMENT.md
│   └── METADATA_MANAGEMENT.md
├── tests/                       # Test suite
│   ├── test_utils.py
│   ├── test_task_builders.py
│   ├── test_metadata_manager.py
│   ├── test_orchestrator.py
│   ├── test_monitor.py
│   └── test_constants.py
├── setup.py                     # Package setup
├── pyproject.toml              # Modern Python packaging
```

## Quick Start

### Installation

```bash
# Install from PyPI
pip install lakeflow-jobs-meta

# Or install from source (development)
pip install -e .

# Or install from wheel
pip install dist/lakeflow_jobs_meta-0.1.0-py3-none-any.whl
```

### Quick Example

```python
import lakeflow_jobs_meta as jm

# Create or update a single job
job_id = jm.create_or_update_job(
    "my_pipeline",
    control_table="catalog.schema.etl_control"
)

# Or create/update all jobs
jobs = jm.create_or_update_jobs(
    control_table="catalog.schema.etl_control",
    auto_run=False
)
```

### Usage as a Lakeflow Job Task (Recommended)

1. **Use the orchestrator example** from `examples/orchestrator_example.ipynb` to create/update jobs on-demand, OR create a continuous monitoring job as shown below

2. **Configure Parameters** via Databricks widgets or base_parameters:
   ```python
   {
       "control_table": "your_catalog.schema.etl_control",  # Required
       "volume_path": "/Volumes/catalog/schema/metadata",  # Optional
       "check_interval_seconds": "60",  # Optional, default: 60
       "max_iterations": ""  # Optional, empty = infinite
   }
   ```

3. **Run the Job** - It will continuously monitor for changes and auto-update jobs

### Option 1: YAML File Ingestion (Recommended)

**Option 1a: Single YAML File**

Load a single YAML file directly:

```python
from lakeflow_jobs_meta import JobOrchestrator

orchestrator = JobOrchestrator(control_table="your_catalog.schema.etl_control")
orchestrator.metadata_manager.load_yaml('./examples/metadata_examples.yaml')

# Or use MetadataManager directly if you only need metadata operations
from lakeflow_jobs_meta import MetadataManager
manager = MetadataManager("your_catalog.schema.etl_control")
manager.load_yaml('./examples/metadata_examples.yaml')

# Or use convenience functions
import lakeflow_jobs_meta as jm
jm.load_yaml('./examples/metadata_examples.yaml', control_table="your_catalog.schema.etl_control")
```

**Option 1b: Unity Catalog Volume with File Arrival Trigger (Recommended for Production)**

For production environments, use Databricks file arrival triggers to automatically process YAML files when they're uploaded to a Unity Catalog volume:

1. **Configure File Arrival Trigger**: Set up a file arrival trigger on your job to monitor the Unity Catalog volume where YAML files are stored. See [Databricks File Arrival Triggers](https://docs.databricks.com/aws/en/jobs/file-arrival-triggers) for details.

2. **Upload YAML Files**: When YAML files are uploaded to the monitored volume, the job automatically triggers.

3. **Sync and Update**: The job calls `sync_from_volume()` to load all YAML files and update jobs:

```python
import lakeflow_jobs_meta as jm

# This runs automatically when file arrival trigger fires
tasks_loaded = jm.sync_from_volume(
    '/Volumes/catalog/schema/metadata_volume',
    control_table="your_catalog.schema.etl_control"
)

# Then create/update jobs
jobs = jm.create_or_update_jobs(control_table="your_catalog.schema.etl_control")
```

**Benefits of File Arrival Triggers:**
- Automatic processing when files arrive (no polling needed)
- Efficient: Only triggers when files actually change
- Scalable: Handles large numbers of files efficiently
- No need for continuous monitoring jobs

**Note:** File arrival triggers require Unity Catalog volumes or external locations. The trigger monitors the root or subpath of the volume and recursively checks for new files in all subdirectories.

### Option 2: Direct Delta Table Updates

For advanced use cases, you can update metadata directly in the Delta table:

```sql
-- Insert new task
INSERT INTO your_catalog.schema.etl_control VALUES (
  'my_pipeline',           -- job_name
  'sql_task_1',            -- task_key
  '[]',                     -- depends_on (JSON array of task_keys, empty for no dependencies)
  'sql_query',             -- task_type
  '{"catalog": "my_cat"}', -- parameters (JSON string)
  '{"warehouse_id": "abc123", "sql_query": "SELECT * FROM bronze.customers"}', -- task_config (JSON string)
  false                    -- disabled
);
```

The monitoring job will automatically detect the change and update the job.

### Option 3: On-Demand Orchestration

Run the orchestrator manually (for development/testing):

```python
from lakeflow_jobs_meta import JobOrchestrator

orchestrator = JobOrchestrator(control_table="your_catalog.schema.etl_control")
jobs = orchestrator.create_or_update_jobs(auto_run=True)

# Or use convenience function
import lakeflow_jobs_meta as jm
jobs = jm.create_or_update_jobs(control_table="your_catalog.schema.etl_control", auto_run=True)
```

Or with custom table names:

```python
orchestrator = JobOrchestrator(
    control_table="your_catalog.schema.control_table",
    jobs_table="your_catalog.schema.custom_jobs_table",
    default_warehouse_id="your-warehouse-id"
)
jobs = orchestrator.create_or_update_jobs(auto_run=True)
```

**Note:** The framework automatically detects changes and updates existing jobs.

## Testing

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest

# Run with coverage
pytest --cov=lakeflow_jobs_meta --cov-report=html

# Run specific test file
pytest tests/test_utils.py

# Run specific test
pytest tests/test_utils.py::TestSanitizeTaskKey::test_basic_sanitization
```

### Test Structure

Tests are organized in the `tests/` directory:
- `test_utils.py` - Utility function tests
- `test_task_builders.py` - Task creation tests
- `test_metadata_manager.py` - Metadata management tests
- `test_orchestrator.py` - Orchestration tests
- `test_monitor.py` - Monitoring tests
- `test_constants.py` - Constants validation

All tests use pytest with mocking for external dependencies (Databricks SDK, Spark, dbutils).

## Supported Task Types

### 1. Notebook Tasks

Execute Databricks notebooks with parameters.

```yaml
tasks:
  - task_key: "my_notebook_task"
    task_type: "notebook"
    depends_on: []  # List of task_key strings this task depends on
    file_path: "/Workspace/path/to/notebook"
    parameters:
      catalog: "my_catalog"
      schema: "my_schema"
      source_table: "source_table"
      target_table: "target_table"
```

**Parameters passed to notebook:**
- `task_key`: Unique identifier for this task
- `control_table`: Name of the control table containing metadata
- All other parameters from the `parameters` field in YAML

### 2. SQL Query Tasks

Execute SQL queries. Queries can be inline SQL or reference saved queries by `query_id`.

**Note:** `warehouse_id` is required for SQL tasks. It can be specified in `task_config.warehouse_id` or provided via `default_warehouse_id` when initializing the orchestrator.

```yaml
tasks:
  - task_key: "my_sql_query_task"
    task_type: "sql_query"
    depends_on: []  # List of task_key strings this task depends on
    warehouse_id: "your-warehouse-id"
    sql_query: |
      SELECT * FROM bronze.customers WHERE date > :start_date
    parameters:
      start_date: "{{job.start_time.iso_date}}"
```

Or use a saved query:

```yaml
tasks:
  - task_key: "my_saved_query_task"
    task_type: "sql_query"
    depends_on: []  # List of task_key strings this task depends on
    warehouse_id: "your-warehouse-id"
    query_id: "abc123-def456-ghi789"
    parameters:
      threshold: "5.0"
```

### 3. SQL File Tasks

Execute SQL from files in your workspace or Git repository.

**Note:** `warehouse_id` is required for SQL tasks. It can be specified in `task_config.warehouse_id` or provided via `default_warehouse_id` when initializing the orchestrator.

```yaml
tasks:
  - task_key: "my_sql_file_task"
    task_type: "sql_file"
    depends_on: []  # List of task_key strings this task depends on
    warehouse_id: "your-warehouse-id"
    file_path: "/Workspace/path/to/query.sql"
    file_source: "WORKSPACE"  # Optional: WORKSPACE or GIT (default: WORKSPACE)
    parameters:
      catalog: "my_catalog"
      schema: "my_schema"
      max_hours: "24"
```

SQL files should use parameter syntax (`:parameter_name`) for values passed via `parameters`. Parameters can be static values or Databricks dynamic value references like `{{job.id}}`, `{{task.name}}`, `{{job.start_time.iso_date}}`, etc.

## Metadata Schema

Each job in your YAML must have:

```yaml
jobs:
  - job_name: "my_pipeline"      # Required: Job name (becomes job_name in control table)
    description: "Pipeline description"  # Optional: Description
    job_config:                      # Optional: Job-level settings
      timeout_seconds: 7200          # Optional: Job timeout (default: 7200)
      max_concurrent_runs: 2         # Optional: Max concurrent runs (default: 1)
      queue:                         # Optional: Job queue settings
        enabled: true
      continuous:                    # Optional: Continuous job settings
        pause_status: UNPAUSED
        task_retry_mode: ON_FAILURE
      trigger:                       # Optional: Job trigger (file_arrival, table_update, or periodic)
        pause_status: UNPAUSED
        file_arrival:
          url: /Volumes/catalog/schema/folder/
      # OR use schedule instead of trigger:
      # schedule:                    # Optional: Scheduled job (cron)
      #   quartz_cron_expression: "13 2 15 * * ?"
      #   timezone_id: UTC
      #   pause_status: UNPAUSED
    tasks:
      - task_key: "unique_id"        # Required: Unique task identifier
        task_type: "sql_query"       # Required: Type of task (notebook, sql_query, sql_file)
        depends_on: []               # Optional: List of task_key strings this task depends on (default: [])
        disabled: false              # Optional: Whether task is disabled (default: false)
        timeout_seconds: 3600        # Optional: Task-level timeout (default: 3600)
        warehouse_id: "abc123"      # Required for SQL tasks: Warehouse ID
        sql_query: "SELECT * FROM ..."  # For sql_query: Inline SQL query
        # OR query_id: "abc123"      # For sql_query: Use saved query ID
        # OR file_path: "/path/to.sql"  # For sql_file or notebook: File path
        parameters:                  # Optional: Task parameters
          catalog: "my_catalog"
          schema: "my_schema"
          start_date: "{{job.start_time.iso_date}}"
```

### Control Table Schema

The control table has the following schema:

```sql
CREATE TABLE control_table (
    job_name STRING,              -- Job name (from job_name in YAML)
    task_key STRING,              -- Unique task identifier
    depends_on STRING,            -- JSON array of task_key strings this task depends on
    task_type STRING,             -- Task type: notebook, sql_query, or sql_file
    parameters STRING,            -- JSON string with task parameters
    task_config STRING,           -- JSON string with task-specific config (file_path, warehouse_id, sql_query, etc.)
    disabled BOOLEAN DEFAULT false, -- Whether task is disabled
    created_by STRING,            -- Username who created the record
    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
    updated_by STRING,            -- Username who last updated the record
    updated_timestamp TIMESTAMP DEFAULT current_timestamp()
)
```

### Jobs Table Schema

The jobs tracking table has the following schema:

```sql
CREATE TABLE jobs_table (
    job_name STRING,              -- Job name (same as job_name in control table)
    job_id BIGINT,                -- Databricks job ID
    created_by STRING,            -- Username who created the record
    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
    updated_by STRING,            -- Username who last updated the record
    updated_timestamp TIMESTAMP DEFAULT current_timestamp()
)
```

**Note:** `job_name` in the jobs table is the same as `job_name` in the control table. Both refer to the same job name from the YAML `job_name` field.

### Job-Level Settings

Job-level settings (`job_config`) control the behavior of the entire Databricks job:

- `timeout_seconds`: Maximum time the job can run (default: 7200 seconds)
- `max_concurrent_runs`: Maximum number of concurrent runs (default: 1)
- `queue`: Job queue settings (`enabled: true/false`)
- `continuous`: Continuous job settings
  - `pause_status`: `UNPAUSED` or `PAUSED`
  - `task_retry_mode`: `ON_FAILURE` or `NEVER`
- `trigger`: Job trigger settings (file arrival, table update, or periodic)
  - `pause_status`: `UNPAUSED` or `PAUSED`
  - `file_arrival`: Trigger on file arrival
    - `url`: Path to monitor (e.g., `/Volumes/catalog/schema/folder/`)
  - `table_update`: Trigger on table updates
    - `table_names`: List of table names to monitor (e.g., `["catalog.schema.table"]`)
  - `periodic`: Periodic trigger
    - `interval`: Interval number
    - `unit`: Time unit (`DAYS`, `HOURS`, `MINUTES`, etc.)
- `schedule`: Scheduled job using cron expression
  - `quartz_cron_expression`: Cron expression (e.g., `"13 2 15 * * ?"`)
  - `timezone_id`: Timezone (e.g., `"UTC"`)
  - `pause_status`: `UNPAUSED` or `PAUSED`

### Task-Level Settings

Task-level settings at the task level:

- `timeout_seconds`: Maximum time the task can run (default: 3600 seconds)
- `disabled`: Whether the task is disabled (default: false)
- `warehouse_id`: Required for SQL tasks (can be provided via `default_warehouse_id` in orchestrator)
- `file_path`: Required for notebook and sql_file tasks
- `sql_query` or `query_id`: Required for sql_query tasks

### Task Disabling

The `disabled` field in the control table controls whether a task is disabled in the Databricks job:

- `disabled: false` → Task is enabled (default)
- `disabled: true` → Task is disabled

Disabled tasks are included in the job definition but will not execute when the job runs. This allows you to temporarily disable tasks without removing them from the job. Dependencies on disabled tasks are still satisfied (the task exists, it just doesn't execute).

### Audit Fields

Both the control table and jobs table automatically track user information:

- `created_by`: Username of the user who created the record (set only on INSERT, never updated)
- `updated_by`: Username of the user who last updated the record (set on INSERT and UPDATE)
- `created_timestamp`: Timestamp when the record was created
- `updated_timestamp`: Timestamp when the record was last updated

These fields are automatically populated using Spark SQL's `current_user()` function.

## Task Dependencies

Tasks use the `depends_on` field to specify dependencies. Tasks without dependencies (or with empty `depends_on`) run first in parallel. Tasks with dependencies wait for all their dependencies to complete before running.

Example:
```yaml
tasks:
  - task_key: "task_a"
    depends_on: []  # No dependencies, runs first
  - task_key: "task_b"
    depends_on: []  # No dependencies, runs in parallel with task_a
  - task_key: "task_c"
    depends_on: ["task_a", "task_b"]  # Waits for both task_a and task_b to complete
```

The framework automatically resolves dependencies and creates the correct execution order. Circular dependencies are detected and will cause an error.

## Examples

See `examples/metadata_examples.yaml` for comprehensive examples including:
- Data quality checks
- Bronze to Silver transformations
- Mixed task type pipelines
- End-to-end data pipelines

## Best Practices

1. **Use SQL Files for Reusable Logic**: Store common SQL transformations in files
2. **Use Task Dependencies**: Design your pipelines with clear dependencies using `depends_on`
3. **Parameterize SQL**: Use parameters and Databricks dynamic value references for flexible SQL
4. **Follow Naming Conventions**: Use clear, descriptive `task_key` values
5. **Document Your Pipelines**: Add descriptions to jobs
6. **Use Task Parameters**: Leverage Databricks dynamic value references like `{{job.id}}`, `{{task.name}}`, `{{job.start_time.iso_date}}` for runtime values
7. **Unified Parameters**: Use the `parameters` field for all task parameters instead of separate `source_config`/`target_config`

## Future Enhancements

- [ ] Python script and wheels tasks
- [ ] Pipeline tasks (Lakeflow Declarative Pipelines)
- [ ] dbt tasks
- [ ] PowerBI refresh Tasks
- [ ] Enhanced error handling and retry logic
- [ ] Execution monitoring dashboard
- [ ] Data quality framework
- [ ] Databricks Apps

## Contributing

This is an internal project. For questions or enhancements, contact Peter Park (peter.park@databricks.com).