# Lakeflow Job Meta

A metadata-driven framework for orchestrating Databricks Lakeflow Jobs. Package as a library and run as a single task in a Lakeflow Job to continuously monitor for metadata changes and automatically update jobs.

## Features

- ✅ **Packaged as Library**: Install and run as a single task in Lakeflow Jobs
- ✅ **Continuous Monitoring**: Automatically detects changes from Delta tables and Unity Catalog volumes
- ✅ **Multiple Task Types**: Support for Notebook, SQL Query, and SQL File tasks
- ✅ **Delta Table as Source of Truth**: Manage metadata directly in Delta tables
- ✅ **YAML Support**: Optional YAML file ingestion for bulk updates
- ✅ **Dynamic Job Generation**: Automatically creates/updates Databricks jobs from metadata
- ✅ **Change Detection**: Automatically detects metadata changes and updates jobs
- ✅ **Dependency Management**: Handles execution order and task dependencies
- ✅ **Job Lifecycle**: Tracks and manages job IDs in Delta tables

## Architecture

```
┌─────────────────────────────────────────┐
│ Monitoring Job (Single Task)            │
│  - Watches Delta Table                  │
│  - Watches Unity Catalog Volume (YAML) │
│  - Auto-updates Jobs on Changes         │
└─────────────────────────────────────────┘
           │                    │
           ▼                    ▼
    Delta Control Table    YAML Files (UC Volume)
           │                    │
           └──────────┬──────────┘
                      ▼
              Job Generator
                      ▼
            Databricks Jobs
```

## Package Structure

```
.
├── lakeflow_job_meta/          # Main package
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
# Install from source
pip install -e .

# Or install from wheel
pip install dist/lakeflow_job_meta-0.1.0-py3-none-any.whl
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

### Option 1: Direct Delta Table Updates (Recommended)

Update metadata directly in the Delta table:

```sql
-- Insert new source
INSERT INTO your_catalog.schema.etl_control VALUES (
  'sql_task_1',
  'my_pipeline',
  'sql',
  1,
  '{}',
  '{}',
  '{"task_type": "sql_query", "sql_task": {"warehouse_id": "abc123", "sql_query": "SELECT * FROM bronze.customers", "parameters": {}}}',
  true
);
```

The monitoring job will automatically detect the change and update the job.

### Option 2: YAML File Ingestion

1. Place YAML files in Unity Catalog volume (if `volume_path` is configured in monitor)
2. The monitoring job will automatically detect new/modified YAML files and sync them
3. Or manually load:

```python
from lakeflow_job_meta import MetadataManager

manager = MetadataManager("your_catalog.schema.etl_control")
manager.load_yaml('./examples/metadata_examples.yaml')
```

### Option 3: On-Demand Orchestration

Run the orchestrator manually (for development/testing):

```python
from lakeflow_job_meta import JobOrchestrator

orchestrator = JobOrchestrator("your_catalog.schema.etl_control")
jobs = orchestrator.run_all_modules(auto_run=True)
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
pytest --cov=lakeflow_job_meta --cov-report=html

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
transformation_config:
  task_type: "notebook"
  notebook_path: "/Workspace/path/to/notebook"
```

**Parameters passed to notebook:**
- `source_id`: Unique identifier for querying the control table
- `control_table`: Name of the control table containing metadata

### 2. SQL Query Tasks

Execute SQL queries. Queries can be inline SQL or reference saved queries by `query_id`.

**Note:** `warehouse_id` is required for SQL tasks. It can be specified in `sql_task.warehouse_id` or provided via `default_warehouse_id` when initializing the orchestrator.

```yaml
transformation_config:
  task_type: "sql_query"
  sql_task:
    warehouse_id: "your-warehouse-id"
    sql_query: |
      SELECT * FROM bronze.customers WHERE date > :start_date
    parameters:
      start_date: "{{job.start_time.iso_date}}"
```

Or use a saved query:

```yaml
transformation_config:
  task_type: "sql_query"
  sql_task:
    warehouse_id: "your-warehouse-id"
    query_id: "abc123-def456-ghi789"
    parameters:
      threshold: "5.0"
```

### 3. SQL File Tasks

Execute SQL from files in your workspace or Git repository.

**Note:** `warehouse_id` is required for SQL tasks. It can be specified in `sql_task.warehouse_id` or provided via `default_warehouse_id` when initializing the orchestrator.

```yaml
transformation_config:
  task_type: "sql_file"
  sql_task:
    warehouse_id: "your-warehouse-id"
    sql_file_path: "/Workspace/path/to/query.sql"
    file_source: "WORKSPACE"  # Optional: WORKSPACE or GIT (default: WORKSPACE)
    parameters:
      catalog: "my_catalog"
      schema: "my_schema"
      max_hours: "24"
```

SQL files should use parameter syntax (`:parameter_name`) for values passed via `parameters`. Parameters can be static values or Databricks dynamic value references like `{{job.id}}`, `{{task.name}}`, `{{job.start_time.iso_date}}`, etc.

## Metadata Schema

Each module in your YAML must have:

```yaml
modules:
  - module_name: "my_pipeline"      # Required: Module name
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
    sources:
      - source_id: "unique_id"       # Required: Unique identifier
        source_type: "sql"           # Required: Type of source
        execution_order: 1           # Required: Execution order (for dependencies)
        source_config: {}            # Optional: Source-specific config (JSON)
        target_config: {}            # Optional: Target-specific config (JSON)
        transformation_config:       # Required: Task configuration
          task_type: "sql_query"      # Required: Type of task
          timeout_seconds: 3600      # Optional: Task-level timeout (default: 3600)
          # ... task-specific config
```

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

Task-level settings in `transformation_config`:

- `timeout_seconds`: Maximum time the task can run (default: 3600 seconds)

### Audit Fields

Both the control table and jobs table automatically track user information:

- `created_by`: Username of the user who created the record (set only on INSERT, never updated)
- `updated_by`: Username of the user who last updated the record (set on INSERT and UPDATE)
- `created_timestamp`: Timestamp when the record was created
- `updated_timestamp`: Timestamp when the record was last updated

These fields are automatically populated using Spark SQL's `current_user()` function.

## Execution Order and Dependencies

Tasks with the same `execution_order` run in parallel. Tasks with higher `execution_order` wait for all tasks in previous orders to complete.

Example:
```yaml
- execution_order: 1  # Tasks A and B run in parallel
- execution_order: 2  # Task C waits for A and B to complete
```

## Examples

See `examples/metadata_examples.yaml` for comprehensive examples including:
- Data quality checks
- Bronze to Silver transformations
- Mixed task type pipelines
- End-to-end data pipelines

## Best Practices

1. **Use SQL Files for Reusable Logic**: Store common SQL transformations in files
2. **Leverage Execution Order**: Design your pipelines with clear stages
3. **Parameterize SQL**: Use parameters and Databricks dynamic value references for flexible SQL
4. **Follow Naming Conventions**: Use clear, descriptive `source_id` values
5. **Document Your Pipelines**: Add descriptions to modules
6. **Use Task Parameters**: Leverage Databricks dynamic value references like `{{job.id}}`, `{{task.name}}`, `{{job.start_time.iso_date}}` for runtime values

## Future Enhancements

- [ ] Python Script Tasks
- [ ] Pipeline Tasks (Delta Live Tables)
- [ ] dbt Tasks
- [ ] Enhanced error handling and retry logic
- [ ] Execution monitoring dashboard
- [ ] Data quality framework

## Contributing

This is an internal framework. For questions or enhancements, contact the data engineering team.

## License

Internal use only.