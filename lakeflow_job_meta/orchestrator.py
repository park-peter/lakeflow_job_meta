"""Main orchestration functions and classes for generating and managing Databricks jobs"""

import json
import logging
from typing import Optional, List, Dict, Any
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, SqlTaskQuery
from delta.tables import DeltaTable

from lakeflow_job_meta.task_builders import (
    create_task_from_config,
    convert_task_config_to_sdk_task,
    serialize_task_for_api,
)
from lakeflow_job_meta.metadata_manager import MetadataManager


class JobSettingsWithDictTasks(JobSettings):
    """Custom JobSettings that handles pre-serialized dict tasks.

    Overrides as_dict() to correctly handle tasks that are already dictionaries
    (serialized for API calls) rather than Task objects. This is necessary when
    updating jobs, as tasks may be serialized dicts with query_id references
    for SQL queries.
    """

    def as_dict(self) -> Dict[str, Any]:
        """Override as_dict() to handle dict tasks correctly."""
        result: Dict[str, Any] = {}
        if self.name:
            result["name"] = self.name
        if self.tasks:
            # If tasks are dicts, use them directly; otherwise call as_dict()
            result["tasks"] = [task if isinstance(task, dict) else task.as_dict() for task in self.tasks]
        if self.max_concurrent_runs is not None:
            result["max_concurrent_runs"] = self.max_concurrent_runs
        if self.timeout_seconds is not None:
            result["timeout_seconds"] = self.timeout_seconds
        if hasattr(self, "queue") and self.queue is not None:
            result["queue"] = (
                self.queue
                if isinstance(self.queue, dict)
                else self.queue.as_dict() if hasattr(self.queue, "as_dict") else self.queue
            )
        if hasattr(self, "continuous") and self.continuous is not None:
            result["continuous"] = (
                self.continuous
                if isinstance(self.continuous, dict)
                else self.continuous.as_dict() if hasattr(self.continuous, "as_dict") else self.continuous
            )
        if hasattr(self, "trigger") and self.trigger is not None:
            result["trigger"] = (
                self.trigger
                if isinstance(self.trigger, dict)
                else self.trigger.as_dict() if hasattr(self.trigger, "as_dict") else self.trigger
            )
        if hasattr(self, "schedule") and self.schedule is not None:
            result["schedule"] = (
                self.schedule
                if isinstance(self.schedule, dict)
                else self.schedule.as_dict() if hasattr(self.schedule, "as_dict") else self.schedule
            )
        return result


logger = logging.getLogger(__name__)


def _get_spark():
    """Get active Spark session (always available in Databricks runtime)."""
    return SparkSession.getActiveSession()


def _get_current_user() -> str:
    """Get current username from Spark SQL context.

    Returns:
        Current username as string, or 'unknown' if unable to determine
    """
    try:
        spark = _get_spark()
        if spark:
            result = spark.sql("SELECT current_user() as user").first()
            if result:
                return result["user"] or "unknown"
    except Exception:
        pass
    return "unknown"


class JobOrchestrator:
    """Orchestrates Databricks Jobs based on metadata in control table.

    Encapsulates job creation, updates, and management operations.
    Reduces parameter passing by maintaining state for control_table,
    workspace_client, and jobs_table.

    Example:
        ```python
        orchestrator = JobOrchestrator("catalog.schema.control_table")
        orchestrator.ensure_setup()  # Create tables if needed
        job_id = orchestrator.create_or_update_job("my_module")

        # Or run all modules
        results = orchestrator.run_all_modules(auto_run=True)
        ```
    """

    def __init__(
        self,
        control_table: str,
        workspace_client: Optional[WorkspaceClient] = None,
        default_warehouse_id: Optional[str] = None,
    ):
        """Initialize JobOrchestrator.

        Args:
            control_table: Name of the control table (e.g., "catalog.schema.table")
            workspace_client: Optional WorkspaceClient instance (creates new if not provided)
            default_warehouse_id: Optional default SQL warehouse ID for SQL tasks.
                This acts as a fallback when SQL tasks don't specify warehouse_id in their
                sql_task configuration. If neither is provided, task creation will fail.
                Useful when all SQL tasks in your modules use the same warehouse.
        """
        if not control_table or not isinstance(control_table, str):
            raise ValueError("control_table must be a non-empty string")

        self.control_table = control_table
        self.jobs_table = f"{control_table}_jobs"
        self.workspace_client = workspace_client or WorkspaceClient()
        self.metadata_manager = MetadataManager(control_table)
        self.default_warehouse_id = default_warehouse_id

    def _create_job_tracking_table(self) -> None:
        """Create Delta table to track job IDs for each module (internal)."""
        spark = _get_spark()
        try:
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.jobs_table} (
                    module_name STRING,
                    job_id BIGINT,
                    job_name STRING,
                    created_by STRING,
                    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
                    updated_by STRING,
                    updated_timestamp TIMESTAMP DEFAULT current_timestamp()
                )
                TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported')
            """
            )
            logger.info(f"Job tracking table {self.jobs_table} created/verified successfully")
        except Exception as e:
            raise RuntimeError(f"Failed to create job tracking table '{self.jobs_table}': {str(e)}")

    def ensure_setup(self) -> None:
        """Ensure control table and jobs tracking table exist."""
        self.metadata_manager.ensure_exists()
        self._create_job_tracking_table()

    def _get_stored_job_id(self, module_name: str) -> Optional[int]:
        """Get stored job_id for a module from Delta table (internal)."""
        spark = _get_spark()
        try:
            job_id_rows = (
                spark.table(self.jobs_table)
                .filter(F.col("module_name") == module_name)
                .select("job_id")
                .limit(1)
                .collect()
            )

            if job_id_rows:
                return job_id_rows[0]["job_id"]
            return None
        except Exception as e:
            logger.warning(f"Module '{module_name}': Could not retrieve job_id: {str(e)}")
            return None

    def _store_job_id(self, module_name: str, job_id: int, job_name: str) -> None:
        """Store/update job_id for a module in Delta table (internal)."""
        spark = _get_spark()
        try:
            current_user = _get_current_user()
            source_data = [
                Row(
                    module_name=module_name,
                    job_id=job_id,
                    job_name=job_name,
                    created_by=current_user,
                    updated_by=current_user,
                )
            ]
            source_df = spark.createDataFrame(source_data).withColumn("updated_timestamp", F.current_timestamp())
            source_df.createOrReplaceTempView("source_data")

            spark.sql(
                f"""
                MERGE INTO {self.jobs_table} AS target
                USING source_data AS source
                ON target.module_name = source.module_name
                WHEN MATCHED THEN
                    UPDATE SET
                        job_name = source.job_name,
                        updated_by = source.updated_by,
                        updated_timestamp = source.updated_timestamp
                WHEN NOT MATCHED THEN
                    INSERT (module_name, job_id, job_name, created_by, updated_by, updated_timestamp)
                    VALUES (
                        source.module_name,
                        source.job_id,
                        source.job_name,
                        source.created_by,
                        source.updated_by,
                        source.updated_timestamp,
                    )
            """
            )

            logger.info(f"Module '{module_name}': Stored job_id {job_id}")
        except Exception as e:
            logger.error(f"Could not store job_id: {str(e)}")
            raise RuntimeError(f"Failed to store job_id for module '{module_name}': {str(e)}") from e

    def get_job_settings_for_module(self, module_name: str) -> Dict[str, Any]:
        """Get job-level settings for a module from metadata.

        Retrieves job_config stored in the first source's source_config (set when loading YAML).
        Falls back to defaults if not found.

        Args:
            module_name: Name of the module

        Returns:
            Dictionary with job settings (timeout_seconds, max_concurrent_runs, queue, continuous, trigger, schedule)
        """
        from lakeflow_job_meta.constants import JOB_TIMEOUT_SECONDS, MAX_CONCURRENT_RUNS

        spark = _get_spark()
        try:
            # Get first source to retrieve job_config
            module_sources = (
                spark.table(self.control_table)
                .filter((F.col("module_name") == module_name) & (F.col("is_active") == True))
                .orderBy("execution_order")
                .limit(1)
                .collect()
            )

            # Default settings
            job_settings = {
                "timeout_seconds": JOB_TIMEOUT_SECONDS,
                "max_concurrent_runs": MAX_CONCURRENT_RUNS,
                "queue": None,
                "continuous": None,
                "trigger": None,
                "schedule": None,
            }

            # Check if job_config is stored in first source's source_config
            if module_sources:
                source_row = module_sources[0]
                if hasattr(source_row, "asDict"):
                    source = source_row.asDict()
                else:
                    source = dict(source_row)

                source_config_str = source.get("source_config", "{}")
                try:
                    source_config = (
                        json.loads(source_config_str) if isinstance(source_config_str, str) else source_config_str
                    )
                    job_config = source_config.get("_job_config", {})

                    if job_config:
                        if "timeout_seconds" in job_config:
                            job_settings["timeout_seconds"] = job_config["timeout_seconds"]
                        if "max_concurrent_runs" in job_config:
                            job_settings["max_concurrent_runs"] = job_config["max_concurrent_runs"]
                        if "queue" in job_config:
                            job_settings["queue"] = job_config["queue"]
                        if "continuous" in job_config:
                            job_settings["continuous"] = job_config["continuous"]
                        if "trigger" in job_config:
                            job_settings["trigger"] = job_config["trigger"]
                        if "schedule" in job_config:
                            job_settings["schedule"] = job_config["schedule"]
                except (json.JSONDecodeError, TypeError):
                    pass

            return job_settings

        except Exception as e:
            logger.warning(f"Could not retrieve job settings for module '{module_name}': {str(e)}. Using defaults.")
            return {
                "timeout_seconds": JOB_TIMEOUT_SECONDS,
                "max_concurrent_runs": MAX_CONCURRENT_RUNS,
                "queue": None,
                "continuous": None,
                "trigger": None,
                "schedule": None,
            }

    def generate_tasks_for_module(self, module_name: str) -> List[Dict[str, Any]]:
        """Generate task configurations for a module based on metadata.

        Args:
            module_name: Name of the module to generate tasks for

        Returns:
            List of task configuration dictionaries

        Raises:
            ValueError: If module_name is invalid, no active sources found, or config is invalid
            RuntimeError: If control table doesn't exist or is inaccessible
        """
        if not module_name or not isinstance(module_name, str):
            raise ValueError("module_name must be a non-empty string")

        spark = _get_spark()
        try:
            module_sources = (
                spark.table(self.control_table)
                .filter((F.col("module_name") == module_name) & (F.col("is_active") == True))
                .orderBy("execution_order")
                .collect()
            )
        except Exception as e:
            raise RuntimeError(f"Failed to read control table '{self.control_table}': {str(e)}")

        if not module_sources:
            raise ValueError(f"No active sources found for module '{module_name}'")

        # Group by execution order
        order_groups: Dict[int, List[Dict[str, Any]]] = {}
        for source_row in module_sources:
            if hasattr(source_row, "asDict"):
                source = source_row.asDict()
            elif isinstance(source_row, dict):
                source = source_row
            else:
                source = dict(source_row)
            order = source["execution_order"]
            if order not in order_groups:
                order_groups[order] = []
            order_groups[order].append(source)

        # Create task structure with dependencies
        tasks: List[Dict[str, Any]] = []
        previous_order_tasks: List[str] = []

        for order in sorted(order_groups.keys()):
            current_order_tasks: List[str] = []

            for source in order_groups[order]:
                try:
                    task_config = create_task_from_config(
                        source=source,
                        control_table=self.control_table,
                        previous_order_tasks=previous_order_tasks,
                        default_warehouse_id=self.default_warehouse_id,
                    )
                    tasks.append(task_config)
                    current_order_tasks.append(task_config["task_key"])
                except Exception as e:
                    logger.error(f"Failed to create task for source_id '{source['source_id']}': {str(e)}")
                    raise

            previous_order_tasks = current_order_tasks

        return tasks

    def create_or_update_job(self, module_name: str, cluster_id: Optional[str] = None) -> int:
        """Create new job or update existing job using stored job_id.

        Follows Databricks Jobs API requirements:
        - Uses JobSettings for both create and update operations
        - Tasks are required (will raise ValueError if empty)
        - Job name format: "Metadata_Driven_ETL_{module_name}"

        Args:
            module_name: Name of the module
            cluster_id: Optional cluster ID to use for tasks (applies to all tasks)

        Returns:
            The job ID (either updated or newly created)

        Raises:
            ValueError: If inputs are invalid or no tasks found
            RuntimeError: If job creation/update fails
        """
        if not module_name or not isinstance(module_name, str):
            raise ValueError("module_name must be a non-empty string")

        job_name = f"Metadata_Driven_ETL_{module_name}"

        # Get stored job_id
        stored_job_id = self._get_stored_job_id(module_name)

        # Generate task definitions
        task_definitions = self.generate_tasks_for_module(module_name)

        if not task_definitions or len(task_definitions) == 0:
            raise ValueError(f"No active tasks found for module '{module_name}'. Cannot create job without tasks.")

        sdk_task_objects = []
        sdk_task_dicts = []
        for task_def in task_definitions:
            sdk_task = convert_task_config_to_sdk_task(task_def, cluster_id)

            needs_query_creation = (
                hasattr(sdk_task, "sql_task")
                and sdk_task.sql_task
                and not sdk_task.sql_task.file
                and isinstance(sdk_task.sql_task.query, dict)
                and "query" in sdk_task.sql_task.query
                and "query_id" not in sdk_task.sql_task.query
            )
            if needs_query_creation:
                query_text = sdk_task.sql_task.query["query"]
                warehouse_id = sdk_task.sql_task.warehouse_id

                try:
                    from databricks.sdk.service.sql import CreateQueryRequestQuery

                    query_name = f"LakeflowJobMeta_{module_name}_{sdk_task.task_key}"
                    created_query = self.workspace_client.queries.create(
                        query=CreateQueryRequestQuery(
                            display_name=query_name, warehouse_id=warehouse_id, query_text=query_text
                        )
                    )
                    query_id = created_query.id
                    logger.debug(f"Created query '{query_name}' (ID: {query_id})")

                    sdk_task.sql_task.query = SqlTaskQuery(query_id=query_id)
                except Exception as e:
                    logger.error(f"Failed to create query for task '{sdk_task.task_key}': {str(e)}")
                    raise RuntimeError(f"Failed to create query for task '{sdk_task.task_key}': {str(e)}") from e

            sdk_task_objects.append(sdk_task)
            task_dict = serialize_task_for_api(sdk_task)
            sdk_task_dicts.append(task_dict)

        # Get job-level settings from metadata
        job_settings_config = self.get_job_settings_for_module(module_name)

        # Try to update existing job first
        if stored_job_id:
            try:
                job_settings = JobSettingsWithDictTasks(
                    name=job_name,
                    tasks=sdk_task_dicts,
                    max_concurrent_runs=job_settings_config["max_concurrent_runs"],
                    timeout_seconds=job_settings_config["timeout_seconds"],
                )

                # Add queue, continuous, trigger, and schedule if specified
                if job_settings_config.get("queue"):
                    job_settings.queue = job_settings_config["queue"]
                if job_settings_config.get("continuous"):
                    job_settings.continuous = job_settings_config["continuous"]
                if job_settings_config.get("trigger"):
                    job_settings.trigger = job_settings_config["trigger"]
                if job_settings_config.get("schedule"):
                    job_settings.schedule = job_settings_config["schedule"]

                self.workspace_client.jobs.update(
                    job_id=stored_job_id,
                    new_settings=job_settings,
                )
                logger.info(f"Module '{module_name}': Job updated successfully (Job ID: {stored_job_id})")
                self._store_job_id(module_name, stored_job_id, job_name)
                return stored_job_id

            except Exception as e:
                error_str = str(e).lower()
                if "does not exist" in error_str or "not found" in error_str:
                    logger.warning(
                        f"Module '{module_name}': Stored job ID {stored_job_id} no longer exists. Creating new job..."
                    )
                    try:
                        spark_del = _get_spark()
                        delta_table = DeltaTable.forName(spark_del, self.jobs_table)
                        delta_table.delete(F.col("module_name") == module_name)
                    except Exception as delete_error:
                        logger.warning(f"Could not delete invalid job_id record: {str(delete_error)}")
                else:
                    logger.error(f"Module '{module_name}': Error updating job {stored_job_id}: {str(e)}")
                    raise RuntimeError(
                        f"Failed to update job {stored_job_id} for module '{module_name}': {str(e)}"
                    ) from e

        # Create new job
        try:
            job_settings_kwargs = {
                "name": job_name,
                "tasks": sdk_task_objects,
                "max_concurrent_runs": job_settings_config["max_concurrent_runs"],
                "timeout_seconds": job_settings_config["timeout_seconds"],
            }

            # Add queue, continuous, trigger, and schedule if specified
            if job_settings_config.get("queue"):
                job_settings_kwargs["queue"] = job_settings_config["queue"]
            if job_settings_config.get("continuous"):
                job_settings_kwargs["continuous"] = job_settings_config["continuous"]
            if job_settings_config.get("trigger"):
                job_settings_kwargs["trigger"] = job_settings_config["trigger"]
            if job_settings_config.get("schedule"):
                job_settings_kwargs["schedule"] = job_settings_config["schedule"]

            created_job = self.workspace_client.jobs.create(**job_settings_kwargs)
            created_job_id = created_job.job_id
            if not created_job_id:
                raise RuntimeError(f"Job creation succeeded but no job_id returned: {created_job}")

            logger.info(f"Module '{module_name}': Job created successfully (Job ID: {created_job_id})")

            self._store_job_id(module_name, created_job_id, job_name)

            return created_job_id

        except Exception as e:
            logger.error(f"Module '{module_name}': Error creating job: {str(e)}")
            raise RuntimeError(f"Failed to create job for module '{module_name}': {str(e)}") from e

    def run_all_modules(
        self, auto_run: bool = True, yaml_path: Optional[str] = None, sync_yaml: bool = False
    ) -> List[Dict[str, Any]]:
        """Create and optionally run jobs for all modules in control table.

        Args:
            auto_run: Whether to automatically run jobs after creation (default: True)
            yaml_path: Optional path to YAML file to load before orchestrating
            sync_yaml: Whether to load YAML file if provided (default: False)

        Returns:
            List of dictionaries with module names and job IDs
        """
        logger.info(f"Starting orchestration with control_table: {self.control_table}")

        # Ensure setup
        self.ensure_setup()

        # Optionally load YAML if provided
        if yaml_path and sync_yaml:
            try:
                sources_loaded = self.metadata_manager.load_yaml(yaml_path)
                logger.info(f"Loaded {sources_loaded} sources from YAML before orchestrating")
            except FileNotFoundError:
                logger.warning(f"YAML file not found: {yaml_path}. Continuing with existing table data.")
            except Exception as e:
                logger.warning(f"Failed to load YAML file: {str(e)}. Continuing with existing table data.")

        modules = self.metadata_manager.get_all_modules()

        if not modules:
            logger.warning(f"No modules found in control table '{self.control_table}'")
            return []

        created_jobs = []
        failed_modules = []

        for module_name in modules:
            try:
                job_id = self.create_or_update_job(module_name)
                created_jobs.append({"module": module_name, "job_id": job_id})

                if auto_run:
                    try:
                        run_result = self.workspace_client.jobs.run_now(job_id=job_id)
                        logger.info(
                            f"Module '{module_name}': Started job run (Job ID: {job_id}, Run ID: {run_result.run_id})"
                        )
                    except Exception as run_error:
                        logger.error(
                            f"Module '{module_name}': Failed to start job run (Job ID: {job_id}): {str(run_error)}"
                        )

            except Exception as e:
                logger.error(f"Module '{module_name}': Failed to create/run job: {str(e)}")
                failed_modules.append({"module": module_name, "error": str(e)})

        if failed_modules:
            logger.warning(f"Failed to process {len(failed_modules)} module(s): {failed_modules}")

        logger.info(f"Orchestration completed. Managed {len(created_jobs)} job(s) successfully")
        return created_jobs
