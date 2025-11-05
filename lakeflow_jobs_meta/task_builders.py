"""Task builder functions for creating different types of Databricks tasks"""

import json
import logging
from typing import Dict, Any, Optional
from databricks.sdk.service.jobs import Task, NotebookTask, SqlTask, SqlTaskQuery, SqlTaskFile, TaskDependency, Source
from lakeflow_jobs_meta.constants import (
    TASK_TYPE_NOTEBOOK,
    TASK_TYPE_SQL_QUERY,
    TASK_TYPE_SQL_FILE,
    TASK_TIMEOUT_SECONDS,
)
from lakeflow_jobs_meta.utils import sanitize_task_key, validate_notebook_path

logger = logging.getLogger(__name__)


def create_task_from_config(
    task_data: Dict[str, Any],
    control_table: str,
    depends_on_task_keys: Optional[List[str]] = None,
    cluster_id: Optional[str] = None,
    default_warehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a task configuration from task metadata.

    Args:
        task_data: Task dictionary from control table (can be dict or Row-like object)
        depends_on_task_keys: List of task_key strings this task depends on
        cluster_id: Optional cluster ID for the task
        default_warehouse_id: Optional default SQL warehouse ID for SQL tasks

    Returns:
        Task configuration dictionary

    Raises:
        ValueError: If task configuration is invalid
    """
    if not isinstance(task_data, dict):
        if hasattr(task_data, "asDict"):
            task_data = task_data.asDict()
        else:
            try:
                task_data = dict(task_data)
            except (TypeError, ValueError):
                pass

    task_key_raw = task_data.get("task_key")
    if not task_key_raw:
        raise ValueError("Task data must have 'task_key' field")
    task_key = sanitize_task_key(task_key_raw)

    task_type = task_data.get("task_type")
    if not task_type:
        raise ValueError(f"Task '{task_key}' must have 'task_type' field")

    # Parse task_config JSON
    try:
        task_config_json = json.loads(task_data.get("task_config", "{}"))
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"Invalid task_config JSON for task_key '{task_key}': {str(e)}")

    # Parse parameters JSON
    try:
        parameters_json = json.loads(task_data.get("parameters", "{}"))
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"Invalid parameters JSON for task_key '{task_key}': {str(e)}")

    if task_type == TASK_TYPE_NOTEBOOK:
        task_config = create_notebook_task_config(task_key, task_config_json, parameters_json, control_table)
    elif task_type == TASK_TYPE_SQL_QUERY:
        task_config = create_sql_query_task_config(task_key, task_config_json, parameters_json, default_warehouse_id)
    elif task_type == TASK_TYPE_SQL_FILE:
        task_config = create_sql_file_task_config(task_key, task_config_json, parameters_json, default_warehouse_id)
    else:
        raise ValueError(f"Unsupported task_type '{task_type}' for task_key '{task_key}'")

    if depends_on_task_keys:
        task_config["depends_on"] = [{"task_key": task_key} for task_key in depends_on_task_keys]
    
    # Add task-level timeout_seconds if specified in task_config
    if "timeout_seconds" in task_config_json:
        task_config["timeout_seconds"] = task_config_json["timeout_seconds"]
    
    # Set disabled flag from task metadata (defaults to False if not specified)
    task_config["disabled"] = task_data.get("disabled", False)

    return task_config


def create_notebook_task_config(
    task_key: str, task_config: Dict[str, Any], parameters: Dict[str, Any], control_table: str
) -> Dict[str, Any]:
    """Create notebook task configuration.

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains file_path, timeout_seconds, etc.)
        parameters: Task parameters dictionary
        control_table: Name of the control table containing metadata

    Returns:
        Notebook task configuration dictionary
    """
    file_path = task_config.get("file_path")
    if not file_path:
        raise ValueError(f"Missing file_path in task_config for task_key: {task_key}")

    validate_notebook_path(file_path)

    # Build base_parameters from task parameters + framework parameters
    base_parameters = dict(parameters)
    base_parameters["task_key"] = task_key
    base_parameters["control_table"] = control_table

    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_NOTEBOOK,
        "notebook_task": {
            "notebook_path": file_path,
            "base_parameters": base_parameters,
        },
    }


def _build_sql_task_parameters(parameters: Dict[str, Any]) -> Dict[str, str]:
    """Build SQL task parameters from parameters dictionary.
    
    Parameters can contain static values or Databricks dynamic value references.
    See: https://docs.databricks.com/aws/en/jobs/dynamic-value-references
    
    Args:
        parameters: Parameters dictionary
        
    Returns:
        Dictionary of parameter names to string values
    """
    return {k: str(v) for k, v in parameters.items()} if parameters else {}


def create_sql_query_task_config(
    task_key: str, task_config: Dict[str, Any], parameters: Dict[str, Any], default_warehouse_id: Optional[str] = None
) -> Dict[str, Any]:
    """Create SQL query task configuration.

    Note: warehouse_id is REQUIRED for SQL tasks per Databricks Jobs API.
    If not provided in task_config, will use default_warehouse_id if available.

    SQL queries should use parameter syntax (:parameter_name). Parameters are defined in 
    parameters dictionary and can use Databricks dynamic value references.
    
    See: https://docs.databricks.com/aws/en/jobs/dynamic-value-references

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains warehouse_id, sql_query, query_id, etc.)
        parameters: Task parameters dictionary
        default_warehouse_id: Optional default SQL warehouse ID to use if not specified in config

    Returns:
        SQL query task configuration dictionary

    Raises:
        ValueError: If warehouse_id is missing or neither sql_query nor query_id is provided
    """
    warehouse_id = task_config.get("warehouse_id")
    if warehouse_id and warehouse_id.lower() in ("your-warehouse-id", "your_warehouse_id"):
        warehouse_id = None
    warehouse_id = warehouse_id or default_warehouse_id
    if not warehouse_id:
        raise ValueError(
            f"Missing warehouse_id for task_key: {task_key}. "
            f"Either specify warehouse_id in task_config or provide default_warehouse_id to orchestrator."
        )

    sql_query = task_config.get("sql_query")
    query_id = task_config.get("query_id")

    if not sql_query and not query_id:
        raise ValueError(
            f"Must provide either sql_query or query_id for task_key: {task_key}"
        )

    task_parameters = _build_sql_task_parameters(parameters)

    result = {
        "task_key": task_key,
        "task_type": TASK_TYPE_SQL_QUERY,
        "sql_task": {
            "warehouse_id": warehouse_id,
            "parameters": task_parameters,
        },
    }

    if query_id:
        result["sql_task"]["query"] = {"query_id": query_id}
    else:
        result["sql_task"]["query"] = {"query": sql_query}

    return result


def create_sql_file_task_config(
    task_key: str, task_config: Dict[str, Any], parameters: Dict[str, Any], default_warehouse_id: Optional[str] = None
) -> Dict[str, Any]:
    """Create SQL file task configuration.

    SQL file tasks reference SQL files directly. SQL files should use parameter syntax 
    (:parameter_name). Parameters are defined in parameters dictionary and can use Databricks 
    dynamic value references.
    
    See: https://docs.databricks.com/aws/en/jobs/dynamic-value-references

    Note: warehouse_id is REQUIRED for SQL tasks per Databricks Jobs API.
    If not provided in task_config, will use default_warehouse_id if available.

    Args:
        task_key: Sanitized task key
        task_config: Task configuration dictionary (contains warehouse_id, file_path, etc.)
        parameters: Task parameters dictionary
        default_warehouse_id: Optional default SQL warehouse ID to use if not specified in config

    Returns:
        SQL file task configuration dictionary

    Raises:
        ValueError: If warehouse_id or file_path is missing
    """
    warehouse_id = task_config.get("warehouse_id")
    if warehouse_id and warehouse_id.lower() in ("your-warehouse-id", "your_warehouse_id"):
        warehouse_id = None
    warehouse_id = warehouse_id or default_warehouse_id
    if not warehouse_id:
        raise ValueError(
            f"Missing warehouse_id for task_key: {task_key}. "
            f"Either specify warehouse_id in task_config or provide default_warehouse_id to orchestrator."
        )

    file_path = task_config.get("file_path")
    if not file_path:
        raise ValueError(f"Missing file_path for task_key: {task_key}")

    file_source = task_config.get("file_source", "WORKSPACE")
    task_parameters = _build_sql_task_parameters(parameters)

    return {
        "task_key": task_key,
        "task_type": TASK_TYPE_SQL_FILE,
        "sql_task": {
            "warehouse_id": warehouse_id,
            "file": {
                "path": file_path,
                "source": file_source,
            },
            "parameters": task_parameters,
        },
    }


def convert_task_config_to_sdk_task(task_config: Dict[str, Any], cluster_id: Optional[str] = None) -> Task:
    """Convert task configuration dictionary to Databricks SDK Task object.

    Args:
        task_config: Task configuration dictionary
        cluster_id: Optional cluster ID

    Returns:
        Databricks SDK Task object
    """
    task_key = task_config["task_key"]
    task_type = task_config.get("task_type", TASK_TYPE_NOTEBOOK)

    task_dependencies = None
    if "depends_on" in task_config:
        task_dependencies = [TaskDependency(task_key=dep["task_key"]) for dep in task_config["depends_on"]]
    
    # Get task-level timeout_seconds if specified, otherwise use default
    task_timeout = task_config.get("timeout_seconds", TASK_TIMEOUT_SECONDS)
    task_disabled = task_config.get("disabled", False)

    # Create Task object (disabled is handled in serialization, not in constructor)
    task_obj = None
    
    if task_type == TASK_TYPE_NOTEBOOK:
        notebook_config = task_config["notebook_task"]
        task_obj = Task(
            task_key=task_key,
            notebook_task=NotebookTask(
                notebook_path=notebook_config["notebook_path"],
                base_parameters=notebook_config.get("base_parameters", {}),
            ),
            depends_on=task_dependencies,
            existing_cluster_id=cluster_id,
            timeout_seconds=task_timeout,
        )

    elif task_type == TASK_TYPE_SQL_QUERY:
        sql_config = task_config["sql_task"]
        query_config = sql_config.get("query", {})

        query_id = query_config.get("query_id")
        query_text = query_config.get("query")

        if query_id:
            sql_query = SqlTaskQuery(query_id=query_id)
        elif query_text:
            sql_query = {"query": query_text}
        else:
            raise ValueError(f"SQL query task '{task_key}' must have either query_id or query in query config")

        task_obj = Task(
            task_key=task_key,
            sql_task=SqlTask(
                warehouse_id=sql_config["warehouse_id"], query=sql_query, parameters=sql_config.get("parameters", {})
            ),
            depends_on=task_dependencies,
            timeout_seconds=task_timeout,
        )

    elif task_type == TASK_TYPE_SQL_FILE:
        sql_config = task_config["sql_task"]
        file_config = sql_config.get("file", {})

        if not file_config:
            raise ValueError(f"SQL file task '{task_key}' must have file configuration")

        file_path = file_config.get("path")
        file_source = file_config.get("source", "WORKSPACE")

        if not file_path:
            raise ValueError(f"SQL file task '{task_key}' must have file.path")

        try:
            source_enum = Source(file_source) if isinstance(file_source, str) else file_source
        except (ValueError, TypeError):
            source_enum = Source.WORKSPACE
        
        sql_file = SqlTaskFile(path=file_path, source=source_enum)

        task_obj = Task(
            task_key=task_key,
            sql_task=SqlTask(
                warehouse_id=sql_config["warehouse_id"],
                file=sql_file,
                parameters=sql_config.get("parameters", {}),
            ),
            depends_on=task_dependencies,
            timeout_seconds=task_timeout,
        )

    else:
        raise ValueError(f"Unsupported task_type '{task_type}' for task_key '{task_key}'")
    
    # Set disabled attribute after creation (SDK Task doesn't accept it in constructor)
    if task_disabled:
        task_obj.disabled = True
    
    return task_obj


def serialize_task_for_api(task: Task) -> Dict[str, Any]:
    """Serialize Task object to dictionary for API calls.

    Args:
        task: Task object to serialize

    Returns:
        Dictionary representation of the task suitable for jobs.create()/update()
    """
    result: Dict[str, Any] = {
        "task_key": task.task_key,
    }

    if task.depends_on:
        result["depends_on"] = [{"task_key": dep.task_key} for dep in task.depends_on]

    if task.timeout_seconds:
        result["timeout_seconds"] = task.timeout_seconds
    
    if hasattr(task, "disabled") and task.disabled is not None:
        result["disabled"] = task.disabled

    if task.existing_cluster_id:
        result["existing_cluster_id"] = task.existing_cluster_id

    if task.job_cluster_key:
        result["job_cluster_key"] = task.job_cluster_key

    if task.new_cluster:
        result["new_cluster"] = (
            task.new_cluster.as_dict() if hasattr(task.new_cluster, "as_dict") else task.new_cluster
        )

    if task.notebook_task:
        if hasattr(task.notebook_task, "as_dict"):
            result["notebook_task"] = task.notebook_task.as_dict()
        else:
            result["notebook_task"] = {
                "notebook_path": task.notebook_task.notebook_path,
                "base_parameters": task.notebook_task.base_parameters or {},
            }

    if task.sql_task:
        sql_dict: Dict[str, Any] = {"warehouse_id": task.sql_task.warehouse_id}

        if isinstance(task.sql_task.query, str):
            sql_dict["query"] = {"query": task.sql_task.query}
        elif isinstance(task.sql_task.query, dict):
            sql_dict["query"] = task.sql_task.query
        elif hasattr(task.sql_task.query, "as_dict"):
            query_dict = task.sql_task.query.as_dict()
            if "query_id" in query_dict and query_dict["query_id"]:
                sql_dict["query"] = {"query_id": query_dict["query_id"]}
            else:
                raise ValueError("SqlTaskQuery must have query_id")
        elif task.sql_task.query:
            sql_dict["query"] = {"query_id": task.sql_task.query.query_id}

        if task.sql_task.parameters:
            sql_dict["parameters"] = task.sql_task.parameters

        if task.sql_task.alert:
            if hasattr(task.sql_task.alert, "as_dict"):
                sql_dict["alert"] = task.sql_task.alert.as_dict()
            else:
                sql_dict["alert"] = task.sql_task.alert

        if task.sql_task.dashboard:
            if hasattr(task.sql_task.dashboard, "as_dict"):
                sql_dict["dashboard"] = task.sql_task.dashboard.as_dict()
            else:
                sql_dict["dashboard"] = task.sql_task.dashboard

        if task.sql_task.file:
            if hasattr(task.sql_task.file, "as_dict"):
                sql_dict["file"] = task.sql_task.file.as_dict()
            else:
                sql_dict["file"] = {
                    "path": task.sql_task.file.path,
                    "source": (
                        task.sql_task.file.source.value
                        if hasattr(task.sql_task.file.source, "value")
                        else task.sql_task.file.source
                    ),
                }

        result["sql_task"] = sql_dict

    if task.spark_python_task:
        if hasattr(task.spark_python_task, "as_dict"):
            result["spark_python_task"] = task.spark_python_task.as_dict()
        else:
            result["spark_python_task"] = task.spark_python_task

    if task.spark_submit_task:
        if hasattr(task.spark_submit_task, "as_dict"):
            result["spark_submit_task"] = task.spark_submit_task.as_dict()
        else:
            result["spark_submit_task"] = task.spark_submit_task

    if task.python_wheel_task:
        if hasattr(task.python_wheel_task, "as_dict"):
            result["python_wheel_task"] = task.python_wheel_task.as_dict()
        else:
            result["python_wheel_task"] = task.python_wheel_task

    return result
