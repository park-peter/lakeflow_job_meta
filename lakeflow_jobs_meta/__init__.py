"""
Lakeflow Jobs Meta - Metadata-driven framework for Databricks Lakeflow Jobs

A library for orchestrating Databricks Jobs from metadata stored in Delta tables
or YAML files.
"""

__version__ = "0.1.0"

from typing import Optional, List, Dict, Any
from lakeflow_jobs_meta.orchestrator import JobOrchestrator
from lakeflow_jobs_meta.metadata_manager import MetadataManager
from lakeflow_jobs_meta.monitor import MetadataMonitor

__all__ = [
    "JobOrchestrator",
    "MetadataManager",
    "MetadataMonitor",
    "create_or_update_job",
    "create_or_update_jobs",
    "load_yaml",
    "sync_from_volume",
]


def create_or_update_job(
    job_name: str,
    control_table: Optional[str] = None,
    cluster_id: Optional[str] = None,
    default_warehouse_id: Optional[str] = None,
    jobs_table: Optional[str] = None,
    workspace_client: Optional[Any] = None,
    default_queries_path: Optional[str] = None,
) -> int:
    """Convenience function to create or update a single job.

    Args:
        job_name: Name of the job to create or update
        control_table: Name of the control table (defaults to
            "main.default.job_metadata_control_table")
        cluster_id: Optional cluster ID to use for tasks
        default_warehouse_id: Optional default SQL warehouse ID for SQL tasks
        jobs_table: Optional custom name for the jobs tracking table
        workspace_client: Optional WorkspaceClient instance
        default_queries_path: Optional directory path where inline SQL queries
            will be saved (e.g., "/Workspace/Shared/LakeflowQueriesMeta")

    Returns:
        The job ID (either updated or newly created)

    Example:
        ```python
        import lakeflow_jobs_meta as jm

        job_id = jm.create_or_update_job(
            "my_pipeline",
            control_table="catalog.schema.etl_control",
            default_queries_path="/Workspace/Shared/Queries"
        )
        ```
    """
    orchestrator = JobOrchestrator(
        control_table=control_table,
        jobs_table=jobs_table,
        workspace_client=workspace_client,
        default_warehouse_id=default_warehouse_id,
        default_queries_path=default_queries_path,
    )
    return orchestrator.create_or_update_job(job_name, cluster_id=cluster_id)


def create_or_update_jobs(
    control_table: Optional[str] = None,
    auto_run: bool = True,
    yaml_path: Optional[str] = None,
    sync_yaml: bool = False,
    default_warehouse_id: Optional[str] = None,
    jobs_table: Optional[str] = None,
    workspace_client: Optional[Any] = None,
    default_queries_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Convenience function to create or update all jobs in the control table.

    Args:
        control_table: Name of the control table (defaults to
            "main.default.job_metadata_control_table")
        auto_run: Whether to automatically run jobs after creation (default: True)
        yaml_path: Optional path to YAML file to load before orchestrating
        sync_yaml: Whether to load YAML file if provided (default: False)
        default_warehouse_id: Optional default SQL warehouse ID for SQL tasks
        jobs_table: Optional custom name for the jobs tracking table
        workspace_client: Optional WorkspaceClient instance
        default_queries_path: Optional directory path where inline SQL queries
            will be saved (e.g., "/Workspace/Shared/LakeflowQueriesMeta")

    Returns:
        List of dictionaries with job names and job IDs

    Example:
        ```python
        import lakeflow_jobs_meta as jm

        jobs = jm.create_or_update_jobs(
            control_table="catalog.schema.etl_control",
            auto_run=False,
            default_queries_path="/Workspace/Shared/Queries"
        )
        ```
    """
    orchestrator = JobOrchestrator(
        control_table=control_table,
        jobs_table=jobs_table,
        workspace_client=workspace_client,
        default_warehouse_id=default_warehouse_id,
        default_queries_path=default_queries_path,
    )
    return orchestrator.create_or_update_jobs(
        auto_run=auto_run, yaml_path=yaml_path, sync_yaml=sync_yaml
    )


def load_yaml(
    yaml_path: str,
    control_table: Optional[str] = None,
    validate_file_exists: bool = True,
) -> int:
    """Convenience function to load YAML metadata file into control table.

    Args:
        yaml_path: Path to YAML file
        control_table: Name of the control table (defaults to
            "main.default.job_metadata_control_table")
        validate_file_exists: Whether to check if file exists before loading

    Returns:
        Number of tasks loaded

    Example:
        ```python
        import lakeflow_jobs_meta as jm

        tasks_loaded = jm.load_yaml(
            "./examples/metadata_examples.yaml",
            control_table="catalog.schema.etl_control"
        )
        ```
    """
    manager = MetadataManager(control_table or "main.default.job_metadata_control_table")
    return manager.load_yaml(yaml_path, validate_file_exists=validate_file_exists)


def sync_from_volume(
    volume_path: str,
    control_table: Optional[str] = None,
) -> int:
    """Convenience function to sync all YAML files from Unity Catalog volume.

    Args:
        volume_path: Path to Unity Catalog volume
            (e.g., '/Volumes/catalog/schema/volume')
        control_table: Name of the control table (defaults to
            "main.default.job_metadata_control_table")

    Returns:
        Total number of tasks loaded across all YAML files

    Example:
        ```python
        import lakeflow_jobs_meta as jm

        tasks_loaded = jm.sync_from_volume(
            "/Volumes/catalog/schema/metadata_volume",
            control_table="catalog.schema.etl_control"
        )
        ```
    """
    manager = MetadataManager(control_table or "main.default.job_metadata_control_table")
    return manager.sync_from_volume(volume_path)
