"""Tests for JobOrchestrator class"""

import pytest
from unittest.mock import MagicMock, patch, Mock
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.jobs import SqlTaskQuery

from lakeflow_job_meta.orchestrator import JobOrchestrator


class TestJobOrchestrator:
    """Tests for JobOrchestrator class."""
    
    def test_init_success(self):
        """Test successful initialization."""
        orchestrator = JobOrchestrator("test_table")
        
        assert orchestrator.control_table == "test_table"
        assert orchestrator.jobs_table == "test_table_jobs"
        assert isinstance(orchestrator.workspace_client, WorkspaceClient)
    
    def test_init_custom_workspace_client(self, mock_workspace_client):
        """Test initialization with custom WorkspaceClient."""
        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        
        assert orchestrator.workspace_client == mock_workspace_client
    
    def test_init_invalid_table_name(self):
        """Test error with invalid table name."""
        with pytest.raises(ValueError):
            JobOrchestrator("")
        
        with pytest.raises(ValueError):
            JobOrchestrator(None)
    
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_create_job_tracking_table_success(self, mock_get_spark, mock_spark_session):
        """Test successful job tracking table creation."""
        mock_get_spark.return_value = mock_spark_session
        
        orchestrator = JobOrchestrator("test_catalog.schema.control_table")
        orchestrator._create_job_tracking_table()
        
        mock_spark_session.sql.assert_called_once()
        call_args = mock_spark_session.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_args
        assert "control_table_jobs" in call_args
    
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_create_job_tracking_table_error(self, mock_get_spark, mock_spark_session):
        """Test handling of table creation errors."""
        mock_get_spark.return_value = mock_spark_session
        mock_spark_session.sql.side_effect = Exception("Database error")
        
        orchestrator = JobOrchestrator("test_table")
        
        with pytest.raises(RuntimeError, match="Failed to create job tracking table"):
            orchestrator._create_job_tracking_table()
    
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_get_stored_job_id_found(self, mock_get_spark, mock_spark_session):
        """Test retrieving existing job ID."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_df = MagicMock()
        mock_df.filter.return_value.select.return_value.limit.return_value.count.return_value = 1
        
        mock_row = MagicMock()
        mock_row.__getitem__.return_value = 12345
        mock_df.filter.return_value.select.return_value.limit.return_value.collect.return_value = [mock_row]
        
        mock_spark_session.table.return_value = mock_df
        
        orchestrator = JobOrchestrator("test_table")
        result = orchestrator._get_stored_job_id("test_module")
        
        assert result == 12345
    
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_get_stored_job_id_not_found(self, mock_get_spark, mock_spark_session):
        """Test when job ID doesn't exist."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_df = MagicMock()
        mock_df.filter.return_value.select.return_value.limit.return_value.count.return_value = 0
        mock_spark_session.table.return_value = mock_df
        
        orchestrator = JobOrchestrator("test_table")
        result = orchestrator._get_stored_job_id("test_module")
        
        assert result is None
    
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_get_stored_job_id_error(self, mock_get_spark, mock_spark_session):
        """Test error handling during retrieval."""
        mock_get_spark.return_value = mock_spark_session
        mock_spark_session.table.side_effect = Exception("Table not found")
        
        orchestrator = JobOrchestrator("test_table")
        result = orchestrator._get_stored_job_id("test_module")
        
        # Should return None on error
        assert result is None
    
    @patch('lakeflow_job_meta.orchestrator.DeltaTable')
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_store_job_id_success(self, mock_get_spark, mock_delta_table, mock_spark_session):
        """Test successful storage of job ID."""
        mock_get_spark.return_value = mock_spark_session
        mock_delta_instance = MagicMock()
        mock_delta_table.forName.return_value = mock_delta_instance
        
        orchestrator = JobOrchestrator("test_table")
        orchestrator._store_job_id("test_module", 12345, "test_job")
        
        mock_delta_instance.alias.assert_called_once()
    
    @patch('lakeflow_job_meta.orchestrator.DeltaTable')
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_store_job_id_error(self, mock_get_spark, mock_delta_table, mock_spark_session):
        """Test error handling during storage."""
        mock_get_spark.return_value = mock_spark_session
        mock_delta_table.forName.side_effect = Exception("Storage error")
        
        orchestrator = JobOrchestrator("test_table")
        
        with pytest.raises(RuntimeError, match="Failed to store job_id"):
            orchestrator._store_job_id("test_module", 12345, "test_job")
    
    @patch('lakeflow_job_meta.orchestrator.create_task_from_config')
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_generate_tasks_for_module(self, mock_get_spark, mock_create_task, mock_spark_session):
        """Test generating tasks for a module."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_source = MagicMock()
        mock_source.__getitem__.side_effect = lambda key: {
            'source_id': 'source1',
            'execution_order': 1,
            'module_name': 'module1',
            'is_active': True
        }.get(key, None)
        
        mock_df = MagicMock()
        mock_df.filter.return_value.orderBy.return_value.collect.return_value = [mock_source]
        mock_spark_session.table.return_value = mock_df
        
        mock_create_task.return_value = {
            "task_key": "task1",
            "task_type": "notebook"
        }
        
        orchestrator = JobOrchestrator("test_table")
        tasks = orchestrator.generate_tasks_for_module("module1")
        
        assert len(tasks) == 1
        assert tasks[0]["task_key"] == "task1"
    
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_generate_tasks_no_sources(self, mock_get_spark, mock_spark_session):
        """Test error when no active sources found."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_df = MagicMock()
        mock_df.filter.return_value.orderBy.return_value.collect.return_value = []
        mock_spark_session.table.return_value = mock_df
        
        orchestrator = JobOrchestrator("test_table")
        
        with pytest.raises(ValueError, match="No active sources found"):
            orchestrator.generate_tasks_for_module("module1")
    
    @patch('lakeflow_job_meta.orchestrator.serialize_task_for_api')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator.generate_tasks_for_module')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator._get_stored_job_id')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator._store_job_id')
    @patch('lakeflow_job_meta.orchestrator.convert_task_config_to_sdk_task')
    def test_create_or_update_job_create_new(self, mock_convert, mock_store, mock_get_job_id, mock_generate_tasks, mock_serialize, mock_workspace_client):
        """Test creating a new job."""
        mock_get_job_id.return_value = None  # No existing job
        mock_generate_tasks.return_value = [
            {
                "task_key": "task1",
                "task_type": "notebook",
                "notebook_task": {"notebook_path": "/test/notebook", "base_parameters": {}}
            }
        ]
        # Mock Task object without sql_task (notebook task)
        mock_task = MagicMock()
        mock_task.sql_task = None  # No SQL task
        mock_convert.return_value = mock_task
        mock_serialize.return_value = {"task_key": "task1", "notebook_task": {}}
        
        # Mock jobs.create for new job creation
        mock_created_job = MagicMock()
        mock_created_job.job_id = 12345
        mock_workspace_client.jobs.create.return_value = mock_created_job
        
        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        job_id = orchestrator.create_or_update_job("test_module")
        
        assert job_id == 12345
        mock_workspace_client.jobs.create.assert_called_once()
        mock_store.assert_called_once()
    
    @patch('lakeflow_job_meta.orchestrator.serialize_task_for_api')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator.generate_tasks_for_module')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator._get_stored_job_id')
    @patch('lakeflow_job_meta.orchestrator.convert_task_config_to_sdk_task')
    def test_create_or_update_job_update_existing(self, mock_convert, mock_get_job_id, mock_generate_tasks, mock_serialize, mock_workspace_client):
        """Test updating an existing job."""
        mock_get_job_id.return_value = 12345  # Existing job
        mock_generate_tasks.return_value = [
            {
                "task_key": "task1",
                "task_type": "notebook",
                "notebook_task": {"notebook_path": "/test/notebook", "base_parameters": {}}
            }
        ]
        # Mock Task object without sql_task (notebook task)
        mock_task = MagicMock()
        mock_task.sql_task = None  # No SQL task
        mock_convert.return_value = mock_task
        mock_serialize.return_value = {"task_key": "task1", "notebook_task": {}}
        
        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        job_id = orchestrator.create_or_update_job("test_module")
        
        assert job_id == 12345
        mock_workspace_client.jobs.update.assert_called_once()
    
    @patch('lakeflow_job_meta.orchestrator.serialize_task_for_api')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator.generate_tasks_for_module')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator._get_stored_job_id')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator._store_job_id')
    @patch('lakeflow_job_meta.orchestrator.convert_task_config_to_sdk_task')
    def test_create_or_update_job_with_inline_sql_query(self, mock_convert, mock_store, mock_get_job_id, mock_generate_tasks, mock_serialize, mock_workspace_client):
        """Test creating a job with inline SQL query (auto-creates query)."""
        mock_get_job_id.return_value = None  # No existing job
        mock_generate_tasks.return_value = [
            {
                "task_key": "sql_task1",
                "task_type": "sql_query",
                "sql_task": {"warehouse_id": "warehouse123", "query": {"query": "SELECT 1"}}
            }
        ]
        # Mock Task object with inline SQL query (dict format)
        mock_task = MagicMock()
        mock_task.task_key = "sql_task1"
        mock_sql_task = MagicMock()
        mock_sql_task.warehouse_id = "warehouse123"
        mock_sql_task.query = {"query": "SELECT 1"}  # Inline SQL as dict
        mock_task.sql_task = mock_sql_task
        mock_convert.return_value = mock_task
        
        # Mock query creation
        mock_created_query = MagicMock()
        mock_created_query.id = "query_abc123"
        mock_workspace_client.queries.create.return_value = mock_created_query
        
        mock_serialize.return_value = {"task_key": "sql_task1", "sql_task": {"query": {"query_id": "query_abc123"}}}

        # Mock jobs.create for new job creation
        mock_created_job = MagicMock()
        mock_created_job.job_id = 12345
        mock_workspace_client.jobs.create.return_value = mock_created_job

        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        job_id = orchestrator.create_or_update_job("test_module")

        assert job_id == 12345
        # Verify query was created
        mock_workspace_client.queries.create.assert_called_once()
        # Verify task query was updated (check that SqlTaskQuery was assigned)
        # After query creation, sql_task.query should be SqlTaskQuery object
        assert isinstance(mock_task.sql_task.query, SqlTaskQuery)
        assert mock_task.sql_task.query.query_id == "query_abc123"
        mock_workspace_client.jobs.create.assert_called_once()
        mock_store.assert_called_once()
    
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator.generate_tasks_for_module')
    def test_create_or_update_job_no_tasks_error(self, mock_generate_tasks, mock_workspace_client):
        """Test error when no tasks are found."""
        mock_generate_tasks.return_value = []
        
        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        
        with pytest.raises(ValueError, match="No active tasks found"):
            orchestrator.create_or_update_job("test_module")
    
    def test_create_or_update_job_invalid_inputs(self, mock_workspace_client):
        """Test error handling for invalid inputs."""
        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        
        with pytest.raises(ValueError):
            orchestrator.create_or_update_job("")
    
    @patch('lakeflow_job_meta.orchestrator.MetadataManager')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator.ensure_setup')
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_run_all_modules_success(self, mock_get_spark, mock_ensure_setup, mock_metadata_manager, mock_spark_session, mock_workspace_client):
        """Test successful orchestration."""
        mock_get_spark.return_value = mock_spark_session
        
        # Mock modules
        mock_manager = MagicMock()
        mock_manager.get_all_modules.return_value = ['module1']
        mock_metadata_manager.return_value = mock_manager
        
        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        orchestrator.create_or_update_job = MagicMock(return_value=12345)
        
        jobs = orchestrator.run_all_modules(auto_run=False)
        
        assert len(jobs) == 1
        assert jobs[0]['module'] == 'module1'
        assert jobs[0]['job_id'] == 12345
    
    @patch('lakeflow_job_meta.orchestrator.MetadataManager')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator.ensure_setup')
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_run_all_modules_no_modules(self, mock_get_spark, mock_ensure_setup, mock_metadata_manager, mock_spark_session, mock_workspace_client):
        """Test handling when no modules exist."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_manager = MagicMock()
        mock_manager.get_all_modules.return_value = []
        mock_metadata_manager.return_value = mock_manager
        
        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        jobs = orchestrator.run_all_modules(auto_run=False)
        
        assert len(jobs) == 0
    
    @patch('lakeflow_job_meta.orchestrator.MetadataManager')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator.ensure_setup')
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_run_all_modules_partial_failure(self, mock_get_spark, mock_ensure_setup, mock_metadata_manager, mock_spark_session, mock_workspace_client):
        """Test handling when some modules fail."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_manager = MagicMock()
        mock_manager.get_all_modules.return_value = ['module1', 'module2']
        mock_metadata_manager.return_value = mock_manager
        
        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        # First succeeds, second fails
        orchestrator.create_or_update_job = MagicMock(side_effect=[12345, Exception("Job creation failed")])
        
        jobs = orchestrator.run_all_modules(auto_run=False)
        
        # Should have one successful job
        assert len(jobs) == 1
        assert jobs[0]['module'] == 'module1'
    
    @patch('lakeflow_job_meta.orchestrator.MetadataManager')
    @patch('lakeflow_job_meta.orchestrator.JobOrchestrator.ensure_setup')
    @patch('lakeflow_job_meta.orchestrator._get_spark')
    def test_run_all_modules_with_yaml(self, mock_get_spark, mock_ensure_setup, mock_metadata_manager, mock_spark_session, mock_workspace_client):
        """Test orchestration with YAML file loading."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_manager = MagicMock()
        mock_manager.get_all_modules.return_value = ['module1']
        mock_manager.load_yaml.return_value = 5  # 5 sources loaded
        mock_metadata_manager.return_value = mock_manager
        
        orchestrator = JobOrchestrator("test_table", workspace_client=mock_workspace_client)
        orchestrator.create_or_update_job = MagicMock(return_value=12345)
        jobs = orchestrator.run_all_modules(
            yaml_path='./test.yaml',
            sync_yaml=True,
            auto_run=False
        )
        
        mock_manager.load_yaml.assert_called_once_with('./test.yaml')
        assert len(jobs) == 1
