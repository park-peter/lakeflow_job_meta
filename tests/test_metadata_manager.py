"""Tests for MetadataManager class"""

import pytest
import tempfile
import os
import yaml
from unittest.mock import MagicMock, patch
from lakeflow_jobs_meta.metadata_manager import MetadataManager


class TestMetadataManager:
    """Tests for MetadataManager class."""
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_ensure_exists_success(self, mock_get_spark, mock_spark_session):
        """Test successful table creation."""
        mock_get_spark.return_value = mock_spark_session
        
        manager = MetadataManager("test_catalog.schema.control_table")
        manager.ensure_exists()
        
        mock_spark_session.sql.assert_called_once()
        call_args = mock_spark_session.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_args
        assert "test_catalog.schema.control_table" in call_args
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_ensure_exists_table_creation_error(self, mock_get_spark, mock_spark_session):
        """Test handling of table creation errors."""
        mock_get_spark.return_value = mock_spark_session
        mock_spark_session.sql.side_effect = Exception("Database error")
        
        manager = MetadataManager("test_table")
        
        with pytest.raises(RuntimeError, match="Failed to create control table"):
            manager.ensure_exists()
    
    def test_init_invalid_table_name(self):
        """Test error with invalid table name."""
        with pytest.raises(ValueError):
            MetadataManager("")
        
        with pytest.raises(ValueError):
            MetadataManager(None)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_valid(self, mock_get_spark, mock_spark_session, sample_yaml_config):
        """Test loading valid YAML file."""
        mock_get_spark.return_value = mock_spark_session
        
        # Create temporary YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(sample_yaml_config, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock DataFrame operations
            mock_df = MagicMock()
            mock_df.createOrReplaceTempView = MagicMock()
            mock_spark_session.createDataFrame = MagicMock(return_value=mock_df)
            mock_spark_session.sql = MagicMock()
            
            manager = MetadataManager("test_table")
            result = manager.load_yaml(tmp_path)
            
            assert result == 1  # One task loaded
            mock_spark_session.createDataFrame.assert_called_once()
        finally:
            os.unlink(tmp_path)
    
    def test_load_yaml_file_not_found(self):
        """Test error when YAML file doesn't exist."""
        manager = MetadataManager("test_table")
        
        with pytest.raises(FileNotFoundError):
            manager.load_yaml("nonexistent.yaml")
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_file_not_found_skip_validation(self, mock_get_spark, mock_spark_session):
        """Test skipping file existence validation."""
        mock_get_spark.return_value = mock_spark_session
        
        manager = MetadataManager("test_table")
        
        with pytest.raises(ValueError):  # Should fail later trying to parse
            mock_spark_session.sql.side_effect = Exception("Cannot parse")
            manager.load_yaml("nonexistent.yaml", validate_file_exists=False)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_invalid(self, mock_get_spark, mock_spark_session):
        """Test handling of invalid YAML."""
        mock_get_spark.return_value = mock_spark_session
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            tmp.write("invalid: yaml: content: [")
            tmp_path = tmp.name
        
        try:
            manager = MetadataManager("test_table")
            with pytest.raises(ValueError, match="Failed to parse YAML"):
                manager.load_yaml(tmp_path)
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_empty_jobs(self, mock_get_spark, mock_spark_session):
        """Test handling of YAML with no jobs."""
        mock_get_spark.return_value = mock_spark_session
        
        empty_config = {'jobs': []}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(empty_config, tmp)
            tmp_path = tmp.name
        
        try:
            mock_df = MagicMock()
            mock_spark_session.createDataFrame = MagicMock(return_value=mock_df)
            mock_spark_session.sql = MagicMock()
            
            manager = MetadataManager("test_table")
            result = manager.load_yaml(tmp_path)
            assert result == 0
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_missing_jobs_key(self, mock_get_spark, mock_spark_session):
        """Test error when YAML lacks jobs key."""
        mock_get_spark.return_value = mock_spark_session
        
        invalid_config = {}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(invalid_config, tmp)
            tmp_path = tmp.name
        
        try:
            manager = MetadataManager("test_table")
            with pytest.raises(ValueError, match="must contain 'jobs' key"):
                manager.load_yaml(tmp_path)
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_load_yaml_missing_task_type(self, mock_get_spark, mock_spark_session):
        """Test error when task_type is missing."""
        mock_get_spark.return_value = mock_spark_session
        
        config = {
            'jobs': [{
                'job_name': 'test_job',
                'tasks': [{
                    'task_key': 'task1',
                    'depends_on': []
                    # Missing task_type - should raise error
                }]
            }]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            yaml.dump(config, tmp)
            tmp_path = tmp.name
        
        try:
            manager = MetadataManager("test_table")
            with pytest.raises(ValueError, match="must have 'task_type' field"):
                manager.load_yaml(tmp_path)
        finally:
            os.unlink(tmp_path)
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_detect_changes_no_changes(self, mock_get_spark, mock_spark_session):
        """Test detection when no changes exist."""
        mock_get_spark.return_value = mock_spark_session
        
        # Mock DataFrame with no changes
        mock_df = MagicMock()
        mock_df.filter.return_value.count.return_value = 0
        mock_df.select.return_value.distinct.return_value.collect.return_value = []
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        changes = manager.detect_changes("2024-01-01T00:00:00")
        
        assert changes['new_jobs'] == []
        assert changes['updated_jobs'] == []
        assert changes['disabled_jobs'] == []
        assert changes['changed_tasks'] == []
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_detect_changes_updated(self, mock_get_spark, mock_spark_session):
        """Test detection of updated jobs."""
        mock_get_spark.return_value = mock_spark_session
        
        # Mock DataFrame with changes
        mock_df = MagicMock()
        mock_changed_df = MagicMock()
        mock_changed_df.count.return_value = 1
        
        mock_job_row = MagicMock()
        mock_job_row.__getitem__.side_effect = lambda key: 'job1' if key == 'job_name' else None
        
        mock_changed_df.select.return_value.distinct.return_value.collect.return_value = [mock_job_row]
        mock_df.filter.return_value = mock_changed_df
        mock_df.select.return_value.distinct.return_value.collect.return_value = []
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        changes = manager.detect_changes("2024-01-01T00:00:00")
        
        assert 'job1' in changes['updated_jobs']
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_detect_changes_new_jobs(self, mock_get_spark, mock_spark_session):
        """Test detection of new jobs."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_df = MagicMock()
        mock_job_row = MagicMock()
        mock_job_row.__getitem__.side_effect = lambda key: 'new_job' if key == 'job_name' else None
        
        # Mock for updated jobs (empty)
        mock_changed_df = MagicMock()
        mock_changed_df.count.return_value = 0
        
        # Mock for all jobs
        mock_all_jobs_df = MagicMock()
        mock_all_jobs_df.select.return_value.distinct.return_value.collect.return_value = [mock_job_row]
        mock_df.select.return_value.distinct.return_value.collect.return_value = [mock_job_row]
        mock_df.filter.return_value = mock_changed_df
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        changes = manager.detect_changes(None)
        
        assert 'new_job' in changes['new_jobs']
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_detect_changes_spark_error_handling(self, mock_get_spark, mock_spark_session):
        """Test error handling when Spark operations fail."""
        mock_get_spark.return_value = mock_spark_session
        mock_spark_session.table.side_effect = Exception("Spark error")
        
        manager = MetadataManager("test_table")
        changes = manager.detect_changes()
        
        # Should return empty changes dict
        assert changes['new_jobs'] == []
        assert changes['updated_jobs'] == []
        assert changes['disabled_jobs'] == []
        assert changes['changed_tasks'] == []
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_get_all_jobs(self, mock_get_spark, mock_spark_session):
        """Test getting all jobs."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_job_row1 = MagicMock()
        mock_job_row1.__getitem__.side_effect = lambda key: 'job1' if key == 'job_name' else None
        
        mock_job_row2 = MagicMock()
        mock_job_row2.__getitem__.side_effect = lambda key: 'job2' if key == 'job_name' else None
        
        mock_df = MagicMock()
        mock_df.select.return_value.distinct.return_value.collect.return_value = [mock_job_row1, mock_job_row2]
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        jobs = manager.get_all_jobs()
        
        assert len(jobs) == 2
        assert 'job1' in jobs
        assert 'job2' in jobs
    
    @patch('lakeflow_jobs_meta.metadata_manager._get_spark')
    def test_get_job_tasks(self, mock_get_spark, mock_spark_session):
        """Test getting tasks for a job."""
        mock_get_spark.return_value = mock_spark_session
        
        mock_task_row = MagicMock()
        mock_task_row.__getitem__.side_effect = lambda key: {
            'task_key': 'task1',
            'job_name': 'job1',
            'depends_on': '[]',
            'disabled': False
        }.get(key)
        
        mock_df = MagicMock()
        mock_df.filter.return_value.collect.return_value = [mock_task_row]
        mock_spark_session.table.return_value = mock_df
        
        manager = MetadataManager("test_table")
        tasks = manager.get_job_tasks("job1")
        
        assert len(tasks) == 1
        assert tasks[0]['task_key'] == 'task1'
    
    @patch('lakeflow_jobs_meta.metadata_manager.MetadataManager.load_yaml')
    @patch('lakeflow_jobs_meta.metadata_manager.dbutils')
    def test_sync_from_volume(self, mock_dbutils, mock_load_yaml):
        """Test syncing YAML files from volume."""
        # Mock file listing
        mock_file1 = MagicMock()
        mock_file1.name = "config1.yaml"
        mock_file1.path = "/Volumes/test/config1.yaml"
        
        mock_file2 = MagicMock()
        mock_file2.name = "config2.yaml"
        mock_file2.path = "/Volumes/test/config2.yaml"
        
        mock_file3 = MagicMock()
        mock_file3.name = "data.txt"  # Not a YAML file
        mock_file3.path = "/Volumes/test/data.txt"
        
        mock_dbutils.fs.ls.return_value = [mock_file1, mock_file2, mock_file3]
        mock_dbutils.fs.head.return_value = "test: yaml\ncontent: true"
        
        mock_load_yaml.return_value = 5  # 5 tasks loaded
        
        manager = MetadataManager("test_table")
        # Mock the load_yaml method
        manager.load_yaml = mock_load_yaml
        
        result = manager.sync_from_volume("/Volumes/test/volume")
        
        assert result == 10  # 5 + 5 from two files
        assert mock_dbutils.fs.ls.called
        assert mock_load_yaml.call_count == 2  # Called for each YAML file
    
    @patch('lakeflow_jobs_meta.metadata_manager.dbutils')
    def test_sync_from_volume_no_files(self, mock_dbutils):
        """Test handling when no YAML files exist."""
        mock_dbutils.fs.ls.return_value = []
        
        manager = MetadataManager("test_table")
        result = manager.sync_from_volume("/Volumes/test/volume")
        
        assert result == 0
    
    def test_sync_from_volume_dbutils_not_available(self):
        """Test error when dbutils is not available."""
        manager = MetadataManager("test_table")
        
        # Simulate dbutils not being available by patching the import inside the method
        with patch('lakeflow_jobs_meta.metadata_manager.dbutils', side_effect=NameError("dbutils not available")):
            with pytest.raises(RuntimeError, match="dbutils not available"):
                manager.sync_from_volume("/Volumes/test/volume")
