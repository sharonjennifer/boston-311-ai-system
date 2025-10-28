"""Unit tests for DAG utility functions"""
import pytest
from unittest.mock import Mock, patch, mock_open
import os
import json
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from dag_utils import (
    get_recent_iso,
    file_exists,
    generate_merge_sql,
    generate_overwrite_sql,
    write_records_to_jsonl
)


class TestGetRecentIso:
    """Tests for get_recent_iso function"""
    
    def test_returns_iso_format(self):
        """Test function returns ISO formatted date string"""
        result = get_recent_iso(days=28)
        
        assert isinstance(result, str)
        assert 'T00:00:00Z' in result
        assert len(result) == 20
    
    def test_different_day_ranges(self):
        """Test with different day ranges"""
        result_7 = get_recent_iso(days=7)
        result_28 = get_recent_iso(days=28)
        result_1 = get_recent_iso(days=1)
        
        assert all(isinstance(r, str) for r in [result_7, result_28, result_1])
        assert all('T00:00:00Z' in r for r in [result_7, result_28, result_1])


class TestFileExists:
    """Tests for file_exists function"""
    
    def test_returns_true_when_exists(self):
        """Test returns True when file exists"""
        with patch('os.path.exists', return_value=True):
            result = file_exists("/tmp/test.txt")
            assert result == True
    
    def test_returns_false_when_missing(self):
        """Test returns False when file missing"""
        with patch('os.path.exists', return_value=False):
            result = file_exists("/tmp/nonexistent.txt")
            assert result == False


class TestGenerateMergeSql:
    """Tests for generate_merge_sql function"""
    
    def test_generates_valid_sql(self):
        """Test SQL generation"""
        columns = ['_id', 'case_enquiry_id', 'case_status']
        sql = generate_merge_sql("staging", "target", "project-id", "dataset", columns)
        
        assert isinstance(sql, str)
        assert "MERGE" in sql
        assert "staging" in sql
        assert "target" in sql
    
    def test_includes_deduplication(self):
        """Test SQL includes ROW_NUMBER deduplication"""
        columns = ['_id', 'case_enquiry_id']
        sql = generate_merge_sql("stg", "tgt", "proj", "ds", columns)
        
        assert "ROW_NUMBER()" in sql
        assert "PARTITION BY case_enquiry_id" in sql
    
    def test_filters_nulls(self):
        """Test SQL filters null case_enquiry_id"""
        columns = ['_id', 'case_enquiry_id']
        sql = generate_merge_sql("stg", "tgt", "proj", "ds", columns)
        
        assert "case_enquiry_id IS NOT NULL" in sql
    
    def test_has_update_and_insert(self):
        """Test SQL has MATCHED and NOT MATCHED clauses"""
        columns = ['_id', 'case_enquiry_id', 'status']
        sql = generate_merge_sql("stg", "tgt", "proj", "ds", columns)
        
        assert "WHEN MATCHED" in sql
        assert "UPDATE SET" in sql
        assert "WHEN NOT MATCHED" in sql
        assert "INSERT" in sql


class TestGenerateOverwriteSql:
    """Tests for generate_overwrite_sql function"""
    
    def test_generates_valid_sql(self):
        """Test overwrite SQL generation"""
        columns = ['_id', 'case_enquiry_id']
        sql = generate_overwrite_sql("staging", "target", "project", "dataset", columns)
        
        assert "CREATE OR REPLACE TEMP TABLE" in sql
        assert "TRUNCATE TABLE" in sql
        assert "INSERT INTO" in sql
    
    def test_includes_deduplication(self):
        """Test includes deduplication logic"""
        columns = ['_id', 'case_enquiry_id']
        sql = generate_overwrite_sql("stg", "tgt", "proj", "ds", columns)
        
        assert "ROW_NUMBER()" in sql


class TestWriteRecordsToJsonl:
    """Tests for write_records_to_jsonl function"""
    
    def test_writes_records(self):
        """Test records are written to file"""
        records = [
            {"_id": 1, "case_enquiry_id": 101},
            {"_id": 2, "case_enquiry_id": 102}
        ]
        
        with patch('builtins.open', mock_open()) as m:
            with patch('os.makedirs'):
                count = write_records_to_jsonl(records, "/tmp/test.jsonl")
                
                assert count == 2
                assert m().write.call_count == 2
    
    def test_adds_ingestion_timestamp(self):
        """Test _ingested_at field is added"""
        records = [{"_id": 1}]
        written_data = []
        
        def capture_write(data):
            written_data.append(data)
        
        with patch('builtins.open', mock_open()) as m:
            m().write.side_effect = capture_write
            with patch('os.makedirs'):
                write_records_to_jsonl(records, "/tmp/test.jsonl")
                
                written_json = json.loads(written_data[0].strip())
                assert '_ingested_at' in written_json
    
    def test_creates_directory(self):
        """Test output directory is created"""
        records = [{"_id": 1}]
        
        with patch('os.makedirs') as mock_makedirs:
            with patch('builtins.open', mock_open()):
                write_records_to_jsonl(records, "/tmp/subdir/test.jsonl")
                
                mock_makedirs.assert_called_once()
    
    def test_handles_empty_records(self):
        """Test handles empty record list"""
        records = []
        
        with patch('builtins.open', mock_open()) as m:
            with patch('os.makedirs'):
                count = write_records_to_jsonl(records, "/tmp/test.jsonl")
                
                assert count == 0