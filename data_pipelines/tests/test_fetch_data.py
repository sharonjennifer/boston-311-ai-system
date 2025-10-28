"""Unit tests for data acquisition module"""
import pytest
from unittest.mock import Mock, patch
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import fetch_data


class TestFetchWindow:
    """Test suite for fetch_window function"""
    
    def test_fetch_window_basic(self):
        """Test basic fetch_window functionality"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = [
                {"_id": 1, "case_enquiry_id": 101, "case_status": "Open"},
                {"_id": 2, "case_enquiry_id": 102, "case_status": "Closed"}
            ]
            
            result = fetch_data.fetch_window("open_dt >= '2025-01-01'", start_after_id=0, limit=10)
            
            assert len(result) == 2
            assert result[0]["_id"] == 1
            assert result[1]["case_enquiry_id"] == 102
    
    def test_fetch_window_with_where_clause(self):
        """Test SQL construction with WHERE clause"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            fetch_data.fetch_window("open_dt >= '2025-01-01'", start_after_id=100, limit=50)
            
            call_args = mock_request.call_args[0][0]
            sql = call_args["sql"]
            assert "open_dt >= '2025-01-01'" in sql
            assert "_id > 100" in sql
            assert "LIMIT 50" in sql
    
    def test_fetch_window_null_where(self):
        """Test fetch_window with None where clause"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            fetch_data.fetch_window(None, start_after_id=0, limit=10)
            
            call_args = mock_request.call_args[0][0]
            sql = call_args["sql"]
            assert "_id > 0" in sql
    
    def test_fetch_window_empty_response(self):
        """Test handling of empty API response"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            result = fetch_data.fetch_window("1=1", start_after_id=0, limit=10)
            
            assert result == []
            assert isinstance(result, list)


class TestDoRequest:
    """Test suite for _do_request function"""
    
    def test_do_request_success(self):
        """Test successful API request"""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {
                "success": True,
                "result": {"records": [{"_id": 1, "case_status": "Open"}]}
            }
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = fetch_data._do_request({"sql": "SELECT * FROM test"})
            
            assert len(result) == 1
            assert result[0]["_id"] == 1
    
    def test_do_request_retry_on_failure(self):
        """Test retry mechanism on API failure"""
        with patch('requests.get') as mock_get:
            with patch('time.sleep'):
                # First two fail, third succeeds
                mock_get.side_effect = [
                    Exception("Network error"),
                    Exception("Network error"),
                    Mock(
                        json=lambda: {"success": True, "result": {"records": []}},
                        raise_for_status=lambda: None
                    )
                ]
                
                result = fetch_data._do_request({"sql": "SELECT * FROM test"})
                
                assert result == []
                assert mock_get.call_count == 3
    
    def test_do_request_max_retries_exceeded(self):
        """Test behavior when max retries exceeded"""
        with patch('requests.get') as mock_get:
            with patch('time.sleep'):
                mock_get.side_effect = Exception("Network error")
                
                with pytest.raises(Exception, match="Network error"):
                    fetch_data._do_request({"sql": "SELECT * FROM test"})
                
                assert mock_get.call_count == 3
    
    def test_do_request_malformed_response(self):
        """Test handling of malformed API response"""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {"success": False, "error": "Bad request"}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            with pytest.raises(RuntimeError, match="CKAN returned success=False"):
                fetch_data._do_request({"sql": "SELECT * FROM test"})


class TestEdgeCases:
    """Test edge cases and anomalies"""
    
    def test_missing_records_field(self):
        """Test handling when records field is missing"""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {"success": True, "result": {}}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = fetch_data._do_request({"sql": "SELECT * FROM test"})
            assert result == []
    
    def test_records_with_missing_values(self):
        """Test handling of records with missing values"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = [
                {"_id": 1, "case_enquiry_id": None, "case_status": "Open"},
                {"_id": 2, "case_enquiry_id": 102},  # Missing case_status
                {"_id": None, "case_enquiry_id": 103}  # Missing _id
            ]
            
            result = fetch_data.fetch_window(None, start_after_id=0, limit=10)
            
            assert len(result) == 3
    
    def test_large_limit_value(self):
        """Test with very large limit value"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            fetch_data.fetch_window(None, start_after_id=0, limit=100000)
            
            call_args = mock_request.call_args[0][0]
            assert "LIMIT 100000" in call_args["sql"]
    
    def test_negative_start_id(self):
        """Test with negative start_after_id"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            fetch_data.fetch_window(None, start_after_id=-1, limit=10)
            
            call_args = mock_request.call_args[0][0]
            assert "_id > -1" in call_args["sql"]
    
    def test_special_characters_in_where(self):
        """Test handling of special characters in WHERE clause"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            where_clause = "type = 'Pothole' AND neighborhood = 'Dorchester'"
            fetch_data.fetch_window(where_clause, start_after_id=0, limit=10)
            
            call_args = mock_request.call_args[0][0]
            assert where_clause in call_args["sql"]


class TestAnomalies:
    """Test anomaly detection in data"""
    
    def test_duplicate_ids(self):
        """Test detection of duplicate record IDs"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = [
                {"_id": 1, "case_enquiry_id": 101},
                {"_id": 1, "case_enquiry_id": 101},  # Duplicate
                {"_id": 2, "case_enquiry_id": 102}
            ]
            
            result = fetch_data.fetch_window(None, start_after_id=0, limit=10)
            
            # Check for duplicates
            ids = [r["_id"] for r in result]
            assert len(ids) != len(set(ids)), "Duplicate IDs detected"
    
    def test_out_of_order_ids(self):
        """Test records with non-sequential IDs"""
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = [
                {"_id": 5, "case_enquiry_id": 101},
                {"_id": 3, "case_enquiry_id": 102},  # Out of order
                {"_id": 10, "case_enquiry_id": 103}
            ]
            
            result = fetch_data.fetch_window(None, start_after_id=0, limit=10)
            assert len(result) == 3
    
    def test_extremely_large_response(self):
        """Test handling of extremely large responses"""
        with patch('fetch_data._do_request') as mock_request:
            # Simulate 1000 records
            large_response = [
                {"_id": i, "case_enquiry_id": i+1000} 
                for i in range(1000)
            ]
            mock_request.return_value = large_response
            
            result = fetch_data.fetch_window(None, start_after_id=0, limit=1000)
            assert len(result) == 1000
    
    def test_invalid_json_in_response(self):
        """Test handling of invalid JSON structure"""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            with pytest.raises(ValueError):
                fetch_data._do_request({"sql": "SELECT * FROM test"})


# Run with: pytest data_pipelines/tests/test_fetch_data.py -v