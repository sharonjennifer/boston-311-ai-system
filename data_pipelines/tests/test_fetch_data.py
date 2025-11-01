import pytest
import data_pipelines.scripts.fetch_data as fetch_data

from unittest.mock import Mock, patch

class TestFetchWindow:
    
    def test_fetch_window_basic(self):
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
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            fetch_data.fetch_window("open_dt >= '2025-01-01'", start_after_id=100, limit=50)
            
            call_args = mock_request.call_args[0][0]
            sql = call_args["sql"]
            assert "open_dt >= '2025-01-01'" in sql
            assert "_id > 100" in sql
            assert "LIMIT 50" in sql
    
    def test_fetch_window_null_where(self):
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            fetch_data.fetch_window(None, start_after_id=0, limit=10)
            
            call_args = mock_request.call_args[0][0]
            sql = call_args["sql"]
            assert "_id > 0" in sql
    
    def test_fetch_window_empty_response(self):
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            result = fetch_data.fetch_window("1=1", start_after_id=0, limit=10)
            
            assert result == []
            assert isinstance(result, list)


class TestDoRequest:
    def test_do_request_success(self):
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
        with patch('requests.get') as mock_get:
            with patch('time.sleep'):
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
        with patch('requests.get') as mock_get:
            with patch('time.sleep'):
                mock_get.side_effect = Exception("Network error")
                
                with pytest.raises(Exception, match="Network error"):
                    fetch_data._do_request({"sql": "SELECT * FROM test"})
                
                assert mock_get.call_count == 3
    
    def test_do_request_malformed_response(self):
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {"success": False, "error": "Bad request"}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            with pytest.raises(RuntimeError, match="CKAN returned success=False"):
                fetch_data._do_request({"sql": "SELECT * FROM test"})


class TestEdgeCases:
    def test_missing_records_field(self):
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {"success": True, "result": {}}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = fetch_data._do_request({"sql": "SELECT * FROM test"})
            assert result == []
    
    def test_records_with_missing_values(self):
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = [
                {"_id": 1, "case_enquiry_id": None, "case_status": "Open"},
                {"_id": 2, "case_enquiry_id": 102},
                {"_id": None, "case_enquiry_id": 103}
            ]
            
            result = fetch_data.fetch_window(None, start_after_id=0, limit=10)
            
            assert len(result) == 3
    
    def test_large_limit_value(self):
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            fetch_data.fetch_window(None, start_after_id=0, limit=100000)
            
            call_args = mock_request.call_args[0][0]
            assert "LIMIT 100000" in call_args["sql"]
    
    def test_negative_start_id(self):
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            fetch_data.fetch_window(None, start_after_id=-1, limit=10)
            
            call_args = mock_request.call_args[0][0]
            assert "_id > -1" in call_args["sql"]
    
    def test_special_characters_in_where(self):
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = []
            
            where_clause = "type = 'Pothole' AND neighborhood = 'Dorchester'"
            fetch_data.fetch_window(where_clause, start_after_id=0, limit=10)
            
            call_args = mock_request.call_args[0][0]
            assert where_clause in call_args["sql"]


class TestAnomalies:
    def test_duplicate_ids(self):
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = [
                {"_id": 1, "case_enquiry_id": 101},
                {"_id": 1, "case_enquiry_id": 101},
                {"_id": 2, "case_enquiry_id": 102}
            ]
            
            result = fetch_data.fetch_window(None, start_after_id=0, limit=10)
            
            # Check for duplicates
            ids = [r["_id"] for r in result]
            assert len(ids) != len(set(ids)), "Duplicate IDs detected"
    
    def test_out_of_order_ids(self):
        with patch('fetch_data._do_request') as mock_request:
            mock_request.return_value = [
                {"_id": 5, "case_enquiry_id": 101},
                {"_id": 3, "case_enquiry_id": 102}, 
                {"_id": 10, "case_enquiry_id": 103}
            ]
            
            result = fetch_data.fetch_window(None, start_after_id=0, limit=10)
            assert len(result) == 3
    
    def test_extremely_large_response(self):
        with patch('fetch_data._do_request') as mock_request:
            large_response = [
                {"_id": i, "case_enquiry_id": i+1000} 
                for i in range(1000)
            ]
            mock_request.return_value = large_response
            
            result = fetch_data.fetch_window(None, start_after_id=0, limit=1000)
            assert len(result) == 1000
    
    def test_invalid_json_in_response(self):
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            with pytest.raises(ValueError):
                fetch_data._do_request({"sql": "SELECT * FROM test"})