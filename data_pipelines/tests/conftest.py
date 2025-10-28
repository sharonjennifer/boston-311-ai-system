"""Pytest configuration and fixtures"""
import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_api_response():
    """Sample successful API response"""
    return {
        "success": True,
        "result": {
            "records": [
                {
                    "_id": 1,
                    "case_enquiry_id": 101001,
                    "open_dt": "2025-01-15T10:30:00",
                    "case_status": "Open",
                    "case_title": "Pothole on Main St",
                    "type": "Street Repair",
                    "neighborhood": "Dorchester",
                    "latitude": 42.3601,
                    "longitude": -71.0589
                },
                {
                    "_id": 2,
                    "case_enquiry_id": 101002,
                    "open_dt": "2025-01-15T11:00:00",
                    "case_status": "Closed",
                    "case_title": "Graffiti Removal",
                    "type": "Code Enforcement",
                    "neighborhood": "South Boston",
                    "latitude": 42.3351,
                    "longitude": -71.0412
                }
            ]
        }
    }


@pytest.fixture
def sample_records():
    """Sample records for testing"""
    return [
        {"_id": 1, "case_enquiry_id": 101, "case_status": "Open", "type": "Pothole"},
        {"_id": 2, "case_enquiry_id": 102, "case_status": "Closed", "type": "Graffiti"},
        {"_id": 3, "case_enquiry_id": 103, "case_status": "Open", "type": "Streetlight"}
    ]


@pytest.fixture
def sample_records_with_nulls():
    """Sample records with missing values"""
    return [
        {"_id": 1, "case_enquiry_id": None, "case_status": "Open"},
        {"_id": 2, "case_enquiry_id": 102, "case_status": None},
        {"_id": None, "case_enquiry_id": 103, "case_status": "Open"}
    ]