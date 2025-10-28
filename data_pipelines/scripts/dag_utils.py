"""Utility functions used by DAGs - extracted for testing"""
import os
import json
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Import fetch_data from same directory
try:
    import fetch_data
except ImportError:
    from . import fetch_data


def get_recent_iso(days=28):
    """Get ISO timestamp for N days ago
    
    Args:
        days: Number of days to look back
        
    Returns:
        ISO formatted timestamp string
    """
    d = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    return f"{d}T00:00:00Z"


def file_exists(path):
    """Check if file exists at given path
    
    Args:
        path: File path to check
        
    Returns:
        True if file exists, False otherwise
    """
    exists = os.path.exists(path)
    if not exists:
        logger.info(f"File not found: {path}")
    return exists


def generate_merge_sql(staging_table, target_table, project_id, dataset, columns):
    """Generate MERGE SQL for deduplication
    
    Args:
        staging_table: Staging table name
        target_table: Target table name
        project_id: GCP project ID
        dataset: BigQuery dataset name
        columns: List of column names
        
    Returns:
        SQL string for MERGE operation
    """
    sets = ",\n    ".join([f"{c} = S.{c}" for c in columns if c not in ("case_enquiry_id", "_id")])
    cols = ", ".join(columns)
    scols = ", ".join([f"S.{c}" for c in columns])
    
    return f"""
    MERGE `{project_id}.{dataset}.{target_table}` T
    USING (
      SELECT *
      FROM `{project_id}.{dataset}.{staging_table}`
      WHERE case_enquiry_id IS NOT NULL
      QUALIFY ROW_NUMBER()
             OVER (PARTITION BY case_enquiry_id ORDER BY _ingested_at DESC) = 1
    ) S
    ON T.case_enquiry_id = S.case_enquiry_id
    WHEN MATCHED AND (T._ingested_at IS NULL OR S._ingested_at >= T._ingested_at) THEN
      UPDATE SET
        {sets}
    WHEN NOT MATCHED THEN
      INSERT ({cols})
      VALUES ({scols});
    """


def generate_overwrite_sql(staging_table, target_table, project_id, dataset, columns):
    """Generate SQL for full table overwrite
    
    Args:
        staging_table: Staging table name
        target_table: Target table name
        project_id: GCP project ID
        dataset: BigQuery dataset name
        columns: List of column names
        
    Returns:
        SQL string for overwrite operation
    """
    cols = ", ".join(columns)
    
    return f"""
    CREATE OR REPLACE TEMP TABLE _dedup AS
    SELECT *
    FROM `{project_id}.{dataset}.{staging_table}`
    WHERE case_enquiry_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY case_enquiry_id ORDER BY _ingested_at DESC) = 1;
    
    TRUNCATE TABLE `{project_id}.{dataset}.{target_table}`;

    INSERT INTO `{project_id}.{dataset}.{target_table}` ({cols})
    SELECT {cols}
    FROM _dedup;
    """


def write_records_to_jsonl(records, output_path, ingestion_timestamp=None):
    """Write records to JSONL file
    
    Args:
        records: List of record dictionaries
        output_path: Path to output file
        ingestion_timestamp: ISO timestamp to add (default: now)
        
    Returns:
        Number of records written
    """
    if ingestion_timestamp is None:
        ingestion_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    count = 0
    with open(output_path, 'w', encoding='utf-8') as f:
        for record in records:
            record['_ingested_at'] = ingestion_timestamp
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
            count += 1
    
    return count