"""Download sample data from BigQuery for DVC tracking"""
from google.cloud import bigquery
import pandas as pd
import os
import sys

def download_sample_data(limit=5000):
    try:
        print(f"Authenticating with BigQuery...")
        client = bigquery.Client(project="boston311-mlops")
        
        query = f"""
        SELECT *
        FROM `boston311-mlops.boston311.service_requests_2025`
        ORDER BY open_dt DESC
        LIMIT {limit}
        """
        
        print(f"Downloading {limit} records from BigQuery...")
        df = client.query(query).to_dataframe()
        
        # Save to CSV
        output_dir = "data_pipelines/data/raw"
        os.makedirs(output_dir, exist_ok=True)
        
        csv_file = f"{output_dir}/boston311_real_data.csv"
        df.to_csv(csv_file, index=False)
        
        # Save as Parquet (more efficient)
        parquet_file = f"{output_dir}/boston311_real_data.parquet"
        df.to_parquet(parquet_file, index=False)
        
        # Create summary statistics
        summary_file = f"{output_dir}/data_summary.txt"
        with open(summary_file, 'w') as f:
            f.write("Boston 311 Data Summary\n")
            f.write("=" * 60 + "\n")
            f.write(f"Total records: {len(df)}\n")
            f.write(f"Columns: {len(df.columns)}\n")
            f.write(f"Date range: {df['open_dt'].min()} to {df['open_dt'].max()}\n")
            f.write(f"Unique neighborhoods: {df['neighborhood'].nunique()}\n")
            f.write(f"Unique complaint types: {df['type'].nunique()}\n")
            f.write(f"Open cases: {len(df[df['case_status'] == 'Open'])}\n")
            f.write(f"Closed cases: {len(df[df['case_status'] == 'Closed'])}\n")
        
        print(f"SUCCESS: Downloaded {len(df)} records")
        print(f"CSV file: {csv_file} ({os.path.getsize(csv_file) / 1024 / 1024:.2f} MB)")
        print(f"Parquet file: {parquet_file} ({os.path.getsize(parquet_file) / 1024 / 1024:.2f} MB)")
        print(f"Summary: {summary_file}")
        
        return csv_file
        
    except Exception as e:
        print(f"ERROR: {e}")
        print(f"Error type: {type(e).__name__}")
        print("\nTroubleshooting:")
        print("1. Run: gcloud auth application-default login")
        print("2. Verify BigQuery access in GCP project")
        sys.exit(1)

if __name__ == "__main__":
    download_sample_data(limit=5000)