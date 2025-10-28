"""Download sample data from BigQuery for DVC tracking"""
from google.cloud import bigquery
import pandas as pd
import os

def download_sample_data():
    """Download recent 1000 records from BigQuery"""
    try:
        client = bigquery.Client(project="boston311-mlops")
        
        query = """
        SELECT *
        FROM `boston311-mlops.boston311.service_requests_2025`
        ORDER BY open_dt DESC
        LIMIT 1000
        """
        
        print("📥 Downloading data from BigQuery...")
        df = client.query(query).to_dataframe()
        
        # Save to CSV
        output_dir = "data_pipelines/data/raw"
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = f"{output_dir}/boston311_sample.csv"
        df.to_csv(output_file, index=False)
        
        print(f"✅ Downloaded {len(df)} records to {output_file}")
        print(f"📊 File size: {os.path.getsize(output_file) / 1024:.2f} KB")
        print(f"📋 Columns: {len(df.columns)}")
        
        return output_file
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Creating dummy data for DVC demonstration...")
        
        # Create dummy data if BigQuery fails
        dummy_df = pd.DataFrame({
            '_id': range(1, 101),
            'case_enquiry_id': range(101001, 101101),
            'case_status': ['Open', 'Closed'] * 50,
            'case_title': ['Sample pothole'] * 100,
            'neighborhood': ['Dorchester', 'South Boston', 'Back Bay'] * 33 + ['Roxbury'],
            'type': ['Street Repair', 'Graffiti', 'Streetlight'] * 33 + ['Trash'],
            'latitude': [42.35] * 100,
            'longitude': [-71.05] * 100
        })
        
        output_dir = "data_pipelines/data/raw"
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/boston311_sample.csv"
        dummy_df.to_csv(output_file, index=False)
        print(f"✅ Created dummy data: {output_file}")
        return output_file

if __name__ == "__main__":
    download_sample_data()