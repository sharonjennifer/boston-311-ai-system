import json
from google.cloud import aiplatform_v1
from google.api_core.client_options import ClientOptions
from google.api import httpbody_pb2

PROJECT_ID = "boston311-mlops"
REGION = "us-central1"
ENDPOINT_ID = "8571536463932424192"



client = aiplatform_v1.PredictionServiceClient(
    client_options=ClientOptions(api_endpoint=f"{REGION}-aiplatform.googleapis.com")
)

endpoint_path = client.endpoint_path(
    project=PROJECT_ID, location=REGION, endpoint=ENDPOINT_ID
)

prompt = """### Task
Generate a BigQuery SQL query to answer [QUESTION]which hour of the day sees the most parking tickets[/QUESTION]

### Schema
CREATE TABLE `boston311.service_requests_2025` (case_enquiry_id INTEGER, open_dt TIMESTAMP, closed_dt TIMESTAMP, sla_target_dt TIMESTAMP, case_status STRING, on_time STRING, closure_reason STRING, source STRING, department STRING, subject STRING, reason STRING, type STRING, queue STRING, case_title STRING, neighborhood STRING, location STRING, location_street_name STRING, location_zipcode INTEGER, latitude FLOAT, longitude FLOAT, geom_4326 STRING, precinct STRING, ward STRING, city_council_district STRING, fire_district STRING, police_district STRING, pwd_district STRING, neighborhood_services_district STRING, submitted_photo STRING, closed_photo STRING);

### Hints
type='Parking Enforcement'

### Answer
"""

request_body = {
    "model": "openapi",
    "prompt": prompt,
    "max_tokens": 256,
    "temperature": 0.1,
}

http_body = httpbody_pb2.HttpBody(
    content_type="application/json",
    data=json.dumps(request_body).encode("utf-8"),
)

response = client.raw_predict(
    endpoint=endpoint_path,
    http_body=http_body,
)

resp_json = json.loads(response.data.decode("utf-8"))
# print("Full response JSON:\n", json.dumps(resp_json, indent=2))

try:
    sql = resp_json["choices"][0].get("text", "").strip()
    print("SQL: ", sql)
except (KeyError, IndexError) as e:
    sql = None
    print("Error extracting SQL from response:", e)