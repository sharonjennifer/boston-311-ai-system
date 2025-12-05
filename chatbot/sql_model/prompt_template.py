import textwrap

DEFAULT_SCHEMA = textwrap.dedent("""
CREATE TABLE `boston311.service_requests_2025` (
  case_enquiry_id INTEGER,
  open_dt TIMESTAMP,
  closed_dt TIMESTAMP,
  sla_target_dt TIMESTAMP,
  case_status STRING,
  on_time STRING,
  closure_reason STRING,
  source STRING,
  department STRING,
  subject STRING,
  reason STRING,
  type STRING,
  queue STRING,
  case_title STRING,
  neighborhood STRING,
  location STRING,
  location_street_name STRING,
  location_zipcode INTEGER,
  latitude FLOAT,
  longitude FLOAT,
  geom_4326 STRING,
  precinct STRING,
  ward STRING,
  city_council_district STRING,
  fire_district STRING,
  police_district STRING,
  pwd_district STRING,
  neighborhood_services_district STRING
);
""")

SQL_PROMPT_TEMPLATE = """### Task
Generate a BigQuery SQL query to answer [QUESTION]{question}[/QUESTION]

### Schema
{schema}

### Hints
{hints}

### Answer
"""



def build_prompt(question, keywords):
    hints = ", ".join(keywords) if keywords else "None"

    prompt = SQL_PROMPT_TEMPLATE.format(
        question=question,
        schema=DEFAULT_SCHEMA,
        hints=hints,
    )

    return prompt