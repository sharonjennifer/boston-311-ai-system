import textwrap

DEFAULT_SCHEMA = textwrap.dedent("""
TABLE: boston311.service_requests_2025

COLUMNS:
- case_enquiry_id (INTEGER): Unique case ID
- open_dt (TIMESTAMP): When the case was opened
- closed_dt (TIMESTAMP): When the case was closed
- sla_target_dt (TIMESTAMP): Target resolution date
- case_status (STRING): 'Open' or 'Closed'
- on_time (STRING): 'ONTIME' or 'OVERDUE'
- closure_reason (STRING): Reason for closure
- source (STRING): How the case was reported (e.g., 'City Worker App', 'Constituent Call')
- department (STRING): Department handling the case
- subject (STRING): Subject area
- reason (STRING): General category (e.g., 'Sanitation', 'Recycling')
- type (STRING): Specific issue type (e.g., 'Pothole', 'Recycling Cart Return')
- queue (STRING): Processing queue
- case_title (STRING): Case title/description
- neighborhood (STRING): Boston neighborhood
- location (STRING): Full address
- location_street_name (STRING): Street name
- location_zipcode (INTEGER): ZIP code
- latitude (FLOAT): Latitude coordinate
- longitude (FLOAT): Longitude coordinate
- ward (STRING): Political ward
- city_council_district (STRING): City council district
- fire_district (STRING): Fire district
- police_district (STRING): Police district
- pwd_district (STRING): Public Works district
- neighborhood_services_district (STRING): Services district
""")

SQL_EXAMPLES = textwrap.dedent("""
EXAMPLES:

Q: How many cases are open?
SQL: SELECT COUNT(*) as count FROM `boston311.service_requests_2025` WHERE case_status = 'Open';

Q: How many potholes in Dorchester?
SQL: SELECT COUNT(*) as count FROM `boston311.service_requests_2025` WHERE type LIKE '%Pothole%' AND neighborhood = 'Dorchester';

Q: Show me open cases in South End
SQL: SELECT case_enquiry_id, case_title, open_dt, type FROM `boston311.service_requests_2025` WHERE neighborhood = 'South End' AND case_status = 'Open' ORDER BY open_dt DESC LIMIT 100;

Q: What are the most common complaint types?
SQL: SELECT type, COUNT(*) as count FROM `boston311.service_requests_2025` GROUP BY type ORDER BY count DESC LIMIT 10;

Q: Which neighborhoods have the most open cases?
SQL: SELECT neighborhood, COUNT(*) as count FROM `boston311.service_requests_2025` WHERE case_status = 'Open' GROUP BY neighborhood ORDER BY count DESC LIMIT 10;
""")

SQL_INSTRUCTIONS = textwrap.dedent("""
INSTRUCTIONS:
1. For COUNT questions ("how many"), use SELECT COUNT(*) as count
2. For LIST questions ("show me", "what are"), use SELECT specific columns and LIMIT 100
3. For GROUP BY queries, always include ORDER BY count DESC and LIMIT 10
4. Always use backticks around table name: `boston311.service_requests_2025`
5. Use LIKE '%keyword%' for partial text matching
6. Never use SELECT * - always specify columns
7. For date filters, use comparison operators with TIMESTAMP values
8. Return ONLY the SQL query, no explanations
""")

SQL_PROMPT_TEMPLATE = """### Task
Generate a BigQuery SQL query to answer this question:
[QUESTION]{question}[/QUESTION]

### Database Schema
{schema}

{examples}

{instructions}

### Hints (validated values from database)
{hints}

### SQL Query (return ONLY the query, no formatting):
"""


def build_prompt(question, keywords):
    hints = ", ".join(keywords) if keywords else "None"

    prompt = SQL_PROMPT_TEMPLATE.format(
        question=question,
        schema=DEFAULT_SCHEMA,
        examples=SQL_EXAMPLES,
        instructions=SQL_INSTRUCTIONS,
        hints=hints,
    )

    return prompt