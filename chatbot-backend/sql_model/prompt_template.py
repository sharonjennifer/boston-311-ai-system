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
1. Generate ONLY a valid BigQuery SQL SELECT statement
2. Use backticks for table names: `boston311.service_requests_2025`
3. Use LIKE '%keyword%' for partial text matching (case-insensitive)
4. ALWAYS use proper date functions for time-based queries
5. Include reasonable LIMITs (default 100 for lists, 10 for aggregations)
6. Use meaningful column aliases (as count, as avg_days, etc.)
7. For counts: Always use COUNT(*) or COUNT(DISTINCT column_name)
8. For time periods: Use DATE_SUB with CURRENT_TIMESTAMP()
9. DO NOT include markdown code fences or extra text
10. Return ONLY the SQL query
11. IMPORTANT: If the question asks "what about X?" or "how about Y?", maintain the same query TYPE (COUNT/LIST/AGGREGATE) as the previous question, just change the filter conditions
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