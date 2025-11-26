from app.config import FULL_RAW

TEMPLATES = {
    # A1) Case by ID (RAW)
    "CASE_BY_ID": {
        "sql": f"""
        SELECT case_enquiry_id, open_dt, closed_dt, case_status,
               department, reason, type, subject, case_title, location, neighborhood,
               longitude, latitude
        FROM {FULL_RAW}
        WHERE CAST(case_enquiry_id AS STRING) = @case_id
        LIMIT 1;""",
        "params": ["case_id"],
    },

    # B1) Count (RAW; optional filters)
    "COUNT": {
        "sql": f"""
        SELECT COUNT(*) AS n
        FROM {FULL_RAW}
        WHERE open_dt BETWEEN @start_ts AND @end_ts
          AND (@department IS NULL OR UPPER(TRIM(department)) = UPPER(TRIM(@department)))
          AND (@reason     IS NULL OR UPPER(TRIM(reason))     = UPPER(TRIM(@reason)))
          AND (@type       IS NULL OR UPPER(TRIM(type))       = UPPER(TRIM(@type)))
          AND (@status     IS NULL OR UPPER(TRIM(case_status))= UPPER(TRIM(@status)));""",
        "params": ["start_ts","end_ts","department?","reason?","type?","status?"],
    },

    # B2) Top-N neighborhoods (serving)
    "TOPN_NEIGHBORHOODS": {
        "sql": f"""
        SELECT neighborhood, COUNT(*) AS cases
        FROM {FULL_RAW}
        WHERE open_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @weeks WEEK)
          AND neighborhood IS NOT NULL
        GROUP BY neighborhood
        ORDER BY cases DESC
        LIMIT @k;""",
        "params": ["weeks","k"],
    },

    # C1) Weekly trend by neighborhood (serving)
    "TREND_NEIGHBORHOOD": {
        "sql": f"""
        SELECT DATE_TRUNC(DATE(open_dt, 'America/New_York'), WEEK) as week_start, 
               neighborhood, 
               COUNT(*) AS cases
        FROM {FULL_RAW}
        WHERE open_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @weeks WEEK)
          AND neighborhood IS NOT NULL
        GROUP BY week_start, neighborhood
        ORDER BY week_start, neighborhood;""",
        "params": ["weeks"],
    },

    # C2) Weekly trend by department (serving)
    "TREND_DEPARTMENT": {
        "sql": f"""
        SELECT DATE_TRUNC(DATE(open_dt, 'America/New_York'), WEEK) as week_start, 
               department, 
               COUNT(*) AS cases
        FROM {FULL_RAW}
        WHERE open_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @weeks WEEK)
          AND department IS NOT NULL
        GROUP BY week_start, department
        ORDER BY week_start, department;""",
        "params": ["weeks"],
    },
    
    # D1) Avg days to close by department (serving)
    "AVG_DAYS_TO_CLOSE_BY_DEPT": {
        "sql": f"""
        SELECT department,
               AVG(TIMESTAMP_DIFF(closed_dt, open_dt, HOUR) / 24.0) AS avg_days_to_close,
               COUNT(*) AS closed_cases
        FROM {FULL_RAW}
        WHERE open_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
          AND case_status = 'Closed'
          AND closed_dt IS NOT NULL
          AND open_dt IS NOT NULL
        GROUP BY department
        ORDER BY avg_days_to_close;""",
        "params": ["days"],
    },

    # D2) Long-open backlog (RAW)
    "BACKLOG": {
        "sql": f"""
        SELECT CAST(_id AS STRING) AS case_enquiry_id, open_dt, department, reason, type, subject, case_title, location
        FROM {FULL_RAW}
        WHERE case_status = 'Open'
          AND open_dt < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @older_than_days DAY)
        ORDER BY open_dt ASC
        LIMIT @k;""",
        "params": ["older_than_days","k"],
    },

    # E1) Cases inside neighborhood polygon (RAW + WKB→GEOG)
    "GEO_POLYGON": {
        "sql": f"""
        SELECT case_enquiry_id, open_dt, case_status, department, subject, case_title, location
        FROM {FULL_RAW}
        WHERE open_dt BETWEEN @start_ts AND @end_ts
          AND UPPER(TRIM(neighborhood)) = UPPER(TRIM(@neighborhood))
        ORDER BY open_dt DESC
        LIMIT @k;""",
        "params": ["neighborhood","start_ts","end_ts","k"],
    },

    # E2) Within radius of point (RAW + WKB→GEOG)
    "GEO_RADIUS": {
        "sql": f"""
        WITH pts AS (
          SELECT CAST(_id AS STRING) AS case_enquiry_id, open_dt, case_status, department,
                 subject, case_title, location,
                 ST_GEOGFROMWKB(FROM_HEX(geom_4326)) AS geom
          FROM {FULL_RAW}
          WHERE open_dt BETWEEN @start_ts AND @end_ts
            AND geom_4326 IS NOT NULL
        )
        SELECT case_enquiry_id, open_dt, case_status, department, subject, case_title, location,
               ST_DISTANCE(geom, ST_GEOGPOINT(@center_lon, @center_lat)) AS meters
        FROM pts
        WHERE geom IS NOT NULL
          AND ST_DWITHIN(geom, ST_GEOGPOINT(@center_lon, @center_lat), @radius_m)
        ORDER BY meters ASC, open_dt DESC
        LIMIT @k;""",
        "params": ["center_lon","center_lat","radius_m","start_ts","end_ts","k"],
    },

    # F1) Text search (RAW; case_title/location)
    "TEXT_SEARCH": {
        "sql": f"""
        SELECT CAST(_id AS STRING) AS case_enquiry_id,
               open_dt, case_status, department, subject, case_title, location
        FROM {FULL_RAW}
        WHERE open_dt BETWEEN @start_ts AND @end_ts
          AND (SEARCH(case_title, @q) OR SEARCH(location, @q))
        ORDER BY open_dt DESC
        LIMIT @k;""",
        "params": ["q","start_ts","end_ts","k"],
    },

    # I1) Freshness/meta (RAW)
    "FRESHNESS": {
        "sql": f"SELECT MAX(_ingested_at) AS last_ingested_at FROM {FULL_RAW};",
        "params": [],
    },
}
