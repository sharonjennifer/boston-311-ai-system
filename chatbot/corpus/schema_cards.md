Tables (read-only)

RAW: boston311.service_requests_2025
- Columns: _id, open_dt, closed_dt, case_status, department, reason, type, subject, case_title, location, neighborhood, longitude, latitude, geom_4326 (WKB hex)
- Notes: Use ST_GEOGFROMWKB(FROM_HEX(geom_4326)) to get GEOGRAPHY point. Text search is supported on case_title/location with SEARCH().

SERVING: boston311_service.tbl_counts_by_neighborhood_week
- week_start (DATE), neighborhood, type, n (count)

SERVING: boston311_service.tbl_counts_by_department_week
- week_start (DATE), department, n (count)

SERVING: boston311_service.tbl_case_durations
- case_enquiry_id, department, reason, type, open_date_local, open_dt, closed_dt, days_to_close

SERVING: boston311_service.ref_neighborhoods
- neighborhood_name, geom (GEOGRAPHY polygon)
- Join pattern: ST_CONTAINS(nei.geom, <GEOG point from RAW>)