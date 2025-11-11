Time rules (America/New_York)
- "today": [today 00:00 → now]
- "this week": [Mon 00:00 → now]
- "last 30 days" / "this month": [now - 30 days → now]
- "last N weeks": [now - N weeks → now]
Defaults: counts/search → 30 days; trends → 12 weeks

Geo rules
- Neighborhood must match boston311_service.ref_neighborhoods.neighborhood_name (UPPER TRIM).
- Use polygons from ref_neighborhoods for GEO_POLYGON.
- For RAW points, convert WKB hex to GEOGRAPHY via ST_GEOGFROMWKB(FROM_HEX(geom_4326)).

Taxonomy rules
- Normalize department/reason/type via UPPER(TRIM(...)).
- If user supplies these filters, pass them as exact UPPER values.