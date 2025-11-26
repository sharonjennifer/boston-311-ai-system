def create_mapping(official_list, synonyms_dict=None):
    mapping = {val: val for val in official_list}
    
    if synonyms_dict:
        for slang, target in synonyms_dict.items():
            if target not in mapping.values():
                pass 
            mapping[slang] = target
            
    return mapping

# ==========================================
# 1. RAW OFFICIAL LISTS
# ==========================================

RAW_NEIGHBORHOODS = [
    "South End", "Roxbury", "Roslindale", "Boston", "Jamaica Plain",
    "Allston / Brighton", "East Boston", "West Roxbury", "South Boston",
    "Downtown / Financial District", "Fenway / Kenmore / Audubon Circle / Longwood",
    "Beacon Hill", "Hyde Park", "Charlestown", "Greater Mattapan", "Mission Hill",
    "Brighton", "South Boston / South Boston Waterfront", "Dorchester",
    "Back Bay", "Mattapan", "Allston", "Chestnut Hill"
]

RAW_DEPARTMENTS = [
    "ISD", "BWSC", "GEN_", "PWDx", "ONS_", "BPD_", "PROP", "GRNi",
    "INFO", "DND_", "ANML", "ECON", "BTDT", "PARK", "BHA_", "BPS_", "DISB"
]

RAW_SOURCES = [
    "Maximo Integration", "Self Service", "Employee Generated", "City Worker App",
    "Citizens Connect App", "Constituent Call"
]

RAW_SUBJECTS = [
    "Boston Police Department", "Neighborhood Services", "Transportation - Traffic Division",
    "Mayor's 24 Hour Hotline", "Parks & Recreation Department", "Public Works Department",
    "Inspectional Services", "Boston Water & Sewer Commission", "Property Management",
    "Animal Control"
]

RAW_REASONS = [
    "Abandoned Bicycle", "Animal Issues", "Noise Disturbance", "Housing", "Building",
    "Street Cleaning", "Cemetery", "Neighborhood Services Issues", "Quality of Life",
    "Street Lights", "Parking Complaints", "Trees", "Notification", "Air Pollution Control",
    "Sidewalk Cover / Manhole", "Valet", "Enforcement & Abandoned Vehicles", "Massport",
    "Alert Boston", "Administrative & General Requests", "Boston Bikes", "Code Enforcement",
    "Traffic Management & Engineering", "Graffiti", "Weights and Measures",
    "Generic Noise Disturbance", "Environmental Services", "Pothole", "Recycling",
    "Billing", "Bridge Maintenance", "Catchbasin", "Fire Hydrant",
    "Employee & General Comments", "Needle Program", "Health", "Programs",
    "Highway Maintenance", "Park Maintenance & Safety", "Sanitation", "Signs & Signals",
    "Operations", "General Request", "Office of The Parking Clerk", "Rodent Activity"
]

RAW_TYPES = [
    "Encampments", "Sidewalk Cover / Manhole", "Sidewalk Repair (Make Safe)", "Recycling Cart Return",
    "Poor Conditions of Property", "Contractor Complaints", "No Utilities Residential - Gas", "Exceeding Terms of Permit",
    "Electrical", "Notification", "Space Savers", "Parking on Front/Back Yards (Illegal Parking)", "Illegal Dumping",
    "Abandoned Bicycle", "Fire in Food Establishment", "Parks General Request", "BWSC Pothole", "Construction Debris",
    "Cross Metering - Sub-Metering", "Egress", "Food Alert - Confirmed", "Illegal Occupancy", "Illegal Vending",
    "Item Price Missing", "Litter Basket Maintenance", "Major System Failure", "Occupying W/Out A Valid CO/CI",
    "Parking Meter Repairs", "Requests for Directional or Roadway Changes", "Roadway Flooding", "Short Term Rental",
    "Squalid Living Conditions", "Street Light Knock Downs", "Tree Maintenance Requests", "Undefined Noise Disturbance",
    "Unsafe Dangerous Conditions", "Alert Boston", "Boston Public Health Commission (BPHC)", "Lead",
    "Pole Compliance", "Upgrade Existing Lighting", "Sign Repair", "Pick up Dead Animal", "Ground Maintenance",
    "Requests for Traffic Signal Studies or Reviews", "New Sign  Crosswalk or Pavement Marking", "CE Collection",
    "Maintenance Complaint - Residential", "Empty Litter Basket", "Roadway Repair", "Pest Infestation - Residential",
    "Unsanitary Conditions - Food", "Automotive Noise Disturbance", "BWSC General Request", "Building Inspection Request",
    "Cemetery Maintenance Request", "Illegal Rooming House", "Knockdown Replacement", "Overflowing or Un-kept Dumpster",
    "Pavement Marking Inspection", "Poor Ventilation", "Protection of Adjoining Property", "Rooftop & Mechanical Disturbances",
    "Scanning Overcharge", "Traffic Signal Repair", "Trash on Vacant Lot", "Tree Emergencies",
    "Unsanitary Conditions - Employees", "Unsatisfactory Utilities - Electrical  Plumbing", "Animal Generic Request",
    "Big Buildings Recycling (INTERNAL)", "Billing Complaint", "General Lighting Request", "Illegal Use",
    "Install New Lighting", "Mosquitoes (West Nile)", "Product Short Measure", "Request for Snow Plowing", 
    "Sewage/Septic Back-Up", "State/Cass Shelter Sites", "Utility Call-In", "Park Improvement Requests",
    "Missed Trash/Recycling/Yard Waste/Bulk Item", "Graffiti Removal", "Unsatisfactory Living Conditions", "Sticker Request",
    "Loud Parties/Music/People", "Transportation General Request", "Heat - Excessive  Insufficient", 
    "Request for Pothole Repair", "General Traffic Engineering Request", "Working Beyond Hours", 
    "Rodent Activity", "Abandoned Building", "Bed Bugs", "Bicycle Issues", "Big Buildings Online Request", "Catchbasin",
    "Contractors Complaint", "Dumpster & Loading Noise Disturbances", "Food Alert - Unconfirmed", "Mechanical", 
    "Mice Infestation - Residential", "Municipal Parking Lot Complaints", "New Tree Requests", 
    "No Utilities - Food Establishment - Water", "Overcrowding", "Private Parking Lot Complaints", 
    "Request for Litter Basket Installation", "Street Light Outages", "Utility Casting Repair", 
    "Work Hours-Loud Noise Complaints", "Work w/out Permit", "Aircraft Noise Disturbance", "Animal Noise Disturbances", 
    "Carbon Monoxide", "No Utilities - Food Establishment - Flood", "Planting", "Rat Bite", 
    "Request for Snow Plowing (Emergency Responder)", "Short Measure - Gas", "Student Move-in Issues", 
    "Unit Pricing Wrong/Missing", "Parking Enforcement", "Needle Pickup", "Request for Recycling Cart", 
    "Traffic Signal Inspection", "Abandoned Vehicles", "Requests for Street Cleaning", "PWD Graffiti", 
    "Improper Storage of Trash (Barrels)", "Equipment Repair", "Parks Lighting/Electrical Issues",
    "General Comments For a Program or Policy","Fire Hydrant", "Sidewalk Repair", "Public Works General Request",
    "Maintenance - Homeowner", "Unsanitary Conditions - Establishment", "Chronic Dampness/Mold",
    "General Comments For An Employee", "Illegal Auto Body Shop", "Illegal Posting of Signs", "Missing Sign",
    "No Price on Gas/Wrong Price", "No Utilities Residential - Electricity", "No Utilities Residential - Water",
    "No-Tow Complaint Confirmation", "Pigeon Infestation", "Plumbing", "Tree in Park", "Valet Parking Problems",
    "Zoning", "Bridge Maintenance", "City/State Snow Issues", "Misc. Snow Complaint", "News Boxes",
    "No Utilities - Food Establishment - Electricity", "No Utilities - Food Establishment - Sewer",
    "Rental Unit Delivery Conditions", "Sign Shop WO", "Unshoveled Sidewalk", "Walk-In Service Inquiry"
]

# ==========================================
# 2. SYNONYM MAPPINGS 
# ==========================================

SYN_NEIGHBORHOODS = {
    "Southie": "South Boston",
    "The Dot": "Dorchester",
    "Rozzie": "Roslindale",
    "JP": "Jamaica Plain",
    "Eastie": "East Boston",
    "Charlie Town": "Charlestown",
    "Financial District": "Downtown / Financial District",
    "Downtown": "Downtown / Financial District",
    "Fenway": "Fenway / Kenmore / Audubon Circle / Longwood",
    "Kenmore": "Fenway / Kenmore / Audubon Circle / Longwood"
}

SYN_DEPARTMENTS = {
    "Property Management Department": "PROP",
    "Boston Animal Control": "ANML",
    "Department of Neighborhood Development": "DND_",
    "Boston Transportation Department (Traffic)": "BTDT",
    "Boston Water and Sewer Commission": "BWSC",
    "Boston Police Department": "BPD_",
    "Parks and Recreation Department": "PARK",
    "Mayor's 24-Hour Hotline": "GEN_",
    "Inspectional Services Department": "ISD",
    "Disabilities Commission": "DISB",
    "Office of Neighborhood Services": "ONS_",
    "Office of Economic Opportunity & Inclusion": "ECON",
    "Boston 311 Information": "INFO",
    "Public Works Department": "PWDx",
    "Boston Public Schools": "BPS_",
    "Boston Housing Authority": "BHA_",
    "Environment Department": "GRNI"
}

SYN_TYPES = {
    "Trash missed": "Missed Trash/Recycling/Yard Waste/Bulk Item",
    "Garbage not picked up": "Missed Trash/Recycling/Yard Waste/Bulk Item",
    "Pothole": "Request for Pothole Repair",
    "Fix road": "Request for Pothole Repair",
    "Street light out": "Street Light Outages",
    "Broken street lamp": "Street Light Outages",
    "Rats": "Rodent Activity",
    "Rat problem": "Rodent Activity",
    "Illegal parking": "Parking Enforcement",
    "Car blocked driveway": "Parking Enforcement",
    "Needle pickup": "Needle Pickup",
    "Syringe on ground": "Needle Pickup",
    "Dead tree": "Tree Emergencies",
    "Tree fell": "Tree Emergencies"
}

# DEF_DEPARTMENT = {
#     "Manages City-owned buildings (City Hall, etc.) and often handles graffiti removal on public property.": "PROP",
#     "Handles stray animals, wildlife issues, and dog licensing.": "ANML",
#     "Note: Recently renamed to the Mayor's Office of Housing (MOH). Handles housing development and supportive housing services.": "DND_",
#     "The T suffix often denotes the Traffic Division. Handles traffic signals, signs, and parking enforcement.": "BTDT",
#     "Independent agency managing water mains, sewers, and storm drains.": "BWSC",
#     "Non-emergency police matters (e.g., noise complaints, past crimes). For emergencies, call 911.": "BPD_",
#     "Maintenance of public parks, trees, and playgrounds.": "PARK",
#     "Handles general requests that don't fit other categories, often routed to the central constituent service team.": "GEN_",
#     "Enforces building codes, housing standards, restaurant health inspections, and sanitation.": "ISD",
#     "Handles accessibility issues (e.g., handicap ramp requests, disability access).": "DISB",
#     "Liaisons for specific neighborhoods; handles community issues and neighborhood improvement.": "ONS_",
#     "Formerly Economic Development. Supports small businesses and business licensing.": "ECON",
#     "Used for general inquiries or information requests rather than specific service actions.": "INFO",
#     "The x is a system suffix (often denotes district-level routing). Handles trash, recycling, potholes, and street lights.": "PWDx",
#     "Facilities and operational issues related to public school buildings.": "BPS_",
#     "Manages public housing communities and Section 8 vouchers.": "BHA_",
#     "Likely maps to Greenovate Boston or Green Infrastructure initiatives. Handles environmental programs and sustainability efforts.": "GRNI"
# }

SYN_SOURCES = {
    "Requests automatically generated by or synced from the Public Works Department's internal asset management system (IBM Maximo).": "Maximo Integration",
    "Requests submitted by residents through the official City of Boston website portal (boston.gov).": "Self Service",
    "Issues reported manually by city staff who identify problems while on duty (distinct from the specific mobile app below).": "Employee Generated",
    "Requests logged via the internal-only mobile application designed for field staff to report issues on the go.": "City Worker App",
    "Requests submitted via the public mobile app (this is the legacy name for the current BOS:311 app).": "Citizens Connect App",
    "Traditional requests made by citizens calling the 311 hotline and speaking to an agent.": "Constituent Call"
}

SYN_REASONS = {
    "Removal of bicycles left locked to public property for extended periods.": "Abandoned Bicycle",
    "Stray animals, barking dogs, wildlife nuisances, and dog licensing.": "Animal Issues",
    "Complaints regarding loud music, parties, or construction noise.": "Noise Disturbance",
    "Residential code violations, heating complaints, and tenant sanitary concerns.": "Housing",
    "Structural safety issues, illegal construction, and building permit inquiries.": "Building",
    "Requests regarding street sweeping schedules or missed cleaning.": "Street Cleaning",
    "Maintenance and landscaping issues within city-owned cemeteries.": "Cemetery",
    "Community-specific concerns routed to neighborhood liaisons.": "Neighborhood Services Issues",
    "General nuisances affecting neighborhood livability (e.g., hoarding, unsanitary conditions).": "Quality of Life",
    "Reports of outages, flickering lights, or damaged light poles.": "Street Lights",
    "Illegal parking enforcement, blocked driveways, and resident permit zones.": "Parking Complaints",
    "Requests for pruning, removal of dead trees, or planting new street trees.": "Trees",
    "System-generated alerts or general status updates.": "Notification",
    "Reports of dust, chemical odors, or idling vehicles.": "Air Pollution Control",
    "Loose, missing, or noisy utility covers and manholes on sidewalks.": "Sidewalk Cover / Manhole",
    "Issues regarding valet parking zones, licensing, or operations.": "Valet",
    "Towing and tagging of abandoned cars or vehicles with expired registration.": "Enforcement & Abandoned Vehicles",
    "Issues on Port Authority property (Airport/Seaport), usually referred to Massport police.": "Massport",
    "Inquiries or subscriptions for the city's emergency notification system.": "Alert Boston",
    "Internal queries, misclassified requests, or general information seeking.": "Administrative & General Requests",
    "Maintenance of bike lanes, racks, and Bluebikes station issues.": "Boston Bikes",
    "Sanitation violations (improper trash storage, failure to clear snow, illegal dumping).": "Code Enforcement",
    "Traffic signal timing, traffic calming studies, and signage engineering.": "Traffic Management & Engineering",
    "Removal of graffiti from public or private property.": "Graffiti",
    "Inspections of gas pumps, deli scales, and taxi meters for accuracy.": "Weights and Measures",
    "Unspecified or general noise complaints.": "Generic Noise Disturbance",
    "Hazardous waste disposal and environmental safety violations.": "Environmental Services",
    "Requests to fill potholes on city streets.": "Pothole",
    "Missed recycling pickups or requests for recycling carts.": "Recycling",
    "Inquiries regarding water, sewer, or tax bills.": "Billing",
    "Repair and maintenance of city-owned bridges.": "Bridge Maintenance",
    "Cleaning of clogged storm drains to prevent flooding.": "Catchbasin",
    "Reports of leaking, damaged, or snow-covered fire hydrants.": "Fire Hydrant",
    "Feedback, compliments, or complaints regarding city staff or service.": "Employee & General Comments",
    "Safe removal and disposal of discarded syringes found in public.": "Needle Program",
    "Restaurant inspections, public health sanitation, and hygiene concerns.": "Health",
    "Inquiries about specific city-run programs or events.": "Programs",
    "Maintenance of major roadways and structural street repairs.": "Highway Maintenance",
    "Mowing, playground repairs, and lighting issues within public parks.": "Park Maintenance & Safety",
    "Household trash collection, missed pickups, and bulk item disposal.": "Sanitation",
    "Maintenance of traffic signs (Stop, Yield) and traffic signal hardware.": "Signs & Signals",
    "Internal operational workflows or broad departmental tasks.": "Operations",
    "Uncategorized inquiries routed to the general 311 hotline.": "General Request",
    "Parking ticket appeals, payments, and late fee disputes.": "Office of The Parking Clerk",
    "Reports of rat sightings and requests for baiting in public areas.": "Rodent Activity"
}

SYN_SUBJECTS = {
    "Handles non-emergency enforcement issues like noise complaints and reports of past crimes.": "Boston Police Department",
    "Acts as a liaison for specific neighborhoods to address community issues and coordination.": "Neighborhood Services",
    "Manages traffic signals, signage, and parking enforcement regulations.": "Transportation - Traffic Division",
    "The central service hub for general inquiries and requests not covered by specific departments.": "Mayor's 24 Hour Hotline",
    "Oversees the maintenance and safety of public parks, playgrounds, and urban forestry.": "Parks & Recreation Department",
    "Responsible for street maintenance, lighting, refuse collection, and recycling.": "Public Works Department",
    "Enforces building codes, housing standards, and health regulations for restaurants and sanitation.": "Inspectional Services",
    "Manages water distribution, sewer systems, and storm drains.": "Boston Water & Sewer Commission",
    "Maintains city-owned buildings and manages graffiti removal on public structures.": "Property Management",
    "Deals with stray animals, wildlife nuisances, and pet licensing.": "Animal Control"
}

# ==========================================
# 3. FINAL EXPORT
# ==========================================

VALID_VALUES = {
    "neighborhood": create_mapping(RAW_NEIGHBORHOODS, SYN_NEIGHBORHOODS),
    "department": create_mapping(RAW_DEPARTMENTS, SYN_DEPARTMENTS),
    "source": create_mapping(RAW_SOURCES, SYN_SOURCES),
    "type": create_mapping(RAW_TYPES, SYN_TYPES),
    "subject": create_mapping(RAW_SUBJECTS, SYN_SUBJECTS),
    "reason": create_mapping(RAW_REASONS, SYN_REASONS),
    "case_status": ["Open", "Closed"],
    "on_time": ["ONTIME", "OVERDUE"]
}