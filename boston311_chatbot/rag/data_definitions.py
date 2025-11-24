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
    "Inspectional Services": "ISD",
    "Boston Water & Sewer": "BWSC",
    "Water Department": "BWSC",
    "Public Works": "PWDx",
    "PWD": "PWDx",
    "Neighborhood Services": "ONS_",
    "Boston Police": "BPD_",
    "Police": "BPD_",
    "Property Management": "PROP",
    "Animal Control": "ANML",
    "Parks Department": "PARK",
    "Boston Housing Authority": "BHA_",
    "Boston Public Schools": "BPS_",
    "Disability Commission": "DISB"
}

SYN_SOURCES = {
    "App": "Citizens Connect App",
    "Mobile": "Citizens Connect App",
    "Phone": "Constituent Call",
    "Call": "Constituent Call",
    "City Worker": "City Worker App"
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

# ==========================================
# 3. FINAL EXPORT
# ==========================================

VALID_VALUES = {
    "neighborhood": create_mapping(RAW_NEIGHBORHOODS, SYN_NEIGHBORHOODS),
    "department": create_mapping(RAW_DEPARTMENTS, SYN_DEPARTMENTS),
    "source": create_mapping(RAW_SOURCES, SYN_SOURCES),
    "type": create_mapping(RAW_TYPES, SYN_TYPES),
    "subject": RAW_SUBJECTS,
    "reason": RAW_REASONS,
    "case_status": ["Open", "Closed"],
    "on_time": ["ONTIME", "OVERDUE"]
}