import geopandas as gpd
import maup
import pandas as pd
import re

census_block = gpd.read_file(r"la_pl2020_b\la_pl2020_b.shp").to_crs("EPSG:5070")
block_group = gpd.read_file(r"la_race_2023_bg\la_race_2023_bg.shp").to_crs("EPSG:5070")
block_group_cvap = gpd.read_file(r"la_cvap_2023_bg\la\la_cvap_2023_bg.shp").to_crs("EPSG:5070")
precinct = gpd.read_file(r"la_2024_gen_prec\la_2024_gen_all_prec\la_2024_gen_all_prec.shp").to_crs("EPSG:5070")
income_bg = gpd.read_file(r"la_inc_2023_bg\la_inc_2023_bg.shp").to_crs("EPSG:5070")

# Filter precinct columns
keep_fields = {"UNIQUE_ID", "GEOID20", "geometry"}

for col in precinct.columns:
    if re.search(r"(G24PRE|GCON)", col) and re.search(r"(DEM|REP)", col):
        keep_fields.add(col)

precinct = precinct[[col for col in precinct.columns if col in keep_fields]]

# Rename census block columns
census_block = census_block.rename(columns={
    "P0020001": "TOT_POP20",
    "P0020002": "HSP_POP20",
    "P0020003": "NHSP_POP20",
    "P0020005": "WHT_POP20",
    "P0020006": "BLK_POP20",
    "P0020007": "AIA_POP20",
    "P0020008": "ASN_POP20",
    "P0020009": "HPI_POP20",
    "P0020010": "OTH_POP20",
    "P0020011": "2OM_POP20",
    "P0040001": "TOT_VAP20",
    "P0040002": "HSP_VAP20",
    "P0040003": "NHSP_VAP20",
    "P0040005": "WHT_VAP20",
    "P0040006": "BLK_VAP20",
    "P0040007": "AIA_VAP20",
    "P0040008": "ASN_VAP20",
    "P0040009": "HPI_VAP20",
    "P0040010": "OTH_VAP20",
    "P0040011": "2OM_VAP20",
})

# Prepare block group race data
block_group["WHT_POP23"] = block_group["WHT_NHSP23"]
block_group["BLK_POP23"] = block_group["BLK_NHSP23"]
block_group["AIA_POP23"] = block_group["AIA_NHSP23"]
block_group["ASN_POP23"] = block_group["ASN_NHSP23"]
block_group["HPI_POP23"] = block_group["HPI_NHSP23"]
block_group["OTH_POP23"] = block_group["OTH_NHSP23"]
block_group["2OM_POP23"] = block_group["2OM_NHSP23"]

# IMPORTANT: Only prorate base categories, not totals
race_columns_to_prorate = [
    "HSP_POP23",      # Hispanic (any race)
    "WHT_POP23",      # Non-Hispanic White
    "BLK_POP23",      # Non-Hispanic Black
    "AIA_POP23",      # Non-Hispanic AIAN
    "ASN_POP23",      # Non-Hispanic Asian
    "HPI_POP23",      # Non-Hispanic NHPI
    "OTH_POP23",      # Non-Hispanic Other
    "2OM_POP23"       # Non-Hispanic 2 or More
]

# Prorate race data from block group to block
b_to_bg_assignment = maup.assign(census_block, block_group)
block_race_estimates = {}

for identity in race_columns_to_prorate:
    identity20 = identity.replace("23", "20")
    bg_values = block_group[identity]
    
    bg_totals = census_block.groupby(b_to_bg_assignment)[identity20].transform("sum")
    weights = census_block[identity20] / bg_totals
    weights = weights.fillna(0)
    
    prorated = maup.prorate(b_to_bg_assignment, bg_values, weights)
    block_race_estimates[identity] = prorated.round().astype(int)

# Add prorated values to blocks
for identity in race_columns_to_prorate:
    census_block[identity] = block_race_estimates[identity]

# Aggregate blocks to precincts
blocks_to_precinct_assignment = maup.assign(census_block, precinct)
precinct[race_columns_to_prorate] = census_block[race_columns_to_prorate].groupby(blocks_to_precinct_assignment).sum()

# NOW calculate the totals from components (this ensures they match)
precinct["NHSP_POP23"] = (
    precinct["WHT_POP23"] + precinct["BLK_POP23"] + 
    precinct["AIA_POP23"] + precinct["ASN_POP23"] + 
    precinct["HPI_POP23"] + precinct["OTH_POP23"] + 
    precinct["2OM_POP23"]
)

precinct["TOT_POP23"] = precinct["HSP_POP23"] + precinct["NHSP_POP23"]

# Calculate block group totals the same way for fair comparison
block_group["NHSP_POP23_CALC"] = (
    block_group["WHT_POP23"] + block_group["BLK_POP23"] + 
    block_group["AIA_POP23"] + block_group["ASN_POP23"] + 
    block_group["HPI_POP23"] + block_group["OTH_POP23"] + 
    block_group["2OM_POP23"]
)
block_group["TOT_POP23_CALC"] = block_group["HSP_POP23"] + block_group["NHSP_POP23_CALC"]

# Compare using calculated totals
all_race_columns = race_columns_to_prorate + ["NHSP_POP23", "TOT_POP23"]

source_totals = block_group[[col for col in all_race_columns if col in block_group.columns or col + "_CALC" in block_group.columns]]
# Use calculated versions for totals
source_totals_dict = {}
for col in race_columns_to_prorate:
    source_totals_dict[col] = block_group[col].sum()
source_totals_dict["NHSP_POP23"] = block_group["NHSP_POP23_CALC"].sum()
source_totals_dict["TOT_POP23"] = block_group["TOT_POP23_CALC"].sum()

source_totals = pd.Series(source_totals_dict)
target_totals = precinct[all_race_columns].sum()
differences = target_totals - source_totals
percent_diff = (differences / source_totals.replace(0, pd.NA)) * 100

comparison = pd.DataFrame({
    "Source_ACS_BG": source_totals,
    "Target_Precinct": target_totals,
    "Difference": differences,
    "Pct_Difference": percent_diff.round(8)
})

print("\n=== Population Comparison by Race ===")
print(comparison)
print("\n=== Total difference across all races ===")
print(differences.sum())

# ===== CVAP Processing =====
block_group_cvap["TOT_CVAP23"] = block_group_cvap["CVAP_TOT23"]
block_group_cvap["HSP_CVAP23"] = block_group_cvap["CVAP_HSP23"]
block_group_cvap["WHT_CVAP23"] = block_group_cvap["CVAP_WHT23"]
block_group_cvap["BLK_CVAP23"] = block_group_cvap["CVAP_BLA23"]
block_group_cvap["ASN_CVAP23"] = block_group_cvap["CVAP_ASI23"]
block_group_cvap["AIA_CVAP23"] = block_group_cvap["CVAP_AMI23"]
block_group_cvap["HPI_CVAP23"] = block_group_cvap["CVAP_NHP23"]

# Combine all 2+ race categories since CVAP doesn't have OTH
block_group_cvap["2OM_CVAP23"] = (
    block_group_cvap["CVAP_2OM23"] + 
    block_group_cvap["CVAP_AIW23"] + 
    block_group_cvap["CVAP_ASW23"] +
    block_group_cvap["CVAP_BLW23"] +
    block_group_cvap["CVAP_AIB23"]
)

# Only prorate base categories
cvap_columns_to_prorate = [
    "HSP_CVAP23",
    "WHT_CVAP23",
    "BLK_CVAP23",
    "ASN_CVAP23",
    "AIA_CVAP23",
    "HPI_CVAP23",
    "2OM_CVAP23"
]

# Prorate CVAP from block group to block
b_to_bg_cvap_assignment = maup.assign(census_block, block_group_cvap)
block_cvap_estimates = {}

for category in cvap_columns_to_prorate:
    category20 = category.replace("CVAP23", "VAP20")
    
    if category == "2OM_CVAP23":
        # For 2OM_CVAP, we need to combine 2OM and OTH from VAP20
        weight_column = census_block["2OM_VAP20"] + census_block["OTH_VAP20"]
    else:
        weight_column = census_block[category20]
    
    bg_values = block_group_cvap[category]
    bg_totals = census_block.groupby(b_to_bg_cvap_assignment)[category20].transform("sum")
    
    if category == "2OM_CVAP23":
        bg_totals = (
            census_block.groupby(b_to_bg_cvap_assignment)["2OM_VAP20"].transform("sum") +
            census_block.groupby(b_to_bg_cvap_assignment)["OTH_VAP20"].transform("sum")
        )
        weights = weight_column / bg_totals
    else:
        weights = weight_column / bg_totals
    
    weights = weights.fillna(0)
    
    prorated = maup.prorate(b_to_bg_cvap_assignment, bg_values, weights)
    block_cvap_estimates[category] = prorated.fillna(0).round().astype(int)

# Add prorated CVAP to blocks
for category in cvap_columns_to_prorate:
    census_block[category] = block_cvap_estimates[category]

# Aggregate to precincts
precinct[cvap_columns_to_prorate] = census_block[cvap_columns_to_prorate].groupby(blocks_to_precinct_assignment).sum()

# Calculate totals from components
precinct["NHSP_CVAP23"] = (
    precinct["WHT_CVAP23"] + precinct["BLK_CVAP23"] + 
    precinct["AIA_CVAP23"] + precinct["ASN_CVAP23"] + 
    precinct["HPI_CVAP23"] + precinct["2OM_CVAP23"]
)

precinct["TOT_CVAP23"] = precinct["HSP_CVAP23"] + precinct["NHSP_CVAP23"]

# Calculate block group totals the same way
block_group_cvap["NHSP_CVAP23_CALC"] = (
    block_group_cvap["WHT_CVAP23"] + block_group_cvap["BLK_CVAP23"] + 
    block_group_cvap["AIA_CVAP23"] + block_group_cvap["ASN_CVAP23"] + 
    block_group_cvap["HPI_CVAP23"] + block_group_cvap["2OM_CVAP23"]
)
block_group_cvap["TOT_CVAP23_CALC"] = block_group_cvap["HSP_CVAP23"] + block_group_cvap["NHSP_CVAP23_CALC"]

all_cvap_columns = cvap_columns_to_prorate + ["NHSP_CVAP23", "TOT_CVAP23"]

source_cvap_dict = {}
for col in cvap_columns_to_prorate:
    source_cvap_dict[col] = block_group_cvap[col].sum()
source_cvap_dict["NHSP_CVAP23"] = block_group_cvap["NHSP_CVAP23_CALC"].sum()
source_cvap_dict["TOT_CVAP23"] = block_group_cvap["TOT_CVAP23_CALC"].sum()

source_cvap_totals = pd.Series(source_cvap_dict)
target_cvap_totals = precinct[all_cvap_columns].sum()
cvap_differences = target_cvap_totals - source_cvap_totals
cvap_percent_diff = (cvap_differences / source_cvap_totals.replace(0, pd.NA)) * 100

cvap_comparison = pd.DataFrame({
    "Source_ACS_BG": source_cvap_totals,
    "Target_Precinct": target_cvap_totals,
    "Difference": cvap_differences,
    "Pct_Difference": cvap_percent_diff.round(8)
})

print("\n=== CVAP Comparison by Race ===")
print(cvap_comparison)
print("\n=== Total CVAP difference across all races ===")
print(cvap_differences.sum())

# Save diagnostics
comparison.to_csv("population_comparison.csv")
cvap_comparison.to_csv("cvap_comparison.csv")

# Save precinct file
precinct.to_crs("EPSG:4326").to_file("precinct_all_pop.geojson", driver="GeoJSON")