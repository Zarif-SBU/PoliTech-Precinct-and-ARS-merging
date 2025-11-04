import geopandas as gpd
import maup
import pandas as pd
import re
import os

STATE_ABBR = "sc"
CENSUS_YEAR = 20
ACS_YEAR = 23
PRECINCT_YEAR = 24
OUTPUT_CRS = "EPSG:4326"
INPUT_CRS = "EPSG:5070"
OUTPUT_DIR = "Final_Precincts"
state_dir = os.path.join(OUTPUT_DIR, STATE_ABBR.lower())
os.makedirs(state_dir, exist_ok=True)

census_block = gpd.read_file(r"manual_downloads\extracted\sc\sc_pl2020_b\sc_pl2020_b.shp").to_crs("EPSG:5070")  # Census 2020 blocks
block_group = gpd.read_file(r"manual_downloads\extracted\sc\sc_race_2023_bg\sc_race_2023_bg.shp").to_crs("EPSG:5070") # ACS 2023 block groups for race
block_group_cvap = gpd.read_file(r"manual_downloads\extracted\sc\sc_cvap_2023_bg\sc\sc_cvap_2023_bg.shp").to_crs("EPSG:5070") # ACS 2023 CVAP block groups
precinct = gpd.read_file(r"manual_downloads\extracted\sc\sc_2024_gen_prec\sc_2024_gen_st_prec\sc_2024_gen_st_prec.shp").to_crs("EPSG:5070") # 2024 general election precincts
income_bg = gpd.read_file(r"manual_downloads\extracted\sc\sc_inc_2023_bg\sc_inc_2023_bg.shp").to_crs("EPSG:5070") # ACS 2023 income block groups

# Store original precinct columns - we'll filter at the end after adding all data
original_precinct_fields = {"UNIQUE_ID", "GEOID20", "geometry"}

# Keep only Harris and Trump for presidential, and all congressional candidates
for col in precinct.columns:
    # Keep Harris and Trump but obviously different for future races
    if col in ["G24PREDHAR", "G24PRERTRU"]:
        original_precinct_fields.add(col)
    # Keep all congressional district candidates
    # elif re.search(r"GCON\d+", col):
    #     original_precinct_fields.add(col)

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

# disaggragate race data from block group to block
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

# Add disaggrageted values to blocks
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

# disaggragate CVAP from block group to block
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

# Add disaggrageted CVAP to blocks
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

# ===== Income Processing =====
print("\n=== Starting Income Proration from Block Group to Precinct ===")

# Income bracket columns (excluding total households)
income_bracket_columns = [
    "LESS_10K23", "10K_15K23", "15K_20K23", "20K_25K23", "25K_30K23",
    "30K_35K23", "35K_40K23", "40K_45K23", "45K_50K23", "50K_60K23",
    "60K_75K23", "75K_100K23", "100_125K23", "125_150K23",
    "150_200K23", "200K_MOR23"
]

# Assign blocks to income block groups
b_to_bg_income_assignment = maup.assign(census_block, income_bg)
block_income_estimates = {}

# Prorate each income bracket separately
for category in income_bracket_columns:
    if category not in income_bg.columns:
        print(f"Warning: {category} not found in income_bg, skipping.")
        continue
    
    # Use total population as weight (best available proxy at block level)
    bg_values = income_bg[category]
    bg_totals = census_block.groupby(b_to_bg_income_assignment)["TOT_POP20"].transform("sum")
    weights = (census_block["TOT_POP20"] / bg_totals).fillna(0)
    
    prorated = maup.prorate(b_to_bg_income_assignment, bg_values, weights)
    block_income_estimates[category] = prorated.fillna(0).round().astype(int)

# Attach prorated income estimates to blocks
for category in income_bracket_columns:
    if category in block_income_estimates:
        census_block[category] = block_income_estimates[category]

# Aggregate to precincts
precinct[income_bracket_columns] = census_block[income_bracket_columns].groupby(blocks_to_precinct_assignment).sum()

# Calculate total households from sum of brackets (ensures consistency)
precinct["TOT_HOUS23"] = precinct[income_bracket_columns].sum(axis=1)

# Calculate block group totals the same way for fair comparison
income_bg["TOT_HOUS23_CALC"] = income_bg[income_bracket_columns].sum(axis=1)

# Diagnostic comparison
all_income_columns = income_bracket_columns + ["TOT_HOUS23"]

source_income_dict = {}
for col in income_bracket_columns:
    source_income_dict[col] = income_bg[col].sum()
source_income_dict["TOT_HOUS23"] = income_bg["TOT_HOUS23_CALC"].sum()

source_income_totals = pd.Series(source_income_dict)
target_income_totals = precinct[all_income_columns].sum()
income_differences = target_income_totals - source_income_totals
income_percent_diff = (income_differences / source_income_totals.replace(0, pd.NA)) * 100

income_comparison = pd.DataFrame({
    "Source_ACS_BG": source_income_totals,
    "Target_Precinct": target_income_totals,
    "Difference": income_differences,
    "Pct_Difference": income_percent_diff.round(8)
})

print("\n=== Household Income Comparison ===")
print(income_comparison)
print("\n=== Total Income Difference Across All Brackets ===")
print(income_differences.sum())

# ===== Calculate Median Household Income =====
print("\n=== Calculating Median Household Income by Precinct ===")

# Define income bin midpoints (more accurate than linear interpolation for open-ended brackets)
income_bins = [
    ("LESS_10K23", 5000),      # Midpoint of $0-$9,999
    ("10K_15K23", 12500),      # Midpoint of $10,000-$14,999
    ("15K_20K23", 17500),
    ("20K_25K23", 22500),
    ("25K_30K23", 27500),
    ("30K_35K23", 32500),
    ("35K_40K23", 37500),
    ("40K_45K23", 42500),
    ("45K_50K23", 47500),
    ("50K_60K23", 55000),
    ("60K_75K23", 67500),
    ("75K_100K23", 87500),
    ("100_125K23", 112500),
    ("125_150K23", 137500),
    ("150_200K23", 175000),
    ("200K_MOR23", 250000)     # Approximate for $200K+ (could use 225K or higher)
]

# def compute_median_income(row):
#     """
#     Calculate median household income using cumulative distribution method.
#     Finds the income bracket where cumulative households >= 50% of total,
#     then interpolates within that bracket.
#     """
#     total_households = row["TOT_HOUS23"]
    
#     if pd.isna(total_households) or total_households <= 0:
#         return pd.NA
    
#     cumulative = 0
#     median_position = total_households / 2.0
    
#     # Define bracket boundaries for interpolation
#     bin_boundaries = [
#         (0, 10000), (10000, 15000), (15000, 20000), (20000, 25000),
#         (25000, 30000), (30000, 35000), (35000, 40000), (40000, 45000),
#         (45000, 50000), (50000, 60000), (60000, 75000), (75000, 100000),
#         (100000, 125000), (125000, 150000), (150000, 200000), (200000, 300000)
#     ]
    
#     for i, (col_name, _) in enumerate(income_bins):
#         count = row[col_name]
        
#         if pd.isna(count):
#             count = 0
        
#         cumulative += count
        
#         # Check if median falls in this bracket
#         if cumulative >= median_position:
#             lower_bound, upper_bound = bin_boundaries[i]
#             prev_cumulative = cumulative - count
            
#             # Handle edge case where bracket has no households
#             if count == 0:
#                 return lower_bound
            
#             # Linear interpolation within the bracket
#             position_in_bracket = (median_position - prev_cumulative) / count
#             median_income = lower_bound + (upper_bound - lower_bound) * position_in_bracket
            
#             return round(median_income, 2)
    
#     # If we get here, return the upper bound of the highest bracket
#     return bin_boundaries[-1][1]

# Apply median calculation to all precincts
# precinct["MEDN_INC23"] = precinct.apply(compute_median_income, axis=1)

# print(f"\nCalculated median income for {precinct['MEDN_INC23'].notna().sum()} precincts")
# if precinct['MEDN_INC23'].notna().sum() > 0:
#     print(f"Median income range: ${precinct['MEDN_INC23'].min():.2f} - ${precinct['MEDN_INC23'].max():.2f}")

# set place holder median income to 0 for now
precinct["MEDN_INC23"] = 0

# Filter to final columns we want to keep (original precinct fields + all our calculated fields)
final_columns = list(original_precinct_fields) + all_race_columns + all_cvap_columns + all_income_columns + ["MEDN_INC23"]
# Keep only columns that exist in the precinct dataframe
final_columns = [col for col in final_columns if col in precinct.columns]
precinct = precinct[final_columns]

numeric_cols = all_race_columns + all_cvap_columns + all_income_columns
for col in numeric_cols:
    if col in precinct.columns:
        precinct[col] = precinct[col].fillna(0).round().astype("Int64")

print(f"\n=== Final precinct contains {len(precinct.columns)} columns ===")

# Save diagnostics
comparison.to_csv(os.path.join(state_dir, f"{STATE_ABBR}_population_comparison.csv"), index=False)
cvap_comparison.to_csv(os.path.join(state_dir, f"{STATE_ABBR}_cvap_comparison.csv"), index=False)
income_comparison.to_csv(os.path.join(state_dir, f"{STATE_ABBR}_income_comparison.csv"), index=False)

# Save precinct file
precinct_outfile = os.path.join(state_dir, f"{STATE_ABBR}_precinct_all_pop.geojson")
precinct.to_crs("EPSG:4326").to_file(precinct_outfile, driver="GeoJSON")

print(f"\n=== Files saved to: {state_dir} ===")