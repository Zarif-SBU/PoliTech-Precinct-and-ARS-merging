import geopandas as gpd
import pandas as pd
import maup
import re

# ========== CONFIGURABLE VARIABLES ==========
STATE_NAME = "la"           # Example: "la" or "tx"
CENSUS_YEAR = 20          # Census PL year
ACS_YEAR = 23             # ACS year for block group race and income
PRECINCT_YEAR = 24        # Precinct shapefile year

# Filepaths – fill manually
CENSUS_BLOCK_PATH = f"la_pl2020_b\la_pl2020_b.shp"
BLOCK_GROUP_RACE_PATH = f"la_race_2023_bg\la_race_2023_bg.shp"
BLOCK_GROUP_CVAP_PATH = f"{STATE_NAME}_cvap_{ACS_YEAR}_bg/{STATE_NAME}/{STATE_NAME}_cvap_{ACS_YEAR}_bg.shp"
INCOME_BG_PATH = f"{STATE_NAME}_inc_{ACS_YEAR}_bg/{STATE_NAME}_inc_{ACS_YEAR}_bg.shp"
PRECINCT_PATH = f"{STATE_NAME}_{PRECINCT_YEAR}_gen_prec/{STATE_NAME}_{PRECINCT_YEAR}_gen_all_prec/{STATE_NAME}_{PRECINCT_YEAR}_gen_all_prec.shp"

# ============================================


# ---------- LOAD AND PREPARE DATA ----------
def load_data():
    census_block = gpd.read_file(CENSUS_BLOCK_PATH).to_crs("EPSG:5070")
    block_group = gpd.read_file(BLOCK_GROUP_RACE_PATH).to_crs("EPSG:5070")
    block_group_cvap = gpd.read_file(BLOCK_GROUP_CVAP_PATH).to_crs("EPSG:5070")
    income_bg = gpd.read_file(INCOME_BG_PATH).to_crs("EPSG:5070")
    precinct = gpd.read_file(PRECINCT_PATH).to_crs("EPSG:5070")
    return census_block, block_group, block_group_cvap, income_bg, precinct


def rename_census_columns(census_block):
    return census_block.rename(columns={
        "P0020001": "TOT_POP{CENSUS_YEAR}", "P0020002": "HSP_POP{CENSUS_YEAR}", "P0020003": "NHSP_POP{CENSUS_YEAR}",
        "P0020005": "WHT_POP{CENSUS_YEAR}", "P0020006": "BLK_POP{CENSUS_YEAR}", "P0020007": "AIA_POP{CENSUS_YEAR}",
        "P0020008": "ASN_POP{CENSUS_YEAR}", "P0020009": "HPI_POP{CENSUS_YEAR}", "P0020010": "OTH_POP{CENSUS_YEAR}",
        "P0020011": "2OM_POP{CENSUS_YEAR}", "P0040001": "TOT_VAP{CENSUS_YEAR}", "P0040002": "HSP_VAP{CENSUS_YEAR}",
        "P0040003": "NHSP_VAP{CENSUS_YEAR}", "P0040005": "WHT_VAP{CENSUS_YEAR}", "P0040006": "BLK_VAP{CENSUS_YEAR}",
        "P0040007": "AIA_VAP{CENSUS_YEAR}", "P0040008": "ASN_VAP{CENSUS_YEAR}", "P0040009": "HPI_VAP{CENSUS_YEAR}",
        "P0040010": "OTH_VAP{CENSUS_YEAR}", "P0040011": "2OM_VAP{CENSUS_YEAR}",
    })


def select_precinct_fields(precinct):
    fields = {"UNIQUE_ID", "GEOID20", "geometry"}
    for col in precinct.columns:
        if col in ["G24PREDHAR", "G24PRERTRU"] or re.search(r"GCON\d+", col):
            fields.add(col)
    return fields


# ---------- RACE / POPULATION PROCESS ----------
def prorate_race_data(census_block, block_group, precinct):
    # Prepare race columns
    block_group["WHT_POP23"] = block_group["WHT_NHSP23"]
    block_group["BLK_POP23"] = block_group["BLK_NHSP23"]
    block_group["AIA_POP23"] = block_group["AIA_NHSP23"]
    block_group["ASN_POP23"] = block_group["ASN_NHSP23"]
    block_group["HPI_POP23"] = block_group["HPI_NHSP23"]
    block_group["OTH_POP23"] = block_group["OTH_NHSP23"]
    block_group["2OM_POP23"] = block_group["2OM_NHSP23"]

    race_cols = ["HSP_POP23","WHT_POP23","BLK_POP23","AIA_POP23","ASN_POP23","HPI_POP23","OTH_POP23","2OM_POP23"]

    # Disaggregate block group → block
    b_to_bg = maup.assign(census_block, block_group)
    for col in race_cols: # We make seperate weights for each race
        bg_values = block_group[col]
        bg_totals = census_block.groupby(b_to_bg)[col.replace("23", "20")].transform("sum")
        weights = (census_block[col.replace("23", "20")] / bg_totals).fillna(0)
        census_block[col] = maup.prorate(b_to_bg, bg_values, weights).round().astype(int)

    # Aggregate to precinct
    b_to_prec = maup.assign(census_block, precinct)
    precinct[race_cols] = census_block[race_cols].groupby(b_to_prec).sum()

    # Totals
    precinct["NHSP_POP23"] = precinct[["WHT_POP23","BLK_POP23","AIA_POP23","ASN_POP23","HPI_POP23","OTH_POP23","2OM_POP23"]].sum(axis=1)
    precinct["TOT_POP23"] = precinct["HSP_POP23"] + precinct["NHSP_POP23"]

    return precinct, race_cols


# ---------- CVAP PROCESS ----------
def prorate_cvap_data(census_block, block_group_cvap, precinct):
    block_group_cvap["TOT_CVAP23"] = block_group_cvap["CVAP_TOT23"]
    block_group_cvap["HSP_CVAP23"] = block_group_cvap["CVAP_HSP23"]
    block_group_cvap["WHT_CVAP23"] = block_group_cvap["CVAP_WHT23"]
    block_group_cvap["BLK_CVAP23"] = block_group_cvap["CVAP_BLA23"]
    block_group_cvap["ASN_CVAP23"] = block_group_cvap["CVAP_ASI23"]
    block_group_cvap["AIA_CVAP23"] = block_group_cvap["CVAP_AMI23"]
    block_group_cvap["HPI_CVAP23"] = block_group_cvap["CVAP_NHP23"]

    block_group_cvap["2OM_CVAP23"] = (
        block_group_cvap["CVAP_2OM23"] + block_group_cvap["CVAP_AIW23"] +
        block_group_cvap["CVAP_ASW23"] + block_group_cvap["CVAP_BLW23"] + block_group_cvap["CVAP_AIB23"]
    )

    cols = ["HSP_CVAP23","WHT_CVAP23","BLK_CVAP23","ASN_CVAP23","AIA_CVAP23","HPI_CVAP23","2OM_CVAP23"]
    b_to_bg = maup.assign(census_block, block_group_cvap)
    for col in cols:
        if col == "2OM_CVAP23":
            weight_col = census_block["2OM_VAP20"] + census_block["OTH_VAP20"]
            bg_total = (
                census_block.groupby(b_to_bg)["2OM_VAP20"].transform("sum") +
                census_block.groupby(b_to_bg)["OTH_VAP20"].transform("sum")
            )
        else:
            weight_col = census_block[col.replace("CVAP23", "VAP20")]
            bg_total = census_block.groupby(b_to_bg)[col.replace("CVAP23", "VAP20")].transform("sum")

        weights = (weight_col / bg_total).fillna(0)
        census_block[col] = maup.prorate(b_to_bg, block_group_cvap[col], weights).fillna(0).round().astype(int)

    # Aggregate
    b_to_prec = maup.assign(census_block, precinct)
    precinct[cols] = census_block[cols].groupby(b_to_prec).sum()

    precinct["NHSP_CVAP23"] = precinct[["WHT_CVAP23","BLK_CVAP23","AIA_CVAP23","ASN_CVAP23","HPI_CVAP23","2OM_CVAP23"]].sum(axis=1)
    precinct["TOT_CVAP23"] = precinct["HSP_CVAP23"] + precinct["NHSP_CVAP23"]

    return precinct, cols


# ---------- INCOME PROCESS ----------
def prorate_income_data(census_block, income_bg, precinct):
    income_cols = [
        "LESS_10K23","10K_15K23","15K_20K23","20K_25K23","25K_30K23",
        "30K_35K23","35K_40K23","40K_45K23","45K_50K23","50K_60K23",
        "60K_75K23","75K_100K23","100_125K23","125_150K23","150_200K23","200K_MOR23"
    ]

    b_to_bg = maup.assign(census_block, income_bg)
    for col in income_cols:
        if col not in income_bg.columns:
            print(f"Skipping missing column: {col}")
            continue
        bg_values = income_bg[col]
        bg_totals = census_block.groupby(b_to_bg)["TOT_POP20"].transform("sum")
        weights = (census_block["TOT_POP20"] / bg_totals).fillna(0)
        census_block[col] = maup.prorate(b_to_bg, bg_values, weights).fillna(0).round().astype(int)

    # Aggregate
    b_to_prec = maup.assign(census_block, precinct)
    precinct[income_cols] = census_block[income_cols].groupby(b_to_prec).sum()
    precinct["TOT_HOUS23"] = precinct[income_cols].sum(axis=1)

    # Median income calculation
    income_bins = [
        ("LESS_10K23", 5000), ("10K_15K23", 12500), ("15K_20K23", 17500), ("20K_25K23", 22500),
        ("25K_30K23", 27500), ("30K_35K23", 32500), ("35K_40K23", 37500), ("40K_45K23", 42500),
        ("45K_50K23", 47500), ("50K_60K23", 55000), ("60K_75K23", 67500), ("75K_100K23", 87500),
        ("100_125K23", 112500), ("125_150K23", 137500), ("150_200K23", 175000), ("200K_MOR23", 250000)
    ]

    def compute_median_income(row):
        total = row["TOT_HOUS23"]
        if pd.isna(total) or total <= 0:
            return pd.NA
        cumulative, median_pos = 0, total / 2
        bin_bounds = [(0, 10000), (10000,15000),(15000,20000),(20000,25000),
                      (25000,30000),(30000,35000),(35000,40000),(40000,45000),
                      (45000,50000),(50000,60000),(60000,75000),(75000,100000),
                      (100000,125000),(125000,150000),(150000,200000),(200000,300000)]
        for i,(col,_) in enumerate(income_bins):
            count = row.get(col, 0)
            cumulative += count
            if cumulative >= median_pos:
                lb, ub = bin_bounds[i]
                prev = cumulative - count
                if count == 0:
                    return lb
                frac = (median_pos - prev) / count
                return round(lb + (ub - lb) * frac, 2)
        return bin_bounds[-1][1]

    precinct["MEDN_INC23"] = precinct.apply(compute_median_income, axis=1)
    return precinct, income_cols


# ---------- MAIN ----------
def main():
    census_block, block_group, block_group_cvap, income_bg, precinct = load_data()

    census_block = rename_census_columns(census_block)
    original_fields = select_precinct_fields(precinct)

    precinct, race_cols = prorate_race_data(census_block, block_group, precinct)
    precinct, cvap_cols = prorate_cvap_data(census_block, block_group_cvap, precinct)
    precinct, income_cols = prorate_income_data(census_block, income_bg, precinct)

    final_cols = list(original_fields) + race_cols + cvap_cols + income_cols + ["MEDN_INC23"]
    final_cols = [c for c in final_cols if c in precinct.columns]
    precinct = precinct[final_cols]

    out_file = f"{STATE_NAME}_precinct_all_pop.geojson"
    precinct.to_crs("EPSG:4326").to_file(out_file, driver="GeoJSON")
    print(f"\n=== Saved to {out_file} ===")


if __name__ == "__main__":
    main()
