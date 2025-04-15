#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
UK Financial Sanctions List Processing Script

Objective:
Extract, cleanse, and prepare the UK financial sanctions data from the provided
CSV ('ConList.csv') into a structured format suitable for comparison against
customer records. The output is a CSV file ('Structured_Sanctions_Data.csv')
with one row per unique Group ID.

Process Overview:
1. Load raw data.
2. Parse and categorize Date of Birth information, handling various formats.
3. Cleanse and standardize country name fields (handle inconsistencies & typos, remove prefixes, punctuation, whitespace).
4. Construct full name strings from component fields for each record.
5. Aggregate data by Group ID, consolidating names, aliases, dates, countries,
   and other relevant identifiers.
6. Format aggregated dates and handle multi-value fields using a delimiter.
7. Perform post-aggregation cleaning (remove invisible LRM characters).
8. Convert relevant metadata date strings to datetime objects.
9. Perform post-aggregation cleaning (trim excess whitespace).
10. Conduct final data quality and consistency checks.
11. Sort the final dataset by Group ID.
12. Export the cleaned, structured data to CSV.
"""

import pandas as pd  # Main data processing library used for DataFrame operations
import re
import sys
import os
from tqdm.auto import tqdm  # For progress bars
import argparse  # For command-line argument parsing

# --- Configuration Constants ---
INPUT_CSV_PATH = "ConList.csv"
OUTPUT_CSV_PATH = "Structured_Sanctions_Data.csv"
ENCODING = "utf-8"
CSV_HEADER_ROW = 1
LIST_DELIMITER = "; "
VERBOSE_MODE = True


# --- Helper Functions ---
def parse_dob_comprehensive(dob_str):
    if pd.isna(dob_str) or str(dob_str).strip().lower() == "nan":
        return (pd.NaT, "Missing")
    dob_str = str(dob_str).strip()
    current_year = pd.Timestamp.now().year
    parsed_date = pd.NaT
    precision = "Unknown/Failed"
    try:
        if not dob_str.startswith("00/"):
            temp_date = pd.to_datetime(dob_str, format="%d/%m/%Y", errors="raise")
            if 1900 <= temp_date.year <= current_year:
                parsed_date = temp_date
                precision = "Full Date"
    except (ValueError, TypeError):
        pass
    if pd.isna(parsed_date):
        match_00_mm = re.match(r"^00/(\d{1,2})/(\d{4})$", dob_str)
        if match_00_mm:
            month, year = map(int, match_00_mm.groups())
            if 1 <= month <= 12 and 1900 <= year <= current_year:
                try:
                    parsed_date = pd.Timestamp(year=year, month=month, day=1)
                    precision = "Month/Year Only"
                except ValueError:
                    pass
    if pd.isna(parsed_date):
        match_00_00 = re.match(r"^00/00/(\d{4})$", dob_str)
        if match_00_00:
            year = int(match_00_00.group(1))
            if 1900 <= year <= current_year:
                try:
                    parsed_date = pd.Timestamp(year=year, month=1, day=1)
                    precision = "Year Only"
                except ValueError:
                    pass
    if pd.isna(parsed_date) and precision == "Unknown/Failed":
        precision = "Unknown/Failed"
    return (parsed_date, precision)


country_extract_pattern = re.compile(r"^(?:\(\w{1,3}\)\s*)*(.*?)(\s*\(\d+\).*)*$")


def clean_country_string(country_str):
    if pd.isna(country_str):
        return pd.NA
    country_str = str(country_str)
    match = country_extract_pattern.match(country_str)
    if match:
        cleaned_name = match.group(1).strip()
    else:
        cleaned_name = country_str.strip()
    if cleaned_name:
        cleaned_name = cleaned_name.rstrip("., ")
    return cleaned_name if cleaned_name else pd.NA


def construct_full_name(row):
    name_parts = []
    if pd.notna(row["Title"]):
        name_parts.append(str(row["Title"]).strip())
    for i in range(1, 7):
        col = f"Name {i}"
        if pd.notna(row[col]):
            name_parts.append(str(row[col]).strip())
    name_parts = [part for part in name_parts if part]
    return " ".join(name_parts) if name_parts else None


def get_unique_sorted_list(series):
    try:
        unique_vals = series.dropna().unique()
        valid_vals_str = [str(val) for val in unique_vals if pd.notna(val)]
        valid_vals_filtered = [
            val
            for val in valid_vals_str
            if val.strip().lower() not in ("nan", "none", "<na>", "nat")
        ]
        return (
            sorted(list(valid_vals_filtered)) if len(valid_vals_filtered) > 0 else None
        )
    except Exception:
        return None


def aggregate_sanctions_data(group):
    """Aggregates data for a single Group ID (passed via groupby.apply)."""
    primary_row = group[group["Alias Type"] == "Primary name"].head(1)
    if primary_row.empty:
        primary_row = group[group["Alias Type"] == "Primary name variation"].head(1)
    if primary_row.empty:
        primary_row = group.iloc[[0]]
    primary_name = primary_row["Constructed_Name"].iloc[0]
    primary_name_nl = primary_row["Name_Non_Latin_Raw"].iloc[0]
    group_type = primary_row["Group Type"].iloc[0]
    regime = primary_row["Regime"].iloc[0]
    all_constructed_names = set(group["Constructed_Name"].dropna())
    aliases = (
        sorted(list(all_constructed_names - {primary_name}))
        if primary_name
        else sorted(list(all_constructed_names))
    )
    all_nl_names = set(group["Name_Non_Latin_Raw"].dropna())
    aliases_nl = (
        sorted(list(all_nl_names - {primary_name_nl}))
        if pd.notna(primary_name_nl)
        else sorted(list(all_nl_names))
    )
    dob_raw_agg = get_unique_sorted_list(group["DOB_raw"])
    unique_parsed_dobs = group["DOB_parsed"].dropna().unique()
    dob_parsed_agg_list = (
        sorted(list(unique_parsed_dobs)) if len(unique_parsed_dobs) > 0 else None
    )
    dob_year_agg = get_unique_sorted_list(group["DOB_year"])
    dob_precision_agg = get_unique_sorted_list(group["DOB_Precision"])
    countries_birth = get_unique_sorted_list(group["Country of Birth"])
    nationalities = get_unique_sorted_list(group["Nationality"])
    countries_addr = get_unique_sorted_list(group["Country"])
    all_countries_set = set()
    if countries_birth:
        all_countries_set.update(countries_birth)
    if nationalities:
        all_countries_set.update(nationalities)
    if countries_addr:
        all_countries_set.update(countries_addr)
    all_associated_countries = (
        sorted([c for c in all_countries_set if pd.notna(c)])
        if all_countries_set
        else None
    )
    positions = get_unique_sorted_list(group["Position"])
    passport_nums = get_unique_sorted_list(group["Passport Number"])
    nat_ids = get_unique_sorted_list(group["National Identification Number"])
    addr_cols = [f"Address {i}" for i in range(1, 7)] + ["Post/Zip Code"]
    address_components = []
    for col in addr_cols:
        address_components.extend(list(group[col].astype(str).dropna().unique()))
    address_components = [
        addr for addr in address_components if str(addr).strip().lower() != "nan"
    ]
    full_address_agg = ", ".join(address_components) if address_components else None
    dob_parsed_str_agg = None
    if dob_parsed_agg_list:
        try:
            formatted_dates = [dt.strftime("%Y-%m-%d") for dt in dob_parsed_agg_list]
            dob_parsed_str_agg = LIST_DELIMITER.join(formatted_dates)
        except Exception:
            dob_parsed_str_agg = LIST_DELIMITER.join(map(str, dob_parsed_agg_list))
    final_data = {
        "Primary_Name": primary_name,
        "Aliases": LIST_DELIMITER.join(map(str, aliases)) if aliases else None,
        "Primary_Name_Non_Latin": (
            primary_name_nl if pd.notna(primary_name_nl) else None
        ),
        "Aliases_Non_Latin": (
            LIST_DELIMITER.join(map(str, aliases_nl)) if aliases_nl else None
        ),
        "Group_Type": group_type,
        "Regime": regime,
        "DOB_Raw_Agg": (
            LIST_DELIMITER.join(map(str, dob_raw_agg)) if dob_raw_agg else None
        ),
        "DOB_Parsed_Agg": dob_parsed_str_agg,
        "DOB_Year_Agg": (
            LIST_DELIMITER.join(map(str, dob_year_agg)) if dob_year_agg else None
        ),
        "DOB_Precision_Agg": (
            LIST_DELIMITER.join(map(str, dob_precision_agg))
            if dob_precision_agg
            else None
        ),
        "Countries_of_Birth": (
            LIST_DELIMITER.join(map(str, countries_birth)) if countries_birth else None
        ),
        "Nationalities": (
            LIST_DELIMITER.join(map(str, nationalities)) if nationalities else None
        ),
        "Countries_Address": (
            LIST_DELIMITER.join(map(str, countries_addr)) if countries_addr else None
        ),
        "All_Associated_Countries": (
            LIST_DELIMITER.join(map(str, all_associated_countries))
            if all_associated_countries
            else None
        ),
        "Positions": LIST_DELIMITER.join(map(str, positions)) if positions else None,
        "Passport_Numbers_Agg": (
            LIST_DELIMITER.join(map(str, passport_nums)) if passport_nums else None
        ),
        "National_IDs_Agg": LIST_DELIMITER.join(map(str, nat_ids)) if nat_ids else None,
        "Full_Address_Agg": full_address_agg,
        "Listed_On": primary_row["Listed On"].iloc[0],
        "UK_Sanctions_List_Date_Designated": primary_row[
            "UK Sanctions List Date Designated"
        ].iloc[0],
        "Last_Updated": primary_row["Last Updated"].iloc[0],
    }
    return pd.Series(final_data)


# --- Post-Aggregation Cleaning & Check Functions ---
def remove_lrm(df, verbose=VERBOSE_MODE):
    """Removes Left-to-Right Mark (U+200E) from string/object columns."""
    if verbose:
        print("  Removing LRM characters (U+200E)...")
    lrm_char = "\u200e"
    string_cols = df.select_dtypes(include=["object", "string"]).columns
    processed_count = 0
    for col in tqdm(
        string_cols, desc="Cleaning LRM", leave=False, disable=(not verbose)
    ):
        if col in df.columns:
            try:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    continue
                is_na_mask = pd.isna(df[col])
                str_series = (
                    df[col]
                    .fillna("")
                    .astype(str)
                    .str.replace(lrm_char, "", regex=False)
                )
                str_series = str_series.replace(
                    {"nan": pd.NA, "None": pd.NA, "<NA>": pd.NA, "": pd.NA}, regex=False
                )
                df[col] = str_series.mask(is_na_mask, pd.NA).astype("string")
                processed_count += 1
            except Exception as e:
                if verbose:
                    print(
                        f"    Warning: Could not process column '{col}' for LRM removal - {e}"
                    )
    if verbose:
        print(
            f"  LRM character removal attempted on {processed_count} applicable columns."
        )
    return df


def clean_all_whitespace(df, verbose=VERBOSE_MODE):
    """
    Trims leading/trailing whitespace AND replaces multiple internal spaces
    with a single space for all string/object columns.
    """
    if verbose:
        print("  Cleaning excess whitespace (leading/trailing/internal)...")
    # Select columns that are likely to contain strings
    string_cols = df.select_dtypes(include=["object", "string"]).columns
    processed_count = 0
    for col in tqdm(
        string_cols, desc="Cleaning Whitespace", leave=False, disable=(not verbose)
    ):
        if col in df.columns:
            try:
                # Skip actual datetime columns
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    continue

                # Check if the column has string methods available
                if hasattr(df[col].str, "strip") and hasattr(df[col].str, "replace"):
                    is_na_mask = pd.isna(df[col])  # Remember original NAs
                    # Apply strip and replace for multiple spaces
                    # Fill NA temporarily to avoid errors during string ops
                    cleaned_series = df[col].fillna("").astype(str)
                    cleaned_series = cleaned_series.str.strip()
                    cleaned_series = cleaned_series.str.replace(
                        r"\s{2,}", " ", regex=True
                    )
                    # Convert empty strings back to NA, then restore original NAs
                    cleaned_series = cleaned_series.replace({"": pd.NA}, regex=False)
                    df[col] = cleaned_series.mask(is_na_mask, pd.NA).astype(
                        "string"
                    )  # Ensure final type is nullable string
                    processed_count += 1
                elif (
                    df[col].dtype == "object"
                ):  # Fallback for pure object columns if necessary
                    is_na_mask = pd.isna(df[col])
                    # Apply transformations row by row (slower)
                    cleaned_values = []
                    for x in df[col]:
                        if pd.isna(x):
                            cleaned_values.append(pd.NA)
                        else:
                            text = str(x).strip()
                            text = re.sub(r"\s{2,}", " ", text) if text else text
                            cleaned_values.append(text if text else pd.NA)
                    df[col] = pd.Series(cleaned_values, index=df.index).astype("string")
                    processed_count += 1

            except Exception as e:
                if verbose:
                    print(
                        f"    Warning: Could not process column '{col}' for whitespace cleaning - {e}"
                    )
    if verbose:
        print(
            f"  Whitespace cleaning attempted on {processed_count} applicable columns."
        )
    return df


def convert_metadata_dates(df, verbose=VERBOSE_MODE):
    """Converts metadata date string columns to datetime objects."""
    if verbose:
        print("  Converting metadata date columns...")
    date_cols_metadata = [
        "Listed_On",
        "UK_Sanctions_List_Date_Designated",
        "Last_Updated",
    ]
    warnings = 0
    for col in date_cols_metadata:
        if col in df.columns:
            original_nulls = df[col].isnull().sum()
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
            new_nulls = df[col].isnull().sum()
            if new_nulls > original_nulls and verbose:
                print(
                    f"    Warning: {new_nulls - original_nulls} values in '{col}' failed parse -> NaT."
                )
                warnings += 1
        elif verbose:
            print(f"    Info: Metadata date column '{col}' not found.")
    if warnings == 0 and verbose:
        print("  Metadata date conversions successful.")
    return df


def standardize_countries_in_agg_col(df, columns_to_standardize, verbose=VERBOSE_MODE):
    """
    Standardizes specific country names within semi-colon delimited string columns.
    Handles splitting, replacing, deduplicating, sorting, and rejoining.
    """
    if verbose:
        print("  Standardizing specific country names in aggregated columns...")
    processed_count = 0

    # Define a dictionary to map known variations, typos, abbreviations,
    # historical names, and adjectival forms found in the source country/nationality
    # fields to a standardized country name.
    country_replacement_map = {
        "Russian Federation": "Russia",
        "Russian": "Russia",
        "RUSSIA": "Russia",
        "Belarusian SSR, (now Belarus)": "Belarus",
        "Uzbekhistan": "Uzbekistan",
        "Ukrainian SSR now Ukraine": "Ukrainian SSR (now Ukraine)",
        "Ukrainian SSR (Ukraine)": "Ukrainian SSR (now Ukraine)",
        "Ukrainian SSR": "Ukrainian SSR (now Ukraine)",
        "Kazakh Soviet Socialist Republic (now Kazakhstan)": "Kazakh SSR (now Kazakhstan)",
        "Kazakh Soviet Socialist Republic": "Kazakh SSR (now Kazakhstan)",
        "Kazakh SSR": "Kazakh SSR (now Kazakhstan)",
        "Bosnia-Herzegovina": "Bosnia and Herzegovina",
        "Uzbek SSR": "Uzbekistan SSR (now Uzbekistan)",
        "USSR": "Russia (USSR)",
        "United Republic of Tanzania": "Tanzania",
        "German": "Germany",
        "Democratic People's Republic of Korea": "North Korea",
        "DPRK": "North Korea",
        "Türkiye": "Turkey",
        "United States of America": "United States",
        "Guinea-Bissau": "Guinea Bissau",
        # "Palestinian Territories": "Occupied Palestinian Territories", # These are historically and politically sensitive/different
        # NOTE: Add more known and relevant variations/typos here if discovered
    }

    # Define the function to apply to each cell in the target columns
    def standardize_cell(cell_value):
        if pd.isna(cell_value):
            return pd.NA  # Return pandas NA for consistency

        # Split the potentially delimited string
        countries = [
            country.strip() for country in str(cell_value).split(LIST_DELIMITER)
        ]
        standardized_countries = set()  # Use a set for automatic deduplication

        for country in countries:
            # Apply replacements - use get to default to original if no replacement needed
            standardized_name = country_replacement_map.get(country, country)
            # Add the standardized name to the set (only if it's not empty/NA)
            if pd.notna(standardized_name) and standardized_name:
                standardized_countries.add(standardized_name)

        # Sort and rejoin the unique standardized names
        if standardized_countries:
            return LIST_DELIMITER.join(sorted(list(standardized_countries)))
        else:
            return pd.NA  # Return NA if the result is empty

    # Apply the function to specified columns
    for col in tqdm(
        columns_to_standardize,
        desc="Standardizing Countries",
        leave=False,
        disable=(not verbose),
    ):
        if col in df.columns:
            try:
                # Apply the cell-wise standardization
                df[col] = df[col].apply(standardize_cell).astype("string")
                processed_count += 1
            except Exception as e:
                if verbose:
                    print(
                        f"    Warning: Could not process column '{col}' for country standardization - {e}"
                    )

    if verbose:
        print(
            f"  Country name standardization attempted on {processed_count} applicable columns."
        )
    return df


def run_final_checks(df, verbose=VERBOSE_MODE):
    """Performs final sanity checks on the structured DataFrame."""
    if not verbose:
        return True
    print("\n--- Running Final Data Sanity Checks ---")
    ready_for_export = True
    issues_found = []
    critical_cols = ["Group ID", "Primary_Name", "Group_Type", "Regime"]
    null_check = df[critical_cols].isnull().sum()
    critical_nulls = null_check[null_check > 0]
    if not critical_nulls.empty:
        issues_found.append(
            f"FAIL: Nulls found in critical columns: {critical_nulls.to_dict()}"
        )
        ready_for_export = False
    else:
        print("  OK: No critical nulls.")
    if df.duplicated(subset=["Group ID"]).sum() > 0:
        issues_found.append("FAIL: Duplicate 'Group ID'.")
        ready_for_export = False
    else:
        print("  OK: 'Group ID' is unique.")
    if not pd.api.types.is_integer_dtype(df["Group ID"]):
        issues_found.append("FAIL: 'Group ID' not integer.")
        ready_for_export = False
    else:
        print("  OK: 'Group ID' is integer type.")
    date_cols_metadata = [
        "Listed_On",
        "UK_Sanctions_List_Date_Designated",
        "Last_Updated",
    ]
    types_ok = True
    for col in date_cols_metadata:
        if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
            issues_found.append(f"WARN: '{col}' not datetime.")
            types_ok = False
    if types_ok:
        print("  OK: Metadata dates are datetime type.")
    text_cols_check = ["Primary_Name", "Group_Type", "Regime"]
    empty_string_issues = {}
    for col in text_cols_check:
        if col in df.columns and df[col].dtype in ["object", "string"]:
            try:
                empty_count = (df[col].astype(str).str.strip() == "").sum()
            except Exception:
                empty_count = 0
            if empty_count > 0:
                empty_string_issues[col] = empty_count
    if empty_string_issues:
        issues_found.append(f"WARN: Empty strings found: {empty_string_issues}")
    else:
        print("  OK: No empty strings in key text columns.")
    expected_group_types = ["Individual", "Entity", "Ship"]
    actual_group_types = df["Group_Type"].dropna().unique()
    unexpected_types = [t for t in actual_group_types if t not in expected_group_types]
    if unexpected_types:
        issues_found.append(f"FAIL: Unexpected 'Group_Type': {unexpected_types}")
        ready_for_export = False
    else:
        print(f"  OK: 'Group_Type' values as expected.")
    print("\n--- Sanity Check Summary ---")
    df.info()
    if not issues_found:
        print("\n All checks passed.")
    else:
        print("\n Issues found:")
        [print(f"  - {issue}") for issue in issues_found]
    if not ready_for_export:
        print("  Recommendation: Address critical issues before using output.")
    print("------------------------------")
    return ready_for_export


# --- Main Processing Orchestration ---


def main(input_path=INPUT_CSV_PATH, output_path=OUTPUT_CSV_PATH, verbose=True):
    """Main function to orchestrate the processing pipeline."""
    global VERBOSE_MODE
    VERBOSE_MODE = verbose

    if verbose:
        print("Starting UK Sanctions List Processing Script...")
        print("=" * 50)
    if not os.path.exists(input_path):
        print(f"FATAL ERROR: Input file not found: '{input_path}'")
        sys.exit(1)

    tqdm.pandas(desc="Processing Rows", disable=(not verbose))

    try:
        # --- Pipeline Steps ---
        # 1. Load Data
        if verbose:
            print(f"\n[1/12] Loading data from: '{input_path}'...")
        uk_sanctions_raw = pd.read_csv(
            input_path, encoding=ENCODING, header=CSV_HEADER_ROW, low_memory=False
        )
        if verbose:
            print(f"  Loaded {len(uk_sanctions_raw)} rows.")

        # 2. DOB Processing
        if verbose:
            print("\n[2/12] Processing Date of Birth (DOB)...")
        if "DOB" not in uk_sanctions_raw.columns:
            raise ValueError("Required 'DOB' column not found.")
        uk_sanctions_raw["DOB_raw"] = uk_sanctions_raw["DOB"].astype("string")
        parsing_results = uk_sanctions_raw["DOB_raw"].progress_apply(
            parse_dob_comprehensive
        )
        temp_df = pd.DataFrame(
            parsing_results.tolist(),
            index=uk_sanctions_raw.index,
            columns=["DOB_parsed", "DOB_Precision"],
        )
        uk_sanctions_raw["DOB_parsed"] = pd.to_datetime(temp_df["DOB_parsed"])
        uk_sanctions_raw["DOB_Precision"] = temp_df["DOB_Precision"].astype("string")
        uk_sanctions_raw["DOB_year"] = uk_sanctions_raw["DOB_parsed"].dt.year.astype(
            "Int64"
        )
        uk_sanctions_raw.drop(columns=["DOB"], inplace=True)
        if verbose:
            print("  DOB processing complete.")

        # 3. Clean Country Fields
        if verbose:
            print("\n[3/12] Cleaning Country Fields...")
        country_cols = ["Country of Birth", "Nationality", "Country"]
        for col in country_cols:
            if col in uk_sanctions_raw.columns:
                uk_sanctions_raw[col] = (
                    uk_sanctions_raw[col]
                    .astype("string")
                    .progress_apply(clean_country_string)
                )
        if verbose:
            print("  Country field cleaning complete.")

        # 4. Construct Intermediate Names
        if verbose:
            print("\n[4/12] Constructing Full Names...")
        uk_sanctions_raw["Constructed_Name"] = uk_sanctions_raw.progress_apply(
            construct_full_name, axis=1
        )
        uk_sanctions_raw["Name_Non_Latin_Raw"] = uk_sanctions_raw[
            "Name Non-Latin Script"
        ].astype("string")
        if verbose:
            print("  Full name construction complete.")

        # 5. Aggregation by Group ID
        if verbose:
            print("\n[5/12] Aggregating data by Group ID...")
        grouped = uk_sanctions_raw.groupby("Group ID")
        structured_sanctions = grouped.progress_apply(aggregate_sanctions_data)
        structured_sanctions = structured_sanctions.reset_index()
        if verbose:
            print(
                f"  Aggregation complete. Result: {len(structured_sanctions)} unique entities."
            )

        # 6. Refine Columns
        if verbose:
            print("\n[6/12] Refining final columns...")
        if "DOB_Raw_Agg" in structured_sanctions.columns:
            structured_sanctions.drop(columns=["DOB_Raw_Agg"], inplace=True)
            if verbose:
                print("  Dropped 'DOB_Raw_Agg' column.")
        final_columns_order = [
            "Group ID",
            "Primary_Name",
            "Aliases",
            "Primary_Name_Non_Latin",
            "Aliases_Non_Latin",
            "Group_Type",
            "Regime",
            "DOB_Parsed_Agg",
            "DOB_Year_Agg",
            "DOB_Precision_Agg",
            "Countries_of_Birth",
            "Nationalities",
            "Countries_Address",
            "All_Associated_Countries",
            "Positions",
            "Passport_Numbers_Agg",
            "National_IDs_Agg",
            "Full_Address_Agg",
            "Listed_On",
            "UK_Sanctions_List_Date_Designated",
            "Last_Updated",
        ]
        final_columns_order_existing = [
            col for col in final_columns_order if col in structured_sanctions.columns
        ]
        structured_sanctions = structured_sanctions[final_columns_order_existing]
        if verbose:
            print("  Columns refined and reordered.")

        # 7. Post-Aggregation Cleaning: LRM Characters
        if verbose:
            print("\n[7/12] Performing post-aggregation cleaning (LRM)...")
        structured_sanctions = remove_lrm(structured_sanctions, verbose=verbose)

        # 8. Post-Aggregation Cleaning: Metadata Dates
        if verbose:
            print("\n[8/12] Performing post-aggregation cleaning (Dates)...")
        structured_sanctions = convert_metadata_dates(
            structured_sanctions, verbose=verbose
        )

        # 9. Post-Aggregation Cleaning: Whitespace
        if verbose:
            print("\n[9/12] Performing post-aggregation cleaning (Whitespace)...")
        structured_sanctions = clean_all_whitespace(
            structured_sanctions, verbose=verbose
        )

        # 10. Post-Aggregation Cleaning: Country Standardization
        if verbose:
            print("\n[10/12] Performing post-aggregation cleaning (Country Names)...")
        agg_country_cols = [
            "Countries_of_Birth",
            "Nationalities",
            "Countries_Address",
            "All_Associated_Countries",
        ]
        structured_sanctions = standardize_countries_in_agg_col(
            structured_sanctions, agg_country_cols, verbose=verbose
        )

        # 11. Final Checks
        if verbose:
            print("\n[11/12] Running final data quality checks...")
        is_ready = run_final_checks(structured_sanctions, verbose=verbose)
        if not is_ready and verbose:
            print(
                "\nWARNING: Final checks identified issues. Output generated but may require review."
            )

        # 12. Sorting and Export
        if verbose:
            print("\n[12/12] Sorting and Exporting...")  # Corrected step number
        structured_sanctions = structured_sanctions.sort_values(
            by="Group ID"
        ).reset_index(drop=True)
        structured_sanctions.to_csv(
            output_path, index=False, encoding=ENCODING, date_format="%Y-%m-%d"
        )
        if verbose:
            print(f"  Export successful to: '{output_path}'")

    except ValueError as ve:
        print(f"\nFATAL ERROR during processing: {ve}")
        sys.exit(1)
    except KeyError as ke:
        print(f"\nFATAL ERROR: Missing expected column: {ke}")
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL ERROR: An unexpected error occurred: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    if verbose:
        print("\n--- UK Sanctions List Processing Finished ---")
        print("=" * 50)
    return structured_sanctions


# --- Script Execution Guard ---
if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(
        description="Process the UK Financial Sanctions List CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,  # Shows defaults in help
    )
    parser.add_argument(
        "-i",
        "--input",
        default=INPUT_CSV_PATH,
        help="Path to the input CSV file (e.g., ConList.csv).",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=OUTPUT_CSV_PATH,
        help="Path to save the processed output CSV file.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true", 
        help="Run in quiet mode (suppress progress messages).",
    )

    args = parser.parse_args()  # Parse command-line arguments

    # --- Call Main Processing Function ---
    # Determine verbosity based on the --quiet flag
    is_verbose = not args.quiet

    print(
        f"Starting script with Input: '{args.input}', Output: '{args.output}', Verbose: {is_verbose}"
    )

    # Call the main function with potentially overridden paths and verbosity
    final_df = main(input_path=args.input, output_path=args.output, verbose=is_verbose)
