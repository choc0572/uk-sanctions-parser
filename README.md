# UK Sanctions List Data Preparation Script

This Python script processes the raw CSV version of the UK Government's Consolidated List of Financial Sanctions Targets. It cleanses, standardizes, and transforms the data into a structured format where each row represents a single, unique sanctioned entity (identified by `Group ID`).

The primary goal is to create a dataset that is significantly easier to use for analysis and integration purposes, such as comparing the sanctions list against internal organisational records (e.g., customer or supplier lists).

## Data Source

The script uses the publicly available data from the UK Government's Office of Financial Sanctions Implementation (OFSI):

- **Source:** Consolidated List of Financial Sanctions Targets
- **Link:** [https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets/consolidated-list-of-targets](https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets/consolidated-list-of-targets)
- **Format Used:** CSV (`ConList.csv`)

**What the list means:** This official list contains individuals, entities (like companies or organisations), and ships designated by the UK government as being subject to financial sanctions. This typically involves asset freezes and restrictions on providing funds or economic resources, aimed at counter-terrorism, counter-proliferation, or upholding international law.

## Why This Script is Useful

The raw data file from the government website, while comprehensive, presents challenges for direct use:

1.  **Denormalized Structure:** Multiple rows often represent the same entity (e.g., primary name plus several aliases), making direct comparison difficult.
2.  **Inconsistent Data:** Fields like Date of Birth and Country names can have various formats, typos, or prefixes that hinder automated processing.
3.  **Whitespace & Formatting:** Includes extraneous whitespace or non-standard characters (like Unicode LRM) that can interfere with data matching.

This script addresses these issues by:

- **Aggregating Data:** Consolidating all information related to a unique `Group ID` into a single row.
- **Standardizing Names:** Identifying a primary name and collecting all other variations into a delimited `Aliases` field.
- **Cleaning Dates:** Parsing various Date of Birth formats (`DD/MM/YYYY`, `00/MM/YYYY`, `00/00/YYYY`) into standardized representations (`YYYY-MM-DD` strings in the aggregated output) and tracking the original data's precision. Metadata dates are converted to datetime objects.
- **Standardizing Countries:** Applying basic cleaning and specific mappings to country names found across different fields (`Country of Birth`, `Nationality`, `Country`) before and after aggregation for better consistency.
- **General Cleaning:** Removing invisible control characters (LRM) and trimming excess whitespace from text fields.

The resulting `Structured_Sanctions_Data.csv` file provides a cleaner, entity-centric view optimized for reliable automated screening and analysis.

## Requirements

- Python 3.8+
- Required Python packages (install via `pip install -r requirements.txt`):
  - pandas
  - tqdm

## Usage

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/choc0572/uk-sanctions-parser.git
    cd uk-sanctions-parser
    ```
2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Download Data:** Obtain the latest `ConList.csv` file from the official [UK Government source link](https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets/consolidated-list-of-targets) and place it in the same directory as the script (`uk-sanctions-parser`), or specify its path using the `-i` argument.
4.  **Run the script:**

    - **Using Defaults:** This will look for `ConList.csv` in the current directory and save the output as `Structured_Sanctions_Data.csv`.
      ```bash
      python process_sanctions.py
      ```
    - **Specifying Input/Output:** Use flags to provide different file paths.
      ```bash
      python process_sanctions.py --input path/to/source/ConList.csv --output path/to/save/processed_list.csv
      ```
      _(You can also use `-i` for input and `-o` for output)_
    - **Quiet Mode:** Run without progress messages.
      ```bash
      python process_sanctions.py --quiet
      ```
      _(You can also use `-q`)_
    - **Help:** Show available command-line options and their defaults.
      ```bash
      python process_sanctions.py --help
      ```
      _(You can also use `-h`)_

## Output File

The script generates a CSV file (default: `Structured_Sanctions_Data.csv`) encoded in UTF-8 with the following key characteristics:

- **One Row Per Entity:** Each row corresponds to a unique `Group ID` from the source list.
- **Aggregated Data:** Fields like Aliases, DOBs (Parsed, Year, Precision), Countries, Positions, and Identifiers contain unique values aggregated from all source rows for that `Group ID`, joined by `"; "`.
- **Standardized Fields:** Includes cleaned primary names, standardized country names (where possible), and parsed/formatted dates.
- **Key Columns:** `Group ID`, `Primary_Name`, `Aliases`, `Group_Type`, `DOB_Parsed_Agg` (YYYY-MM-DD format), `DOB_Year_Agg`, `DOB_Precision_Agg`, `All_Associated_Countries`, `Listed_On`, `Last_Updated`, etc.

This structured output is designed for easier loading into databases or use in automated comparison processes.
