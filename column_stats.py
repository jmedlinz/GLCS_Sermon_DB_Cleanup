"""
This script reads the headers TSV file, counts the occurrences of each unique New_Column_Name,
and reports them in descending order of frequency.
"""

import csv
import os
from collections import Counter

from config import (
    APP_ENVIRONMENT,
    APP_NAME,
    COMPUTERNAME,
    HEADERS_TSV_PATH,
    USERNAME,
)
from report import (
    report_comment,
    report_error,
    report_header,
    report_info,
    report_section,
    report_subsection,
)

report_header(APP_NAME, COMPUTERNAME, APP_ENVIRONMENT, USERNAME)

report_section("Column Name Statistics")

# Check if the headers file exists
if not os.path.exists(HEADERS_TSV_PATH):
    report_error(f"Error: Headers file not found at '{HEADERS_TSV_PATH}'")
    exit(1)

# Read the headers file and count column names
report_subsection("Reading headers file")
column_counter = Counter()
total_columns = 0
# Track which tables each column appears in
column_tables = {}

try:
    with open(HEADERS_TSV_PATH, encoding="utf-8") as tsv_file:
        reader = csv.reader(tsv_file, delimiter="\t")
        next(reader)  # Skip header row

        # Create a dictionary to track columns by table
        table_columns = {}

        for row in reader:
            if len(row) >= 6:  # Ensure the row has all needed columns
                table_id = row[0]
                table_name = row[1]
                column_name = row[4]  # Column_Name is in the 5th column
                new_column_name = row[5]  # New_Column_Name is in the 6th column

                # Track columns by table
                if table_name not in table_columns:
                    table_columns[table_name] = set()
                table_columns[table_name].add(column_name)

                # Track tables by column name
                if new_column_name not in column_tables:
                    column_tables[new_column_name] = set()
                column_tables[new_column_name].add(table_name)

                column_counter[new_column_name] += 1
                total_columns += 1

    report_info(f"Processed {total_columns} total columns")
    report_info(f"Found {len(column_counter)} unique column names")
    report_info(f"Found {len(table_columns)} distinct tables")

    # Report the column statistics
    report_subsection("Column Name Frequency (Sorted by Count)")

    # Get sorted items (most frequent first)
    sorted_columns = column_counter.most_common()

    # Print table header
    report_info(f"{'Column Name':<50} | {'Count':<10}")
    report_info("-" * 64)

    # Print each column with its count
    for column_name, count in sorted_columns:
        # Base display without percentage
        report_info(f"{column_name:<50} | {count:<10}")

        # For counts of 5 or less, show which tables have this column
        if count <= 5:
            tables_with_column = sorted(column_tables[column_name])
            report_comment(f"   Found in tables: {', '.join(tables_with_column)}")

    # Define essential columns
    essential_columns = ["Date", "Title", "Text"]

    # Report tables missing essential columns
    report_subsection(f"Tables Missing Essential Columns ({', '.join(essential_columns)})")
    missing_essential = []

    for table_name, columns in table_columns.items():
        missing = [col for col in essential_columns if col not in columns]
        if missing:
            missing_essential.append((table_name, missing))

    if missing_essential:
        report_info(f"Found {len(missing_essential)} tables missing essential columns:")
        for table_name, missing in sorted(missing_essential):
            report_comment(f"   Table: {table_name} - Missing: {', '.join(missing)}")
    else:
        report_info("All tables contain the essential columns (Date, Title, Text)")

except Exception as ex:
    report_error(f"Error processing headers file: {ex}")
    exit(1)
