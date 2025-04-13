import csv
import os

import pyodbc

# --- Configuration ---
# Set the full path to your Access database file
db_file = r"C:\Users\jmedlin\Documents\GitHub\GLCS_Sermon_DB_Cleanup\data\JN Sermons Library_for JMM to edit.accdb"
# Output TSV file name
output_tsv_file = "tables.tsv"
# --- End Configuration ---

# Check if the database file exists
if not os.path.exists(db_file):
    print(f"Error: Database file not found at '{db_file}'")
    exit()

# Construct the connection string
# You might need to adjust the driver name based on your installed version
# Common drivers:
# 'Microsoft Access Driver (*.mdb, *.accdb)'
# 'Microsoft Access Driver (*.mdb)'
conn_str = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" r"DBQ=" + db_file + ";"

table_names = []
conn = None  # Initialize conn to None
cursor = None  # Initialize cursor to None

print(f"Connecting to database: {db_file}")

try:
    # Establish the database connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    print("Successfully connected. Fetching table names...")

    # Fetch table names (excluding system tables starting with 'MSys')
    for row in cursor.tables(tableType="TABLE"):
        table_name = row.table_name
        if not table_name.startswith("MSys"):
            table_names.append(table_name)

    print(f"Found {len(table_names)} user tables.")

    # Write table names to TSV file with additional "ID", "Active", "Event", and "Series" columns
    print(f"Writing table names to {output_tsv_file}...")
    with open(output_tsv_file, "w", newline="", encoding="utf-8") as tsvfile:
        writer = csv.writer(tsvfile, delimiter="\t")  # Use tab as the delimiter
        writer.writerow(["ID", "Table Name", "Active", "Event", "Series"])  # Write header with additional columns
        for idx, table in enumerate(sorted(table_names), start=1):  # Sort names alphabetically and add ID
            writer.writerow(
                [idx, table, 1, 0, 0]
            )  # Set "Active" to 1, "Event" to 0, and "Series" to 0 for all rows

    print("Successfully wrote table names to TSV.")

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    print(f"Error connecting to database or fetching tables.")
    print(f"SQLSTATE: {sqlstate}")
    print(f"Message: {ex}")
    if "IM002" in sqlstate:
        print("This error often means the ODBC driver is not installed or not found.")
        print(
            "Ensure the Microsoft Access Database Engine Redistributable is installed (32-bit or 64-bit matching your Python)."
        )


finally:
    # Ensure the connection is closed
    if cursor:
        cursor.close()
        print("Cursor closed.")
    if conn:
        conn.close()
        print("Database connection closed.")
