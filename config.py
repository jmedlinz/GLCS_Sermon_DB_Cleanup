import os

APP_NAME = "GLCS Sermon DB Cleanup"

# *** Define the environment and determine if we're on the Dev, Test, or Prod environment
DEV_ENV = "DEV"
PROD_ENV = "PROD"
TEST_ENV = "TEST"

APP_ENVIRONMENT = PROD_ENV

# *** Env info
USERNAME = os.getenv("USERNAME")
COMPUTERNAME = os.getenv("COMPUTERNAME")

# --- Configuration ---
# Set the folder where your input/output files are located
DATA_DIR = r"C:\Users\jmedlin\Documents\GitHub\GLCS_Sermon_DB_Cleanup\data"
# Set the full path to your Access database file
SOURCE_DB_FILE = "JN Sermons Library_for JMM to edit.accdb"
SOURCE_DB_PATH = os.path.join(DATA_DIR, SOURCE_DB_FILE)

MASTER_TSV_FILE = "master_table.tsv"
MASTER_TSV_PATH = os.path.join(DATA_DIR, MASTER_TSV_FILE)
