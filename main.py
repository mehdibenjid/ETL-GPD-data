import sqlite3
from extract_transform_load.ETL import *
from constants.constants import *

# log the start of the ETL process
log_progress("Premilinaries completed. Starting ETL process...")

# extract data
df = extract(URL, TABLE_ATTRIBS)

# log the completion of the extraction process
log_progress("Extraction completed, Initiating transformation process...")

# transform data
df = transform(df)

# log the completion of the transformation process
log_progress("Transformation completed, Initiating loading process...")

# load data to csv
load_to_csv(df, CVS_PATH)

# log the save to csv completion
log_progress("Loading to csv completed, Initiating loading to database...")

# create a connection to the database
conn = sqlite3.connect(DB_NAME)

# load data to database
load_to_db(df, conn, TABLE_NAME)

# log the save to database completion
log_progress("Loading to database completed, Initiating query...")

# run a query on the database
query_statement = f"SELECT * from {TABLE_NAME} WHERE GDP_USD_billions >= 100"
run_query(query_statement, conn)

# log the completion of the ETL process
log_progress("ETL process completed.")

# close the connection to the database
conn.close()
