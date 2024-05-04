###################################################################################################################
####################### file to call the main loop.py file which contains metaflow code ###########################
#### this file reads data from the POSTGRESQL and triggers a function call for each minute of data retrieved ######
###################################################################################################################



import pandas as pd
import subprocess
import time
import os
from sqlalchemy import create_engine, text

# Set Metaflow environment variables
os.environ["METAFLOW_SERVICE_URL"] = "http://localhost:8080"
os.environ["METAFLOW_DEFAULT_METADATA"] = "service"

"""
# Read your data into a DataFrame (replace this with your actual data loading logic)
from datetime import datetime
d_parser = lambda x : datetime.strptime(x,"%d.%m.%Y %H:%M:%S.%f")
# Read your data into a DataFrame (replace this with your actual data loading logic)
df = pd.read_csv('/data2/naman/metaflow/00.24.56_01.24.56.txt', sep='\t', parse_dates=['Time'], date_parser=d_parser)
print(df.head())
#df.set_index('Time', inplace=True)
tagnames_df = pd.read_excel("/data2/naman/metaflow/Finalised_region_combined_tagnames.xlsx")
tagnames_list = ["Time"] 
sensors = list(tagnames_df["Sensor_ID"])   
for i in sensors:
    tagnames_list.append(i)
print(tagnames_list)       
df = df[tagnames_list]
df = df.loc[:,~df.columns.duplicated()]
#df = df.iloc[:,:5]
print(df.head())
#print(df.dtypes)
"""

# Map Pandas data types to SQL data types
sql_data_types = {
    'float64': 'FLOAT',
    'int64': 'BIGINT',
    'datetime64[ns]': 'TIMESTAMP'
}


# Define PostgreSQL connection parameters
conn_str = 'postgresql://postgres:naman123@127.0.0.1:5432/bsp'

# Create a SQLAlchemy engine
engine = create_engine(conn_str)

# Define the table name
table_name = 'profile_16mm'

#code to create table, schema and insert data into PostgreSql DB
"""
# Generate the SQL table schema based on DataFrame columns
schema = ','.join([f'"{col}" {sql_data_types.get(str(df[col].dtype))}' for col in df.columns])

print(schema)
# Create the table
try:
    print("Table creation starting....")
    with engine.connect() as conn:
        conn.execute(text(f'CREATE TABLE IF NOT EXISTS {table_name} ({schema})'))

    print("Table creation finished....")
except Exception as e:
    print("An error occurred:", e)


# Insert data from DataFrame into PostgreSQL table
df.to_sql(table_name, engine, if_exists='append', index=False)
"""


# Define a function to call the Metaflow workflow for a batch of data
def call_workflow(batch_df):
    print("Calling subprocess...")
    try:
        subprocess.run(["python", "/data2/naman/metaflow/loop.py", "run", "--data_path", batch_df])
    except subprocess.CalledProcessError as e:
        print("Command failed with exit code:", e.returncode)
    print("Subprocess completed.")

last_timestamp = "2024-02-01 00:24:56.53"
query = f'SELECT * FROM {table_name} WHERE "Time" > %s ORDER BY "Time" LIMIT 6000'

# Process the DataFrame in batches of 6000 rows
start_time = time.time()
count = 0
while True:
    # Fetch data from PostgreSQL
    df = pd.read_sql_query(query, engine, params=(last_timestamp,))

    if 'index' in df.columns:
        df.drop(['index'], axis=1, inplace=True)
    
    # If there is no new data, wait for some time before trying again
    if len(df) == 0:
        print("No new data found. Waiting for 1 minute...")
        time.sleep(60)
        count=count+1
        if count==2:
            break
        continue
    
    # Update the last timestamp processed
    last_timestamp = df['Time'].max()
    df.set_index('Time', inplace=True)
    print("In while loop...", df.head())

    
    # Process the DataFrame in batches of 6000 rows
    if len(df) == 6000:
        # Write the batch data to a temporary CSV file
        file_path = 'temp_data.csv'
        df.to_csv(file_path, index=True)
        
        # Call the workflow for the batch
        call_workflow(file_path)
        
    
    # If there are remaining rows, process them as well
    elif len(df) < 6000:
        print("Last Iteration, Processing remaining rows...")
        # Write the remaining data to a temporary CSV file
        df.to_csv('temp_data.csv', index=False)
        
        # Call the workflow for the remaining data
        call_workflow('temp_data.csv')
        break


end_time = time.time()
total_time = end_time - start_time
print(f"Total time taken: {total_time} seconds")
