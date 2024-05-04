###################################################################################################################
####################### file to call the main loop.py file which contains metaflow code ###########################
#### this file reads data from the csv file and triggers a function call for each minute of data retrieved ########
###################################################################################################################





import pandas as pd
import subprocess
import time
import os

# Set Metaflow environment variables
os.environ["METAFLOW_SERVICE_URL"] = "http://localhost:8080"
os.environ["METAFLOW_DEFAULT_METADATA"] = "service"

# Read your data into a DataFrame (replace this with your actual data loading logic)
from datetime import datetime
d_parser = lambda x : datetime.strptime(x,"%d.%m.%Y %H:%M:%S.%f")
# Read your data into a DataFrame (replace this with your actual data loading logic)
df = pd.read_csv('/data2/naman/metaflow/00.24.56_01.24.56.txt', sep='\t', parse_dates=['Time'], date_parser=d_parser)
#df.set_index('Time', inplace=True)
print(df.head())


# Define a function to call the Metaflow workflow for a batch of data
def call_workflow(batch_df):
    print("Calling subprocess...")
    try:
        subprocess.run(["python", "/data2/naman/metaflow/loop.py", "run", "--data_path", batch_df])
    except subprocess.CalledProcessError as e:
        print("Command failed with exit code:", e.returncode)
    print("Subprocess completed.")

# Process the DataFrame in batches of 6000 rows
start_time = time.time()
while len(df) >= 6000:
    # Extract a batch of 6000 rows
    batch_df = df[:6000]
    
    # Write the batch data to a temporary CSV file
    file_path = 'temp_data.csv'
    batch_df.to_csv(file_path, index=True)
    
    # Call the workflow for the batch
    call_workflow(file_path)
    
    # Drop the processed rows from the DataFrame
    df = df[6000:]

# If there are remaining rows, process them as well
if len(df) > 0:
    print("Processing remaining rows...")
    # Write the remaining data to a temporary CSV file
    df.to_csv('temp_data.csv', index=False)
    
    # Call the workflow for the remaining data
    call_workflow('temp_data.csv')

end_time = time.time()
total_time = end_time - start_time
print(f"Total time taken: {total_time} seconds")
