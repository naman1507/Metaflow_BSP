###################################################################################################################
####################### the code extracts the sensor name from excel file #########################################
#######################  and stores it in json file with weight as 1  #############################################
###################################################################################################################



import pandas as pd
import json

# Read the Excel file
df = pd.read_excel('/data2/naman/metaflow/Finalised_region_combined_tagnames.xlsx')

# Convert the dataframe to a list of dictionaries
sensor_data = []
for index, row in df.iterrows():
    sensor_data.append({
        'sensor': str(row['Sensor_ID']) + '_' + row['Tagnames'],
        'weight': 1
    })

# Write the list of dictionaries to a JSON file
with open('sensor_data_weights.json', 'w') as json_file:
    json.dump(sensor_data, json_file, indent=4)
