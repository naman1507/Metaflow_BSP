# Metaflow_BSP
# Real-Time Sensor Data Processing and Inference

This repository contains Python scripts for real-time sensor data processing and inference. Below is an overview of the key scripts included in this repository:

## Scripts:

1. **convert_sensor_weights_to_json.py**:
   - This script extracts sensor names from an Excel file and stores them in a JSON file with a default weight of 1.

2. **metaflow_inference.py**:
   - Performs inference on sensor data.
   - All preprocessing is conducted in a single step.
   - Data is pushed to the database for loss and cobble steps in chunks of 30 rows.
   - Designed for 1-second granularity.
   - Database ingestion is executed using Point (No Batching).

3. **mf_inference_enhanced_db.py**:
   - Conducts inference on sensor data.
   - All preprocessing is conducted in a single step.
   - Data is pushed to the database for loss and cobble steps in chunks of 6000 rows.
   - Batching is implemented for data ingestion with a batch size of 5000 points.

4. **loop_main.py**:
   - Calls the main `loop.py` file which contains Metaflow code.
   - Reads data from a CSV file and triggers a function call for each minute of data retrieved.

5. **loop_main_with_db.py**:
   - Calls the main `loop.py` file which contains Metaflow code.
   - Reads data from a PostgreSQL database and triggers a function call for each minute of data retrieved.

6. **loop.py**:
   - Contains Metaflow code for data cleaning, prediction, and InfluxDB ingestion.

## Usage:
1. To run the metaflow code use:
   python3 loop_main_with_db.py
   
