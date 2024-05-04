########################################################################################
#-----This code is to do inference where all the preprocessing is done at one go,------# 
#-----only push to database of loss and cobble step is done in chunk of 6000 rows------#
#-----Batching is done for data ingestion (batch_size of 5000 points)------------------#
########################################################################################


from metaflow import FlowSpec, step, retry, Parameter
import logging
import pandas as pd
import numpy as np 
import pickle
import json
from datetime import datetime
from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient, WriteOptions



logging.basicConfig(filename='metalog.log', level=logging.INFO)
logger = logging.getLogger(__name__)

org = "iit_bh"
token = "2Urm4kdKYxcqKRDtEewhbunw-AWmjbqaRdT7-cJVJJt2Fdu1oU52oaYg6Myz6R_FJlZQiq91GXsWr3caI8XZYw=="
url = "http://127.0.0.1:8086"
bucket = "metaflow_test_milliseconds"

client = InfluxDBClient(url=url, token=token, timeout=25000)
wo = WriteOptions(batch_size=5000)


class LinearFlow(FlowSpec):

    config = Parameter('config', help = "Name of profile configuration file", default = "config_file_20mm")
    
    

    def push_to_influx(self, df, measurement_name):
        try:
            with client.write_api(write_options=wo) as write_client:
                write_client.write(bucket=bucket, org=org, record=df,
                    #data_frame_tag_columns=['tag']
                    data_frame_measurement_name=measurement_name
                    )

        except Exception as e:
                logger.error(measurement_name)
                logger.error(f"An error occurred while pushing data to InfluxDB: {e}")

    
    @step
    def start(self):
        try:
            #input_file_path json contains current directory info, input data file info and sensor_id tagname mapping excel file
            with open("input_file_path.json", 'r') as file:
                self.data= json.load(file)
                self.main_directory_path = self.data["main_directory_path"]
                self.input_data_file_path = self.data["input_file_path"]
                self.sensor_id_tagnames_mapping = self.data["sensor_id_tagnames_mapping"]

            #json configuration file containing weights of individual signals
            with open("sensor_data_weights.json", 'r') as file:
                sensor_weights = json.load(file)

            self.sensor_weights_df = pd.DataFrame(sensor_weights)
            self.sensor_weights_df = self.sensor_weights_df.set_index('sensor')
            #print(self.sensor_weights_df)



            if self.input_data_file_path.split(".")[-1] == "pkl":
                self.df = pd.read_pickle(self.input_data_file_path)

            elif self.input_data_file_path.split(".")[-1] == "txt":
                from datetime import datetime
                d_parser = lambda x : datetime.strptime(x,"%d.%m.%Y %H:%M:%S.%f")
                self.df = pd.read_csv(self.input_data_file_path, sep='\t', parse_dates=['Time'], date_parser=d_parser)
                self.df.set_index('Time', inplace=True)
            #self.df = self.df.head(500)
            print(self.df.head())
            
        
        except Exception as e:
            print(f"An error occurred in start step: {e}")
            logger.error(f"An error occurred in start step: {e}")
        
        self.next(self.filter_columns)
            
        
    @step
    def filter_columns(self):
        try:
            self.tagnames_df = pd.read_excel(self.sensor_id_tagnames_mapping)
            #self.df.index = pd.to_datetime(self.df.index.astype(str).str.split('.').str[0]) 
            #filtered_columns = self.df.columns[tagnames_df.loc[:, "Sensor_ID"]]
            self.df = self.df[list(self.tagnames_df["Sensor_ID"])]

        except Exception as e:
            logger.error(f"An error occurred in filter_columns step: {e}")

        self.next(self.read_config_file_and_call_for_each_region)
            

    @step
    def read_config_file_and_call_for_each_region(self): 
        try:
            #print("In read_config_file_and_call_for_each_region....")  
            #print(self.config) 

            #read the jsonfile for regions information
            with open(self.main_directory_path+self.config, 'r') as file:
                self.regions_info = json.load(file)
        
        except Exception as e:
            logger.error(f"An error occurred in read_config_file_and_call_for_each_region step: {e}")

        self.next(self.read_data_for_each_regions, foreach='regions_info')
           

    @step
    def read_data_for_each_regions(self):
        try:
            self.region_info = self.input
            self.region_name = self.region_info['name']
            self.model_path = self.region_info['model_path']
            self.scaling_path = self.region_info['scaling_path']
            self.columns_path = self.region_info['columns_path']
            self.threshold = self.region_info['threshold']
            self.max_td = self.region_info['max_td']
            self.k = self.region_info['k']
            
            print("Region_Name: ", self.region_name)
            #print("Model Path", self.model_path)
            #print("Scaler Path", self.scaling_path)
            #print("Columns path", self.columns_path)
            #print("Region Threshold", self.threshold)
            #print("Time Difference", self.max_td)
            #print("Count k", self.k)
            #print(self.config)
            
        
        except Exception as e:
            logger.error(f"The region is: {self.region_name}")
            logger.error(f"An error occurred in read_data_for_each_regions step: {e}")

        self.next(self.preprocess_data_for_each_regions)
            

    @step
    def preprocess_data_for_each_regions(self):
        try:
            #print(self.region_name)
            with open(self.columns_path, 'rb') as file:
                column_names = pickle.load(file)

            #print(column_names)

            self.df1 = self.df[column_names]
            self.df1 = self.df1.loc[:, ~self.df1.columns.duplicated()]

            #print("column names are as follows:..", self.df1.columns)

            merged_df = pd.merge(self.df1.T, self.tagnames_df, left_index=True, right_on="Sensor_ID", how="left")
            merged_df['Tagnames'] = merged_df['Tagnames'].fillna("Missing_TagName")
            column_mapping = dict(zip(merged_df["Sensor_ID"], merged_df["Sensor_ID"]+'_'+merged_df["Tagnames"]))
            self.df1.rename(columns=column_mapping, inplace=True)
            #print(len(self.df1.columns))

            self.df1.interpolate(axis=0, inplace=True)
            self.df1.dropna(axis=0, inplace=True)

            #print("Input df file:", self.df1)

            with open(self.scaling_path, 'rb') as file:
                scaler = pickle.load(file)
                #print("Printing Scaler... ", scaler)
            
            x=self.df1.values
            x_scaled = scaler.transform(x)

            self.df_normalized = pd.DataFrame(x_scaled, index=self.df1.index, columns= self.df1.columns)
            #print("After applying scaling and normalization:.............")
            #print("The normalized df is:", self.df_normalized)

        except Exception as e:
            print(self.region_name, e)
            logger.error(f"The region is: {self.region_name}")
            logger.error(f"An error occurred in preprocess_data_for_each_regions step: {e}")

        self.next(self.implement_pca)
            

    @step
    def implement_pca(self):
        try:
            print("Shape of Normalized df...", self.df_normalized.shape)
            #print(self.df_normalized)
            with open(self.model_path, 'rb') as file:
                loaded_model = pickle.load(file)
                #print(loaded_model)

            self.x_test_pca = pd.DataFrame(loaded_model.transform(self.df_normalized.values), index=self.df1.index)
            print("Shape of transformed Normalized df...", self.x_test_pca.shape)
            #print("Printing x_test_pca", self.x_test_pca)

            self.df_restored = pd.DataFrame(loaded_model.inverse_transform(self.x_test_pca), index=self.df_normalized.index, columns= self.df_normalized.columns)
            
            print("Shape of reconstructed df...",self.df_restored.shape)
            #print("The restored df is:", self.df_restored)
        
        except Exception as e:
            print(e)
            logger.error(f"The region is: {self.region_name}")
            logger.error(f"An error occurred in implement_pca step: {e}")

        self.next(self.get_anomaly_scores)
            

    @step
    def get_anomaly_scores(self):
        try:
            loss = (np.sum(np.abs(np.array(self.df_normalized) -np.array(self.df_restored)), axis=1))/len(self.df_normalized.columns)
            self.anomaly_score_df = self.df1
            self.anomaly_score_df["anomaly_score"] = loss
            self.anomaly_score_df["predicted_cobble"] = 0
        
        except Exception as e:
            logger.error(f"The region is: {self.region_name}")
            logger.error(f"An error occurred in get_anomaly_scores step: {e}")

        self.next(self.find_cobble_intervals, self.get_loss_per_signal)
        

    @retry
    @step
    def get_loss_per_signal(self):
        try:
            loss = abs((np.array(self.df_normalized) - np.array(self.df_restored)))
            loss_per_signal_df = pd.DataFrame(loss, index=self.df_normalized.index, columns=self.df_normalized.columns)
            
            #print("In loss per signal function.......")

            #print("before applying the weight logic.....")
            #print(loss_per_signal_df.head())

            # Get the column names (sensor names) from loss_per_signal_df
            relevant_columns = loss_per_signal_df.columns

            # Filter the sensor_weights_df to include only rows corresponding to these sensor names
            relevant_weights = self.sensor_weights_df[self.sensor_weights_df.index.isin(relevant_columns)]
            #print("relevant_weights", relevant_weights.shape)
            result_dict = {}
            for index, row in relevant_weights.iterrows():
                result_dict[index] = row['weight']

            chunk_size = 6000
            num_chunks = len(loss_per_signal_df) // chunk_size

            for i in range(num_chunks + 1):
                start_index = i * chunk_size
                end_index = min((i + 1) * chunk_size, len(loss_per_signal_df))
                chunk_df = loss_per_signal_df.iloc[start_index:end_index]
                chunk_df = chunk_df.mul(result_dict)
                #print("chunk_df.......", chunk_df)

                self.push_to_influx(chunk_df, f"{self.region_name}_loss_per_signal")
        
        except Exception as e:
            logger.error(f"The region is: {self.region_name}")
            print("Exception in get loss per signal...", e)
            logger.error("Step Name: get_loss_per_signal")
            logger.error(f"An error occurred in get_loss_per_signal step: {e}")

        self.next(self.join1)
          
    @retry
    @step
    def find_cobble_intervals(self):
        try:
            self.start_time = None
            self.curr_time = None
            self.count = 0
            chunk_size = 6000
            chunk_buffer = pd.DataFrame(columns=self.anomaly_score_df.columns)
            #print("The length of anomaly score df is ...", len(self.anomaly_score_df.columns))

            for idx,row in self.anomaly_score_df.iterrows():
                #print("idx....", idx)
                #print("length of row is..", len(row))
                #print("row......", row)
                if row["anomaly_score"] > self.threshold:
                    if self.start_time and (idx-self.curr_time).seconds <= self.max_td:
                        self.count += 1
                        if self.count >= self.k:
                            #print(self.region_name, idx)
                            #self.anomaly_score_df.at[idx,"predicted_cobble"] = 1
                            row["predicted_cobble"] = 1
                        self.curr_time = idx
                    else:
                        self.start_time = idx
                        self.curr_time = idx
                        self.count = 1
                row_values = row.tolist()
                #print("row_values......", row_values)
                row_df = pd.DataFrame([row_values], columns=chunk_buffer.columns, index=[idx])
                chunk_buffer = pd.concat([chunk_buffer, row_df])
                #print("chunk_buffer....", chunk_buffer)

                if len(chunk_buffer) == chunk_size:
                    self.push_to_influx(chunk_buffer, f"{self.region_name}_anomaly_score_cobble")
                    chunk_buffer = pd.DataFrame(columns=self.anomaly_score_df.columns)
        
        except Exception as e:
            logger.error(f"The region is: {self.region_name}")
            print("Exception in find_cobble_intervals...", e)
            logger.error("Step Name: find_cobble_intervals")
            logger.error(f"An error occurred in find_cobble_intervals step: {e}")

        self.next(self.join1)
           

    @step
    def join1(self,inputs):
        print("In join1....")
        print(inputs)
        self.next(self.join)
        

    @step
    def join(self, inputs):
        print("Joining..........")
        self.next(self.end)
        
        
    @step
    def end(self):
        print("Ending......")

if __name__ == "__main__":
    start_time = datetime.now()
    LinearFlow()
    print("Completed LinearFlow.......")
    end_time = datetime.now()
    print("Time Taken ..", end_time-start_time)


       
