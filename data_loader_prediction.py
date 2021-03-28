import pandas as pd
from AzureManagement import AzureManagement
from App_logging import App_LoggerDB
class Data_Getter_Pred:

    def __init__(self,log_database,log_collection,execution_id):
        self.log_database=log_database
        self.log_collection=log_collection
        self.prediction_directory="prediction-file-from-db"
        self.filename="InputFile.csv"
        self.log_db_writer=App_LoggerDB(execution_id=execution_id)
        self.az_blob_mgt=AzureManagement()


    def get_data(self):


        self.log_db_writer.log(self.log_database, self.log_collection,"Entered the get_data method of the Data_Getter class")
        try:

            self.data = self.az_blob_mgt.get_csv_file(self.prediction_directory, self.filename)
            self.log_db_writer.log(self.log_database, self.log_collection,"Data Load Successful.Exited the get_data method of the Data_Getter class")
            return self.data
        except Exception as e:

            self.log_db_writer.log(self.log_database, self.log_collection,"Exception occured in get_data method of the Data_Getter class. Exception message:Data Load Unsuccessful.Exited the get_data method of the Data_Getter class ")
            raise Exception()



