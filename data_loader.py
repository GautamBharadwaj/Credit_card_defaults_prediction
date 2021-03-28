import pandas as pd

from App_logging import App_LoggerDB
from AzureManagement import AzureManagement

class Data_Getter:

    def __init__(self,log_database,log_collection,execution_id):
        self.log_database=log_database
        self.log_collection=log_collection
        self.training_directory="training-file-from-db"
        self.filename="InputFile.csv"
        self.log_db_writer=App_LoggerDB(execution_id=execution_id)
        self.az_blob_mgt=AzureManagement()


    def get_data(self):

        self.log_db_writer.log(self.log_database,self.log_collection,"Entered the get_data method of the Data_Getter class")
        try:

            self.data=self.az_blob_mgt.get_csv_file(self.training_directory,self.filename)
            print("my name is gautam")
            print(self.data)
            self.log_db_writer.log(self.log_database,self.log_collection,"Data Load Successful.Exited the get_data method of the Data_Getter class")
            return self.data
        except Exception as e:

            self.log_db_writer.log(self.log_database,self.log_collection,"Exception occured in get_data method of the Data_Getter class. Exception message:Data Load Unsuccessful.Exited the get_data method of the Data_Getter class ")
            raise Exception()


