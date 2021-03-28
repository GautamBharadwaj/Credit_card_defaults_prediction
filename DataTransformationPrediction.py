from datetime import datetime
import pandas
from AzureManagement import AzureManagement
from App_logging import App_LoggerDB
from db_operation import MongoDBOperation
class dataTransformPredict:

     def __init__(self,execution_id):
          self.execution_id=execution_id

          self.goodDataPath="good-raw-file-prediction-validated"
          self.log_db_writer=App_LoggerDB(execution_id=execution_id)
          self.log_database="cred_prediction_log"
          self.az_blob_mgt=AzureManagement()

     def replaceMissingWithNull(self,Column_Names):

          try:
               log_collection="data_transform_log"

               onlyfiles=self.az_blob_mgt.get_list_of_blobs_in_container(self.goodDataPath)
               for file in onlyfiles:
                    csv=self.az_blob_mgt.get_csv_file(self.goodDataPath,file)
                    csv = csv[Column_Names]
                    csv.fillna('NULL',inplace=True)

                    self.az_blob_mgt.saveDataFrameTocsv(self.goodDataPath,file,csv)

                    self.log_db_writer.log(self.log_database,log_collection,"File {0} transformed successfully".format(file))


          except Exception as e:
               self.log_db_writer.log(self.log_database,log_collection,'Data Transformation failed because:'+str(e))
               raise e
