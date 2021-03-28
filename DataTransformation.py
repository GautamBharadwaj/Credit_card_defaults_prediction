from App_logging import App_LoggerDB
from AzureManagement import AzureManagement

class dataTransform:
     def __init__(self,execution_id):

          self.goodDataPath="good-raw-file-train-validated"
          self.execution_id=execution_id

          self.logger_db_writer=App_LoggerDB(execution_id)
          self.az_blob_mgt=AzureManagement()


     def replaceMissingWithNull(self,column_names):

          log_collection="data_transform_log"
          log_database="credit_training_log"

          try:


               onlyfiles=self.az_blob_mgt.get_list_of_blobs_in_container(self.goodDataPath)
               for file in onlyfiles:

                    csv=self.az_blob_mgt.get_csv_file(self.goodDataPath,file)
                    csv = csv[column_names]
                    csv.fillna('NULL',inplace=True)

                    self.az_blob_mgt.saveDataFrameTocsv(self.goodDataPath,file,csv)
                    self.logger_db_writer.log(log_database,log_collection,"File {0} Transformed successfully!!".format(file))
               goog_files = self.az_blob_mgt.get_list_of_blobs_in_container(self.goodDataPath)
               msg = "total no of files in good directory is {} after data transformation".format(len(goog_files))
               self.logger_db_writer.log(log_database, log_collection, msg)

          except Exception as e:
               msg="Error occured in class:dataTransform method:replaceMissingWithNull error:Data Transformation failed because:"+str(e)
               self.logger_db_writer.log(log_database,log_collection,msg)
               raise e

