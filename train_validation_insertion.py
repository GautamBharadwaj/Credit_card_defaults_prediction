from raw_validation import Raw_Data_validation
from Data_type_validation import DbOperationMongoDB
from App_logging import App_LoggerDB
from AzureManagement import AzureManagement
from DataTransformation import dataTransform

class train_validation:
    def __init__(self,path,execution_id):
        try:
            self.raw_data = Raw_Data_validation(path,execution_id)
            self.dBOperationMongoDB=DbOperationMongoDB(execution_id)

            self.log_database="credit_training_log"
            self.log_collection="training_main_log"

            self.execution_id=execution_id
            self.logDB_write = App_LoggerDB(execution_id=execution_id)
            self.az_blob_mgt=AzureManagement()

        except Exception as e:
            print(str(e))

    def train_validation(self):
        try:

            self.logDB_write.log(self.log_database,self.log_collection,'Start of Validation on files!!')
            print("Start of Validation on files!!")

            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            print("dict data extracted")

            regex = self.raw_data.manualRegexCreation()
            print("got regex")

            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            print("completed file validation")
            self.raw_data.validateColumnLength(noofcolumns,column_names)
            print("completed column validation")
            self.raw_data.validateMissingValuesInWholeColumn(column_names)
            print("completed missing validation")
            self.logDB_write.log(self.log_database,self.log_collection,"Raw Data Validation Complete!!")
            print("Raw Data Validation Complete!!")


            self.logDB_write.log(self.log_database, self.log_collection, "Starting Data Transforamtion!!")

            dataTransform(self.execution_id).replaceMissingWithNull(column_names)

            self.logDB_write.log(self.log_database, self.log_collection, "DataTransformation Completed!!!")

            self.logDB_write.log(self.log_database,self.log_collection,"Creating database and collection if not exist then insert record")
            self.dBOperationMongoDB.insertIntoTableGoodData(column_names)

            self.logDB_write.log(self.log_database,self.log_collection,"Insertion in Table completed!!!")

            #self.logDB_write.log(self.log_database,self.log_collection,"Deleting Good Data Folder!!!")
            #self.raw_data.deleteExistingGoodDataTrainingFolder()
            #self.logDB_write.log(self.log_database,self.log_collection,"Good_Data folder deleted!!!")
            #self.logDB_write.log(self.log_database,self.log_collection,"Moving bad files to Archive and deleting Bad_Data folder!!!")
            #self.raw_data.moveBadFilesToArchiveBad()
            #self.logDB_write.log(self.log_database,self.log_collection,"Bad files moved to archive!! Bad folder Deleted!!")

            self.logDB_write.log(self.log_database,self.log_collection,"Validation Operation completed!!")
            self.logDB_write.log(self.log_database,self.log_collection,"Extracting csv file from table")
            self.dBOperationMongoDB.selectingDatafromtableintocsv()

        except Exception as e:
            raise e


'''
import uuid
path='training-batch-files'
execution_id=str(uuid.uuid4())
obj = train_validation(path,execution_id)
obj.train_validation()

'''

