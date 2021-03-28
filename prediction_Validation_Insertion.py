from datetime import datetime
from predictionDataValidation import Prediction_Data_validation
from DataTypeValidationPrediction import DbOperationMongoDB
from DataTransformationPrediction import dataTransformPredict
from App_logging import App_LoggerDB
from AzureManagement import AzureManagement


class pred_validation:
    def __init__(self,path,execution_id):
        self.log_db_writer=App_LoggerDB(execution_id=execution_id)
        self.log_database="cred_prediction_log"
        self.log_collection="prediction_log"
        self.raw_data = Prediction_Data_validation(path,execution_id)
        self.dataTransform = dataTransformPredict(execution_id)
        self.az_blob_mgt = AzureManagement()
        self.dBOperation=DbOperationMongoDB(execution_id=execution_id)


    def prediction_validation(self):

        try:
            self.log_db_writer.log(self.log_database,self.log_collection, 'Start of Validation on files for prediction!!')

            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,noofcolumns = self.raw_data.valuesFromSchema()

            regex = self.raw_data.manualRegexCreation()

            #self.good_path = "good-raw-file-prediction-validated3"

            #df_real = self.az_blob_mgt.get_csv_file(path,"creditCardFraud_280119601_120210.csv")
            #self.az_blob_mgt.saveDataFrameTocsv(self.good_path,"creditCardFraud_280119601_120210_real.csv",df_real)
            #validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            good_files = self.az_blob_mgt.get_list_of_blobs_in_container("good-raw-file-prediction-validated")
            print("total no of files in good path is {} after file_validation".format(len(good_files)))
            #df = self.az_blob_mgt.get_csv_file("good-raw-file-prediction-validated","creditCardFraud_280119601_120210.csv")
            #self.az_blob_mgt.saveDataFrameTocsv(self.good_path,"creditCardFraud_280119601_120210_after_file.csv",df)

            #validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns,column_names)
            good_files = self.az_blob_mgt.get_list_of_blobs_in_container("good-raw-file-prediction-validated")
            print("total no of files in good path is {} after column validation".format(len(good_files)))
            #df2 = self.az_blob_mgt.get_csv_file("good-raw-file-prediction-validated", "creditCardFraud_280119601_120210.csv")
            #self.az_blob_mgt.saveDataFrameTocsv(self.good_path,"creditCardFraud_280119601_120210_after_column.csv", df2)
            #validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn(column_names)
            good_files = self.az_blob_mgt.get_list_of_blobs_in_container("good-raw-file-prediction-validated")
            print("total no of files in good path is {} after missing validation".format(len(good_files)))
            #df3 = self.az_blob_mgt.get_csv_file("good-raw-file-prediction-validated","creditCardFraud_280119601_120210.csv")
            #self.az_blob_mgt.saveDataFrameTocsv(self.good_path, "creditCardFraud_280119601_120210_after_missing.csv",df3)
            self.log_db_writer.log(self.log_database,self.log_collection,"Raw Data Validation Complete!!")

            self.log_db_writer.log(self.log_database,self.log_collection,"Starting Data Transforamtion!!")
            #replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull(column_names)



            self.log_db_writer.log(self.log_database,self.log_collection,"DataTransformation Completed!!!")
            good_files = self.az_blob_mgt.get_list_of_blobs_in_container("good-raw-file-prediction-validated")
            print("total no of files in good path is {} after data transformation".format(len(good_files)))
            self.log_db_writer.log(self.log_database, self.log_collection, "total no of files in good path is {}".format(len(good_files)))


            self.log_db_writer.log(self.log_database,self.log_collection,"Creating Prediction_Database and tables on the basis of given schema!!!")

            self.log_db_writer.log(self.log_database,self.log_collection, "Creating database and collection if not exist then insert record")

            self.dBOperation.insertIntoTableGoodData(column_names)

            self.log_db_writer.log(self.log_database,self.log_collection, "Insertion in Table completed!!!")
            #self.log_db_writer.log(self.log_database,self.log_collection, "Deleting Good Data Folder!!!")

            #self.raw_data.deleteExistingGoodDataTrainingFolder()

            #self.log_db_writer.log(self.log_database,self.log_collection,"Good_Data folder deleted!!!")
            #self.log_db_writer.log(self.log_database,self.log_collection, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            #Move the bad files to archive folder
            #self.raw_data.moveBadFilesToArchiveBad()
            #self.log_db_writer.log(self.log_database,self.log_collection, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_db_writer.log(self.log_database,self.log_collection, "Validation Operation completed!!")
            self.log_db_writer.log(self.log_database,self.log_collection, "Extracting csv file from table")

            self.dBOperation.selectingDatafromtableintocsv()

        except Exception as e:
            raise e


import uuid
path='prediction-batch-files'
execution_id=str(uuid.uuid4())
obj = pred_validation(path,execution_id)
obj.prediction_validation()
