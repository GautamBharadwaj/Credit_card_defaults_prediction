import sqlite3
from datetime import datetime
import re
import pandas as pd
from AzureManagement import AzureManagement
from App_logging import App_LoggerDB
from db_operation import MongoDBOperation

class Prediction_Data_validation:

    def __init__(self,path,execution_id):
        self.Batch_Directory = path
        self.execution_id=execution_id

        self.log_collection="schema_prediction"
        self.log_database="credit_card_sys"

        self.logger_db_writer    = App_LoggerDB(execution_id=execution_id)
        self.mongdb              = MongoDBOperation()
        self.az_blob_mgt         = AzureManagement()
        self.good_directory_path = "good-raw-file-prediction-validated"
        self.bad_directory_path  = "bad-raw-file-prediction-validated"



    def valuesFromSchema(self):

        try:
            log_database="cred_prediction_log"
            log_collection="values_from_schema_validation"
            df_schema_training = self.mongdb.get_df_from_collect(self.log_database, self.log_collection)
            dic = {}
            [dic.update({i: df_schema_training.loc[0, i]}) for i in df_schema_training.columns]
            del df_schema_training

            pattern                  = dic['SampleFileName']
            LengthOfDateStampInFile  = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile  = dic['LengthOfTimeStampInFile']
            column_names             = dic['ColName']
            NumberofColumns          = dic['NumberofColumns']

            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"

            self.logger_db_writer.log(log_database,log_collection,message)

        except ValueError:

            self.logger_db_writer.log(self.log_database, self.log_collection,"ValueError:Value not found inside schema_training.json")
            raise ValueError

        except KeyError:
            self.logger_db_writer.log(self.log_database, self.log_collection,"KeyError:Key value error incorrect key passed")
            raise KeyError

        except Exception as e:

            self.logger_db_writer.log(self.log_database, self.log_collection, str(e))

            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):

        regex = "['creditCardFraud']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        try:
            log_database="cred_prediction_log"
            log_collection="general_log"
            self.az_blob_mgt.create_container(self.good_directory_path)
            self.az_blob_mgt.create_container(self.bad_directory_path)
            msg=self.good_directory_path+" and "+self.bad_directory_path+" created successfully."
            self.logger_db_writer.log(log_database,log_collection,msg)
        except Exception as e:
            msg ="Error Occured in class Prediction_Data_validation method:createDirectoryForGoodBadRawData error: Failed to create directory " +self.good_directory_path + " and " + self.bad_directory_path
            self.logger_db_writer.log(self.log_database, self.log_collection,msg)
            raise e


    def deleteExistingGoodDataTrainingFolder(self):

        try:

            log_database = "cred_prediction_log"
            log_collection = "general_log"
            self.az_blob_mgt.delete_container(self.good_directory_path)
            self.logger_db_writer.log(log_database, log_collection,self.good_directory_path + " deleted successfully!!")

        except Exception as e:
            msg="Error Occured in class Raw_Data_validation method:deleteExistingGoodDataTrainingFolder Error occured while deleting :"+self.good_directory_path
            self.logger_db_writer.log(self.log_database, self.log_collection,msg)
            raise e



    def deleteExistingBadDataTrainingFolder(self):

        try:

            log_database = "cred_prediction_log"
            log_collection = "general_log"
            self.az_blob_mgt.delete_container(self.bad_directory_path)
            self.logger_db_writer.log(log_database, log_collection,self.bad_directory_path + " deleted successfully!!")

        except Exception as e:
            msg = "Error Occured in class Raw_Data_validation method:deleteExistingGoodDataTrainingFolder Error occured while deleting :" + self.good_directory_path
            self.logger_db_writer.log(self.log_database, self.log_collection, msg)
            raise e

    def moveBadFilesToArchiveBad(self):


        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            log_database = "cred_prediction_log"
            log_collection = "general_log"

            source = self.bad_directory_path
            destination = "lap-" +self.execution_id
            self.logger_db_writer.log(log_database, log_collection, "Started moving bad raw data..")
            for file in self.az_blob_mgt.get_list_of_blobs_in_container(source):
                self.az_blob_mgt.movefileindirectory(source, destination, file)
                self.logger_db_writer.log(log_database, log_collection,"File:" + file + " moved to directory:" + destination + " successfully.")

            self.logger_db_writer.log(log_database, log_collection,"All bad raw file moved to directory:" + destination)

            self.az_blob_mgt.delete_container(source)
            self.logger_db_writer.log(log_database, log_collection, "Deleting bad raw directory:" + source)

        except Exception as e:
            self.logger_db_writer.log(self.log_database, self.log_collection,"class Raw_Data_validation method:moveBadFilesToArchiveBad Error while moving bad files to archive:"+str(e))
            raise e





    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):

        self.createDirectoryForGoodBadRawData()
        onlyfiles = self.az_blob_mgt.get_list_of_blobs_in_container(self.Batch_Directory)
        try:
            log_database = "cred_prediction_log"
            log_collection = "name_validation_log"
            print(regex)
            for filename in onlyfiles:
                print(filename)
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            self.az_blob_mgt.copy_blob_betw_directory(self.Batch_Directory, self.good_directory_path,filename)
                            self.logger_db_writer.log(log_database, log_collection,"Valid File name!! File moved to " + self.good_directory_path + filename)

                        else:

                            self.az_blob_mgt.copy_blob_betw_directory(self.Batch_Directory, self.bad_directory_path,filename)
                            msg = "time unmatched Invalid File Name !! File moved to " + self.bad_directory_path + filename
                            self.logger_db_writer.log(log_database, log_collection, msg)
                    else:

                        self.az_blob_mgt.copy_blob_betw_directory(self.Batch_Directory, self.bad_directory_path,filename)
                        msg = "date unmatched Invalid File Name !! File moved to " + self.bad_directory_path + filename
                        self.logger_db_writer.log(log_database, log_collection, msg)

                else:

                    self.az_blob_mgt.copy_blob_betw_directory(self.Batch_Directory, self.bad_directory_path,filename)
                    msg = "re unmatched Invalid File Name !! File moved to " + self.bad_directory_path + filename
                    self.logger_db_writer.log(log_database, log_collection, msg)

        except Exception as e:
            msg = "Error occured while validating FileName " + str(e)
            self.logger_db_writer.log(self.log_database, self.log_collection, msg)
            raise e

    def validateColumnLength(self,NumberofColumns,Column_Names):

        try:
            log_database="cred_prediction_log"
            log_collection="column_validation_log"

            self.logger_db_writer.log(log_database,log_collection,"Column length validation Started!!")

            for file in self.az_blob_mgt.get_list_of_blobs_in_container(self.good_directory_path):

                csv=self.az_blob_mgt.get_csv_file(self.good_directory_path,file)
                csv = csv[Column_Names]
                if csv.shape[1] == NumberofColumns:
                    self.az_blob_mgt.saveDataFrameTocsv(self.good_directory_path,file,csv)
                else:
                    self.az_blob_mgt.movefileindirectory(self.good_directory_path,self.bad_directory_path,file)
                    print(NumberofColumns)
                    print(csv.columns)
                    self.logger_db_writer.log(log_database,log_collection,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

            self.logger_db_writer.log(log_database,log_collection,"Column Length Validation Completed!!")

        except Exception as e:

            self.logger_db_writer.log(self.log_database, self.log_collection,'Error Occured::'+str(e) )
            raise e


    def deletePredictionFile(self):
        try:
            log_database = "cred_prediction_log"
            log_collection = "general_log"
            container = "prediction-file"
            filename = "Prediction.csv"
            if container in self.az_blob_mgt.get_list_of_container():
                filenames = self.az_blob_mgt.get_list_of_blobs_in_container(container)
                if filename in filenames:
                    self.az_blob_mgt.delete_container(container, filename)
                    self.logger_db_writer.log(log_database, log_collection,filename + " is deleted from dir:" + container + " successfully")
        except Exception as e:

            self.logger_db_writer.log(log_database, log_collection,"Error occure while deleting prediction file from prediction-file directory" + str(e))
            raise e


    def validateMissingValuesInWholeColumn(self,Column_Names):

        try:
            log_database="cred_prediction_log"
            log_collection="missing_values_in_column"
            self.logger_db_writer.log(log_database,log_collection, "Missing Values Validation Started!!")
            blobs = self.az_blob_mgt.get_list_of_blobs_in_container(self.bad_directory_path)
            self.logger_db_writer.log(log_database, log_collection,"total no of files in bad dir is {}".format(len(blobs)))
            blobs = self.az_blob_mgt.get_list_of_blobs_in_container(self.good_directory_path)
            self.logger_db_writer.log(log_database, log_collection, "total no of files in good dir is {}".format(len(blobs)))

            for file in self.az_blob_mgt.get_list_of_blobs_in_container(self.good_directory_path):

                csv=self.az_blob_mgt.get_csv_file(self.good_directory_path,file)
                csv = csv[Column_Names]
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == 0:
                        count += 1
                if count == (len(csv.columns)):
                    self.az_blob_mgt.saveDataFrameTocsv(self.good_directory_path, file, csv)
                    print("file is going to good directory {}".format(file))
                else:
                    print("missing value present file is going to bad directory {}".format(file))
                    self.az_blob_mgt.movefileindirectory(self.good_directory_path, self.bad_directory_path, file)

        except Exception as e:
            self.logger_db_writer.log(self.log_database, self.log_collection,"Error occured:"+str(e))
            raise e









