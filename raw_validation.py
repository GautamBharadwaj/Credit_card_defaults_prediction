import re
from db_operation import MongoDBOperation
from AzureManagement import AzureManagement
from App_logging import App_LoggerDB



class Raw_Data_validation:

    def __init__(self,path,execution_id):
        self.Batch_Directory = path
        self.exexcution_id=execution_id
        self.collection_name="schema_training"
        self.database_name="credit_card_sys"
        self.mongdb=MongoDBOperation()
        self.logDB_write = App_LoggerDB(execution_id=execution_id)
        self.log_database = "credit_training_log"
        self.log_collection = "raw_Validation_log"
        self.az_blob_mgt=AzureManagement()
        self.good_directory_path="good-raw-file-train-validated"
        self.bad_directory_path="bad-raw-file-train-validated"
        files = self.az_blob_mgt.get_list_of_blobs_in_container(self.Batch_Directory)
        self.logDB_write.log(self.log_database, self.log_collection, "total no of files in batch folder is {}".format(len(files)))


    def valuesFromSchema(self):

        df_schema_training      = self.mongdb.get_df_from_collect(self.database_name,self.collection_name)
        dic={}
        [dic.update({i: df_schema_training.loc[0, i]}) for i in df_schema_training.columns]
        del df_schema_training
        pattern                 = dic['SampleFileName']
        LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
        LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
        column_names            = dic['ColName']
        NumberofColumns         = dic['NumberofColumns']

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualRegexCreation(self):

        regex = "['creditCardFraud']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        self.az_blob_mgt.create_container(self.good_directory_path)
        self.az_blob_mgt.create_container(self.bad_directory_path)

    def deleteExistingGoodDataTrainingFolder(self):

        self.az_blob_mgt.delete_container(self.good_directory_path)

    def deleteExistingBadDataTrainingFolder(self):

        self.az_blob_mgt.delete_container(self.bad_directory_path)

    def moveBadFilesToArchiveBad(self):

        source = self.bad_directory_path
        destination="lat-" + self.exexcution_id
        for file in self.az_blob_mgt.get_list_of_blobs_in_container(self.bad_directory_path):
            self.az_blob_mgt.movefileindirectory(self.bad_directory_path,destination,file)
        self.az_blob_mgt.delete_container(source)


    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):

        cont_list = self.az_blob_mgt.get_list_of_container()
        if self.good_directory_path not in cont_list:
            if self.bad_directory_path not in cont_list:
                self.createDirectoryForGoodBadRawData()
            else:
                pass
        else:
            pass
        onlyfiles=self.az_blob_mgt.get_list_of_blobs_in_container(self.Batch_Directory)
        for filename in onlyfiles:
            if (re.match(regex, filename)):
                splitAtDot = re.split('.csv', filename)
                splitAtDot = (re.split('_', splitAtDot[0]))
                if len(splitAtDot[1]) == LengthOfDateStampInFile:
                    if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                        self.az_blob_mgt.copy_blob_betw_directory(self.Batch_Directory,self.good_directory_path,filename)
                    else:
                        self.az_blob_mgt.copy_blob_betw_directory(self.Batch_Directory, self.bad_directory_path, filename)
                else:
                    self.az_blob_mgt.copy_blob_betw_directory(self.Batch_Directory, self.bad_directory_path,filename)
            else:
                self.az_blob_mgt.copy_blob_betw_directory(self.Batch_Directory, self.bad_directory_path,filename)



    def validateColumnLength(self,NumberofColumns,column_names):

        for file in self.az_blob_mgt.get_list_of_blobs_in_container(self.good_directory_path):
            df = self.az_blob_mgt.get_csv_file(self.good_directory_path,file)
            df = df[column_names]
            if df.shape[1] == NumberofColumns:
                pass
            else:
                self.az_blob_mgt.movefileindirectory(self.good_directory_path,self.bad_directory_path,file)


    def validateMissingValuesInWholeColumn(self,column_names):

        for file in self.az_blob_mgt.get_list_of_blobs_in_container(self.good_directory_path):
            df = self.az_blob_mgt.get_csv_file(self.good_directory_path,file,)
            df = df[column_names]
            count = 0
            for columns in df:
                if (len(df[columns]) - df[columns].count()) == 0:
                    count+=1
            if count==(len(df.columns)):
                self.az_blob_mgt.saveDataFrameTocsv(self.good_directory_path,file,df)
            else:
                self.az_blob_mgt.movefileindirectory(self.good_directory_path, self.bad_directory_path, file)




