import pandas as pd
import preprocessing
import data_loader_prediction
from predictionDataValidation import Prediction_Data_validation
from AzureManagement import AzureManagement
from App_logging import App_LoggerDB

class prediction:

    def __init__(self,path,execution_id):
        self.execution_id=execution_id
        self.log_database="cred_prediction_log"
        self.log_collection="prediction_log"
        self.log_db_writer=App_LoggerDB(execution_id)
        self.az_blob_mgt=AzureManagement()
        if path is not None:
            self.pred_data_val = Prediction_Data_validation(path,execution_id)
            self.LengthOfDateStampInFile, self.LengthOfTimeStampInFile, self.column_names, self.NumberofColumns = self.pred_data_val.valuesFromSchema()

    def predictionFromModel(self):

        try:
            self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            self.log_db_writer.log(self.log_database,self.log_collection,'Start of Prediction')
            data_getter = data_loader_prediction.Data_Getter_Pred(self.log_database,self.log_collection,self.execution_id)
            data = data_getter.get_data()
            data = data[self.column_names]
            path=""
            if data.__len__()==0:
                self.log_db_writer.log(self.log_database,self.log_collection,"No data was present to perform prediction existing prediction method")
                return path,"No data was present to perform prediction"

            preprocessor = preprocessing.Preprocessor(self.log_database, self.log_collection, self.execution_id)

            is_null_present=preprocessor.is_null_present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data)

            cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(data)
            data = preprocessor.remove_columns(data,cols_to_drop)
            kmeans = self.az_blob_mgt.loadObject("models","KMeans")
            print(kmeans)
            print(data.columns)
            clusters = kmeans.predict(data)#drops the first column for cluster prediction
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            for i in clusters:
                cluster_data= data[data['clusters']==i]
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = self.az_blob_mgt.find_correct_model_file("models",i)
                model_name = model_name+'.sav'
                model = self.az_blob_mgt.loadObject("models",model_name)
                #model = file_loader.load_model(model_name)
                result=list(model.predict(cluster_data))
                final = pd.DataFrame(list(zip(result)), columns=['Predictions'])
                path="prediction-output-file"
                self.az_blob_mgt.saveDataFrameTocsv(path,"prediction.csv",final)
            self.log_db_writer.log(self.log_database,self.log_collection,'End of prediction')
        except Exception as ex:
            self.log_db_writer.log(self.log_database,self.log_collection,'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path,final.head().to_json(orient="records")


'''
import uuid
execution_id=str(uuid.uuid4())
path = 'prediction-batch-files'
obj = prediction(path,execution_id)
obj.predictionFromModel()
'''