from datetime import datetime
from db_operation import MongoDBOperation
from AzureManagement import AzureManagement
import pandas as pd


class App_LoggerDB:
    def __init__(self,execution_id):
        self.mongoDBObject=MongoDBOperation()
        self.azureBlobObject=AzureManagement()
        self.execution_id=execution_id

    def log(self, database_name, collection_name,log_message):
        
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        log={
        'Log_updated_date':self.now,
        'Log_update_time':self.current_time,
        'Log_message':log_message,
        'execution_id':self.execution_id
        }
        res = self.mongoDBObject.insert_record_in_collec(database_name,collection_name,log)
        print(res)
        if res>0:
            return True
        else:
            log = {
                'Log_updated_date': [self.now],
                'Log_update_time': [self.current_time],
                'Log_message': [log_message],
                'execution_id':self.execution_id
            }
            self.azureBlobObject.saveDataFrameTocsv("db-fail-log","log_"+self.execution_id,pd.DataFrame(log),mode="a+",header=True)
        return True





