
import pymongo
import json
import pandas as pd

class MongoDBOperation:
    
    def __init__(self):
        
        self.user_name = "mydbUser"
        self.password = "12345"
        self.DB_URL = "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false"
    
    def get_db_client_obj(self):

        try:
            client = pymongo.MongoClient(self.DB_URL)  # creating database client object
            return client
        except Exception as e:
            print(str(e))

    def check_db_present(self,db_name):

        client = self.get_db_client_obj()
        db_list = client.list_database_names()
        if db_name in db_list:
            return True
        else:
            return False   
        
    def create_db(self,client,db_name):

        try:
            return client[db_name]
        except Exception as e:
            print(str(e))
    
    def create_colle_in_db(self,database,coll_name):
        
        #if self.check_db_present(database):
        return database[coll_name]
        #else:
            #client = self.get_db_client_obj()
            #database = self.create_db(client,database)
            #return database[coll_name]
        
    def exist_colle_in_db(self,coll_name,database):
        
        coll_list = database.list_collection_names()
        
        if coll_name in coll_list:
            return True
        else:
            return False
        
    def get_colle_from_db(self,coll_name,database):

        try:
            collection = self.create_colle_in_db(database,coll_name)
            return collection
        except Exception as e:
            print(str(e))
        
        
    def check_record_pres(self,db_name,coll_name,record):

        try:
            client = self.get_db_client_obj()
            database = self.create_db(client,db_name)
            collection = self.create_colle_in_db(database,coll_name)
            recordfound = collection.find(record)
            if recordfound.count()>0:
                client.close()
                return True
            else:
                client.close()
                return False
        except Exception as e:
            print(str(e))

    def create_one_record(self,collection,data):
        
        collection.insert_one(data)
        print("Record Inserted")
        return True
         
    def create_multi_record(self,collection,data):
        
        collection.insert_many(data)
        return len(data)     
        
    def insert_record_in_collec(self,db_name,coll_name,record):
        
        try:
            no_of_row_inserted=0
            client = self.get_db_client_obj()
            database = self.create_db(client,db_name)
            collection = self.get_colle_from_db(coll_name,database)
            if not self.check_record_pres(db_name,coll_name,record):
                no_of_row_inserted = self.create_one_record(collection,record)
            client.close()
            print(no_of_row_inserted)
            return no_of_row_inserted
        except Exception as e:
            print(str(e))
       
    def drop_collection(self,db_name,coll_name):
        
        client = self.get_db_client_obj()
        database = self.create_db(client,db_name)
        if self.exist_colle_in_db(coll_name,database):
            coll_name = self.get_colle_from_db(coll_name,database)
            coll_name.drop()
        return True    
      
    def insert_df(self,db_name,coll_name,df):
        
        records = list(json.loads(df.T.to_json()).values())
        client = self.get_db_client_obj()
        database = self.create_db(client,db_name)
        collection = self.get_colle_from_db(coll_name,database)
        collection.insert_many(records)
        return len(records)  
        
    def get_df_from_collect(self,db_name,coll_name):
        
        client = self.get_db_client_obj()
        database = self.create_db(client, db_name)
        collection = self.get_colle_from_db(coll_name,database)
        df = pd.DataFrame(list(collection.find()))
        if "_id" in df.columns.to_list():
            df = df.drop(columns=["_id"], axis=1)
        return df
    





























