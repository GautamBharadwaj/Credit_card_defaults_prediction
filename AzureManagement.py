

#import pandas as pd
#from io import StringIO
#import os
#from azure.storage.blob import BlobServiceClient
#connection_string = "DefaultEndpointsProtocol=https;AccountName=cement;AccountKey=PIcBK2Np0so6NwgEBi7qiZRDd1AgMUCcYUyiqUxVu3sOY3biT6Z81BfImMW1YY74SlQiqMkdgDNdBvddBLIwjA==;EndpointSuffix=core.windows.net"
#blob_service = BlobServiceClient.from_connection_string(connection_string)
'''
local_file_path = "C:/Users/Gautam/Desktop"
local_file_name = "RESUME7.docx"
dirt= os.path.join(local_file_path, local_file_name)
cont_list = [i.name for i in blob_service.list_containers()]
print("\n",cont_list,"\n")

if [i=="training-batch-files" for i in cont_list]:
    container = blob_service.get_container_client("training-batch-files")
    cont_name = container.container_name
else:
    cont_name = blob_service.create_container("training-batch-files")

blob_client = blob_service.get_blob_client(container = cont_name,blob = local_file_name)
if [blob.name=='RESUME7.docx' for blob in container.list_blobs()]: #blob already present
    pass
else:
    with open(dirt,"rb") as data:
        blob_client.upload_blob(data)


for cont_name in [i.name for i in blob_service.list_containers()]:
    cont = blob_service.get_container_client(cont_name)
    print(cont_name,":")
    for blob_name in [i.name for i in cont.list_blobs()]:
        print("\t\t\t",blob_name)

for i in [i.name for i in container.list_blobs()]:
    container.delete_blobs(i)

local_file_path = "D:/Ineuron_ML_projects/creditCardDefaulters/code/creditCardDefaulters/Training_Batch_Files"
local_files = os.listdir(local_file_path)
for file in local_files:
    blob_client = blob_service.get_blob_client(container = "training-batch-files",blob = file)
    with open(dirt,"rb") as data:
        blob_client.upload_blob(data)
        print("uploaded")


#blob_client = blob_service.get_blob_client(container="training-batch-files", blob="cement_strength_08012020_1200.csv")
#df = pd.read_csv(StringIO(blob_client.download_blob().readall().decode()))

'''





from azure.storage.blob import BlobServiceClient
from io import StringIO
import pandas as pd
import dill

class AzureManagement:

    def __init__(self):
        self.__account_name = 'cement'
        self.__account_key = 'PIcBK2Np0so6NwgEBi7qiZRDd1AgMUCcYUyiqUxVu3sOY3biT6Z81BfImMW1YY74SlQiqMkdgDNdBvddBLIwjA==;EndpointSuffix=core.windows.net'
        connect_str = "DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}".format(self.__account_name,self.__account_key)
        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    def get_required_container(self,cont_name):

        container_client = self.blob_service_client.get_container_client(cont_name)
        return container_client

    def get_required_blob(self,cont_name,blob_name):

        blob_client = self.blob_service_client.get_blob_client(container=cont_name,blob=blob_name)
        return blob_client

    def get_list_of_container(self):
        cont_list = [container_name.name for container_name in self.blob_service_client.list_containers()]
        return cont_list

    def get_list_of_blobs_in_container(self,cont_name):

        container_client = self.get_required_container(cont_name)
        cont_list = self.get_list_of_container()
        if cont_name in cont_list:
            blob_names = [i.name for i in container_client.list_blobs()]
            #print(blob_names)
        return blob_names

    def get_csv_file(self,cont_name,blob_name):
        try:
            blob = self.get_required_blob(cont_name,blob_name)
            data = blob.download_blob().content_as_bytes()
            s = str(data, 'utf-8')
            data = StringIO(s)
            df = pd.read_csv(data)
            return df
        except Exception as e:
            print(str(e))

    def create_container(self,cont_name):
        cont_list = self.get_list_of_container()
        if cont_name not in cont_list:
            self.blob_service_client.create_container(cont_name)
        else:
            return True
        return True

    def delete_container(self,cont_name):
        cont_list = self.get_list_of_container()
        if cont_name not in cont_list:
            return True
        else:
            self.blob_service_client.delete_container(container=cont_name)
            return True

    def delete_blob_from_container(self,blob_name,cont_name):
        cont_list = self.get_list_of_container()
        blob_list = self.get_list_of_blobs_in_container(cont_name)
        if cont_name in cont_list:
            if blob_name in blob_list:
                container = self.get_required_container(cont_name)
                container.delete_blob(blob_name)
        return True


    #def saveDataframe():
    def movefileindirectory(self,source_cont,target_cont,blob_name):

        cont_list = self.get_list_of_container()
        blob_list = self.get_list_of_blobs_in_container(source_cont)
        if source_cont not in  cont_list:
            raise Exception("error occured no container found")
        if target_cont not in cont_list:
            self.create_container(target_cont)

        if blob_name in self.get_list_of_blobs_in_container(target_cont):
            blob_client = self.get_required_blob(target_cont,blob_name)
            blob_client.delete_blob()

        account_name = AzureManagement()._AzureManagement__account_name
        source_blob = (f"https://{account_name}.blob.core.windows.net/{source_cont}/{blob_name}")
        copied_blob = self.blob_service_client.get_blob_client(target_cont, blob_name)
        copied_blob.start_copy_from_url(source_blob)
        remove_blob = self.blob_service_client.get_blob_client(source_cont, blob_name)
        remove_blob.delete_blob()
        return True


    def copy_blob_betw_directory(self,source_cont,target_cont,blob_name):

        cont_list = self.get_list_of_container()
        blob_list = self.get_list_of_blobs_in_container(source_cont)
        if source_cont not in cont_list:
            raise Exception("error occured no container found")
        if target_cont not in cont_list:
            self.create_container(target_cont)

        if blob_name in self.get_list_of_blobs_in_container(target_cont):
            blob_client = self.get_required_blob(target_cont,blob_name)
            blob_client.delete_blob()

        account_name = AzureManagement()._AzureManagement__account_name
        source_blob = (f"https://{account_name}.blob.core.windows.net/{source_cont}/{blob_name}")
        copied_blob = self.blob_service_client.get_blob_client(target_cont, blob_name)
        copied_blob.start_copy_from_url(source_blob)
        return True
    
    def saveDataFrameTocsv(self, cont_name, file_name, df):

        cont_list = self.get_list_of_container()

        if file_name.split(".")[-1] != "csv":
            file_name = file_name + ".csv"
        if cont_name not in cont_list:
            self.create_container(cont_name)
            
        if file_name in self.get_list_of_blobs_in_container(cont_name):
            
            data_frame = self.get_csv_file(cont_name,file_name)
            print("Hi my name is gautam")
            df= data_frame.append(df)

        if file_name in self.get_list_of_blobs_in_container(cont_name):
            
            blob_client = self.get_required_blob(cont_name, file_name)
            blob_client.delete_blob()

        blob_client = self.get_required_blob(cont_name, file_name)
        
        output = df.to_csv(encoding="utf-8")
        blob_client.upload_blob(output)
        return True

    def saveObject(self, cont_name, blob_name, object_name):

        try:
            cont_list = self.get_list_of_container()
            if cont_name not in cont_list:
                self.create_container(cont_name)
            if blob_name in self.get_list_of_blobs_in_container(cont_name):
                blob_client = self.get_required_blob(cont_name,blob_name)
                blob_client.delete_blob()
            blob_client = self.get_required_blob(cont_name,blob_name)
            blob_client.upload_blob(dill.dumps(object_name))
            return True

        except Exception as e:
            raise Exception("Error occured in class: AzureBlobManagement method:createFileForModel error:" + str(e))

    def loadObject(self, cont_name, file_name):

        try:
            cont_list = self.get_list_of_container()
            if cont_name not in cont_list:
                raise Exception("Error occured in class: AzureBlobManagement method:loadModel error:Directory {0} not found".format(cont_name))
            if file_name not in self.get_list_of_blobs_in_container(cont_name):
                raise Exception("Error occured in class: AzureBlobManagement method:loadModel error:File {0} not found".format(file_name))

            blob_client = self.get_required_blob(cont_name, file_name)
            model = dill.loads(blob_client.download_blob().readall())
            return model

        except Exception as e:
            raise Exception("Error occured in class: AzureBlobManagement method:loadModel error:" + str(e))

    def find_correct_model_file(self,cont_name, cluster_number):


        self.cluster_number = cluster_number
        self.cont_name = cont_name
        self.list_of_model_files = []
        self.list_of_files = self.get_list_of_blobs_in_container(self.cont_name)
        for self.file in self.list_of_files:
            try:
                if (self.file.index(str(self.cluster_number)) != -1):
                    self.model_name = self.file
            except:
                continue
        self.model_name = self.model_name.split('.')[0]

        return self.model_name







