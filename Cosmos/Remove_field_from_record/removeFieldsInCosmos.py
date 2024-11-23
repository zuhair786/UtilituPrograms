from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
from concurrent.futures import ThreadPoolExecutor
import os

cosmos_endpoint = "<COSMOS-ENDPOINT>"
cosmos_key = "<COSMOS-KEY>"
cosmos_db = "<COSMOS-DATABASE>"
cosmos_container = "<COSMOS-CONTAINER>"

curPath = os.path.dirname(os.path.abspath(__file__))
directory_path = os.path.join(curPath,'field_removal')

def create_patch_operations():
        operations = [{"op": "remove", "path": "/salary"}]
        return operations

def process_and_patch_records(record):
            try:
                cosmosdb_client = CosmosClient(url=cosmos_endpoint, credential=cosmos_key)
                database = cosmosdb_client.get_database_client(cosmos_db)
                container = database.get_container_client(cosmos_container)
                print("Processing record", record['id'])
                record_id = record['id']
                operations = create_patch_operations()
                container.patch_item(item=record_id, partition_key=record['employeeId'], patch_operations=operations)
                print("Processed record", record['id'])
            except CosmosHttpResponseError as e:
                print("Not updated")


def main_process(line):
    lineArr =line.split(",")
    id = lineArr[0].replace("\"","")
    employeeId = lineArr[1].replace("\"","")
    rec = {
        'id': id,
        'employeeId': employeeId
    }
    process_and_patch_records(rec)

for filename in os.listdir(directory_path):  
        input_file_path = f"{directory_path}\\{filename}"    
        with open(input_file_path, 'r') as f: 
            lines = f.readlines()
            with ThreadPoolExecutor(50) as writer:
                for line in lines:  
                    writer.submit(main_process, line)

# main_process("\"40320\",\"40320\"")