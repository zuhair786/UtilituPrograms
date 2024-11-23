from operator import le
import os
from azure.cosmos import CosmosClient
from concurrent.futures import ThreadPoolExecutor
import queue

output_queue = queue.Queue()
curPath = os.path.dirname(os.path.abspath(__file__))
filename = os.path.join(curPath,'input.txt')
destinatioPath = os.path.join(curPath,'output.txt')

ENDPOINT = "<COSMOS-ENDPOINT>"
KEY = "<COSMOS-KEY>"
DATABASE_ID = "<COSMOS-DATABASE>"
CONTAINER_ID = "<COSMOS-CONTAINER>"

client = CosmosClient(ENDPOINT, KEY)
database = client.get_database_client(DATABASE_ID)
container = database.get_container_client(CONTAINER_ID)

query_attribute = 'employeeId'

def main_process_1(line):
    with open(destinatioPath,'a') as d:
        query = f"SELECT * FROM c WHERE c.{query_attribute} = @query_value"
        lineArr = line.split(",")
        employeeId = lineArr[0].rstrip()
        query_params = [{'name': '@query_value', 'value': employeeId}]
        items = list(container.query_items(
            query=query,
            parameters=query_params,
            enable_cross_partition_query=True
        ))
        if len(items) >= 1:
            index = len(items) - 1
            employeeId = items[index]['employeeId']
            skills = items[index]['skills']
            skillsArr = "+".join(skills)
            line = employeeId + ","+ skillsArr + "\n"
            print(line)
            output_queue.put(line)

def write_to_file(queue,destination):
    queueSize = queue.qsize()
    with open(destination, 'a') as d:
        for i in range(queueSize):
            d.write(queue.get())

with open(filename, 'r') as f: 
    lines = f.readlines()
    with ThreadPoolExecutor(10) as writer:
        for line in lines: 
            writer.submit(main_process_1, line)

write_to_file(output_queue,destinatioPath)


# main_process_1("40320")
