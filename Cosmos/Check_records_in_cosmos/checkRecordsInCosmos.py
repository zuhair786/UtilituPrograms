from operator import le
import os
from azure.cosmos import CosmosClient
from concurrent.futures import ThreadPoolExecutor
import queue

output_queue_1 = queue.Queue()
output_queue_2 = queue.Queue()

curPath = os.path.dirname(os.path.abspath(__file__))
filename = os.path.join(curPath,'input.txt')
passPath = os.path.join(curPath,'pas.txt')
failPath = os.path.join(curPath,'fail.txt')

ENDPOINT = "<COSMOS-ENDPOINT>"
KEY = "<COSMOS-KEY>"
DATABASE_ID = "<COSMOS-DATABASE>"
CONTAINER_ID = "<COSMOS-CONTAINER>"

client = CosmosClient(ENDPOINT, KEY)
database = client.get_database_client(DATABASE_ID)
container = database.get_container_client(CONTAINER_ID)

# Ensure the filter fields are added as indexes
query_attribute_1 = 'division'
query_attribute_2 = 'department'
query_attribute_3 = 'address.place'


def get_employees_count_by_details(line):
    query = f"SELECT * FROM c WHERE c.{query_attribute_1} = @query_value_1 AND c.{query_attribute_2} = @query_value_2"
    lineArr = line.split(",")
    division = lineArr[0].rstrip()
    department = lineArr[1].rstrip()
    query_params = [{'name': '@query_value_1', 'value': division},{'name': '@query_value_2', 'value': department}]
    items = list(container.query_items(
        query=query,
        parameters=query_params,
        enable_cross_partition_query=True
    ))
    if len(items) >= 1:
        print(line)
        output_queue_1.put(f"{line},{len(items)}")
    else:
        output_queue_2.put(line)

def get_employees_count_by_place(line):
    query = f"SELECT * FROM c WHERE c.{query_attribute_3} = @query_value_1"
    lineArr = line.split(",")
    place = lineArr[0].rstrip()
    query_params = [{'name': '@query_value_1', 'value': place}]
    items = list(container.query_items(
        query=query,
        parameters=query_params,
        enable_cross_partition_query=True
    ))
    if len(items) <= 50 and len(items) >=40:
        output_queue_1.put(line)
    else:
        output_queue_2.put(line)

def write_to_file(queue,destination):
    queueSize = queue.qsize()
    with open(destination, 'a') as d:
        for i in range(queueSize):
            d.write(queue.get())

with open(filename, 'r') as f: 
    lines = f.readlines()
    with ThreadPoolExecutor(100) as writer:
        for line in lines: 
            writer.submit(get_licenses_by_sku, line)

write_to_file(output_queue_1,passPath)
write_to_file(output_queue_2,failPath)


# get_employees_count_by_details("Transport,Hardware")
# get_employees_count_by_place("Denver")
# write_to_file(output_queue_1,passPath)
# write_to_file(output_queue_2,failPath)
