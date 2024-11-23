import os
from azure.cosmos import CosmosClient
from concurrent.futures import ThreadPoolExecutor
import queue

ENDPOINT = "<COSMOS-ENDPOINT>"
KEY = "<COSMOS-KEY>"
DATABASE_ID = "<COSMOS-DATABASE>"
CONTAINER_ID = "<COSMOS-CONTAINER>"
SECONDARY_CONTAINER_ID = "<SECONDARY-COSMOS-CONTAINER>"

client = CosmosClient(ENDPOINT, KEY)
database = client.get_database_client(DATABASE_ID)
container = database.get_container_client(CONTAINER_ID)
usage_container = database.get_container_client(SECONDARY_CONTAINER_ID)
query_attribute = 'employeeId'
curPath = os.path.dirname(os.path.abspath(__file__))
filename = os.path.join(curPath,'input.txt')

def create_patch_operations(previousProjects):
        operations = [{"op": "add", "path": "/previousProjects", "value": previousProjects}]
        return operations


def fetch_all_previous_projects(licenseId,source):
        query = f"SELECT c.activityId FROM c WHERE c.licenseId = @query_value and c.source = @query_value_1"
        query_params = [{'name': '@query_value', 'value': licenseId},{'name': '@query_value_1', 'value': source}]
        items = list(usage_container.query_items(
            query=query,
            parameters=query_params,
            enable_cross_partition_query=True
        ))
        return items


def process_and_patch_records(record):
        employeeId = record['employeeId']
        employeeName = record['employeeName']
        activities = fetch_all_previous_projects(employeeId,employeeName)
        acti_list = list()
        for activity in activities:
            acti_list.append(activity['activityId'])
        record_id = record['id']
        print(activities)
        if len(acti_list) > 0:
            operations = create_patch_operations(acti_list)
            container.patch_item(item=record_id, partition_key=record['holderId'], patch_operations=operations)

def main_process(line):
    query = f"SELECT c.id,c.employeeId,c.employeeName FROM c WHERE c.{query_attribute} = @query_value"
    line =line.strip()
    lineArr = line.split(",")
    employeeId = lineArr[4]
    query_params = [{'name': '@query_value', 'value': employeeId}]
    items = list(container.query_items(
        query=query,
        parameters=query_params,
        enable_cross_partition_query=True
    ))
    if items:
           for item in items:
                  process_and_patch_records(item)

with open(filename, 'r') as f: 
    lines = f.readlines()
    with ThreadPoolExecutor(100) as writer:
        for line in lines: 
            writer.submit(main_process, line)

# main_process("1573062099920904,FEA-CONN-MAX-INVITE+FEA-CONN-MAX-STORAGE+FEA-SKP-LMS-FREE+FEA-SKP-EWH-FREE+FEA-CONN-MAX-PRJ+FEA-SKP-FREE-LRN+FEA-SKP-3DW-FREE+FEA-SKP-ADL-FREE+FEA-USERS+FEA-SKP-XRV-FREE,FEA-CON+FEA-CON+FEA-SKP+FEA-SKP+FEA-CON+FEA-SKP+FEA-SKP+FEA-SKP+FEA-USE+FEA-SKP,disabled,86164090")