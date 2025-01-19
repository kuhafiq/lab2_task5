import azure.functions as func
import azure.durable_functions as df
import json
import re
import os
from azure.storage.blob import BlobServiceClient

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# --- 1️⃣ HTTP Trigger to Start the Process ---
@myApp.route(route="orchestrators/{functionName}")
@myApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params.get('functionName')
    instance_id = await client.start_new(function_name)
    response = client.create_check_status_response(req, instance_id)
    return response

# --- 2️⃣ Orchestrator Function ---
@myApp.orchestration_trigger(context_name="context")
def master_orchestrator(context: df.DurableOrchestrationContext):
    # Step 1: Get Input Data from Blob Storage
    input_data = yield context.call_activity("get_input_data", None)
    
    # Step 2: Run Mappers in Parallel
    map_tasks = []
    for line_num, line in input_data:
        map_tasks.append(context.call_activity("mapper", (line_num, line)))
    map_results = yield context.task_all(map_tasks)
    
    # Step 3: Run Shuffler
    shuffled_data = yield context.call_activity("shuffler", map_results)
    
    # Step 4: Run Reducers in Parallel
    reduce_tasks = []
    for word, counts in shuffled_data.items():
        reduce_tasks.append(context.call_activity("reducer", (word, counts)))
    reduce_results = yield context.task_all(reduce_tasks)
    
    return reduce_results

# --- 3️⃣ Read Input Data from Azure Blob Storage ---
@myApp.activity_trigger()
def get_input_data():
    connection_string = os.getenv("AZURE_BLOB_STRING")  # Fetch from GitHub Secret  #use secret env variable
    container_name = "mrinput"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    input_data = []
    for blob in container_client.list_blobs():
        blob_client = container_client.get_blob_client(blob.name)
        file_content = blob_client.download_blob().readall().decode("utf-8")
        for i, line in enumerate(file_content.split("\n")):
            if line.strip():  # Ignore empty lines
                input_data.append((i, line.strip()))
    
    return input_data

# --- 4️⃣ Mapper Function ---
@myApp.activity_trigger()
def mapper(data):
    line_num, line = data
    words = re.findall(r'\b\w+\b', line.lower())  # Tokenize
    return [(word, 1) for word in words]

# --- 5️⃣ Shuffler Function ---
@myApp.activity_trigger()
def shuffler(map_results):
    shuffle_dict = {}
    for word_counts in map_results:
        for word, count in word_counts:
            if word not in shuffle_dict:
                shuffle_dict[word] = []
            shuffle_dict[word].append(count)
    return shuffle_dict

# --- 6️⃣ Reducer Function ---
@myApp.activity_trigger()
def reducer(data):
    word, counts = data
    return (word, sum(counts))
