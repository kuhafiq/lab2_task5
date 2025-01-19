import os
from azure.storage.blob import BlobServiceClient
from app import myApp  # Import from app/init.py

@myApp.activity_trigger()
def get_input_data():
    connection_string = os.getenv("AZURE_BLOB_STRING")  # Fetch from environment variables
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
