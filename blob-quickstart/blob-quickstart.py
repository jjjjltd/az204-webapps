import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

account_url = "https://csb100320003a32f3f6.blob.core.windows.net"
default_credential = DefaultAzureCredential()

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient(account_url, credential=default_credential)

try:
    print("Azure Blob Storage Python quickstart sample")

    # Create a unique name for the container
    container_name = str(uuid.uuid4())

# Create the container
    container_client = blob_service_client.create_container(container_name)
    print(container_name)

except Exception as ex:
    print('Exception:')
    print(ex)