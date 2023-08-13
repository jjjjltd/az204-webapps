import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def housekeeping():
    try:
        local_path = "./data"
        filelist = os.listdir(local_path)
        print(filelist)

        for file in filelist:
            os.remove(f"{local_path}/{file}")

        os.rmdir(local_path)
        print(f"{local_path} removed.")
    except Exception as ex:
        print('Exception:')
        print(ex)

def blob_prep(cleanup):
    """ See Readme to understand how "Program reference 1" * 2 are combined here.  """

    housekeeping()

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

        # Create a local directory to hold blob data
        local_path = "./data"
        os.mkdir(local_path)

        # Create a file in the local data directory to upload and download
        local_file_name = str(uuid.uuid4()) + ".txt"
        upload_file_path = os.path.join(local_path, local_file_name)

        # Write text to the file
        file = open(file=upload_file_path, mode='w')
        file.write("Hello, World!")
        file.close()

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        # Upload the created file
        with open(file=upload_file_path, mode="rb") as data:
            blob_client.upload_blob(data)

        print("\nListing blobs...")

            # List the blobs in the container
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)

        # Download the blob to a local file
        # Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
        download_file_path = os.path.join(local_path, str.replace(local_file_name ,'.txt', 'DOWNLOAD.txt'))
        container_client = blob_service_client.get_container_client(container= container_name) 
        print("\nDownloading blob to \n\t" + download_file_path)

        with open(file=download_file_path, mode="wb") as download_file:
            download_file.write(container_client.download_blob(blob.name).readall())

        #  Proving: Get Properties block
        properties = container_client.get_container_properties()

        print(f"Public access type: {properties.public_access}")
        print(f"Lease status: {properties.lease.status}")
        print(f"Lease state: {properties.lease.state}")
        print(f"Has immutability policy: {properties.has_immutability_policy}")

        # Set Metadata using dictionary
        container_meta = {
            "doctype": "text_documents",
            "category": "guidance"
        }
        container_client.set_container_metadata(container_meta)

        # Retrieve existing metadata, if desired
        metadata = container_client.get_container_properties().metadata
        print(f"Meta data items: {metadata.items}")
        for k, v in metadata.items():
            print(k, v)

        #  End of:  Proving Get Properties block

        if cleanup:
            # Clean up
            print("\nPress the Enter key to begin clean up")
            input()

            print("Deleting blob container...")
            container_client.delete_container()

            print("Deleting the local source and downloaded files...")
            os.remove(upload_file_path)
            os.remove(download_file_path)
            os.rmdir(local_path)

            print("Done")

    except Exception as ex:
        print('Exception:')
        print(ex)


cleanup = True
container_client = blob_prep(cleanup)