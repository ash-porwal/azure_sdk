# Author: Ashish Porwal
# Date Created: 06/Aug/2023
# Date Modified: 06/Aug/2023

# Install library - pip install azure-storage-blob
# In this we connect with Connection string

'''
Steps -
--------------------------------------------------------------------------------

1. Need connection string or storage account key & name.
2. Make Blob Service Client with that connection string.
3. Make container clinet with help of blob service client.

Hierarchy be like - Storage account > Containers > Blobs

Containers Operations:

    * create_container: Creates a new container.
    * delete_container: Deletes a specific container.
    * get_container_client: Retrieves a ContainerClient for a particular container to perform various container-level operations.
    * list_containers: Lists all containers within the storage account.

Account Information:

    * get_account_information: Retrieves information related to the storage account.

Blob Operations (via container clients, but initiated from the service client):

    * get_blob_client: Retrieves a BlobClient for a specific blob, allowing various blob-level operations.

User Delegation Key:
    * get_user_delegation_key: Obtains a user delegation key for the Blob service.

Service Properties:

    *get_service_properties: Gets the properties of a storage account's Blob service, including Azure Storage Analytics.
    *set_service_properties: Sets the properties of a storage account's Blob service, including Azure Storage Analytics.

Service Statistics:

    * get_service_stats: Retrieves statistics related to the Blob service.

Blob Service URL:

    * url: Gets the blob service account URL.

Close the Client:

    * close: Closes the shared connection (if it exists).

Context Manager Support:

    * The client supports being used as a context manager, allowing for better management of connections. 
    * We use Context manager to close automatically all the connections to blob.

Configuration:

    The client provides various configuration options, including retry policy configuration, logging, transport protocols, and more.
--------------------------------------------------------------------------------
'''

import time
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Set your blob account connection string
connect_str = "YOUR_CONNECTION_STRING"

# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(connect_str)


# --------------------------------------------------------------------------------
# List containers 
# --------------------------------------------------------------------------------

def list_all_containers():
    containers_list = blob_service_client.list_containers()
    for container in containers_list:
        print(container['name'])
list_all_containers()

# --------------------------------------------------------------------------------
# within each container, there can be multiple blobs. - listing all blobs
# --------------------------------------------------------------------------------

def list_blobs_in_container(container_name):
    container_client = blob_service_client.get_container_client(container_name)
    blobs_list = container_client.list_blobs()
    
    for blob in blobs_list:
        print(blob.name)
        
list_blobs_in_container("your_container_name")

# -------------------------------------------------------------------------------- 
# if want to list all blobs in each contaienrs - 
# --------------------------------------------------------------------------------

def list_all_blobs_in_all_containers():
    containers_list = blob_service_client.list_containers()
    for container in containers_list:
        print(f"Container: {container['name']}")
        list_blobs_in_container(container['name'])
        print("----------")

list_all_blobs_in_all_containers()

# --------------------------------------------------------------------------------
# Creating container
# --------------------------------------------------------------------------------

def create_new_container(container_name):
    try:
        # Create the container
        container_client = blob_service_client.create_container(container_name)
        print(f"Container '{container_name}' created successfully.")
    except Exception as e:
        print(f"Error creating container '{container_name}'. {e}")

create_new_container("your_new_container_name")


# --------------------------------------------------------------------------------
# Delete container
# --------------------------------------------------------------------------------

def delete_existing_container(container_name):
    try:
        # Delete the container
        blob_service_client.delete_container(container_name)
        print(f"Container '{container_name}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting container '{container_name}'. {e}")

delete_existing_container("your_container_name_to_delete")



# --------------------------------------------------------------------------------
# if you want to connect to a specific blob
# --------------------------------------------------------------------------------

container_name = "YOUR_CONTAINER_NAME"
blob_name = "YOUR_BLOB_NAME"
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# --------------------------------------------------------------------------------
# Upload a file to blob
# --------------------------------------------------------------------------------

with open("your_file_path", "rb") as data:
    blob_client.upload_blob(data)

# --------------------------------------------------------------------------------
# Download a Blob:
# --------------------------------------------------------------------------------

with open("destination_file_path", "wb") as my_blob:
    blob_data = blob_client.download_blob()
    blob_data.readinto(my_blob)

# --------------------------------------------------------------------------------
# Delete a Blob:
# --------------------------------------------------------------------------------

blob_client.delete_blob()

# --------------------------------------------------------------------------------
# Restore soft delete
# --------------------------------------------------------------------------------

# Soft deleting a blob means the blob is marked for deletion but is not immediately removed. 
# Instead, it's retained for a specified period, allowing you to restore it if necessary.

# Before you can use the undelete_blob method, you must have soft delete enabled on your Blob Storage account.
# enabling soft del is done through the Azure Portal, Azure CLI, or ARM templates.

# Restore the soft deleted blob
blob_client.undelete_blob()

# --------------------------------------------------------------------------------
# copy blob to new location
# --------------------------------------------------------------------------------

def copy_blob_to_new_location(src_container_name, src_blob_name, dest_container_name, dest_blob_name):
    # Get the source blob client
    src_blob_client = blob_service_client.get_blob_client(container=src_container_name, blob=src_blob_name)

    # Get the URL of the source blob
    src_blob_url = src_blob_client.url

    # Get the destination blob client
    dest_blob_client = blob_service_client.get_blob_client(container=dest_container_name, blob=dest_blob_name)

    # Start the copy operation
    copy_operation = dest_blob_client.start_copy_from_url(src_blob_url)

    # You can also check the copy status if needed
    properties = dest_blob_client.get_blob_properties()
    while properties['copy']['status'] != 'success':
        time.sleep(1)
        properties = dest_blob_client.get_blob_properties()

copy_blob_to_new_location("source_container", "source_blob.txt", "destination_container", "destination_blob.txt")

# --------------------------------------------------------------------------------
# Abort the copy 
# --------------------------------------------------------------------------------

# This method is used to abort a blob copy operation that's currently in progress in Azure Blob Storage.

dest_blob_client = 'some clinet'
copy_operation = dest_blob_client.start_copy_from_url('src_blob_url')

# aborting above copy
copy_id = copy_operation['copy_id']
blob_client.abort_copy(copy_id)

# --------------------------------------------------------------------------------
# Create Snapshot of blob
# --------------------------------------------------------------------------------
# You can read, copy, or delete a snapshot, but you cannot modify it.
snapshot = blob_client.create_snapshot()

# The snapshot's datetime can be accessed via the 'snapshot' attribute
snapshot_datetime = snapshot['snapshot']

# --------------------------------------------------------------------------------
# Fetch account information

# When you run this code, it will display information like:
    # sku_name: This tells you about your storage account type (e.g., Standard_LRS, Standard_GRS, Premium_LRS, etc.).
    # account_kind: This will be either "Storage", "BlobStorage", "StorageV2", etc., which indicates the kind of storage account.
# --------------------------------------------------------------------------------

account_info = blob_service_client.get_account_information()

# Display the retrieved account information
print("Account Information:")
for key, value in account_info.items():
    print(f"{key}: {value}")

# --------------------------------------------------------------------------------
# Obtain the User Delegation Key:
#   The get_user_delegation_key method retrieves a user delegation key for the Blob service, 
#   which can be used to create a user delegation signature. 
#   This signature can then be used to grant users access to resources in the Blob service for a specified period, 
#   without sharing the account access keys.
# --------------------------------------------------------------------------------

# To get the user delegation key, you'll need to specify a start and expiry time. 
# Typically, you'll set the start time as the current time and the expiry time as some point in the future.
# Define the start and expiry time for the key
start = datetime.utcnow()
expiry = start + timedelta(days=7)  # Set the expiry time 7 days from now

# Fetch the user delegation key
user_delegation_key = blob_service_client.get_user_delegation_key(start, expiry)

# Display the user delegation key properties
print("User Delegation Key:")
print(f" - Signed OID: {user_delegation_key.signed_oid}")
print(f" - Signed TID: {user_delegation_key.signed_tid}")
print(f" - Start Time: {user_delegation_key.signed_start}")
print(f" - Expiry Time: {user_delegation_key.signed_expiry}")
print(f" - Signed Service: {user_delegation_key.signed_service}")
print(f" - Signed Version: {user_delegation_key.signed_version}")



# --------------------------------------------------------------------------------
# Fetch and Display Service Properties -
#   The get_service_properties method allows you to retrieve the properties of 
#   the Blob service, which include configurations like Azure Storage Analytics settings, 
#   CORS (Cross-Origin Resource Sharing) rules, and more.
# --------------------------------------------------------------------------------

# Fetch service properties
service_properties = blob_service_client.get_service_properties()

# Display some of the retrieved service properties
print("Blob Service Properties:")

# Display analytics versioning
print(f"Analytics Logging Version: {service_properties['logging']['version']}")

# Display hour metrics
hour_metrics = service_properties['hour_metrics']
print(f"Hour Metrics Enabled: {hour_metrics['enabled']}")
if hour_metrics['enabled']:
    print(f"Hour Metrics Retention Policy: {hour_metrics['retention_policy']['days']} days")
    print(f"Hour Metrics Level: {hour_metrics['include_apis']}")

# Display CORS rules
print("CORS Rules:")
for rule in service_properties['cors']:
    print(f"Allowed Origins: {rule['allowed_origins']}")
    print(f"Allowed Methods: {rule['allowed_methods']}")
    print("----------")

# --------------------------------------------------------------------------------
# we can set meta data of blobs too:
#   Setting metadata for a blob can be helpful for storing additional information 
#   about the blob that doesn't necessarily belong in the blob's content. 
#   Metadata is represented as a dictionary of string key-value pairs.
# --------------------------------------------------------------------------------

# Get a client representing the container and then the blob
container_name = 'your-container-name'
blob_name = 'your-blob-name'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# Define metadata
metadata = {
    "key1": "value1",
    "key2": "value2"
}

# Set metadata for the blob
blob_client.set_blob_metadata(metadata=metadata)


# A few things to note about blob metadata:

#   1. Metadata key names are 
#      case-preserving - (If a system is case-preserving but case-insensitive in its operations: 
#      You could create or store a file named Example.txt. 
#      When searching for the file, you might use example.txt, EXAMPLE.TXT, or any other case variation, 
#      and the system would still find Example.txt because it's case-insensitive in its search operations).
#      and 
#      case-insensitive(This means that uppercase and lowercase versions of letters are treated as the same), 
#      meaning that you can't have two metadata keys differing only in case.
#   2. All metadata names and values are stored as strings.
#   3. Avoid using special characters in metadata keys and values, and be aware of potential encoding requirements.
#      There are size limits to the combined size of metadata for a blob, 
#      so be cautious not to exceed them. 
#      the total size of blob metadata was limited to 8 KB.

# --------------------------------------------------------------------------------
# Close the Connection
# --------------------------------------------------------------------------------

blob_service_client.close()

# After calling the close method, you shouldn't use the client for further operations unless you instantiate it again.

# Another way to ensure that resources are properly cleaned up is to use the BlobServiceClient as a context manager 
# with the "with" statement. When the with block is exited, the client's connection will be automatically closed:

# Closing connection with context manager
with BlobServiceClient.from_connection_string(connect_str) as client:
    # Use the client for various operations
    pass  # replace with your operations

# Here, outside of the with block, the client's connection is automatically closed.


# --------------------------------------------------------------------------------
# Connecting to Blob Via the constructor:
# --------------------------------------------------------------------------------

from azure.storage.blob import BlobServiceClient

account_name = 'YOUR_STORAGE_ACCOUNT_NAME'
account_key = 'YOUR_STORAGE_ACCOUNT_KEY'

blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
