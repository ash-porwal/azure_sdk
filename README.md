# azure_sdk
The BlobClient is a class in Azure SDK for Python that allows you to interact with Azure Blob Storage.

BlobClient allows you to perform a wide variety of operations on blobs, such as creating, reading, updating, and deleting blobs. A blob is essentially a file of any type and size.

Here are some of the main methods and their uses:

__init__: The constructor for a BlobClient object. You need to provide the blob service endpoint, container name, blob name, and optionally, a credential.

upload_blob: Uploads a blob to the blob service.

download_blob: Downloads a blob from the blob service.

delete_blob: Deletes a blob from the blob service.

get_blob_properties: Returns the properties of the blob.

get_blob_metadata: Returns the metadata of the blob.

set_blob_metadata: Sets user-defined metadata for the blob.

get_blob_tags: Gets the tags associated with the blob.

set_blob_tags: Sets user-defined name-value pairs associated with the blob.

start_copy_from_url: Copies a blob asynchronously via a source URL.

Remember that when interacting with Blob Storage, you'll need appropriate permissions. This is typically handled through the credential you pass when creating the BlobClient.

BlobClient class is available in the azure-storage-blob

