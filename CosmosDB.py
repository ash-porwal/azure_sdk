# Author: Ashish Porwal
# Date Created: 06/Aug/2023
# Date Modified: 06/Aug/2023

# Refer here - https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-python-get-started?tabs=env-virtual%2Cazure-cli%2Cwindows

# Install library - pip install azure-cosmos


# Azure Cosmos DB uses a set of terminologies that differ from traditional relational databases. Here's the hierarchy and the correct terminologies for Cosmos DB:
# Cosmos DB Account > Database (or Cosmos DB Database) > Container > Items (or Documents)


from azure.cosmos import CosmosClient

url = "YOUR_COSMOS_DB_URL"
key = "YOUR_COSMOS_DB_KEY"

# --------------------------------------------------------------------------------
# Create a new instance of the CosmosClient
# --------------------------------------------------------------------------------

client = CosmosClient(url, credential=key)

# --------------------------------------------------------------------------------
# List all databases
# --------------------------------------------------------------------------------

databases = client.list_databases()

# Print database names
for db in databases:
    print(db['id'])

# --------------------------------------------------------------------------------
# Create a new database if it doesn't exist
# --------------------------------------------------------------------------------

database_name = 'TestDB'
database = client.create_database_if_not_exists(id=database_name)

# Create a new container if it doesn't exist
container_name = 'TestContainer'
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key_path="/somePath",
    offer_throughput=400
)

# Insert a new item
container.upsert_item({
    'id': '1',
    'somePath': 'partitionKeyValue',
    'otherProperty': 'value'
})

# --------------------------------------------------------------------------------
# Connect to a specific database
# --------------------------------------------------------------------------------

database_name = "YOUR_DATABASE_NAME"
database_client = client.get_database_client(database_name)

# --------------------------------------------------------------------------------
# List all the tables aka containers in DB
# --------------------------------------------------------------------------------

containers = database_client.list_containers()

# Print container names
for container in containers:
    print(container['id'])

# --------------------------------------------------------------------------------
# Connect to listed containers
# --------------------------------------------------------------------------------

# Name of the container you want to connect to
container_name = "YOUR_CONTAINER_NAME"

# Get a reference (client) for the specific container
container_client = database_client.get_container_client(container_name)

# --------------------------------------------------------------------------------
# Get data from those containers
# --------------------------------------------------------------------------------

# Define a SQL query
query = "SELECT * FROM c WHERE c.someProperty = 'someValue'"

# Execute the query against the container
items = container_client.query_items(query=query, enable_cross_partition_query=True)

# Iterate over and print the results
for item in items:
    print(item)

# --------------------------------------------------------------------------------
# Insert data in that container
# --------------------------------------------------------------------------------

new_item = {
    "id": "someUniqueId", # it's essential to include both a unique id and the partition key in the item you're inserting. 
    "property1": "value1",
    "property2": "value2",
    "partitionKeyProperty": "partitionKeyValue"
    # ... any additional properties ...
}

# Insert the item into the container
created_item = container_client.create_item(body=new_item)

# Print the created item
print(created_item)

# in above we can use upsert_item to insert data too
# if you're unsure whether the item already exists 
# and you want to either insert a new item or update an existing one, 
# you can use the upsert_item method. Upsert will create a new item 
# if it doesn't exist or replace an existing item if it does:

upserted_item = container_client.upsert_item(body=new_item)
print(upserted_item)

# --------------------------------------------------------------------------------
# Delete a particular query data
# --------------------------------------------------------------------------------

# Define a SQL query to find items based on some criteria
query = "SELECT * FROM c WHERE c.propertyName = 'desiredValue'"

# Query for the items you wish to delete
items_to_delete = container_client.query_items(query=query, enable_cross_partition_query=True)

# Iterate over the items and delete each one
for item in items_to_delete:
    partition_key_value = item['partitionKeyPropertyName']  # Adjust this to your actual partition key property name
    item_id = item['id']
    
    container_client.delete_item(item=item_id, partition_key=partition_key_value)

print("Deletion completed!")


# partitionKeyPropertyName in the code should be replaced with the actual name of your partition key property. 
# This is crucial because Cosmos DB requires both the item's id and the value of its partition key to delete it.

# --------------------------------------------------------------------------------
# Creating container with RUs
# --------------------------------------------------------------------------------

# Define container properties and throughput
container_properties = {
    'id': 'YOUR_CONTAINER_NAME',
    'partition_key': {
        'paths': ['/YOUR_PARTITION_KEY'],
        'kind': 'Hash'
    }
}
throughput = 400  # This is the RU setting. Adjust as needed.

# Create the container with the specified throughput
container_client = database_client.create_container_if_not_exists(container_properties, throughput=throughput)

print("Container created!")

'''
What is RUs?

    RU, or Request Unit, is a unit of measure in Azure Cosmos DB 
    used to represent the resources (like CPU, I/O, and memory) that are consumed by a database operation. 
    RUs are a way to abstract and simplify how you deal with performance in Cosmos DB.

    RUs provide predictable performance in Cosmos DB. 
    When you provision a certain number of RUs for a container or a database, Cosmos DB ensures that the amount of resources 
    (like compute and I/O) is available for your operations to achieve the performance level specified by the RUs.

    so, with more number of RUs we get compute power to do query in containers.

throughput?

    In the context of Azure Cosmos DB, 
    "throughput" refers to the total amount of resources available for operations within a specific time frame.
    It's essentially the rate at which operations are processed, and it's measured in Request Units per second (RU/s).

    RU/s: It refer to the number of Request Units (RUs) that can be consumed per second. 
    For instance, if you provision a throughput of 1000 RU/s, 
    this means you can use up to 1000 Request Units worth of operations every second.

    Summary - throughput in Cosmos DB is about ensuring you have the necessary resources (compute, memory, I/O) 
    to meet the demands of your application, and it's measured in terms of how many RUs you can consume every second
'''

# --------------------------------------------------------------------------------
# Copying data of one container to another
# --------------------------------------------------------------------------------

from azure.cosmos import CosmosClient

url = "YOUR_COSMOS_DB_ENDPOINT_URL"
key = "YOUR_COSMOS_DB_KEY"
database_name = "YOUR_DATABASE_NAME"

# Initialize Cosmos client and get database
client = CosmosClient(url, credential=key)
database = client.get_database_client(database_name)

# Source and destination containers
source_container = database.get_container_client("source_container_name")
destination_container = database.get_container_client("destination_container_name")

# Query all documents in the source container
documents = list(source_container.query_items(
    query="SELECT * FROM c",
    enable_cross_partition_query=True
))

# Insert each document into the destination container
for doc in documents:
    destination_container.upsert_item(doc)

# --------------------------------------------------------------------------------
# Delete Containers
# --------------------------------------------------------------------------------

# Initialize Cosmos client and get a reference to the database
client = CosmosClient(url, credential=key)
database = client.get_database_client(database_name)

# Delete the container
database.delete_container(container_name)