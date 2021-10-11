from pymongo import MongoClient


def getMongoClient():
    client = MongoClient("mongodb://127.0.0.1:27017")
    print("Connection Successful")
    #client.close()
    return client


client = getMongoClient()
# Create a new collection
dbname = client["user_1_db"]
collection_name = dbname['user_1_collection']

item_1 = {
"_id" : "U1IT00001",
"item_name" : "Blender",
"max_discount" : "10%",
"batch_number" : "RR450020FRG",
"price" : 340,
"category" : "kitchen appliance"
}

item_2 = {
"_id" : "U1IT00002",
"item_name" : "Egg",
"category" : "food",
"quantity" : 12,
"price" : 36,
"item_description" : "brown country eggs"
}

#collection_name.insert_many([item_1, item_2])
#collection_name.insert_one(item_1)
item_details = collection_name.find()
for item in item_details:
    # This does not give a very readable output
    print(item)

client.close()


#
# client = pymongo.MongoClient()
# db = client[ "testdb" ] # makes a test database called "testdb"
# col = db[ "testcol" ] #makes a collection called "testcol" in the "testdb"
# col.insert_one( {"foo" : "bar" }) #add a document to testdb.testcol
