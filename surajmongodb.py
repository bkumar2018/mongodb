


from pymongo import MongoClient


def getMongoClient():
    client = MongoClient("mongodb://127.0.0.1:27017")
    print("Connection Successful")
    #client.close()
    return client
	
	
	
    
dr_details= {
"dr_id":"001",
"dr_name":"Ramesh",
"join_date":"20-12-2020",
"speciality":"Dentist",
"salary":"50000",
"experience":"4"
}

def add(dr_details):
    try:
        client = getMongoClient()
        print("Connected successfully!!!")
    except:  
        print("Could not connect to MongoDB")
    
    
    db = client[ "doctors" ] # makes a test database called "doctors"
    col = db[ "doctorcol" ] #makes a collection called "doctorcol" in the "doctors"
    rec_id1  = col.insert_one( dr_details) #add a document to doctors.doctorcol
    print("Data inserted with record ids",rec_id1)
    
    display(col)
    
    
def display(col):
    cursor = col.find()
    for record in cursor:
        print(record)
    

def update(dr_updateDetails):
    try:
        client = getMongoClient()
        print("Connected successfully!!!")
    except:  
        print("Could not connect to MongoDB")
  
    # database
    db = client.database
    # Created or Switched to collection names: my_gfg_collection
    collection = db.doctorcol
      
    # update all the employee data whose eid is 24
    result = collection.update_many(dr_updateDetails)
      
    print("Data updated with id",result)
      
    # Print the new record
    cursor = collection.find()
    for record in cursor:
        print(record)
    


def delete():
    try:
        client = getMongoClient()
        print("Connected successfully!!!")
    except:  
        print("Could not connect to MongoDB")
    
    # database
    db = client.database
    # Created or Switched to collection names: my_gfg_collection
    collection = db.doctorcol
    # Delete Document with name
    result = collection.delete_one({"dr_name":"Mr.Ramesh"})    
    
    cursor = collection.find()
    for record in cursor:
        print(record)

    
def exit(client):
    client.close()    


add(dr_details)
