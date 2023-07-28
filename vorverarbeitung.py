from pymongo import MongoClient
import random
from pymongo import UpdateOne


# Datenbankverbindung
client = MongoClient("mongodb://localhost:27017/")
db = client['Operations']
collection = db['Filtered5']

# Generierung des Index int 0-number of clusters
def update_index(seed):
    random.seed(seed)
    total_documents = collection.count_documents({})
    for document in collection.find():
        random_number = random.randint(0, total_documents)
        collection.update_one({'_id': document['_id']}, {'$set': {'random_number': random_number}})

    collection.create_index([('random_number', 1)],name='random_number_index')

update_index(46)
print("process done")
