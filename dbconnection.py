from pymongo import MongoClient


def dbconnect():

    # Usage of try-catch block to establish connection to MongoDB
    try:
        client = MongoClient('localhost', 27017)
        print("connected to database")

    except:
        print("Could not connect")

    # Assigning database and collection
    db = client.nse

    return db
