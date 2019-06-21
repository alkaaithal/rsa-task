import pymongo
from pymongo import MongoClient
import pandas as pd
import glob

client = MongoClient('localhost', 27017)
db = client.nse
path = r'C:\Users\aithaa\Documents\test'  # use your path
all_files = glob.glob(path + "/*.csv")
li = []

for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    records_ = df.to_dict(orient='records')
    results = db.stocks.insert_many(records_)




""" for i in records_:

    db.create_collection(i['SYMBOL'])


 df = pd.read_csv("sec_bhavdata_full.csv")
records_ = df.to_dict(orient='records')

for i in records_:

    db.create_collection(i['SYMBOL'])



for i in records_:
    db.create_collection(i['SYMBOL'])
    posts = db.posts
    result = posts.insert_one(i)

    if i['SYMBOL'] == 'BLUEDART':
        db.BLUEDART.insert_one(i)
       """

        #client.update_one({'SYMBOL': i['SYMBOL']}, i, upsert=True)
#db.i['SYMBOL'].insert_one()
#db.update_one({'SYMBOL': i['SYMBOL']}, i, upsert=True)


# db.i['SYMBOL'].insert_many(i)
   #db.i['SYMBOL'].create_index(i['SYMBOL'], unique=True)
    #collist = db.list_collection_names()
    #if i['SYMBOL'] in collist:
     #   db.i['SYMBOL'].insert_many(i)
#        continue;

#        print(i['SYMBOL'])
#    db.create_collection(i['SYMBOL'])
#    if i['SYMBOL'] in records_:
