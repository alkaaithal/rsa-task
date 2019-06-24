import pymongo
from pymongo import MongoClient
import pandas as pd
import glob
from bson.objectid import ObjectId

company_id = ObjectId()

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
    for i in records_:

        x = db.company_info.insert_one({"symbol": i['SYMBOL'], "series": i[' SERIES'], "date1": i[' DATE1']})
        db.company_info.create_index(
            [("symbol", pymongo.DESCENDING), ("series", pymongo.ASCENDING), ("date1", pymongo.DESCENDING)],
            unique=True
        )
        db.stock_info.insert_one({"company_id": x.inserted_id, "prev_close": i[' PREV_CLOSE'], "open_price": i[' OPEN_PRICE'], "high_price": i[' HIGH_PRICE'], "low_price": i[' LOW_PRICE'], "last_price": i[' LAST_PRICE'], "close_price": i[' CLOSE_PRICE'], "avg_price": i[' AVG_PRICE']})
        db.trade_info.insert_one({"company_id": x.inserted_id, "ttl_trd_qnty": i[' TTL_TRD_QNTY'], "turnover_lacs": i[' TURNOVER_LACS'], "no_of_trades": i[' NO_OF_TRADES'], "deliv_qty": i[' DELIV_QTY'], "deliv_per": i[' DELIV_PER']})

db.company_info.aggregate([
    {
        "$lookup":
            {
                "from": "stock_info",
                "localField": "role_id",
                "foreignField": "_id",
                "as": "stock_info"
            }
    }
])
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
