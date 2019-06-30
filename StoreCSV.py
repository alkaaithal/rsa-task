import pymongo
import pandas as pd
import glob
from bson.objectid import ObjectId
from dbconnection import dbconnect
from unique import unique2

company_id = ObjectId()

path = r'C:\Users\aithaa\Documents\test'    # path of folder in which all csv files are stored
all_files = glob.glob(path + "/*.csv")
li = []
symbol_lst = {}
series_lst = {}
all_list = []
all1_list = []
half_dict = {}
final_dict = {}
id = []

db = dbconnect()
unique = {}
date_lst = []
prev_lst = {}
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    records_ = df.to_dict(orient='records')
    for i in records_:
        symbol_lst = {key: value for key, value in i.items() if key in 'SYMBOL'}
        series_lst = {key: value for key, value in i.items() if key in ' SERIES'}

        date = {key: value for key, value in i.items() if key in ' DATE1'}
        prev_lst = {key: value for key, value in i.items() if key in ' PREV_CLOSE'}
        open_lst = {key: value for key, value in i.items() if key in ' OPEN_PRICE'}
        high_lst = {key: value for key, value in i.items() if key in ' HIGH_PRICE'}
        low_lst = {key: value for key, value in i.items() if key in ' LOW_PRICE'}
        last_lst = {key: value for key, value in i.items() if key in ' LAST_PRICE'}
        close_lst = {key: value for key, value in i.items() if key in ' CLOSE_PRICE'}
        avg_lst = {key: value for key, value in i.items() if key in ' AVG_PRICE'}
        qnty_lst = {key: value for key, value in i.items() if key in ' TTL_TRD_QNTY'}
        turnover_lst = {key: value for key, value in i.items() if key in ' TURNOVER_LACS'}
        trade_lst = {key: value for key, value in i.items() if key in ' NO_OF_TRADES'}
        deliv_qty_lst = {key: value for key, value in i.items() if key in ' DELIV_QTY'}
        deliv_per_lst = {key: value for key, value in i.items() if key in ' DELIV_PER'}

        symbol_lst.update(series_lst)
        all_list.append(symbol_lst)

        prev_lst.update(open_lst)
        prev_lst.update(high_lst)
        prev_lst.update(low_lst)
        prev_lst.update(last_lst)
        prev_lst.update(close_lst)
        prev_lst.update(avg_lst)
        prev_lst.update(qnty_lst)
        prev_lst.update(turnover_lst)
        prev_lst.update(trade_lst)
        prev_lst.update(deliv_qty_lst)
        prev_lst.update(deliv_per_lst)

        date_lst.append(date)
        all1_list.append(prev_lst)

unique = unique2(all_list)

for j in unique:
    x = db.company_info.insert_one({"symbol": j['SYMBOL'], "series": j[' SERIES']})
    id.append(x)
    """db.company_info.create_index(
        [("symbol", pymongo.DESCENDING), ("series", pymongo.ASCENDING)],
        unique=True
    )"""

for (i, k, j) in zip(id, all1_list, date_lst):
    db.stock_info.insert_many([{"company_id": i.inserted_id, j[' DATE1']: [{"prev_close": k[' PREV_CLOSE'], "open_price": k[' OPEN_PRICE'], "high_price": k[' HIGH_PRICE'], "low_price": k[' LOW_PRICE'], "last_price": k[' LAST_PRICE'], "close_price": k[' CLOSE_PRICE'], "avg_price": k[' AVG_PRICE'], "ttl_trd_qnty": k[' TTL_TRD_QNTY'], "turnover_lacs": k[' TURNOVER_LACS'], "no_of_trades": k[' NO_OF_TRADES'], "deliv_qty": k[' DELIV_QTY'], "deliv_per": k[' DELIV_PER']}]}])
    db.stock_info.update_one({"company_id": i.inserted_id, j[' DATE1']: {"$exists": False}}, {"$set": {j[' DATE1']: {"prev_close": k[' PREV_CLOSE'], "open_price": k[' OPEN_PRICE'], "high_price": k[' HIGH_PRICE'], "low_price": k[' LOW_PRICE'], "last_price": k[' LAST_PRICE'], "close_price": k[' CLOSE_PRICE'], "avg_price": k[' AVG_PRICE'], "ttl_trd_qnty": k[' TTL_TRD_QNTY'], "turnover_lacs": k[' TURNOVER_LACS'], "no_of_trades": k[' NO_OF_TRADES'], "deliv_qty": k[' DELIV_QTY'], "deliv_per": k[' DELIV_PER']}}})

    #db.stock_info.update_one({"company_id": i.inserted_id}, {"$set": {"test": {"prev_close": k[' PREV_CLOSE'], "open_price": k[' OPEN_PRICE'], "high_price": k[' HIGH_PRICE'], "low_price": k[' LOW_PRICE'], "last_price": k[' LAST_PRICE'], "close_price": k[' CLOSE_PRICE'], "avg_price": k[' AVG_PRICE'], "ttl_trd_qnty": k[' TTL_TRD_QNTY'], "turnover_lacs": k[' TURNOVER_LACS'], "no_of_trades": k[' NO_OF_TRADES'], "deliv_qty": k[' DELIV_QTY'], "deliv_per": k[' DELIV_PER']}}})
    #db.stock_info.insert_many([{"date1": j[' DATE1'], "prev_close": k[' PREV_CLOSE'], "open_price": k[' OPEN_PRICE'], "high_price": k[' HIGH_PRICE'], "low_price": k[' LOW_PRICE'], "last_price": k[' LAST_PRICE'], "close_price": k[' CLOSE_PRICE'], "avg_price": k[' AVG_PRICE'], "ttl_trd_qnty": k[' TTL_TRD_QNTY'], "turnover_lacs": k[' TURNOVER_LACS'], "no_of_trades": k[' NO_OF_TRADES'], "deliv_qty": k[' DELIV_QTY'], "deliv_per": k[' DELIV_PER']}])
#for (k, i) in zip(all1_list, id):
    #db.stock_info.insert_one({"company_id": i.inserted_id, "date1": k[' DATE1'], "prev_close": k[' PREV_CLOSE'], "open_price": k[' OPEN_PRICE'], "high_price": k[' HIGH_PRICE'], "low_price": k[' LOW_PRICE'], "last_price": k[' LAST_PRICE'], "close_price": k[' CLOSE_PRICE'], "avg_price": k[' AVG_PRICE'], "ttl_trd_qnty": k[' TTL_TRD_QNTY'], "turnover_lacs": k[' TURNOVER_LACS'], "no_of_trades": k[' NO_OF_TRADES'], "deliv_qty": k[' DELIV_QTY'], "deliv_per": k[' DELIV_PER']})
