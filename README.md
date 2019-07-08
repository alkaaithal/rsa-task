# rsa-task

MongoInsert.py reads all csv files in specified folder, iterate through all files and checks for SYMBOL and SERIES and inserts in one collection.
dbconnection.py returns mongodb connection instance.
stock and trade information relating to corresponding SYMBOL and SERIES is inserted in another collection using reference IDs of first colection.
