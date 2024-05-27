
import quandl

import pymongo
import pandas as pd


import datetime as dt

from dateutil.relativedelta import *

def download_data(quandl_code, from_date,client):
    print("Downloading: [{}]".format(quandl_code))
    quandl.ApiConfig.api_key = 'MCEuJTEiwxznGzs3nKdv'
    df = quandl.get(quandl_code, start_date=from_date)
    data = pd.DataFrame(df)
    data.reset_index(inplace=True)
    import_mongodb_csv(data,'data_quandl',quandl_code.replace("/", "_"),client)

def import_mongodb_csv(data, db_name, coll_name, client):
    db = client[db_name]
    coll = db[coll_name]  
    if coll_name in db.list_collection_names():
        coll.drop()
    records = data.to_dict(orient='records')
    coll.insert_many(records)
    print('Đã thêm thành công:', coll_name)
    return coll.count_documents({})



def download_quandl():    
    fred_all = ('DFEDTAR', 'DFEDTARL', 'DFEDTARU', 'DFF', 'GDPC1', 'GDPPOT', 'PCEPILFE', 'CPIAUCSL', 'UNRATE', 'PAYEMS', 'RRSFS', 'HSN1F')
    ism_all = ('MAN_PMI', 'NONMAN_NMI')
    treasury_code = 'USTREASURY/YIELD'


    quandl.ApiConfig.api_key = 'MCEuJTEiwxznGzs3nKdv'
    from_date = '2000-01-01'
    try:
        dt.datetime.strptime(from_date, '%Y-%m-%d')
    except ValueError:
        print("from_date should be in yyyy-mm-dd format. You gave: ", from_date)

    all_data = True
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')


    if all_data:
        for dataset_code in fred_all:
            download_data("FRED/" + dataset_code, from_date,client)
        for dataset_code in ism_all:
            download_data("ISM/" + dataset_code, from_date,client)
        download_data(treasury_code, from_date,client)





