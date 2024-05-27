# Python libraries
import pickle


import pymongo
import gridfs
# Data Science modules
import pandas as pd
from airflow.models import Variable



def get_pickle_from_mongodb(client,filename):
    fs = gridfs.GridFS(client)
    data = client.fs.files.find_one({'filename':filename})
    # df = pd.DataFrame(fs.get(data['_id']).read())

    return pickle.loads(fs.get(data['_id']).read())

def import_mongodb_csv(data, db_name, coll_name, client):
    db = client[db_name]
    coll = db[coll_name]
    records = data.to_dict(orient='records')
    coll.insert_many(records)
    print('Đã thêm thành công:', coll_name)
    return coll.count_documents({})

def import_mongodb_pickle(df,filename,client):
    fs = gridfs.GridFS(client)
    exist_data = client.fs.files.find_one({'filename':filename})
    data = pickle.dumps(df)
    if exist_data:
        fs.delete(exist_data['_id'])
    fs.put(data,filename=filename)
    print('Da them thanh cong:',filename)

def load_data_quandl_Preprocessed(ti):
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    nontext_data = get_pickle_from_mongodb(client.data_pickle,'nontext_data.pickle')
    nontext_ma2 = get_pickle_from_mongodb(client.data_pickle,'nontext_ma2.pickle')
    nontext_ma3 = get_pickle_from_mongodb(client.data_pickle,'nontext_ma3.pickle')
    nontext_ma6 = get_pickle_from_mongodb(client.data_pickle,'nontext_ma6.pickle')
    nontext_ma12 = get_pickle_from_mongodb(client.data_pickle,'nontext_ma12.pickle')

    ti.xcom_push(key='nontext_data',value = nontext_data)
    ti.xcom_push(key='nontext_ma2',value = nontext_ma2)
    ti.xcom_push(key='nontext_ma3',value = nontext_ma3)
    ti.xcom_push(key='nontext_ma6',value = nontext_ma6)
    ti.xcom_push(key='nontext_ma12',value = nontext_ma12) 
def process_data_quandl_to_train(ti):
    nontext_data = ti.xcom_pull(key='nontext_data')
    nontext_ma2 = ti.xcom_pull(key='nontext_ma2')
    nontext_ma3 = ti.xcom_pull(key='nontext_ma3')
    nontext_ma6 = ti.xcom_pull(key='nontext_ma6')
    nontext_ma12 = ti.xcom_pull(key='nontext_ma12')

    nontext_data['prev_decision'] = nontext_data['RateDecision'].shift(1)
    nontext_data['next_decision'] = nontext_data['RateDecision'].shift(-1)

    nontext_train_small = pd.concat([nontext_data[['RateDecision', 'prev_decision', 'GDP_diff_prev', 'PMI_value']],
                                 nontext_ma2[['Employ_diff_prev', 'Rsales_diff_year']], 
                                 nontext_ma3[['Unemp_diff_prev', 'Inertia_diff']], 
                                 nontext_ma12[['Hsales_diff_year', 'Balanced_diff']]], axis=1)
    nontext_train_small.rename(columns={'RateDecision': 'target'}, inplace=True)

    nontext_train_small['prev_decision'].fillna(0, inplace=True)
    # Retail sales growth ratio is difficult to estimate. Though it is not ideal, simply use the average
    nontext_train_small['Employ_diff_prev'].fillna(nontext_train_small['Employ_diff_prev'].mean(), inplace=True)
    nontext_train_small['Rsales_diff_year'].fillna(nontext_train_small['Rsales_diff_year'].mean(), inplace=True)
    nontext_train_small['Inertia_diff'].fillna(nontext_train_small['Inertia_diff'].mean(), inplace=True)
    nontext_train_small['Balanced_diff'].fillna(nontext_train_small['Balanced_diff'].mean(), inplace=True)
    nontext_train_small['target'].fillna(nontext_train_small['target'].mean(), inplace=True)
    nontext_train_small['GDP_diff_prev'].fillna(nontext_train_small['GDP_diff_prev'].mean(), inplace=True)
    nontext_train_small['PMI_value'].fillna(nontext_train_small['PMI_value'].mean(), inplace=True)
    nontext_train_small['Hsales_diff_year'].fillna(nontext_train_small['Hsales_diff_year'].mean(), inplace=True)
    nontext_train_small['Unemp_diff_prev'].fillna(nontext_train_small['Unemp_diff_prev'].mean(), inplace=True)


    Variable.set('nontext_train_small',nontext_train_small.to_json())

    ti.xcom_push(key = 'nontext_train_small',value = nontext_train_small)
def save_train_data(ti):
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    nontext_train_small = pd.read_json(Variable.get('nontext_train_small'))
    if nontext_train_small is None:
        nontext_train_small = ti.xcom_pull(task_ids = 'process_data',key='nontext_train_small')
    else:
        import_mongodb_pickle(nontext_train_small,'nontext_train_small',client.data_train_pickle)
        import_mongodb_csv(nontext_train_small,'data_train_csv','nontext_train_small',client)


