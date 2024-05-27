import base64
# Python libraries
import pickle
from tqdm.notebook import tqdm

import time
import random
from collections import  Counter
# Data Science modules
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

# Import Scikit-learn moduels
from sklearn.metrics import accuracy_score, f1_score
from sklearn.ensemble import RandomForestClassifier

from sklearn import model_selection
from sklearn.ensemble import RandomForestClassifier

import pandas as pd

from sklearn.model_selection import  RandomizedSearchCV

# Import Pytorch modules
from airflow.models import Variable

import pymongo
import gridfs

def get_pickle_from_mongodb(client,filename):
    fs = gridfs.GridFS(client)
    data = client.fs.files.find_one({'filename':filename})
    return pickle.loads(fs.get(data['_id']).read())

def load_data_base_line(ti):
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    train_df = get_pickle_from_mongodb(client.data_train_pickle,'nontext_train_small')
    Variable.set('train_df',train_df.to_json())
    ti.xcom_push(key = 'train_df',value = train_df)

def import_mongodb_pickle(df,filename,client):
    fs = gridfs.GridFS(client)
    exist_data = client.fs.files.find_one(filename)
    data = pickle.dumps(df)
    if exist_data:
        fs.delete(exist_data['_id'])
    fs.put(data,filename=filename)
    print('Da them thanh cong:',filename)
def import_mongodb_csv(data, db_name, coll_name, client):
    # Chuyển đổi giá trị NaT thành None
    data.fillna(0, inplace=True)
    db = client[db_name]
    coll = db[coll_name]  
    records = data.to_dict(orient='records')
    coll.insert_many(records)
    print('Đã thêm thành công:', coll_name)
    return coll.count_documents({})
def convert_class(x):
    if x == 1:
        return 3
    elif x == 0:
        return 2
    elif x == -1:
        return 1

def metric(y_true, y_pred):
    acc = accuracy_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred, average='macro')
    return acc, f1
def balance_the_classe(ti):
    train_df = ti.xcom_pull(key = 'train_df')

    n_hold = sum(1 for each in train_df['target'] if each == 0)
    N_examples = len(train_df)

    keep_prob = 1

    balanced = pd.concat([train_df.loc[train_df['target'] != 0], train_df.loc[train_df['target'] == 0].sample(frac=keep_prob, random_state=1)])
    balanced.sort_index(ascending=True, inplace=True)

    n_hold = sum(1 for each in balanced['target'] if each == 0)
    n_lower = sum(1 for each in balanced['target'] if each == -1)
    n_raise = sum(1 for each in balanced['target'] if each == 1)
    N_examples = len(balanced['target'])
    print('Hold: ', round(n_hold/N_examples, 2))
    print('Lower:', round(n_lower/N_examples, 2))
    print('Raise:', round(n_raise/N_examples, 2))

    Y_balanced = balanced['target'].map(convert_class)
    X_balanced = balanced.drop(columns=['target'])

    ti.xcom_push(key = 'X_balanced',value = X_balanced)
    ti.xcom_push(key = 'Y_balanced',value = Y_balanced.to_list())
    ti.xcom_push(key = 'balanced',value = balanced)
def create_train_test(ti):

    Y_balanced = ti.xcom_pull(task_ids = 'balance_the_classe',key = 'Y_balanced')
    X_balanced = ti.xcom_pull(task_ids = 'balance_the_classe',key = 'X_balanced')

    
    X_train, X_test, Y_train, Y_test = model_selection.train_test_split(X_balanced.values, np.array(Y_balanced), test_size=0.2, shuffle=False)

    print("Training Data: Total {}, {}".format(len(Y_train), Counter(Y_train)))
    print("Test Data: Total {}, {}".format(len(Y_test), Counter(Y_test)))

    
   
    ti.xcom_push(key = 'X_train',value = X_train.tolist())
    ti.xcom_push(key = 'X_test',value = X_test.tolist())
    ti.xcom_push(key = 'Y_train',value = Y_train.tolist())
    ti.xcom_push(key = 'Y_test',value = Y_test.tolist())



def process_RandomForestClassifier(ti):
    random.seed(42)
    np.random.seed(42)


    X_train = np.array(ti.xcom_pull(task_ids = 'create_train_test',key = 'X_train'))
    Y_train = np.array(ti.xcom_pull(task_ids = 'create_train_test',key = 'Y_train'))

    Y_test = np.array(ti.xcom_pull(task_ids = 'create_train_test',key = 'Y_test'))
    X_test = np.array(ti.xcom_pull(task_ids = 'create_train_test',key = 'X_test'))
    rf_clf = RandomForestClassifier()
    
    rf_clf.fit(X_train,Y_train)
   
    value = base64.b64encode(pickle.dumps(rf_clf)).decode('utf-8')

    Variable.set('rf_best',value)

    y_pred = rf_clf.predict(X_test)
    accuracy = accuracy_score(Y_test, y_pred)
    f1 = f1_score(Y_test, y_pred, average='weighted')  

    print(f"Accuracy: {accuracy:.2f}")
    print(f"F1 Score: {f1:.2f}")
def convert_class_predict(x):
    if x == 2:
        return 1
    elif x == 1:
        return 0
    elif x == 0:
        return -1

def print_result(ti):
    rf_clf  = pickle.loads(base64.b64decode(Variable.get('rf_best')))
    balanced = ti.xcom_pull(task_ids = 'balance_the_classe',key = 'balanced')
    print(balanced.iloc[-1].name)

    data_dudoan = balanced.iloc[1].drop('target').values.reshape(1, -1)
    rf_clf.predict(data_dudoan)[0]
    rate = convert_class_predict(rf_clf.predict(data_dudoan)[0])

    print('result:', rate)
    if rate == 0:
        print('Lãi suất giữa nguyên')
    elif rate == 1:
        print('Lãi suất tăng')
    elif rate == -1:
        print('Lãi suất giảm')
