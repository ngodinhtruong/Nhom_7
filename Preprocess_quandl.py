

import pymongo
import pandas as pd
import gridfs
import pickle

import datetime as dt


import numpy as np
from dateutil.relativedelta import *
from airflow.models import Variable
def get_pickle_from_mongodb(client,filename):
    fs = gridfs.GridFS(client)
    data = client.fs.files.find_one({'filename':filename})
    return fs.get(data['_id']).read()
def get_csv_from_mongodb(client,db_name,coll_name):
    db = client[db_name]
    collection = db[coll_name]
    data = list(collection.find({},{'_id': False}))
    return pd.DataFrame(data)
def import_mongodb_csv(data, db_name, coll_name, client):
    db = client[db_name]
    coll = db[coll_name]  
    records = data.to_dict(orient='records')
    coll.insert_many(records)
    print('Đã thêm thành công:', coll_name)
    return coll.count_documents({})

def import_mongodb_pickle(df,filename,client):
    fs = gridfs.GridFS(client)
    exist_data = client.fs.files.find_one(filename)
    data = pickle.dumps(df)
    if exist_data:
        fs.delete(exist_data['_id'])
    fs.put(data,filename=filename)
    print('Da them thanh cong:',filename)
def load_data_calendar(ti):
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')

    print('Loading calendar....')
    calendar = get_pickle_from_mongodb(client.data_pickle,'calendar.pickle')
    fomc_calendar = pickle.loads(calendar)
    fomc_calendar = pd.DataFrame(fomc_calendar)
    # print(fomc_calendar.head())
    print('Successfully load calendar !!!')
    return fomc_calendar

def load_data_fedrate_df():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')

    print('Loading fedtar/fedtaru/fedtarl....')
    fedtar = get_csv_from_mongodb(client,'data_quandl','FRED_DFEDTAR')
    fedtaru =  get_csv_from_mongodb(client,'data_quandl','FRED_DFEDTARU')
    fedtarl =  get_csv_from_mongodb(client,'data_quandl','FRED_DFEDTARL')
    fedtar.rename(columns={'DATE':'Date','VALUE': 'Rate'}, inplace=True)
    fedtaru.rename(columns={'DATE':'Date','VALUE': 'Rate'}, inplace=True)
    fedtarl.rename(columns={'DATE':'Date','Value': 'Rate'}, inplace=True)
    fedrate_df = pd.concat([fedtar, fedtarl], axis=0)
    fedrate_df.index = pd.to_datetime(fedrate_df.Date, format="%Y-%m-%d")
    fedrate_df.drop(columns=['Date'], inplace=True)
    fedrate_df['Rate'] = fedrate_df['Rate'].map(lambda x: float(x))
    # Add difference from previous value
    fedrate_df['diff'] = fedrate_df['Rate'].diff()
    # print(fedrate_df.head())
    print('Successfully load fedtar/fedtaru/fedtarl !!!')
    return fedrate_df
def load_data_FRED_DFF():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading FRED_DFF....')
    dff = get_csv_from_mongodb(client,'data_quandl','FRED_DFF')
    dff.rename(columns={'Date':'DATE','Value': 'DFF'}, inplace=True)
    dff = dff.set_index(pd.to_datetime(dff['DATE'], format="%Y-%m-%d")).drop(columns=['DATE'])
    # Add difference from previous value
    dff['diff'] = dff['DFF'].diff()
    # print(dff.head())
    print('Successfully load FRED_DFF !!!')
    return dff
def load_data_FRED_GDPC1():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading FRED_GDPC1....')
    gdpc1 = get_csv_from_mongodb(client,'data_quandl','FRED_GDPC1')
    gdpc1.rename(columns={'Date':'DATE','Value': 'GDPC1'}, inplace=True)
    gdpc1 = gdpc1.set_index(pd.to_datetime(gdpc1['DATE'], format="%Y-%m-%d")).drop(columns=['DATE'])
    # Add difference from previous value
    gdpc1['diff_prev'] = gdpc1['GDPC1'].diff() / gdpc1['GDPC1'].shift(1) * 100
    gdpc1['diff_year'] = gdpc1['GDPC1'].diff(periods=4) / gdpc1['GDPC1'].shift(4) * 100
    # print(gdpc1.head())
    print('Successfully load complete load FRED_GDPC1 !!!')
    return gdpc1

def load_data_FRED_GDPPOT():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading FRED_GDPPOT....')
    gdppot = get_csv_from_mongodb(client,'data_quandl','FRED_GDPPOT',)
    gdppot.rename(columns={'Date':'DATE','Value': 'GDPPOT'},inplace = True)
    gdppot = gdppot.set_index(pd.to_datetime(gdppot['DATE'], format="%Y-%m-%d")).drop(columns=['DATE'])
    # Add difference from previous value
    gdppot['diff_prev'] = gdppot['GDPPOT'].diff() / gdppot['GDPPOT'].shift(1) * 100
    gdppot['diff_year'] = gdppot['GDPPOT'].diff(periods=4) / gdppot['GDPPOT'].shift(4) * 100
    # print(gdppot.head())
    print('Successfully load FRED_GDPPOT !!!')
    return gdppot

def load_data_FRED_PCEPILFE():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading FRED_PCEPILFE....')
    pcepilfe = get_csv_from_mongodb(client,'data_quandl','FRED_PCEPILFE')
    pcepilfe.rename(columns={'Date':'DATE','Value':'PCEPILFE'},inplace = True)
    pcepilfe = pcepilfe.set_index(pd.to_datetime(pcepilfe['DATE'], format="%Y-%m-%d")).drop(columns=['DATE'])
    # Add difference from previous value
    pcepilfe['diff_prev'] = pcepilfe['PCEPILFE'].diff() / pcepilfe['PCEPILFE'].shift(1) * 100
    pcepilfe['diff_year'] = pcepilfe['PCEPILFE'].diff(periods=12) / pcepilfe['PCEPILFE'].shift(12) * 100
    print('Successfully load FRED_PCEPILFE !!!')
    return pcepilfe

def load_data_FRED_CPIAUCSL():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading FRED_CPIAUCSL....')
    cpiaucsl = get_csv_from_mongodb(client,'data_quandl','FRED_CPIAUCSL')
    cpiaucsl.rename(columns={'Date':'DATE','Value':'CPIAUCSL'},inplace = True)
    cpiaucsl = cpiaucsl.set_index(pd.to_datetime(cpiaucsl['DATE'], format="%Y-%m-%d")).drop(columns=['DATE'])
    # Add difference from previous value
    cpiaucsl['diff_prev'] = cpiaucsl['CPIAUCSL'].diff() / cpiaucsl['CPIAUCSL'].shift(1) * 100
    cpiaucsl['diff_year'] = cpiaucsl['CPIAUCSL'].diff(periods=12) / cpiaucsl['CPIAUCSL'].shift(12) * 100
    # print(cpiaucsl.head())
    print('Succesfully load FRED_CPIAUCSL !!!')
    return cpiaucsl

def load_data_FRED_UNRATE():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading FRED_UNRATE....')
    unrate = get_csv_from_mongodb(client,'data_quandl','FRED_UNRATE')
    unrate.rename(columns={'Date':'DATE','Value':'UNRATE'},inplace = True)
    unrate = unrate.set_index(pd.to_datetime(unrate['DATE'], format="%Y-%m-%d")).drop(columns=['DATE'])
    unrate['UNRATE'] = unrate['UNRATE'] * -1
    # Add difference from previous value
    unrate['diff_prev'] = unrate['UNRATE'].diff()
    unrate['diff_year'] = unrate['UNRATE'].diff(periods=12)
    # print(unrate.head())
    print('Succesfully load FRED_UNRATE !!!')
    Variable.set('unrate',unrate.to_csv())
    return unrate
def load_data_FRED_PAYEMS():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading FRED_PAYEMS....')
    payems = get_csv_from_mongodb(client,'data_quandl','FRED_PAYEMS')
    payems.rename(columns={'Date':'DATE','Value':'PAYEMS'},inplace = True)
    payems = payems.set_index(pd.to_datetime(payems['DATE'], format="%Y-%m-%d")).drop(columns=['DATE'])
    # Add difference from previous value
    payems['diff_prev'] = payems['PAYEMS'].diff()
    payems['diff_year'] = payems['PAYEMS'].diff(periods=12)
    # print(payems.head())
    print('Succesfully load FRED_PAYEMS !!!')
    return payems
def load_data_ISM_MAN_PMI():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading ISM_MAN_PMI....')
    ism_pmi = get_csv_from_mongodb(client,'data_quandl','ISM_MAN_PMI')
    ism_pmi.rename(columns={'DATE':'Date','Value':'PMI'},inplace = True)
    ism_pmi = ism_pmi.sort_values(by=['Date'], ascending=True)
    ism_pmi = ism_pmi.set_index(pd.to_datetime(ism_pmi['Date'], format="%Y-%m-%d")).drop(columns=['Date'])
    # Add difference from previous value
    ism_pmi['diff_prev'] = ism_pmi['PMI'].diff()
    ism_pmi['diff_year'] = ism_pmi['PMI'].diff(periods=12)
    # print(ism_pmi.head())
    print('Succesfully load ISM_MAN_PMI !!!')
    return ism_pmi
def load_data_ISM_NONMAN_NMI():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading ISM_NONMAN_NMI....')
    ism_nmi = get_csv_from_mongodb(client,'data_quandl','ISM_NONMAN_NMI')
    ism_nmi.rename(columns={'DATE':'Date','Value':'NMI'},inplace = True)
    ism_nmi = ism_nmi.sort_values(by=['Date'], ascending=True)
    ism_nmi.columns = ['Date', 'NMI']
    ism_nmi = ism_nmi.set_index(pd.to_datetime(ism_nmi['Date'], format="%Y-%m-%d")).drop(columns=['Date'])
    # Add difference from previous value
    ism_nmi['diff_prev'] = ism_nmi['NMI'].diff()
    ism_nmi['diff_year'] = ism_nmi['NMI'].diff(periods=12)
    # print(ism_nmi.head())
    print('Succesfully load ISM_NONMAN_NMI !!!')
    return ism_nmi
def load_data_FRED_RRSFS():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading FRED_RRSFS....')
    rrsfs = get_csv_from_mongodb(client,'data_quandl','FRED_RRSFS')
    rrsfs.rename(columns={'Date':'DATE','Value':'RRSFS'},inplace = True)
    rrsfs = rrsfs.set_index(pd.to_datetime(rrsfs['DATE'], format="%Y-%m-%d")).drop(columns=['DATE'])
    # Add difference from previous value
    rrsfs['diff_prev'] = rrsfs['RRSFS'].diff() / rrsfs['RRSFS'].shift(1) * 100
    rrsfs['diff_year'] = rrsfs['RRSFS'].diff(periods=12) / rrsfs['RRSFS'].shift(12) * 100
    # print(rrsfs.head())
    print('Succesfully load FRED_RRSFS !!!')
    return rrsfs
def load_data_FRED_HSN1F():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading FRED_HSN1F....')
    hsn1f = get_csv_from_mongodb(client,'data_quandl','FRED_HSN1F')
    hsn1f.rename(columns={'Date':'DATE','Value':'HSN1F'},inplace = True)
    hsn1f = hsn1f.set_index(pd.to_datetime(hsn1f['DATE'], format="%Y-%m-%d")).drop(columns=['DATE'])
    # Add difference from previous value
    hsn1f['diff_prev'] = hsn1f['HSN1F'].diff() / hsn1f['HSN1F'].shift(1) * 100
    hsn1f['diff_year'] = hsn1f['HSN1F'].diff(periods=12) / hsn1f['HSN1F'].shift(12) * 100
    # print(rrsfs.head())
    print('Succesfully load FRED_HSN1F !!!')
    return hsn1f
def load_data_USTREASURY_YIELD():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    print('Loading USTREASURY_YIELD....')
    treasury_q = get_csv_from_mongodb(client,'data_quandl','USTREASURY_YIELD')
    treasury_q = treasury_q.set_index(pd.to_datetime(treasury_q['Date'], format="%Y-%m-%d")).drop(columns=['Date'])
    # print(treasury_q.head())
    print('Succesfully load USTREASURY_YIELD !!!')
    return treasury_q
def get_chairperson(x):
    chairpersons = pd.DataFrame(
    data=[["Volcker", "Paul", dt.datetime(1979,8,6), dt.datetime(1987,8,10)],
          ["Greenspan", "Alan", dt.datetime(1987,8,11), dt.datetime(2006,1,31)], 
          ["Bernanke", "Ben", dt.datetime(2006,2,1), dt.datetime(2014,1,31)], 
          ["Yellen", "Janet", dt.datetime(2014,2,3), dt.datetime(2018,2,3)],
          ["Powell", "Jerome", dt.datetime(2018,2,5), dt.datetime(2024,12,30)]],
    columns=["Surname", "FirstName", "FromDate", "ToDate"])
    '''
    Return a tuple of chairperson's Fullname for the given date x.
    '''
    # If x is string, convert to datetime
    if type(x) is str:
        try:
            x = dt.datetime.strftime(x, '%Y-%m-%d')
        except:
            return None
    
    chairperson = chairpersons.loc[chairpersons['FromDate'] <= x].loc[x <= chairpersons['ToDate']]
    return list(chairperson.FirstName)[0] + " " + list(chairperson.Surname)[0]

def get_available_latest(train_df, index_df, value_col, diff_prev_col, diff_year_col, date_offset):
    
    
    date_list, value_list, diff_prev_list, diff_year_list = [], [], [], []
    
    for i, row_data in train_df.iterrows():
        #print(row_data.name)
        not_available = True
        for j, row_index in index_df.sort_index(ascending=False).iterrows():
            if row_data.name > row_index.name + date_offset:
                #print("   matched: ", row_index.name)
                date_list.append(row_index.name)
                value_list.append(row_index[value_col])
                diff_prev_list.append(row_index[diff_prev_col])
                diff_year_list.append(row_index[diff_year_col])
                not_available = False
                break
        if not_available:
            date_list.append(None)
            value_list.append(None)
            diff_prev_list.append(None)
            diff_year_list.append(None)
    if train_df.shape[0] != len(date_list):
        print("train_df has {} rows but returned {} rows from index_df!".format(train_df.shape[0], len(date_list)))
    
    return date_list, value_list, diff_prev_list, diff_year_list  
  
def add_market_data(ti,calendar_df,window=1):
    gdpc1 = ti.xcom_pull(task_ids = 'load_data_FRED_GDPC1')
    gdppot =ti.xcom_pull(task_ids = 'load_data_FRED_GDPPOT')
    pcepilfe = ti.xcom_pull(task_ids = 'load_data_FRED_PCEPILFE')
    cpiaucsl = ti.xcom_pull(task_ids = 'load_data_FRED_CPIAUCSL')
    unrate = ti.xcom_pull(task_ids = 'load_data_FRED_UNRATE')
    payems =ti.xcom_pull(task_ids = 'load_data_FRED_PAYEMS')
    ism_pmi = ti.xcom_pull(task_ids = 'load_data_ISM_MAN_PMI')
    ism_nmi = ti.xcom_pull(task_ids = 'load_data_ISM_NONMAN_NMI')
    rrsfs = ti.xcom_pull(task_ids = 'load_data_FRED_RRSFS')
    hsn1f = ti.xcom_pull(task_ids = 'load_data_FRED_HSN1F')


    # First copy the caleander dataframe and drop Rate is NaN because this is the answer
    df = calendar_df.copy(deep=True)
    df.dropna(subset=['Rate'], inplace=True)
    
    # GDP is announced quarterly, the end of following month (preliminary)
    print("Processing GDP...")
    df['GDP_date'], df['GDP_value'], df['GDP_diff_prev'], df['GDP_diff_year'] \
    = get_available_latest(df, gdpc1.rolling(window).mean(), 'GDPC1', 'diff_prev', 'diff_year', relativedelta(months=+4, days=-2))

    print("Processing Potential GDP...")
    df['GDPPOT_date'], df['GDPPOT_value'], df['GDPPOT_diff_prev'], df['GDPPOT_diff_year'] \
    = get_available_latest(df, gdppot.rolling(window).mean(), 'GDPPOT', 'diff_prev', 'diff_year', relativedelta(months=+4, days=-2))

    # PCE is announced monthly, at the end of following month
    print("Processing PCE...")
    df['PCE_date'], df['PCE_value'], df['PCE_diff_prev'], df['PCE_diff_year'] \
    = get_available_latest(df, pcepilfe.rolling(window).mean(), 'PCEPILFE', 'diff_prev', 'diff_year', relativedelta(months=+2, days=-1))

    # CPI is announced monthly, around 10th of the following month
    print("Processing CPI...")
    df['CPI_date'], df['CPI_value'], df['CPI_diff_prev'], df['CPI_diff_year'] \
    = get_available_latest(df, cpiaucsl.rolling(window).mean(), 'CPIAUCSL', 'diff_prev', 'diff_year', relativedelta(months=+1, days=+9))

    # Employment statistics is announced monthly, around 3rd of the following month
    print("Processing Unemployemnt...")
    df['Unemp_date'], df['Unemp_value'], df['Unemp_diff_prev'], df['Unemp_diff_year'] \
    = get_available_latest(df, unrate.rolling(window).mean(), 'UNRATE', 'diff_prev', 'diff_year', relativedelta(months=+1, days=+2))

    # Employment statistics is announced monthly, around 3rd of the following month
    print("Processing Employment...")
    df['Employ_date'], df['Employ_value'], df['Employ_diff_prev'], df['Employ_diff_year'] \
    = get_available_latest(df, payems.rolling(window).mean(), 'PAYEMS', 'diff_prev', 'diff_year', relativedelta(months=+1, days=+2))

    # ISM PMI is areannounced monthly, around 1st of the following month
    print("Processing ISM PMI...")
    df['PMI_date'], df['PMI_value'], df['PMI_diff_prev'], df['PMI_diff_year'] \
    = get_available_latest(df, ism_pmi.rolling(window).mean(), 'PMI', 'diff_prev', 'diff_year', relativedelta(months=+1, days=+2))

    # ISM NMI is areannounced monthly, around 1st of the following month
    print("Processing ISM NMI...")
    df['NMI_date'], df['NMI_value'], df['NMI_diff_prev'], df['NMI_diff_year'] \
    = get_available_latest(df, ism_nmi.rolling(window).mean(), 'NMI', 'diff_prev', 'diff_year', relativedelta(months=+1, days=+2))

    # Retail Sales is announced monthly, around 15th of the following month
    print("Processing Retail Sales...")
    df['Rsales_date'], df['Rsales_value'], df['Rsales_diff_prev'], df['Rsales_diff_year'] \
    = get_available_latest(df, rrsfs.rolling(window).mean(), 'RRSFS', 'diff_prev', 'diff_year', relativedelta(months=+1, days=+2))

    # New Home Sales is announced monthly, around a week before the end of the following month
    print("Processing New Home Sales...")
    df['Hsales_date'], df['Hsales_value'], df['Hsales_diff_prev'], df['Hsales_diff_year'] \
    = get_available_latest(df, hsn1f.rolling(window).mean(), 'HSN1F', 'diff_prev', 'diff_year', relativedelta(months=+1, days=+2))
    
    return df

def create_taylor(ti):
    
    fedrate_df = ti.xcom_pull(task_ids = 'load_data_fedrate_df')
    gdpc1 = ti.xcom_pull(task_ids = 'load_data_FRED_GDPC1')
    gdppot = ti.xcom_pull(task_ids = 'load_data_FRED_GDPPOT')
    pcepilfe = ti.xcom_pull(task_ids = 'load_data_FRED_PCEPILFE')
    taylor = fedrate_df.copy(deep=True)
    print('Create_taylor[Y]....')
    # Obtain available index used to calculate Taylor rule each day
    _, taylor['Y'],_,_ = get_available_latest(taylor, gdpc1, 'GDPC1', 'diff_prev', 'diff_year', relativedelta(months=+4, days=-2))

    print('Create_taylor[Yp]....')
    _, taylor['Yp'],_,_ = get_available_latest(taylor, gdppot, 'GDPPOT', 'diff_prev', 'diff_year', relativedelta(months=+4, days=-2))
    taylor['Y-Yp'] = (np.log(taylor['Y']*10**9) - np.log(taylor['Yp']*10**9)) * 100
    print('Create_taylor[Pi]....')
    _, _,_, taylor['Pi'] = get_available_latest(taylor, pcepilfe, 'PCEPILFE', 'diff_prev', 'diff_year', relativedelta(months=+2, days=-1))
    taylor['Pi*'] = 2
    taylor['Pi-Pi*'] = taylor['Pi'] - taylor['Pi*']

    taylor['r'] = 2

    taylor['Taylor'] = taylor['r'] + taylor['Pi'] + 0.5 * taylor['Pi-Pi*'] + 0.5 * taylor['Y-Yp']


    # Calculate Balanced-approach Rule
    taylor['Balanced'] = (taylor['r'] + taylor['Pi'] + 0.5 * taylor['Pi-Pi*'] + taylor['Y-Yp']).map(lambda x: 0 if x < 0 else x)

    # Calculate Inertia Rule
    taylor['Inertia'] = 0.85 * taylor['Rate'] - 0.15 * taylor['Balanced']

    # Drop unnecessary columns
    taylor = taylor.drop(columns = ['Y', 'Yp', 'Y-Yp', 'Pi', 'Pi*', 'Pi-Pi*', 'r', 'diff'])

    taylor['Taylor-Rate'] = taylor['Taylor'] - taylor['Rate']
    taylor['Balanced-Rate'] = taylor['Balanced'] - taylor['Rate']
    taylor['Inertia-Rate'] = taylor['Inertia'] - taylor['Rate']

    taylor['Taylor_diff'] = taylor['Taylor'].diff(1)
    taylor['Balanced_diff'] = taylor['Balanced'].diff(1)
    taylor['Inertia_diff'] = taylor['Inertia'].diff(1)
    Variable.set('taylor',taylor.to_json())

def add_taylor(df,window=1):
    
    taylor = pd.read_json(Variable.get('taylor'))
    taylor_ma = taylor.rolling(window).mean()
    df['Taylor'] = df.index.map(lambda x: taylor_ma.loc[taylor_ma.index == x + relativedelta(days=-1), 'Taylor'].values[0])
    df['Balanced'] = df.index.map(lambda x: taylor_ma.loc[taylor_ma.index == x + relativedelta(days=-1), 'Balanced'].values[0])
    df['Inertia'] = df.index.map(lambda x: taylor_ma.loc[taylor_ma.index == x + relativedelta(days=-1), 'Inertia'].values[0])
    df['Taylor-Rate'] = df.index.map(lambda x: taylor_ma.loc[taylor_ma.index == x + relativedelta(days=-1), 'Taylor-Rate'].values[0])
    df['Balanced-Rate'] = df.index.map(lambda x: taylor_ma.loc[taylor_ma.index == x + relativedelta(days=-1), 'Balanced-Rate'].values[0])
    df['Inertia-Rate'] = df.index.map(lambda x: taylor_ma.loc[taylor_ma.index == x + relativedelta(days=-1), 'Inertia-Rate'].values[0])
    df['Taylor_diff'] = df['Taylor'].diff(1)
    df['Balanced_diff'] = df['Balanced'].diff(1)
    df['Inertia_diff'] = df['Inertia'].diff(1)
    return df

def process_fomc_caledar(ti):
    fomc_calendar= ti.xcom_pull(task_ids  ='load_data_calendar' )
    fedrate_df =ti.xcom_pull(task_ids  ='load_data_fedrate_df' )
    fomc_calendar = fomc_calendar.loc[fomc_calendar['date'] >= dt.datetime(1982, 9, 27)]
    fomc_calendar.set_index('date', inplace=True)
    fomc_calendar['ChairPerson'] = fomc_calendar.index.map(get_chairperson)

    rate_list = []
    decision_list = []
    rate_diff_list = []

    for i in range(len(fomc_calendar)):
        not_found = True
        for j in range(len(fedrate_df)):
            if fomc_calendar.index[i] == fedrate_df.index[j]:
                not_found = False
                rate_list.append(float(fedrate_df['Rate'].iloc[j+3]))
                rate_diff_list.append(float(fedrate_df['Rate'].iloc[j+3]) - float(fedrate_df['Rate'].iloc[j-1]))
                if fedrate_df['Rate'].iloc[j-1] == fedrate_df['Rate'].iloc[j+3]:
                    decision_list.append(0)
                elif fedrate_df['Rate'].iloc[j-1] < fedrate_df['Rate'].iloc[j+3]:
                    decision_list.append(1)
                elif fedrate_df['Rate'].iloc[j-1] > fedrate_df['Rate'].iloc[j+3]:
                    decision_list.append(-1)
                break
        if not_found:
            rate_list.append(np.nan)
            decision_list.append(np.nan)
            rate_diff_list.append(np.nan)

    fomc_calendar.loc[:,'Rate'] = rate_list
    fomc_calendar.loc[:,'RateDiff'] = rate_diff_list
    fomc_calendar.loc[:,'RateDecision'] = decision_list
    fomc_calendar['RateDecision'] = fomc_calendar['RateDecision'].astype('Int8')

    # Remove the future date
    fomc_calendar = fomc_calendar.loc[fomc_calendar.index < dt.datetime.now()]
    # Confirm no null rate remains
    fomc_calendar.dropna(inplace = True)


    if fomc_calendar.loc[fomc_calendar.index == dt.datetime.strptime('2008-11-25', '%Y-%m-%d')].shape[0] == 0:
        fomc_calendar.loc[dt.datetime(2008, 11, 25)] = [True, False, False, 'Ben Bernanke', 0, -1, -1]

    # Make the other timings of QE Expansion lowering events (consider the effect as -0.5%)
    fomc_calendar.loc[fomc_calendar.index == dt.datetime(2009,3,18), ['RateDecision', 'RateDiff']] = (-1, -0.5) # QE1 Expanded
    fomc_calendar.loc[fomc_calendar.index == dt.datetime(2010,11,3), ['RateDecision', 'RateDiff']] = (-1, -0.5) # QE2 Announced
    fomc_calendar.loc[fomc_calendar.index == dt.datetime(2011,9,21), ['RateDecision', 'RateDiff']] = (-1, -0.5) # Operation Twist Announced
    fomc_calendar.loc[fomc_calendar.index == dt.datetime(2012,6,20), ['RateDecision', 'RateDiff']] = (-1, -0.5) # Operation Twist Extended
    fomc_calendar.loc[fomc_calendar.index == dt.datetime(2012,9,13), ['RateDecision', 'RateDiff']] = (-1, -0.5) # QE3 Announced
    fomc_calendar.loc[fomc_calendar.index == dt.datetime(2012,12,12), ['RateDecision', 'RateDiff']] = (-1, -0.5) # QE3 Expanded

    # It was announced QE ends - regard this as hike
    fomc_calendar.loc[fomc_calendar.index == dt.datetime(2013,6,19), ['RateDecision', 'RateDiff']] = (1, 1) # Tapering Announced
    fomc_calendar.loc['2013-12-18', ['RateDecision', 'RateDiff']] = (1, 1) # Tapering Begins
    fomc_calendar.loc['2014-10-29', ['RateDecision', 'RateDiff']] = (1, 0.5) # QE3 Terminated
    fomc_calendar.loc['2017-06-14', ['RateDecision', 'RateDiff']] = (1, 0.5) # Signaled Balance Sheet Normalization
    fomc_calendar.loc['2017-09-20', ['RateDecision', 'RateDiff']] = (1, 0.5) # Stated Balance Sheet Normalization Begins in Oct
    fomc_calendar.sort_index(ascending=True, inplace=True)
    fomc_calendar['RateChanged'] = fomc_calendar['RateDecision'].apply(lambda x: 0 if x == 0 else 1)
    Variable.set('fomc_calendar_p', fomc_calendar.to_json())

def process_nontext_data(ti):
    fomc_calendar= pd.read_json(Variable.get('fomc_calendar_p'))

    print('Nontext_data...')
    nontext_data = add_market_data(ti,fomc_calendar)
    print('t_nontext_data...')
    t_nontext_data = add_taylor(nontext_data)
    t_nontext_data.dropna(inplace = True)
    Variable.set('t_nontext_data',t_nontext_data.to_json())


def process_nontext_ma2(ti):
    fomc_calendar= pd.read_json(Variable.get('fomc_calendar_p'))

    print('nontext_ma2...')
    nontext_ma2 = add_market_data(ti,fomc_calendar,2)

    print('t_nontext_ma2...')
    t_nontext_ma2 = add_taylor(nontext_ma2, 60)
    t_nontext_ma2.dropna(inplace = True)
    Variable.set('t_nontext_ma2',t_nontext_ma2.to_json())


def process_nontext_ma3(ti):
    fomc_calendar= pd.read_json(Variable.get('fomc_calendar_p'))
    print('nontext_ma3...')
    nontext_ma3 = add_market_data(ti,fomc_calendar,3)
    print('t_nontext_ma3...')
    t_nontext_ma3 = add_taylor(nontext_ma3,90)
    t_nontext_ma3.dropna(inplace = True)
    Variable.set('t_nontext_ma3',t_nontext_ma3.to_json())


def process_nontext_ma6(ti):
    fomc_calendar= pd.read_json(Variable.get('fomc_calendar_p'))
    print('nontext_ma6...')
    nontext_ma6 = add_market_data(ti,fomc_calendar,6)
    print('t_nontext_ma6...')
    t_nontext_ma6 = add_taylor(nontext_ma6,180)
    t_nontext_ma6.dropna(inplace = True)
    Variable.set('t_nontext_ma6',t_nontext_ma6.to_json())

def process_nontext_ma12(ti):
    fomc_calendar= pd.read_json(Variable.get('fomc_calendar_p'))
    t_treasury_q = ti.xcom_pull(task_ids  ='load_data_USTREASURY_YIELD' )
    print('nontext_ma12...')
    nontext_ma12 = add_market_data(ti,fomc_calendar,12)
    print('t_nontext_ma12...')
    t_nontext_ma12 = add_taylor(nontext_ma12,360)
   

    fomc_calendar.index.get_level_values(0).values

    fomc_calendar.loc['2020-06-10', 'Rate'] = 0
    fomc_calendar.loc['2020-07-29', 'Rate'] = 0
    fomc_calendar.loc['2020-06-10', 'RateDiff'] = 0
    fomc_calendar.loc['2020-07-29', 'RateDiff'] = 0
    t_nontext_ma12.dropna(inplace = True)

    t_treasury_q.dropna(inplace = True)
    fomc_calendar.dropna(inplace = True)


    Variable.set('t_fomc_calendar',fomc_calendar.to_json())
    Variable.set('t_nontext_ma12',t_nontext_ma12.to_json())
    Variable.set('t_treasury_q',t_treasury_q.to_json())


def save_data_Preprocessed_quandl(ti):
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    fomc_calendar= pd.read_json(Variable.get('t_fomc_calendar'))
    nontext_data =  pd.read_json(Variable.get('t_nontext_data'))
    nontext_ma2 =  pd.read_json(Variable.get('t_nontext_ma2'))
    nontext_ma3 =  pd.read_json(Variable.get('t_nontext_ma3'))
    nontext_ma6 =  pd.read_json(Variable.get('t_nontext_ma6'))
    nontext_ma12 = pd.read_json(Variable.get('t_nontext_ma12'))
    treasury_q =  pd.read_json(Variable.get('t_treasury_q'))
    
    

    # Giả sử df là DataFrame chứa dữ liệu của bạn

    # Duyệt qua từng cột trong DataFrame
    for column in nontext_data.columns:
        # Kiểm tra kiểu dữ liệu của cột
        if nontext_data[column].dtype == 'int64' or nontext_data[column].dtype == 'Int64':
            # Chuyển đổi kiểu dữ liệu của cột từ int64 thành int của Python
            nontext_data[column] = nontext_data[column].astype(int)


    for column in nontext_ma2.columns:
        # Kiểm tra kiểu dữ liệu của cột
        if nontext_ma2[column].dtype == 'int64' or nontext_ma2[column].dtype == 'Int64':
            # Chuyển đổi kiểu dữ liệu của cột từ int64 thành int của Python
            nontext_ma2[column] = nontext_ma2[column].astype(int)
    
    for column in nontext_ma3.columns:
        # Kiểm tra kiểu dữ liệu của cột
        if nontext_ma3[column].dtype == 'int64' or nontext_ma3[column].dtype == 'Int64':
            # Chuyển đổi kiểu dữ liệu của cột từ int64 thành int của Python
            nontext_ma3[column] = nontext_ma3[column].astype(int)
    
    for column in nontext_ma6.columns:
        # Kiểm tra kiểu dữ liệu của cột
        if nontext_ma6[column].dtype == 'int64' or nontext_ma6[column].dtype == 'Int64':
            # Chuyển đổi kiểu dữ liệu của cột từ int64 thành int của Python
            nontext_ma6[column] = nontext_ma6[column].astype(int)
    
    for column in nontext_ma12.columns:
        # Kiểm tra kiểu dữ liệu của cột
        if nontext_ma12[column].dtype == 'int64' or nontext_ma12[column].dtype == 'Int64':
            # Chuyển đổi kiểu dữ liệu của cột từ int64 thành int của Python
            nontext_ma12[column] = nontext_ma12[column].astype(int)

    for column in treasury_q.columns:
        # Kiểm tra kiểu dữ liệu của cột
        if treasury_q[column].dtype == 'int64' or treasury_q[column].dtype == 'Int64':
            # Chuyển đổi kiểu dữ liệu của cột từ int64 thành int của Python
            treasury_q[column] = treasury_q[column].astype(int)

    for column in fomc_calendar.columns:
        # Kiểm tra kiểu dữ liệu của cột
        if fomc_calendar[column].dtype == 'int64' or fomc_calendar[column].dtype == 'Int64':
            # Chuyển đổi kiểu dữ liệu của cột từ int64 thành int của Python
            fomc_calendar[column] = fomc_calendar[column].astype(int)

    import_mongodb_csv(nontext_data,'data_NoNtext_Processed','nontext_data',client)
    import_mongodb_pickle(nontext_data,'nontext_data.pickle',client.data_pickle)

    import_mongodb_csv(nontext_ma2,'data_NoNtext_Processed','nontext_ma2',client)
    import_mongodb_pickle(nontext_ma2,'nontext_ma2.pickle',client.data_pickle)

    import_mongodb_csv(nontext_ma3,'data_NoNtext_Processed','nontext_ma3',client)
    import_mongodb_pickle(nontext_ma3,'nontext_ma3.pickle',client.data_pickle)

    import_mongodb_csv(nontext_ma6,'data_NoNtext_Processed','nontext_ma6',client)
    import_mongodb_pickle(nontext_ma6,'nontext_ma6.pickle',client.data_pickle)

    import_mongodb_csv(nontext_ma12,'data_NoNtext_Processed','nontext_ma12',client)
    import_mongodb_pickle(nontext_ma12,'nontext_ma12.pickle',client.data_pickle)

    import_mongodb_csv(treasury_q,'data_NoNtext_Processed','treasury',client)
    import_mongodb_pickle(treasury_q,'treasury.pickle',client.data_pickle)

    import_mongodb_csv(fomc_calendar,'data_NoNtext_Processed','fomc_calendar',client)
    import_mongodb_pickle(fomc_calendar,'fomc_calendar.pickle',client.data_pickle)


    

