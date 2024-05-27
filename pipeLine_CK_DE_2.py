from airflow import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
import datetime as dt


from Preprocess_quandl import load_data_fedrate_df,load_data_FRED_CPIAUCSL,load_data_FRED_DFF,load_data_FRED_GDPC1,load_data_calendar,load_data_FRED_GDPPOT,load_data_FRED_HSN1F,load_data_FRED_PAYEMS,load_data_FRED_PCEPILFE\
                            , load_data_FRED_RRSFS,load_data_ISM_NONMAN_NMI,load_data_FRED_UNRATE,load_data_ISM_MAN_PMI,load_data_USTREASURY_YIELD,create_taylor\
                            , process_fomc_caledar,process_nontext_data,process_nontext_ma12,process_nontext_ma2,process_nontext_ma3,process_nontext_ma6,save_data_Preprocessed_quandl

from get_quandl import download_quandl
from get_celendar import download_calendar

from Process_nonText import load_data_quandl_Preprocessed,process_data_quandl_to_train,save_train_data

from baseLine import load_data_base_line,balance_the_classe,create_train_test\
    ,process_RandomForestClassifier,print_result
default_args={
    'owner': 'Dinh',
    'start_date': datetime(2024,5,25),
    'retries': 10,
    'retry_delay': timedelta (minutes=1),
}

with DAG('FOMC_CK_DE_2',
            default_args=default_args,
            tags=['prj_CK'],
            description='Xu_Ly_Tin_Tuc_Kinh_Te',
            schedule_interval=timedelta (days=1),
            ) as dag:
    

    # quanDL
    download_quandls = PythonOperator(task_id = 'download_quandl',
                                              python_callable = download_quandl)
    download_calendars = PythonOperator(task_id = 'download_calendars',
                                        python_callable = download_calendar)
    load_calendar = PythonOperator (task_id='load_data_calendar',
                                       python_callable=load_data_calendar)
    load_fedrate_df = PythonOperator (task_id='load_data_fedrate_df',
                                       python_callable=load_data_fedrate_df)
    load_FRED_DFF = PythonOperator (task_id='load_data_FRED_DFF',
                                       python_callable=load_data_FRED_DFF)
    load_FRED_GDPC1 = PythonOperator (task_id='load_data_FRED_GDPC1',
                                       python_callable=load_data_FRED_GDPC1)
    load_FRED_GDPPOT = PythonOperator (task_id='load_data_FRED_GDPPOT',
                                       python_callable=load_data_FRED_GDPPOT)
    load_FRED_PCEPILFE = PythonOperator (task_id='load_data_FRED_PCEPILFE',
                                       python_callable=load_data_FRED_PCEPILFE)
    load_FRED_CPIAUCSL = PythonOperator (task_id='load_data_FRED_CPIAUCSL',
                                       python_callable=load_data_FRED_CPIAUCSL)
    load_UNRATE = PythonOperator (task_id='load_data_FRED_UNRATE',
                                       python_callable=load_data_FRED_UNRATE)
    load_FRED_PAYEMS = PythonOperator (task_id='load_data_FRED_PAYEMS',
                                       python_callable=load_data_FRED_PAYEMS)
    load_ISM_MAN_PMI = PythonOperator (task_id='load_data_ISM_MAN_PMI',
                                       python_callable=load_data_ISM_MAN_PMI)
    load_ISM_NONMAN_NMI = PythonOperator (task_id='load_data_ISM_NONMAN_NMI',
                                       python_callable=load_data_ISM_NONMAN_NMI)
    load_FRED_RRSFS = PythonOperator (task_id='load_data_FRED_RRSFS',
                                       python_callable=load_data_FRED_RRSFS)
    load_FRED_HSN1F = PythonOperator (task_id='load_data_FRED_HSN1F',
                                       python_callable=load_data_FRED_HSN1F)
    load_USTREASURY_YIELD = PythonOperator (task_id='load_data_USTREASURY_YIELD',
                                       python_callable=load_data_USTREASURY_YIELD)
    create_taylors = PythonOperator (task_id='create_taylor',
                                    python_callable=create_taylor)
    process_fomc_caledars = PythonOperator (task_id='process_fomc_caledar',
                                       python_callable=process_fomc_caledar)
    process_nontext_datas = PythonOperator (task_id='process_nontext_data',
                                       python_callable=process_nontext_data)
    process_nontext_ma12s = PythonOperator (task_id='process_nontext_ma12',
                                       python_callable=process_nontext_ma12)
    process_nontext_ma2s = PythonOperator (task_id='process_nontext_ma2',
                                       python_callable=process_nontext_ma2)
    process_nontext_ma3s = PythonOperator (task_id='process_nontext_ma3',
                                       python_callable=process_nontext_ma3)
    process_nontext_ma6s = PythonOperator (task_id='process_nontext_ma6',
                                       python_callable=process_nontext_ma6)
    save_data_Preprocessed_quandls = PythonOperator (task_id='save_data_Preprocessed_quandl',
                                python_callable=save_data_Preprocessed_quandl)
    
    #Process_data_quandl_Preprocessed
    load_data_quandl_Preprocesseds = PythonOperator (task_id='load_data_quandl_Preprocessed',
                                python_callable=load_data_quandl_Preprocessed)
    process_data_quandl_to_trains = PythonOperator(task_id='process_data_quandl_to_train',
                                  python_callable=process_data_quandl_to_train)
    save_data_train = PythonOperator(task_id='save_data_train',
                               python_callable=save_train_data)


    # train model
    load_data_base_lines = PythonOperator(task_id ='load_data_base_line',
                                python_callable = load_data_base_line)
    balance_the_classes = PythonOperator(task_id ='balance_the_classe',
                                python_callable = balance_the_classe)
    create_train_tests = PythonOperator(task_id ='create_train_test',
                                python_callable = create_train_test)
    trai_model = PythonOperator(task_id = 'trai_model',
                                python_callable = process_RandomForestClassifier)
    print_results = PythonOperator(task_id = 'print_results',
                                   python_callable = print_result)
    load_data_base_lines >> balance_the_classes >> create_train_tests >> trai_model >> print_results


# Quandl
# download --> load_data    
[download_quandls, download_calendars] >> load_calendar
[download_quandls, download_calendars] >> load_fedrate_df
[download_quandls, download_calendars] >> load_FRED_DFF
[download_quandls, download_calendars] >> load_FRED_GDPC1
[download_quandls, download_calendars] >> load_FRED_GDPPOT
[download_quandls, download_calendars] >> load_FRED_PCEPILFE
[download_quandls, download_calendars] >> load_FRED_CPIAUCSL
[download_quandls, download_calendars] >> load_UNRATE
[download_quandls, download_calendars] >>load_FRED_PAYEMS
[download_quandls, download_calendars] >> load_ISM_MAN_PMI
[download_quandls, download_calendars] >> load_ISM_NONMAN_NMI
[download_quandls, download_calendars] >> load_FRED_RRSFS
[download_quandls, download_calendars] >>load_FRED_HSN1F
[download_quandls, download_calendars] >> load_USTREASURY_YIELD

# load_data --> [process_calendar, create_taylor]
[load_calendar, load_fedrate_df,load_FRED_DFF,load_FRED_GDPC1,load_FRED_GDPPOT,load_FRED_PCEPILFE,load_FRED_CPIAUCSL,load_UNRATE,load_FRED_PAYEMS,load_ISM_MAN_PMI,load_ISM_NONMAN_NMI, load_FRED_RRSFS,load_FRED_HSN1F,load_USTREASURY_YIELD] >> create_taylors 
[load_calendar, load_fedrate_df,load_FRED_DFF,load_FRED_GDPC1,load_FRED_GDPPOT,load_FRED_PCEPILFE,load_FRED_CPIAUCSL,load_UNRATE,load_FRED_PAYEMS,load_ISM_MAN_PMI,load_ISM_NONMAN_NMI, load_FRED_RRSFS,load_FRED_HSN1F,load_USTREASURY_YIELD] >> process_fomc_caledars 


# [process_calendar, create_taylor] --> process_nontext
[create_taylors, process_fomc_caledars] >> process_nontext_datas

[create_taylors, process_fomc_caledars]  >> process_nontext_ma12s

[create_taylors, process_fomc_caledars]  >> process_nontext_ma2s

[create_taylors, process_fomc_caledars]  >> process_nontext_ma3s

[create_taylors, process_fomc_caledars]  >> process_nontext_ma6s

# process_nontext --> save_data
[process_nontext_datas, process_nontext_ma12s, process_nontext_ma2s, process_nontext_ma3s, process_nontext_ma6s] >> save_data_Preprocessed_quandls

# data_text


# process_data_quandl_Preprocessed
save_data_Preprocessed_quandls >> load_data_quandl_Preprocesseds >> process_data_quandl_to_trains >> save_data_train

save_data_train >> load_data_base_lines >> balance_the_classes >> create_train_tests >> trai_model >> print_results