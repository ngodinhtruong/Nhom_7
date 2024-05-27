from datetime import datetime

import pickle
import re

import pymongo
import gridfs

import pandas as pd

import requests
from bs4 import BeautifulSoup

import gridfs



def import_mongodb_csv(data, db_name, coll_name, client):
    db = client[db_name]
    coll = db[coll_name]  
    if coll_name in db.list_collection_names():
        coll.drop()
    records = data.to_dict(orient='records')
    coll.insert_many(records)
    print('Đã thêm thành công:', coll_name)
    return coll.count_documents({})

def import_mongodb_pickle(df,client,filename):
    fs = gridfs.GridFS(client)
    exist_data = client.fs.files.find_one({'filename':filename})
    data = pickle.dumps(df)
    if exist_data:
        fs.delete(exist_data['_id'])
    fs.put(data,filename=filename)
    print('Da them thanh cong:',filename)


def is_integer(n):
    '''
    Check if an input string can be converted to integer
    '''
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()
    
def download_calendar():
    base_url = 'https://www.federalreserve.gov'
    calendar_url = base_url + '/monetarypolicy/fomccalendars.htm'

    date_list = []
    r = requests.get(calendar_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    panel_divs = soup.find_all('div', {"class": "panel panel-default"})


    from_year = 2000
    print('from_year', from_year)
    for panel_div in panel_divs:
        m_year = panel_div.find('h4').get_text()[:4]
        m_months = panel_div.find_all('div', {"class": "fomc-meeting__month"})
        m_dates = panel_div.find_all('div', {"class": "fomc-meeting__date"})
        print("YEAR: {} - {} meetings found.".format(m_year, len(m_dates)))

        for (m_month, m_date) in zip(m_months, m_dates):
            month_name = m_month.get_text().strip()
            date_text = m_date.get_text().strip()
            is_forecast = False
            is_unscheduled = False
            is_month_short = False

            if ("cancelled" in date_text):
                continue
            elif "notation vote" in date_text:
                date_text = date_text.replace("(notation vote)", "").strip()
            elif "unscheduled" in date_text:
                date_text = date_text.replace("(unscheduled)", "").strip()
                is_unscheduled = True
            
            if "*" in date_text:
                date_text = date_text.replace("*", "").strip()
                is_forecast = True
            
            if "/" in month_name:
                month_name = re.findall(r".+/(.+)$", month_name)[0]
                is_month_short = True
            
            if "-" in date_text:
                date_text = re.findall(r".+-(.+)$", date_text)[0]
            
            meeting_date_str = m_year + "-" + month_name + "-" + date_text
            if is_month_short:
                meeting_date = datetime.strptime(meeting_date_str, '%Y-%b-%d')
            else:
                meeting_date = datetime.strptime(meeting_date_str, '%Y-%B-%d')

            date_list.append({"date": meeting_date, "unscheduled": is_unscheduled, "forecast": is_forecast, "confcall": False})

    # Retrieve FOMC Meeting date older than 2015
    for year in range(from_year, 2015):
        hist_url = base_url + '/monetarypolicy/fomchistorical' + str(year) + '.htm'
        r = requests.get(hist_url)
        soup = BeautifulSoup(r.text, 'html.parser')
        if year in (2011, 2012, 2013, 2014):
            panel_headings = soup.find_all('h5', {"class": "panel-heading"})
        else:
            panel_headings = soup.find_all('div', {"class": "panel-heading"})
        print("YEAR: {} - {} meetings found.".format(year, len(panel_headings)))
        for panel_heading in panel_headings:
            date_text = panel_heading.get_text().strip()
            #print("Date: ", date_text)
            regex = r"(January|February|March|April|May|June|July|August|September|October|November|December).*\s(\d*-)*(\d+)\s+(Meeting|Conference Calls?|\(unscheduled\))\s-\s(\d+)"
            date_text_ext = re.findall(regex, date_text)[0]
            meeting_date_str = date_text_ext[4] + "-" + date_text_ext[0] + "-" + date_text_ext[2]
            #print("   Extracted:", meeting_date_str)
            if meeting_date_str == '1992-June-1':
                meeting_date_str = '1992-July-1'
            elif meeting_date_str == '1995-January-1':
                meeting_date_str = '1995-February-1'
            elif meeting_date_str == '1998-June-1':
                meeting_date_str = '1998-July-1'
            elif meeting_date_str == '2012-July-1':
                meeting_date_str = '2012-August-1'
            elif meeting_date_str == '2013-April-1':
                meeting_date_str = '2013-May-1'

            meeting_date = datetime.strptime(meeting_date_str, '%Y-%B-%d')
            is_confcall = "Conference Call" in date_text_ext[3]
            is_unscheduled = "unscheduled" in date_text_ext[3]
            date_list.append({"date": meeting_date, "unscheduled": is_unscheduled, "forecast": False, "confcall": is_confcall})

    df = pd.DataFrame(date_list).sort_values(by=['date'])
    df.reset_index(drop=True, inplace=True)
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')

    # Save
    import_mongodb_csv(df,'data_calendar','data',client)
    import_mongodb_pickle(date_list,client.data_pickle,'calendar.pickle')
