

import datetime as dt
import re
import pickle

import pymongo
import gridfs
# Data Science modules
import pandas as pd
from airflow.models import Variable

def import_mongodb_csv(data, db_name, coll_name, client):
    db = client[db_name]
    coll = db[coll_name]  
    if coll_name in db.list_collection_names():
        coll.drop()
    records = data.to_dict(orient='records')
    coll.insert_many(records)
    print('Đã thêm thành công:', coll_name)
    return coll.count_documents({})

def get_pickle_from_mongodb(client,filename):
    fs = gridfs.GridFS(client)
    data = client.fs.files.find_one({'filename':filename})
    return pickle.loads(fs.get(data['_id']).read())
def get_csv_from_mongodb(client,db_name,coll_name):
    db = client[db_name]
    collection = db[coll_name]
    data = list(collection.find({},{'_id': False}))
    return pd.DataFrame(data)

def import_mongodb_pickle(df,filename,client):
    fs = gridfs.GridFS(client)
    exist_data = client.fs.files.find_one({'filename':filename})
    data = pickle.dumps(df)
    if exist_data:
        fs.delete(exist_data['_id'])
    fs.put(data,filename=filename)
    print('Da them thanh cong:',filename)

# Functions for map() or apply()

def get_word_count(x):
    '''
    Retun the number of words for the given text x.
    '''
    x = x.replace("[SECTION]", "")
    return len(re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', x))
        
def get_rate_change(x):
    '''
    Returns rate change decision of the FOMC Decision for the given date x.
    x should be of datetime type or yyyy-mm-dd format string.
    '''
    # If x is string, convert to datetime
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    fomc_calendar = get_pickle_from_mongodb(client.data_pickle,'fomc_calendar.pickle')
    if type(x) is str:
        try:
            x = dt.datetime.strptime(x, '%Y-%m-%d')
        except:
            return None
    
    if x in fomc_calendar.index:
        return fomc_calendar.loc[x]['RateDecision']
    else:        
        return None

def get_rate(x):
    '''
    Returns rate of the FOMC Decision for the given date x.
    x should be of datetime type or yyyy-mm-dd format string.
    '''
    # If x is string, convert to datetime
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    fomc_calendar = get_pickle_from_mongodb(client.data_pickle,'fomc_calendar.pickle')
    if type(x) is str:
        try:
            x = dt.datetime.strptime(x, '%Y-%m-%d')
        except:
            return None
        
    if x in fomc_calendar.index:
        return fomc_calendar.loc[x]['Rate']
    else:        
        return None

def get_next_meeting_date(x):
    '''
    Returns the next fomc meeting date for the given date x, referring to fomc_calendar DataFrame.
    Usually FOMC Meetings takes two days, so it starts searching from x+2.
    x should be of datetime type or yyyy-mm-dd format string.
    '''
    # If x is string, convert to datetime
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    fomc_calendar = get_pickle_from_mongodb(client.data_pickle,'fomc_calendar.pickle')
    x = dt.datetime.strptime('2019-01-01', '%Y-%m-%d')
    fomc_calendar.loc[fomc_calendar.index > x]
    if type(x) is str:
        try:
            x = dt.datetime.strptime(x, '%Y-%m-%d')
        except:
            return None

    # Add two days to get the day after next
    x = x + dt.timedelta(days=2)
    
    # Just in case, sort fomc_calendar from older to newer
    fomc_calendar.sort_index(ascending=True, inplace=True)
    
    if fomc_calendar.index[0] > x:
        # If the date is older than the first FOMC Meeting, do not return any date.
        return None
    else:
        for i in range(len(fomc_calendar)):
            if x < fomc_calendar.index[i]:
                return fomc_calendar.index[i]
        # If x is greater than the newest FOMC meeting date, do not return any date.
        return None
    
def get_chairperson(x):
    # FOMC Chairperson's list
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

def reorganize_df(df, doc_type):
    '''
    Reorganize the loaded dataframe, which has been obrained by FomcGetData for further processing
        - Add type
        - Add word count
        - Add rate, decision (for meeting documents, None for the others)
        - Add next meeting date, rate and decision
        - Copy contents to org_text
        - Remove line breaks from contents in text
        - Split contents by "[SECTION]" to list in text_sections
    '''
    print('Reorganize....')
    if doc_type in ('statement', 'minutes', 'presconf_script', 'meeting_script'):
        is_meeting_doc = True
    elif doc_type in ('speech', 'testimony'):
        is_meeting_doc = False
    else:
        print("Invalid doc_type [{}] is given!".format(doc_type))
        return None
    
    dict = {
        'type': doc_type,
        'date': df['date'],
        'title': df['title'],
        'speaker': df['speaker'],
        'word_count': df['contents'].map(get_word_count),
        'decision': df['date'].map(lambda x: get_rate_change(x) if is_meeting_doc else None),
        'rate': df['date'].map(lambda x: get_rate(x) if is_meeting_doc else None),
        'next_meeting': df['date'].map(get_next_meeting_date),
        'next_decision': df['date'].map(get_next_meeting_date).map(get_rate_change),
        'next_rate': df['date'].map(get_next_meeting_date).map(get_rate),        
        'text': df['contents'].map(lambda x: x.replace('\n','').replace('\r','').strip()),
        'text_sections': df['contents'].map(lambda x: x.replace('\n','').replace('\r','').strip().split("[SECTION]")),
        'org_text': df['contents']
    }

    new_df = pd.DataFrame(dict)
    new_df['decision'] = new_df['decision'].astype('Int8')
    new_df['next_decision'] = new_df['next_decision'].astype('Int8')
    print("No rate decision found: ", new_df['decision'].isnull().sum())
    print("Shape of the dataframe: ", new_df.shape)
    print('Finished Reorganize !!!')
    #new_df.dropna(subset=['decision'], axis=0, inplace=True)
    return new_df
def get_split(text, split_len=200, overlap=50):
    '''
    Returns a list of split text of $split_len with overlapping of $overlap.
    Each item of the list will have around split_len length of text.
    '''
    l_total = []
    words = re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', text)
    
    if len(words) < split_len:
        n = 1
    else:
        n = (len(words) - overlap) // (split_len - overlap) + 1
        
    for i in range(n):
        l_parcial = words[(split_len - overlap) * i: (split_len - overlap) * i + split_len]
        l_total.append(" ".join(l_parcial))
    return l_total

def get_split_df(ti,df, split_len=200, overlap=50):
    '''
    Returns a dataframe which is an extension of an input dataframe.
    Each row in the new dataframe has less than $split_len words in 'text'.
    '''
    print('Get_split_df....')
    split_data_list = []

    for i, row in df.iterrows():
        #print("Original Word Count: ", row['word_count'])
        text_list = get_split(row["text"], split_len, overlap)
        for text in text_list:
            row['text'] = text
            #print(len(re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', text)))
            row['word_count'] = len(re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', text))
            split_data_list.append(list(row))
            
    split_df = pd.DataFrame(split_data_list, columns=df.columns)
    split_df['decision'] = split_df['decision'].astype('Int8')
    split_df['next_decision'] = split_df['next_decision'].astype('Int8')
    print('Finished get_split_df !!!')
    # ti.xcom_push(key = 'split_df',value = split_df)
    return split_df

def remove_short_section(df, min_words=50):
    '''
    Using 'text_sections' of the given dataframe, remove sections having less than min_words.
    It concatinate sections with a space, which exceeds min_words and update 'text'.
    As a fallback, keep a text which concatinates sections having more than 20 words and use it
     if there is no section having more than min_words.
    If there is no sections having more than 20 words, remove the row.
    '''
    print('Remove_short_section....')
    new_df = df.copy()
    new_text_list = []
    new_text_section_list = []
    new_wc_list = []
    
    for i, row in new_df.iterrows():
        new_text = ""
        bk_text = ""
        new_text_section = []
        bk_text_section = []
                
        for section in row['text_sections']:
            num_words = len(re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', section))
            if num_words > min_words:
                new_text += " " + section
                new_text_section.append(section)
            elif num_words > 20:
                bk_text += " " + section
                bk_text_section.append(section)
                
        
        new_text = new_text.strip()
        bk_text = bk_text.strip()
        
        if len(new_text) > 0:
            new_text_list.append(new_text)
            new_text_section_list.append(new_text_section)
        elif len(bk_text) > 0:
            new_text_list.append(bk_text)
            new_text_section_list.append(bk_text_section)
        else:
            new_text_list.append("")
            new_text_section_list.append("")
        
        # Update the word count
        new_wc_list.append(len(re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', new_text_list[-1])))
        
    new_df['text'] = new_text_list
    new_df['word_count'] = new_wc_list
    
    print('Finished remove_short_section !!!')
    return new_df.loc[new_df['word_count'] > 0]

def remove_short_nokeyword(df, keywords = ['rate', 'rates', 'federal fund', 'outlook', 'forecast', 'employ', 'economy'], min_times=2, min_words=50):
    '''
    Drop sections which do not have any one of keywords for min_times times
     before applying remove_short_section()
    '''
    
    print('Remove_short_nokeyword....')
    new_df = df.copy()
    new_section_list = []
    
    for i, row in new_df.iterrows():
        new_section = []
                
        for section in row['text_sections']:
            if len(set(section.split()).intersection(keywords)) >= min_times:
                new_section.append(section)
        
        new_section_list.append(new_section)
    
    new_df['text_sections'] = new_section_list
    print('Fished remove_short_nokeyword !!!')
    return remove_short_section(new_df, min_words=min_words)

def load_data_text(ti):
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    fomc_calendar = get_pickle_from_mongodb(client.data_pickle,'fomc_calendar.pickle')
    statement_df = get_pickle_from_mongodb(client.data_pickle,'statement.pickle')
    minutes_df = get_pickle_from_mongodb(client.data_pickle,'minutes.pickle')
    meeting_script_df = get_pickle_from_mongodb(client.data_pickle,'meeting_script.pickle')
    presconf_script_df = get_pickle_from_mongodb(client.data_pickle,'presconf_script.pickle')
    speech_df = get_pickle_from_mongodb(client.data_pickle,'speech.pickle')
    testimony_df = get_pickle_from_mongodb(client.data_pickle,'testimony.pickle')

    

    Variable.set('fomc_calendar',fomc_calendar.to_json())
    Variable.set('statement_df',statement_df.to_json())
    Variable.set('minutes_df',minutes_df.to_json())
    Variable.set('meeting_script_df',meeting_script_df.to_json())
    Variable.set('presconf_script_df',presconf_script_df.to_json())
    Variable.set('speech_df',speech_df.to_json())
    Variable.set('testimony_df',testimony_df.to_json())

def process_statement_df(**context):
    statement_df = pd.read_json(Variable.get('statement_df'))
    if statement_df.loc[statement_df['date'] == dt.datetime.strptime('2008-11-25', '%Y-%m-%d')].shape[0] == 0:
        qe_text = "The Federal Reserve announced on Tuesday that it will initiate a program "\
                "to purchase the direct obligations of housing-related government-sponsored "\
                "enterprises (GSEs)--Fannie Mae, Freddie Mac, and the Federal Home Loan Banks "\
                "--and mortgage-backed securities (MBS) backed by Fannie Mae, Freddie Mac, "\
                "and Ginnie Mae.  Spreads of rates on GSE debt and on GSE-guaranteed mortgages "\
                "have widened appreciably of late.  This action is being taken to reduce the cost "\
                "and increase the availability of credit for the purchase of houses, which in turn "\
                "should support housing markets and foster improved conditions in financial markets "\
                "more generally. Purchases of up to $100 billion in GSE direct obligations under "\
                "the program will be conducted with the Federal Reserve's primary dealers through "\
                "a series of competitive auctions and will begin next week.  Purchases of up to "\
                "$500 billion in MBS will be conducted by asset managers selected via a competitive "\
                "process with a goal of beginning these purchases before year-end.  "\
                "Purchases of both direct obligations and MBS are expected to take place over "\
                "several quarters.  Further information regarding the operational details of this "\
                "program will be provided after consultation with market participants."
        new_row = pd.Series([dt.datetime.strptime('2008-11-25', '%Y-%m-%d'), qe_text, 'Ben Bernanke', 'FOMC statement'], index=statement_df.columns)
        statement_df = pd.concat([statement_df, new_row.to_frame().T], ignore_index=True)
    proc_statement_df = reorganize_df(statement_df, 'statement')
    proc_statement_df = remove_short_section(proc_statement_df, min_words=50)
    split_statement_df = get_split_df(context['ti'],proc_statement_df)
    keyword_statement_df = remove_short_nokeyword(proc_statement_df)
    keyword_statement_df.reset_index(drop=True, inplace=True)
    proc_statement_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    split_statement_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    keyword_statement_df.drop(columns=['text_sections', 'org_text'], inplace=True)

    print(proc_statement_df)
    print(split_statement_df)
    print(keyword_statement_df)

    Variable.set('proc_statement_df',proc_statement_df.to_json())
    Variable.set('split_statement_df',split_statement_df.to_json())
    Variable.set('keyword_statement_df',keyword_statement_df.to_json())
def process_minutes_df(ti):
    minutes_df = pd.read_json(Variable.get('minutes_df'))

    proc_minutes_df = reorganize_df(minutes_df, 'minutes')


    proc_minutes_df = remove_short_section(proc_minutes_df, min_words=50)
    print('-----')
    
    print('Get_split_df....')
    split_data_list = []

    for i, row in proc_minutes_df.iterrows():
        #print("Original Word Count: ", row['word_count'])
        text_list = get_split(row["text"], 200, 50)
        for text in text_list:
            row['text'] = text
            #print(len(re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', text)))
            row['word_count'] = len(re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', text))
            split_data_list.append(list(row))
            
    split_df = pd.DataFrame(split_data_list, columns=proc_minutes_df.columns)
    split_df['decision'] = split_df['decision'].astype('Int8')
    split_df['next_decision'] = split_df['next_decision'].astype('Int8')
    print('Finished get_split_df !!!')
    split_minutes_df = split_df

    
    keyword_minutes_df = remove_short_nokeyword(proc_minutes_df)
    keyword_minutes_df.reset_index(drop=True, inplace=True)
    proc_minutes_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    split_minutes_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    keyword_minutes_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    
    Variable.set('proc_minutes_df',proc_minutes_df.to_json())
    Variable.set('split_minutes_df',split_minutes_df.to_json())
    Variable.set('keyword_minutes_df',keyword_minutes_df.to_json())
    


def process_presconf_script_df(ti):
    presconf_script_df = pd.read_json(Variable.get('presconf_script_df'))
    proc_presconf_script_df = reorganize_df(presconf_script_df, 'presconf_script')

    script_data_list = []

    for i, row in proc_presconf_script_df.iterrows():
        for text in row["text_sections"]:
            match = re.findall(r'(^[A-Za-zŞ. ]*[A-Z]{3}).\d? (.*)', text)
            if len(match) == 0:
                match = re.findall(r'(^[A-Za-zŞ. ]*[A-Z]{3}).\d(.*)', text)
                if len(match) == 0:
                    print("not matched: ", text)
                    print(row['date'])
                    print()
            if len(match) == 1:
                speaker, text = match[0]
                row['speaker'] = speaker
                row['text'] = text
                row['word_count'] = len(re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', text))
                script_data_list.append(list(row))

    col_name = proc_presconf_script_df.columns

    presconf_script_speaker_df = pd.DataFrame(script_data_list, columns = col_name)
    presconf_script_speaker_df = presconf_script_speaker_df.loc[presconf_script_speaker_df['word_count'] >= 50]
    tmp_list = []
    for i, row in presconf_script_speaker_df.iterrows():
        chairperson = get_chairperson(row['date'])
        if chairperson.lower().split()[-1] in row['speaker'].lower():
            row['speaker'] = chairperson
            tmp_list.append(list(row))

    col_names = presconf_script_speaker_df.columns
    presconf_script_chair_df = pd.DataFrame(data=tmp_list, columns=col_names)
    tmp_date = ''
    tmp_speaker = ''
    tmp_data = []

    print('Before: ', presconf_script_chair_df.shape)

    for i, row in presconf_script_chair_df.iterrows():
        if (row['date'] == tmp_date) and (row['speaker'] == tmp_speaker):
            tmp_data[-1]['text'] += row['text']
            tmp_data[-1]['word_count'] += row['word_count']
            tmp_data[-1]['text_sections'].append(row['text'])
        else:
            tmp_date = row['date']
            tmp_speaker = row['speaker']
            row['text_sections'] = [row['text']]
            tmp_data.append(row)

    presconf_script_chair_day_df = pd.DataFrame(tmp_data)

    print('After', presconf_script_chair_day_df.shape)
    presconf_script_split_df = get_split_df(ti,presconf_script_chair_day_df)
    presconf_script_keyword_df = remove_short_nokeyword(presconf_script_chair_day_df)
    presconf_script_keyword_df.reset_index(drop=True, inplace=True)

    presconf_script_chair_day_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    presconf_script_split_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    presconf_script_keyword_df.drop(columns=['text_sections', 'org_text'], inplace=True)

    Variable.set('presconf_script_keyword_df',presconf_script_keyword_df.to_json())
    Variable.set('presconf_script_chair_day_df',presconf_script_chair_day_df.to_json())
    Variable.set('presconf_script_split_df',presconf_script_split_df.to_json())

    

def process_meeting_script_df(ti):
    meeting_script_df = pd.read_json(Variable.get('meeting_script_df'))
    proc_meeting_script_df = reorganize_df(meeting_script_df,'meeting_script')
    script_data_list = []

    for i, row in proc_meeting_script_df.iterrows():
        for text in row["text_sections"]:
            match = re.findall(r'(^[A-Za-zŞ. ]*[A-Z]{3}).\d? (.*)', text)
            if len(match) == 0:
                match = re.findall(r'(^[A-Za-zŞ. ]*[A-Z]{3}).\d(.*)', text)
                if len(match) == 0:
                    print("not matched: ", text)
                    print(row['date'])
                    print()
            if len(match) == 1:
                speaker, text = match[0]
                row['speaker'] = speaker
                row['text'] = text
                row['word_count'] = len(re.findall(r'\b([a-zA-Z]+n\'t|[a-zA-Z]+\'s|[a-zA-Z]+)\b', text))
                script_data_list.append(list(row))

    col_name = proc_meeting_script_df.columns

    meeting_script_speaker_df = pd.DataFrame(script_data_list, columns = col_name)
    print("Before: ", meeting_script_speaker_df.shape)
    meeting_script_speaker_df = meeting_script_speaker_df.loc[meeting_script_speaker_df['word_count'] >= 20]
    print("After: ", meeting_script_speaker_df.shape)

    meeting_script_speaker_df = meeting_script_speaker_df.groupby(['type', 'date', 'title', 'speaker', 'decision', 'rate', 'next_meeting', 'next_decision', 'next_rate'])['text'].apply('[SECTION]'.join).reset_index()
    meeting_script_speaker_df['text_sections'] = meeting_script_speaker_df['text'].map(lambda x: x.split("[SECTION]"))
    meeting_script_speaker_df['text'] = meeting_script_speaker_df['text'].map(lambda x: x.replace("[SECTION]", ""))
    meeting_script_speaker_df['word_count'] = meeting_script_speaker_df['text'].map(get_word_count)

    meeting_script_split_df = get_split_df(ti,meeting_script_speaker_df)
    meeting_script_keyword_df = remove_short_nokeyword(meeting_script_speaker_df)
    meeting_script_keyword_df.reset_index(drop=True, inplace=True)

    meeting_script_speaker_df.drop(columns=['text_sections'], inplace=True)
    meeting_script_split_df.drop(columns=['text_sections'], inplace=True)
    meeting_script_keyword_df.drop(columns=['text_sections'], inplace=True)

    Variable.set('meeting_script_keyword_df',meeting_script_keyword_df.to_json())
    Variable.set('meeting_script_speaker_df',meeting_script_speaker_df.to_json())
    Variable.set('meeting_script_split_df',meeting_script_split_df.to_json())



def process_speech_df(ti):
    speech_df = pd.read_json(Variable.get('speech_df'))
    proc_speech_df = reorganize_df(speech_df,'speech')
    proc_speech_df = remove_short_section(proc_speech_df, min_words=50)

    tmp_list = []
    for i, row in proc_speech_df.iterrows():
        chairperson = get_chairperson(row['date'])
        if chairperson.lower().split()[-1] in row['speaker'].lower():
            row['speaker'] = chairperson
            tmp_list.append(list(row))

    col_names = proc_speech_df.columns
    speech_chair_df = pd.DataFrame(data=tmp_list, columns=col_names)


    speech_split_df = get_split_df(ti,speech_chair_df)
    speech_split_df.reset_index(drop=True, inplace=True)

    speech_keyword_df = remove_short_nokeyword(speech_chair_df)
    speech_keyword_df.reset_index(drop=True, inplace=True)

    speech_chair_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    speech_split_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    speech_keyword_df.drop(columns=['text_sections', 'org_text'], inplace=True)

    Variable.set('speech_chair_df',speech_chair_df.to_json())
    Variable.set('speech_split_df',speech_split_df.to_json())
    Variable.set('speech_keyword_df',speech_keyword_df.to_json())



def process_testimony_df(ti):
    testimony_df = pd.read_json(Variable.get('testimony_df'))
    proc_testimony_df = reorganize_df(testimony_df, 'testimony')
    proc_testimony_df = remove_short_section(proc_testimony_df, min_words=50)

    tmp_list = []
    for i, row in proc_testimony_df.iterrows():
        chairperson = get_chairperson(row['date'])
        if chairperson.lower().split()[-1] in row['speaker'].lower():
            row['speaker'] = chairperson
            tmp_list.append(list(row))

    col_names = proc_testimony_df.columns
    testimony_chair_df = pd.DataFrame(data=tmp_list, columns=col_names)
    testimony_split_df = get_split_df(ti,testimony_chair_df)
    testimony_split_df.reset_index(drop=True, inplace=True)
    testimony_keyword_df = remove_short_nokeyword(testimony_chair_df)
    testimony_keyword_df.reset_index(drop=True, inplace=True)


    testimony_chair_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    testimony_split_df.drop(columns=['text_sections', 'org_text'], inplace=True)
    testimony_keyword_df.drop(columns=['text_sections', 'org_text'], inplace=True)

    Variable.set('testimony_chair_df',testimony_chair_df.to_json())
    Variable.set('testimony_split_df',testimony_split_df.to_json())
    Variable.set('testimony_keyword_df',testimony_keyword_df.to_json())


def combine_data(ti):
    
    proc_statement_df = pd.read_json(Variable.get('proc_statement_df'))
    split_statement_df = pd.read_json(Variable.get('split_statement_df'))
    keyword_statement_df = pd.read_json(Variable.get('keyword_statement_df'))

    proc_minutes_df = pd.read_json(Variable.get('proc_minutes_df'))
    split_minutes_df = pd.read_json(Variable.get('split_minutes_df'))
    keyword_minutes_df = pd.read_json(Variable.get('keyword_minutes_df'))


    presconf_script_chair_day_df = pd.read_json(Variable.get('presconf_script_chair_day_df'))
    presconf_script_split_df = pd.read_json(Variable.get('presconf_script_split_df'))
    presconf_script_keyword_df = pd.read_json(Variable.get('presconf_script_keyword_df'))


    meeting_script_speaker_df = pd.read_json(Variable.get('meeting_script_speaker_df'))
    meeting_script_split_df = pd.read_json(Variable.get('meeting_script_split_df'))
    meeting_script_keyword_df = pd.read_json(Variable.get('meeting_script_keyword_df'))


    speech_chair_df = pd.read_json(Variable.get('speech_chair_df'))
    speech_split_df = pd.read_json(Variable.get('speech_split_df'))
    speech_keyword_df = pd.read_json(Variable.get('speech_keyword_df'))


    testimony_chair_df = pd.read_json(Variable.get('testimony_chair_df'))
    testimony_split_df = pd.read_json(Variable.get('testimony_split_df'))
    testimony_keyword_df = pd.read_json(Variable.get('testimony_keyword_df'))



    text_no_split = pd.concat([proc_statement_df, 
                           proc_minutes_df, 
                           presconf_script_chair_day_df, 
                           meeting_script_speaker_df, 
                           speech_chair_df,
                            testimony_chair_df], sort=False)
    text_no_split.reset_index(drop=True, inplace=True)

    text_split_200 = pd.concat([split_statement_df, 
                                split_minutes_df, 
                                presconf_script_split_df, 
                                meeting_script_split_df, 
                                speech_split_df, 
                                testimony_split_df], sort=False)
    text_split_200.reset_index(drop=True, inplace=True)

    text_keyword = pd.concat([keyword_statement_df,
                            keyword_minutes_df,
                            presconf_script_keyword_df,
                            meeting_script_keyword_df, 
                            speech_keyword_df, 
                            testimony_keyword_df], sort=False)
    text_keyword.reset_index(drop=True, inplace=True)
    text_no_split['next_meeting'] = text_no_split['next_meeting'].astype(str)

    Variable.set('text_no_split',text_no_split.to_json())
    Variable.set('text_split_200',text_split_200.to_json())
    Variable.set('text_keyword',text_keyword.to_json())

    return text_no_split


def save_data_Preprocessed_text(ti):
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')

    text_no_split = pd.read_json(Variable.get('text_no_split'))
    text_split_200 = pd.read_json(Variable.get('text_split_200'))
    text_keyword = pd.read_json(Variable.get('text_keyword'))

    text_no_split['date'] = text_no_split['date'].astype(str)
    text_no_split['next_meeting'] = text_no_split['next_meeting'].astype(str)
    
    text_split_200['date'] = text_split_200['date'].astype(str)
    text_split_200['next_meeting'] = text_split_200['next_meeting'].astype(str)
        
    text_keyword['date'] = text_keyword['date'].astype(str)
    text_keyword['next_meeting'] = text_keyword['next_meeting'].astype(str)

    import_mongodb_csv(text_no_split,'data_Preprocess_csv','text_no_split.csv',client)
    import_mongodb_pickle(text_no_split,'text_no_split',client.data_Preprocess_pickle)

    import_mongodb_csv(text_split_200,'data_Preprocess_csv','text_split_200.csv',client)
    import_mongodb_pickle(text_split_200,'text_split_200',client.data_Preprocess_pickle)

    import_mongodb_csv(text_keyword,'data_Preprocess_csv','text_keyword.csv',client)
    import_mongodb_pickle(text_keyword,'text_keyword',client.data_Preprocess_pickle)

    
    
    
