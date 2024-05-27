import pymongo
from fomc_get_data.FomcMinutes import FomcMinutes
from fomc_get_data.FomcMeetingScript import FomcMeetingScript
from fomc_get_data.FomcStatement import FomcStatement
from fomc_get_data.FomcPresConfScript import FomcPresConfScript
from fomc_get_data.FomcSpeech import FomcSpeech
from fomc_get_data.FomcTestimony import FomcTestimony

# Python libraries


def download_data(fomc, from_year,client,file_name):
    fomc.get_contents(from_year)
    fomc.import_mongo_pickle(client,file_name)


def download_meeting_script():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    from_year = 2000
    print('from_year:',from_year)
    print('FomcMeeting...')
    fomc = FomcMeetingScript(client.data_pdf)
    download_data(fomc,from_year,client.data_pickle,"meeting_script.pickle")

def download_minutes():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    from_year = 2000
    print('FomcMinutes...')
    fomc = FomcMinutes()
    download_data(fomc,from_year,client.data_pickle,'minutes.pickle')
def download_presconf_script():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    from_year = 2000
    print('FomcPresComf...')
    fomc = FomcPresConfScript(client.data_pdf)
    download_data(fomc,from_year,client.data_pickle,'presconf_script.pickle')
def download_statement():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    from_year = 2000
    print('FomcStatement...')
    fomc = FomcStatement()
    download_data(fomc,from_year,client.data_pickle,'statement.pickle')
def download_speech():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    from_year = 2000
    print('FomcSpeech...')
    fomc = FomcSpeech()
    download_data(fomc,from_year,client.data_pickle,'speech.pickle')
def download_testimony():
    client = pymongo.MongoClient('mongodb://admin:admin@mongo:27017/')
    from_year = 2000
    print('FomcTestimony...')
    fomc = FomcTestimony()
    download_data(fomc,from_year,client.data_pickle,'testimony.pickle')
