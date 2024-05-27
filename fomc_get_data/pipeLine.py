import pymongo



import FomcStatement
from FomcMinutes import FomcMinutes
from FomcMeetingScript import FomcMeetingScript
from FomcPresConfScript import FomcPresConfScript
from FomcSpeech import FomcSpeech
from FomcTestimony import FomcTestimony


def mongo_connect():
    try:
        client = pymongo.MongoClient('mongodb://admin:admin@127.0.0.1:27017')
        print(client)
        return client
    except Exception as e:
        print("Lỗi:",e)


def download_data(fomc, from_year,client,file_name):
    fomc.get_contents(from_year)
    fomc.import_mongo_pickle(client,file_name)



if __name__ == "__main__":
    client =mongo_connect()
    

    print('FomcMeeting...')
    fomc = FomcMeetingScript(client.data_pdf)
    download_data(fomc,1980,client.data_pickle,"meeting_script.pickle")


    print('FomcMinutes...')
    fomc = FomcMinutes()
    download_data(fomc,1980,client.data_pickle,'minutes.pickle')

    print('FomcPresComf...')
    fomc = FomcPresConfScript(client.data_pdf)
    download_data(fomc,1980,client.data_pickle,'presconf_script.pickle')

    print('FomcStatement...')
    fomc = FomcStatement()
    download_data(fomc,1980,client.data_pickle,'statement.pickle')

    print('FomcSpeech...')
    fomc = FomcSpeech()
    download_data(fomc,1980,client.data_pickle,'speech.pickle')

    print('FomcTestimony...')
    fomc = FomcTestimony()
    download_data(fomc,1980,client.data_pickle,'testimony.pickle')
