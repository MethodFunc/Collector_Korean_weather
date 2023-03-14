import pymongo.errors
from pymongo import MongoClient, UpdateOne


class MongoConnect:
    """
        범용 mongodb context manager
        중복된 날짜는 중복으로 insert가 되지 않도록 설정되어있음
    """

    def __init__(self, uri, database_name, collection_name):
        self.db_name = database_name
        self.cl_name = collection_name
        self.uri = uri
        self.client = None
        self.collection = None

    def connect(self):
        self.client = MongoClient(self.uri)
        database = self.client[self.db_name]
        self.collection = database[self.cl_name]

    def insert_item(self, data):
        try:
            # 데이터 중복 방지 코드
            requests = [UpdateOne({'DATETIME': item['DATETIME']}, {'$set': item}, upsert=True) for item in data]
            self.collection.bulk_write(requests, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            print(e.details['writeError'])

    def close(self):
        self.client.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
