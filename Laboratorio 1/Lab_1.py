import pandas as pd

df = pd.read_csv('Laboratorio 1/kz.csv.zip')

class Mongo:
    def __init__(self, host='localhost', port=27017, db_name='mydb', collection='data'):
        from pymongo import MongoClient
        self.client = MongoClient(host, port)
        self.collection = self.client[db_name][collection]

    def upload(self, data):
        self.collection.insert_many(data)

    def request_data(self, query=None):
        if query is None:
            return self.collection.find()
        else:
            return self.collection.find(query)

class Redis:
    def __init__(self, host='localhost', port=6379):
        import redis
        self.client = redis.Redis(host=host, port=port)

    def upload(self, data):
        for i, item in enumerate(data):
            self.client.set(f"item:{i}", str(item))

class HBase:
    def __init__(self, host='localhost'):
        import happybase
        self.connection = happybase.Connection(host=host)
        self.table = self.connection.table('my_table')

    def upload(self, data):
        for i, item in enumerate(data):
            self.table.put(f'row{i}', item)