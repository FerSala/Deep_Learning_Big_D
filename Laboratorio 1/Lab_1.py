import pandas as pd

df = pd.read_csv('Laboratorio 1/kz.csv.zip')
print(df.columns)

class Mongo:
    def __init__(self, host='localhost', port=27017, db_name='mydb', collection='data'):
        from pymongo import MongoClient
        self.client = MongoClient(host, port)
        self.collection = self.client[db_name][collection]

    def upload(self, data):
        data = data.to_dict(orient='records')
        self.collection.insert_many(data)

    def request_data(self, query=None):
        if query is None:
            return self.collection.find()
        else:
            return self.collection.find(query)      
#  Redis no maneja bien estructuras complejas como Mongo. Debes guardar los registros como JSON o hash manualmente.
class Redis:
    def __init__(self, host='localhost', port=6379):
        import redis
        self.client = redis.Redis(host=host, port=port)

    def upload(self, data):
        for i, item in enumerate(data):
            self.client.set(f"item:{i}", str(item))

    def request_data(self, key_pattern='item:*'):
        keys = self.client.keys(key_pattern)
        return {key.decode('utf-8'): self.client.get(key).decode('utf-8') for key in keys}

class HBase:
    def __init__(self, host='localhost'):
        import happybase
        self.connection = happybase.Connection(host=host)
        self.table = self.connection.table('my_table')

    def upload(self, data):
        for i, item in enumerate(data):
            self.table.put(f'row{i}', item)
    
    def request_data(self, row_prefix='row'):
        rows = self.table.scan(row_prefix=row_prefix)
        return {row[0]: row[1] for row in rows}