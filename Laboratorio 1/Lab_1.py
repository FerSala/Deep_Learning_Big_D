import pandas as pd
from datetime import datetime
import json
from collections import defaultdict

df = pd.read_csv('Laboratorio 1/kz.csv.zip')
#EDA:
print('Encabezados de columnas: ',df.columns)
print('Cantidad de rows: ',len(df))
print(df.info())
print('Datos nulos: \n',df.isna().sum())
print('Datos vacios: \n', (df == '').sum())
df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
df['event_time'] = df['event_time'].dt.strftime("%Y-%m-%d %H:%M")
# Reemplazar valores NaN de price con 0
df['price'] = df['price'].fillna(0)

class Mongo:
    def __init__(self, host='localhost', port=27017, db_name='mydb', collection='data'):
        from pymongo import MongoClient
        self.client = MongoClient(host, port)
        self.collection = self.client[db_name][collection]
#Para subir datos a MongoDB, se puede usar insert_many o update_one con upsert=True para evitar duplicados.
#Los datos deben ser convertidos a un formato adecuado, como diccionarios o formatos JSON.
#Para realizar consultas, se puede usar find() o aggregate() para operaciones más complejas con calculos matematicos de por medio.
#Para este ejemplo se puede limpiar la db de datos previos con drop() si es necesario para evitar tiempos de procesamiento largos.
#Ya que el codigo esta revisando 1 por 1 los datos para evitar duplicados.
    def upload(self, data, id_field=None):
        if data is None:
            raise ValueError("Data is empty.")
        else:
            data = data.to_dict('records')
        self.collection.drop()  # Limpiar la colección antes de insertar nuevos datos
        try:
            self.collection.insert_many(data, ordered=False) #Ordered=False permite insertar múltiples documentos sin detenerse ante un error.
        except Exception as e:
            print(f"Error inserting data: {e}")
                

    def request_db(self, query=None):
        if query is None:
            return self.collection.find()
        else:
            return self.collection.find(query)
    
    def request_data(self, query=None):
        return list(self.collection.aggregate(query)) if query else self.collection.find()
    
    def close(self):
        self.client.close()
#  Redis no maneja bien estructuras complejas como Mongo. Debes guardar los registros como JSON o hash manualmente.
class Redis:
    def __init__(self, host='localhost', port=6379):
        import redis
        self.client = redis.Redis(host=host, port=port)
    # Para subir datos a Redis, se pueden usar los comandos set o hmset para almacenar datos como hashes.
    # Los datos deben ser convertidos a un formato adecuado, como JSON o diccionarios
    # Para realizar consultas, se puede usar get o hgetall para obtener los datos almacenados.
    # Para evitar duplicados, se puede limpiar la base de datos antes de insertar nuevos datos
    # o usar un esquema de claves que evite colisiones.
    # Redis no es una base de datos relacional, por lo que no soporta consultas complejas como MongoDB.
    # Sin embargo, se pueden usar patrones de claves para organizar los datos y realizar consultas simples.
    # En este ejemplo, se usa un patrón de claves para almacenar los datos como hashes.
    # Se usa un pipeline para mejorar el rendimiento al insertar múltiples registros.
    # Se usa flushall para limpiar la base de datos antes de insertar nuevos datos.
    # Se usa json.dumps para serializar los datos a JSON antes de almacenarlos.
    # Se usa json.loads para deserializar los datos de JSON al obtenerlos.
    # Se usa scan para obtener todas las claves que coincidan con un patrón.
    # Se usa mget para obtener múltiples valores en un solo paso.
    def upload(self, data):
        self.client.flushall()  # Limpiar la base de datos antes de insertar nuevos datos
        pipe = self.client.pipeline()
        data = data.to_dict('records')
        for i, item in enumerate(data):
            pipe.set(f"item:{i}", json.dumps(item, default=str))
        pipe.execute()

    def request_data(self, key_pattern='item:*'):
        #keys = self.client.keys(key_pattern)
        #return [json.loads(self.client.get(key).decode('utf-8')) for key in keys]
        cursor = 0
        all_keys = []

        # Escanea claves que coincidan con el patrón
        while True:
            cursor, keys = self.client.scan(cursor=cursor, match=key_pattern, count=1000)
            all_keys.extend(keys)
            if cursor == 0:
                break

        if not all_keys:
            return []

        # Obtener todos los valores en un solo paso
        values = self.client.mget(all_keys)

        # Decodificar y deserializar JSON
        return [
            json.loads(val.decode("utf-8"))
            for val in values if val is not None
        ]
    def close(self):
        self.client.close()
    
class HBase:
    def __init__(self, host='localhost'):
        import happybase
        self.connection = happybase.Connection(host=host)
        self.table = self.connection.table('my_table')
    #Problemas con tipo de valor binario
    #Problemas con la creacion de la table, no estaba creada y por ende el hbase no podia guardar los datos en ella.
    #Verificar con codigo que la table este creada antes de darle datos.
    #El formato de las columnas debe tener el b'cf':'col' en su key
    def clean_data_for_hbase(self, df):
        clean_df = df.copy()
        # Fill all nulls based on column type
        for col in clean_df.columns:
            if clean_df[col].dtype in ['int64', 'float64']:
                clean_df[col] = clean_df[col].fillna(0)
            else:
                clean_df[col] = clean_df[col].fillna('')
        return clean_df
    
    def _sanitize_value(self, val):
        if isinstance(val, (datetime, pd.Timestamp)):
            return val.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(val, bytes):
            return val.decode('utf-8', errors='ignore')
        elif pd.isnull(val):
            return ""
        else:
            return str(val)

    def upload(self, data, batch_size=10000):
        cleaned_data = self.clean_data_for_hbase(data)
        data = cleaned_data.to_dict('records')
        with self.table.batch(batch_size=batch_size) as batch:
            for i, item in enumerate(data):
                row_key = f'row{i}'

                encoded_data = {b'cf:' + k.encode('utf-8'): 
                    v if isinstance(v, bytes) else str(v or '').encode('utf-8')
                    for k, v in item.items()}
                batch.put(row_key.encode('utf-8'), encoded_data)

    def request_data(self, row_prefix=None):
        if row_prefix:
            rows = self.table.scan(row_prefix=row_prefix.encode('utf-8'))
        else:
            rows = self.table.scan()
        return rows
    
class HBaseClient:
    def __init__(self, host='localhost', table_name='my_table', column_family='cf'):
        import happybase
        self.connection = happybase.Connection(host)
        self.connection.open()
        self.table_name = table_name
        self.cf = column_family.encode('utf-8')
        
        # Verificamos si existe la tabla
        if table_name.encode('utf-8') not in self.connection.tables():
            self.connection.create_table(table_name, {self.cf: dict()})
        self.table = self.connection.table(table_name)

    def sanitize_value(self, value):
        if pd.isnull(value):
            return b"null"
        elif isinstance(value, (pd.Timestamp, datetime)):
            return str(value).encode('utf-8')
        else:
            return str(value).encode('utf-8')

    def upload(self, df, batch_size=1000):
        data = df.to_dict('records')
        
        with self.table.batch(batch_size=batch_size) as batch:
            for i, item in enumerate(data):
                row_key = f"row{i}".encode('utf-8')
                formatted_data = {
                    f"{self.cf.decode('utf-8')}:{k}": self.sanitize_value(v)
                    for k, v in item.items()
                }
                batch.put(row_key, formatted_data)
                if i == 4999:
                    print('Record 5000')
                

    def close(self):
        self.connection.close()
'''
#MongoDB:
start = datetime.now()
print("Subiendo datos a MongoDB...")
mongo = Mongo()
mongo.upload(df, id_field='order_id')
finish = datetime.now()
print("Tiempo de subida a MongoDB:", finish - start, "segundos", '\n')

#Cuál es la categoría más vendida?
start = datetime.now()
ventas_mongo = mongo.request_data([
    {"$group": {"_id": "$category_id", "total_sales": {"$sum": "$price"}}},
    {"$sort": {"total_sales": -1}},
    {"$limit": 1}])
finish = datetime.now()
print("Tiempo de ejecución para la categoría más vendida:", finish - start, "segundos")
print("Categoría más vendida:", ventas_mongo[0]['_id'], "con ventas totales de: ",'$',ventas_mongo[0]['total_sales'], '\n')
#Cuál marca (brand) generó más ingresos brutos?
start = datetime.now()
ventas_mongo_brand = mongo.request_data([
    {"$group": {"_id": "$brand", "total_sales": {"$sum": "$price"}}},
    {"$sort": {"total_sales": -1}},
    {"$limit": 1}])
finish = datetime.now()
print("Tiempo de ejecución para la marca más vendida:", finish - start)
print("Marca con más vendida:", ventas_mongo_brand[0]['_id'], "con ventas totales de: ",'$',ventas_mongo_brand[0]['total_sales'], '\n')

#Qué mes tuvo más ventas? (En UTC)
start = datetime.now()
ventas_mongo_month = mongo.request_data([
    {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$event_time"}}, "total_sales": {"$sum": "$price"}}},
    {"$sort": {"total_sales": -1}},
    {"$limit": 1}])
finish = datetime.now()
print("Tiempo de ejecución para el mes con más ventas:", finish - start, "segundos")
print("Mes con más ventas:", ventas_mongo_month[0]['_id'], "con ventas totales de: ",'$',ventas_mongo_month[0]['total_sales'], '\n')


#Redis:
start = datetime.now()
print("Subiendo datos a Redis...")
redis_client = Redis()
redis_client.upload(df)
finish = datetime.now()
print("Tiempo de subida a Redis:", finish - start, "segundos", '\n')
#Cuál es la categoría más vendida?
start = datetime.now()
ventas_redis = redis_client.request_data()
ventas_redis_grouped = {}
for item in ventas_redis:
    category = item.get('category_id')
    price = item.get('price', 0)
    if category in ventas_redis_grouped:
        ventas_redis_grouped[category] += price
    else:
        ventas_redis_grouped[category] = price
ventas_redis_sorted = sorted(ventas_redis_grouped.items(), key=lambda x: x[1], reverse=True)
finish = datetime.now()
print("Tiempo de ejecución para la categoría más vendida en Redis:", finish - start, "segundos")
if ventas_redis_sorted:
    print("Categoría más vendida:", ventas_redis_sorted[0][0], "con ventas totales de: ",'$',ventas_redis_sorted[0][1], '\n')

#Cuál marca (brand) generó más ingresos brutos?
start = datetime.now()
ventas_redis_brand = {}
for item in ventas_redis:
    brand = item.get('brand')
    price = item.get('price', 0)
    if brand in ventas_redis_brand:
        ventas_redis_brand[brand] += price
    else:
        ventas_redis_brand[brand] = price
ventas_redis_brand_sorted = sorted(ventas_redis_brand.items(), key=lambda x: x[1], reverse=True)
finish = datetime.now()
print("Tiempo de ejecución para la marca más vendida en Redis:", finish - start)
if ventas_redis_brand_sorted:
    print("Marca con más vendida:", ventas_redis_brand_sorted[0][0], "con ventas totales de: ",'$',ventas_redis_brand_sorted[0][1], '\n')

#Qué mes tuvo más ventas?
start = datetime.now()
ventas_redis_month = {}
for item in ventas_redis:
    event_time = item.get('event_time')
    if event_time:
        month = event_time[:7]  # Formato YYYY-MM
        price = item.get('price', 0)
        if month in ventas_redis_month:
            ventas_redis_month[month] += price
        else:
            ventas_redis_month[month] = price
ventas_redis_month_sorted = sorted(ventas_redis_month.items(), key=lambda x: x[1], reverse=True)
finish = datetime.now()
print("Tiempo de ejecución para el mes con más ventas en Redis:", finish - start, "segundos")
if ventas_redis_month_sorted:
    print("Mes con más ventas:", ventas_redis_month_sorted[0][0], "con ventas totales de: ",'$',ventas_redis_month_sorted[0][1], '\n')
'''
#HBase:
start = datetime.now()
print("Subiendo datos a HBase...")
hbase_client = HBase()
#hbase_client.upload(df)
finish = datetime.now()
print("Tiempo de subida a HBase:", finish - start, "segundos", '\n')
data = hbase_client.request_data()


category_sales = defaultdict(float)
brand_sales = defaultdict(float)
sales_by_month = defaultdict(float)

for _, row in data:
    row_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in row.items()}
    
    category = row_data.get('cf:category_id')
    sales = row_data.get('cf:price')
    brand = row_data.get('cf:brand')
    date = row_data.get('cf:event_time')

    if category and sales:
        try:
            category_sales[category] += float(sales)
        except ValueError:
            continue  # Ignorar valores no numéricos
    if brand and sales:
        try:
            brand_sales[brand] += float(sales)
        except ValueError:
            continue  # Ignorar valores no numéricos
    if date and sales:
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d %H:%M')  # o el formato que estés usando
            month_str = date_obj.strftime('%Y-%m')
            sales_by_month[month_str] += float(sales)
        except ValueError:
            continue
#Cuál es la categoría más vendida?
category_most_sold = max(category_sales.items(), key=lambda x: x[1])
print('La categoria con mas ventas fue',category_most_sold[0], 'con: $', category_most_sold[1])

#Cuál marca (brand) generó más ingresos brutos?
brand_most_sold = max(brand_sales.items(), key=lambda x: x[1])
print('La categoria con mas ventas fue',brand_most_sold[0], 'con: $', brand_most_sold[1])
#Qué mes tuvo más ventas?
best_month = max(sales_by_month, key=sales_by_month.get)
print('El mes con mas ventas fue:', best_month[0], 'con: $', best_month[1])
