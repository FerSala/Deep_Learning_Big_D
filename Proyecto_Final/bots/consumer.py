from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    'user-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Conexión a MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['mi_basedatos']
collection = db['mi_coleccion']

print("Consuming messages...")
for msg in consumer:
    print(f"Consumed: {msg.value}\n")
    try:
        data = msg.value
        collection.insert_one(data)
        print('Insertado en MongoDB', data)
    except Exception as e:
        print(f'Error al insertar: {e}')
    