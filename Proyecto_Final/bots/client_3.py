import json
from kafka import KafkaProducer
import pandas as pd
import time
import random

data = pd.read_csv('Proyecto_Final/bots/data.csv', encoding='windows-1252')

comments = data.iloc[18321:27481, :2].to_dict(orient='records')

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
                         )

while True:
    try:
        comment = random.choice(comments)
        key = str(comment['textID']).encode('utf-8')
        value = str(comment['text']).encode('utf-8')
        producer.send('user-topic', value=comment) #Muy importante que el topic sea el mismo que el del consumer
        time.sleep(random.uniform(0.5, 3.0))
        print(f"Sent: {comment}")
    except Exception as e:
        print("Error occurred:", e)

producer.flush()
producer.close()