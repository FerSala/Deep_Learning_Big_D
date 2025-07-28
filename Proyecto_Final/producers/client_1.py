import json
from kafka import KafkaProducer
import pandas as pd
import time
import random

data = pd.read_csv('Proyecto_Final/producers/data.csv', encoding='windows-1252')

comments = data.iloc[:9160, :2].to_dict(orient='records')
print('Comments loaded:', len(comments))

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
                         )


while True:
    try:
        comment = random.choice(comments)
        key = str(comment['id']).encode('utf-8')
        value = str(comment['comment']).encode('utf-8')
        producer.send('comments', key=key, value=value)
        time.sleep(random.uniform(0.5, 3.0))

    except Exception as e:
        print("Error occurred:", e)

producer.flush()
producer.close()
