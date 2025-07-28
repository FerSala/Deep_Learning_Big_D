from kafka import KafkaProducer
import pandas as pd

data = pd.read_csv('Proyecto_Final/producers/data.csv', encoding='windows-1252')

comments = data.iloc[:, :2].to_dict(orient='records')
print(comments)
