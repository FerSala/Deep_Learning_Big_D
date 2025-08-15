# Proyecto Final - Modelos de Machine Learning y Bases de Datos
import torch
import pandas as pd

class SentimentAnalysis:
    def __init__(self, model_name="tabularisai/multilingual-sentiment-analysis"):
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
        
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)

    def predict_sentiment(self, texts):
        inputs = self.tokenizer(texts, return_tensors="pt", truncation=True, padding=True, max_length=512)
        with torch.no_grad():
            outputs = self.model(**inputs)
        probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
        sentiment_map = {0: "Very Negative", 1: "Negative", 2: "Neutral", 3: "Positive", 4: "Very Positive"}
        return [sentiment_map[p] for p in torch.argmax(probabilities, dim=-1).tolist()], probabilities

    def predict(self, texts):
        if not isinstance(texts, list):
            raise ValueError("Input must be a list of strings.")
        if not texts:
            raise ValueError("Input list is empty.")
        return self.predict_sentiment(texts)

class Mongo:

    def __init__(self, host='localhost', port=27017, db_name='mi_basedatos', collection='mi_coleccion'):
        from pymongo import MongoClient
        self.client = MongoClient(host, port)
        self.collection = self.client[db_name][collection]

    def upload(self, data):
        if data is None:
            raise ValueError("Data is empty.")
        else:
            data = data.to_dict('records')
        self.collection.drop()  # Limpiar la colección antes de insertar nuevos datos
        try:
            self.collection.insert_many(data)
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

class MySQL:

    def __init__(self, host='localhost', port=3306, user='proyecto', password='deeplearning', database='mi_basedatos'):
        import mysql.connector
        self.connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor(dictionary=True)

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS mi_tabla (
                id INT AUTO_INCREMENT PRIMARY KEY,
                User_ID VARCHAR(255) NOT NULL,
                Texto VARCHAR(255) NOT NULL,
                Sentimiento VARCHAR(50) NOT NULL,
            )
        """)
        self.connection.commit()
        
    def upload(self, data):
        if data is None:
            raise ValueError("Data is empty.")
        else:
            data = data.to_dict('records')
        self.cursor.execute("DELETE FROM mi_tabla")
        try:
            for record in data:
                self.cursor.execute("""
                    INSERT INTO mi_tabla (User_ID, Texto, Sentimiento)
                    VALUES (%s, %s, %s)
                """, (record['User_ID'], record['Texto'], record['Sentimiento']))
            self.connection.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.connection.rollback()
        
    def close(self):
        self.cursor.close()
        self.connection.close()

mongo = Mongo()
mysql = MySQL()
sentiment_analyzer = SentimentAnalysis()
# Example usage:

while True:
    try:
        # Assuming 'data' is a DataFrame with the necessary columns
        data = mongo.request_db()
        if not data:
            print("No data found in MongoDB.")
            break
        # Convert MongoDB data to a DataFrame
        data = pd.DataFrame(list(data))
        if data.empty:
            print("No data found in MongoDB.")
            break
        print("Data retrieved from MongoDB:", data.head())
        # Predict sentiment
        sentiments, probabilities = sentiment_analyzer.predict(data['text'].tolist())
        data['Sentiment'] = sentiments
        
        # Upload to MongoDB
        # Update MongoDB with processed status
        for record in data.to_dict('records'):
            mongo.collection.update_one(
            {'_id': record['_id']}, 
            {'$set': {'status': 'procesado', 'Sentiment': record['Sentiment']}}
            )
        
        # Upload to MySQL
        mysql.upload(data)
        
        print("Data uploaded successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        mongo.close()
        mysql.close()
        break  # Remove this line if you want to run continuously