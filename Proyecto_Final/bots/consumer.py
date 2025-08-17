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
while True:
    # Consume messages from Kafka
    try:
        msg = next(consumer)
    except StopIteration:
        print("No more messages to consume. Waiting for new messages...")
        continue
    except Exception as e:
        print(f"Error consuming message: {e}")
        continue
    except KeyboardInterrupt: # Press Ctrl+C to stop the loop
        print("Interrupted by user. Exiting...")
        break
    # Process and insert into MongoDB
    if msg.value is not None:
        print(f"Consumed message: {msg.value}")
        try:
            # Add 'Status' field to the message
            msg.value['Status'] = 'Not Processed'
            
            # Insert the modified message into MongoDB
            collection.insert_one(msg.value)
            print("Message inserted into MongoDB with 'Status: Not Processed'.")
        except Exception as e:
            print(f"Error inserting message into MongoDB: {e}")
    else:
        print("Received an empty message.")

    

# Close the consumer and MongoDB connection
consumer.close()
mongo_client.close()