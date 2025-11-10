from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from Goit_HW_topic5_configs import kafka_config, building_sensors, temperature_alerts, humidity_alerts
import json
import uuid
import time
import random

# Створення Kafka Consumer
try:
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        #security_protocol=kafka_config['security_protocol'],
        #sasl_mechanism=kafka_config['sasl_mechanism'],
        #sasl_plain_username=kafka_config['username'],
        #sasl_plain_password=kafka_config['password'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8'),
        auto_offset_reset='earliest',  # Зчитування повідомлень з початку
        enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
        group_id='consumer_group'   # Ідентифікатор групи споживачів
    )
    print("Kafka Consumer created successfully.")
except NoBrokersAvailable:
    print(f"Error: Cannot connect to Kafka brokers at {kafka_config['bootstrap_servers']}.")
    print("Please check connection settings or broker availability.")
    exit()
except Exception as e:  
    print(f"An unexpected error occurred: {e}")
    exit()  

# Підписка на тему
consumer.subscribe([building_sensors])
print(f"Subscribed to topic '{building_sensors}'")

# Створення Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        #security_protocol=kafka_config['security_protocol'],
        #sasl_mechanism=kafka_config['sasl_mechanism'],
        #sasl_plain_username=kafka_config['username'],
        #sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    print("Kafka Producer created successfully.")
except NoBrokersAvailable:
    print("No Kafka brokers are available. Please check the connection settings.")
    exit()
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    exit()

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
        if message.value.get('temperature') is not None and message.value.get('temperature') > 40:
            print(f"ALERT: High temperature detected from sensor {message.value.get('sensor_building_id')}!")
            try:
                temperature_alert = {
                    "alert_timestamp": time.time(),
                    "alert_type": "HIGH_TEMPERATURE",
                    "original_timestamp": message.value.get('timestamp'),
                    "sensor_building_id": message.value.get('sensor_building_id'),
                    "temperature": message.value.get('temperature'),
                    "message": f"Temperature {message.value.get('temperature')}°C exceeds 40°C threshold."
                }
                key_bytes = str(message.value.get('sensor_building_id')).encode('utf-8')
                producer.send(temperature_alerts, key=key_bytes, value=temperature_alert)
                producer.flush()
                print(f"Message sent to topic '{temperature_alerts}' successfully.")
            except Exception as e:
                print(f"An error occurred: {e}")

        if message.value.get('humidity') is not None and (message.value.get('humidity') < 20 or message.value.get('humidity') > 80):
            print(f"ALERT: High humidity detected from sensor {message.value.get('sensor_building_id')}!")
            try:
                humidity_alert = {
                    "alert_timestamp": time.time(),
                    "alert_type": "HIGH_HUMIDITY",
                    "original_timestamp": message.value.get('timestamp'),
                    "sensor_building_id": message.value.get('sensor_building_id'),
                    "humidity": message.value.get('humidity'),
                    "message": f"Humidity {message.value.get('humidity')}% is outside the 20-80% normal range."
                }
                key_bytes = str(message.value.get('sensor_building_id')).encode('utf-8')
                producer.send(humidity_alerts, key=key_bytes, value=humidity_alert)
                producer.flush()
                print(f"Message sent to topic '{humidity_alerts}' successfully.")
            except Exception as e:
                print(f"An error occurred: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    print("Closing consumer and producer...")   
    consumer.close()  # Закриття consumer
    producer.close()  # Закриття producer