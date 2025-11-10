from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from Goit_HW_topic5_configs import kafka_config, temperature_alerts, humidity_alerts
import json

# Створення Kafka Consumer
try:
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        #security_protocol=kafka_config['security_protocol'],
        #sasl_mechanism=kafka_config['sasl_mechanism'],
        #sasl_plain_username=kafka_config['username'],
        #asl_plain_password=kafka_config['password'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8'),
        auto_offset_reset='earliest',  # Зчитування повідомлень з початку
        enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
        group_id='monitoring_group'   # Ідентифікатор групи споживачів
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
consumer.subscribe([temperature_alerts, humidity_alerts])
print(f"Subscribed to topics '{temperature_alerts}' and '{humidity_alerts}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        if message.topic == temperature_alerts:
            print(f"ALERT: Temperature {message.value.get('temperature')}°C exceeds 40°C threshold.")
            print(f"Details:\n {message.value}")
        elif message.topic == humidity_alerts:
            print(f"ALERT: Humidity {message.value.get('humidity')}% is outside the 20-80% normal range.")
            print(f"Details:\n {message.value}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    print("Closing consumer...")
    consumer.close()  # Закриття consumer