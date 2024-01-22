
import json
from kafka import KafkaConsumer

consommateur = KafkaConsumer('type_route', group_id='group_route', bootstrap_servers='localhost:9092', enable_auto_commit=False,  auto_offset_reset='earliest')

for msg in consommateur:
    ligne = json.loads(msg.value)
    print(f'Topic: {msg.topic}, Key: {msg.key.decode()}, Value: {msg.value}')
    consommateur.commit()


