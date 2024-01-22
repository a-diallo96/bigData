
import json
from kafka import KafkaConsumer

consommateur = KafkaConsumer('donnees_localisation', group_id='group_donnees_localisation', bootstrap_servers='localhost:9092', enable_auto_commit=False)

for msg in consommateur:
    ligne = json.loads(msg.value)
    print(f'Topic: {msg.topic}, Key: {msg.key.decode()}, Value: {msg.value}')
    consommateur.commit()


