
import json
from kafka import KafkaConsumer

consommateur = KafkaConsumer('donnees_localisation', group_id='group_donnees_localisation', bootstrap_servers='localhost:9092', enable_auto_commit=False,  auto_offset_reset='earliest')

for msg in consommateur:
    # Afficher la clé et les données lues
    print(f'Topic: {msg.topic}, Key: {msg.key.decode()}, Value: {msg.value}')
    consommateur.commit()


