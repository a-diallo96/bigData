
import json
from kafka import KafkaConsumer

consommateur = KafkaConsumer('type_traffic', group_id='group_traffic', bootstrap_servers='localhost:9092', enable_auto_commit=False)

for msg in consommateur:
    ligne = json.loads(msg.value)
    print(f'Key: {msg.key.decode()}, Value: {ligne}')

    consommateur.commit()


