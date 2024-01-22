import time, json, random
from kafka import KafkaProducer
from datetime import datetime, timedelta
import requests


producteur = KafkaProducer(bootstrap_servers='localhost:9092')

# URL du serveur de test
server_test_url = 'http://127.0.0.1:5000/api'

# Exemple d'instant initial
current_timestamp = datetime.strptime('2024-01-21-01-00', '%Y-%m-%d-%H-%M')




while True:
    try:
        response = requests.get(f'{server_test_url}/{current_timestamp.strftime("%Y-%m-%d-%H-%M")}/json')
        data = response.json()
        for item in data:
            key = item.get('gml_id') 
            message = {
                'timestamp': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'emplacement': item.get('gml_id')
                }

            message_str = json.dumps(message)
            print(message_str)

            producteur.send('donnees_localisation', message_str.encode('utf-8'), key.encode())

        time.sleep(1)
    except requests.exceptions.JSONDecodeError:
        print("Erreur de décodage JSON : La réponse ne contient pas de données JSON valides.")


