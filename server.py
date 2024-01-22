from flask import Flask, jsonify
import json
from pathlib import Path
import csv


app = Flask(__name__)


def load_data(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)


def generate_csv(data):
    # Générer le fichier CSV sous forme de texte
    header = data[0].keys()
    rows = [row.values() for row in data]
    csv_content = '\n'.join([','.join(map(str, header))] + [','.join(map(str, row)) for row in rows])
    return csv_content 

# Route pour obtenir les données à partir d'un instant précis
@app.route('/api/<date>/<format_type>', methods=['GET'])
def get_data(date, format_type):
    try:
        data_file_path = f'{date}.json'

        data = load_data(data_file_path)

        if len(data) == 0:
            return jsonify({'error': 'Aucune donnée disponible pour cet instant.'}), 404
        
            # Convertir les données au format demandé (JSON ou CSV)
        if format_type.lower() == 'json':
            response = jsonify(data)
        elif format_type.lower() == 'csv':
                # Utilisez la fonction generate_csv pour générer le fichier CSV
            response = generate_csv(data)
        else:
            return jsonify({'error': 'Format non pris en charge.'}), 400

        return response

    
    except ValueError:
        return jsonify({'error': 'Format de timestamp invalide.'}), 400



if __name__ == '__main__':
    app.run(debug=True)
