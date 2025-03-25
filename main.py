from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import subprocess
import toml
import os

app = Flask(__name__)
CORS(app)

@app.route('/runNotebook', methods=['POST'])
def run_notebook():
    import pistachio
    return jsonify('Notebook executed successfully')


@app.route('/getBatterReport', methods=['GET'])
def get_batter_report():
    return send_from_directory('reports', 'batter_sWar.csv')


@app.route('/getPitcherReport', methods=['GET'])
def get_pitcher_report():
    return send_from_directory('reports', 'pitcher_sWar.csv')

@app.route('/getLsDir', methods=['GET'])
def get_lsdir():
    files = os.listdir(os.path.dirname(os.path.abspath(__file__)))
    return jsonify(files)

@app.route('/getSettings', methods=['GET'])
def get_settings():
    # list all files in the config directory
    files = os.listdir(os.getcwd())
    print(files)

    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        with open(base_dir + '/config/settings.toml', 'r') as file:
            settings_content = file.read()
            print(settings_content)
        return send_from_directory(os.path.join(base_dir, 'config'), 'settings.toml')
    except FileNotFoundError:
        return jsonify('Settings file not found'), 404


@app.route('/setSettings', methods=['POST'])
def set_settings():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(base_dir, 'config', 'settings.toml')

    with open(settings_path, 'r') as file:
        config = toml.load(file)

    data = request.get_json()
    print("Received Data:", data)

    if 'csv_path' in data and data['csv_path']:
        config['Settings']['csv_path'] = data['csv_path']
    if 'scout_id' in data and data['scout_id']:
        config['Settings']['scout_id'] = int(data['scout_id'])
    if 'team_id' in data:
        config['Settings']['team_id'] = data['team_id']
    if 'gb_weight' in data and data['gb_weight']:
        config['Settings']['gb_weight'] = int(data['gb_weight'])

    with open(settings_path, 'w') as configfile:
        toml.dump(config, configfile)

    return jsonify('Settings updated successfully')


@app.route('/getBatterColumns', methods=['GET'])
def get_batter_columns():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(base_dir + '/config', 'batter-columns.txt')


@app.route('/getPitcherColumns', methods=['GET'])
def get_pitcher_columns():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(base_dir + '/config', 'pitcher-columns.txt')


@app.route('/setBatterColumns', methods=['POST'])
def set_batter_columns():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data = request.get_data(as_text=True)
    with open(base_dir + '/config/batter-columns.txt', 'w') as file:
        file.write(data)
    return jsonify('Batter columns updated successfully')


@app.route('/setPitcherColumns', methods=['POST'])
def set_pitcher_columns():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data = request.get_data(as_text=True)
    with open(base_dir + '/config/pitcher-columns.txt', 'w') as file:
        file.write(data)
    return jsonify('Pitcher columns updated successfully')


@app.route('/getFlagged', methods=['GET'])
def get_flagged():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(base_dir + '/config', 'flagged.txt')


@app.route('/setFlagged', methods=['POST'])
def set_flagged():
    data = request.get_data(as_text=True)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    with open(base_dir + '/config/flagged.txt', 'w') as file:
        file.write(data)
    return jsonify('Flagged players updated successfully')


if __name__ == '__main__':
    import threading
    flask_thread = threading.Thread(target=app.run(host='127.0.0.1', port=5000))
    flask_thread.daemon = True
    flask_thread.start()
