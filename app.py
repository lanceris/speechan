from datetime import datetime
from io import BytesIO
import os


from authlib.flask.client import OAuth
from flask import Flask, jsonify, redirect, session, url_for, request, send_file
import requests
import pandas as pd
import yadisk


from settings import app_id, app_pwd, secret_key
from utils import get_files

app = Flask(__name__)
app.secret_key = secret_key
oauth = OAuth(app)
oauth.register(
    name='yadisk',
    client_id=app_id,
    client_secret=app_pwd,
    access_token_url='https://oauth.yandex.ru/token',
    authorize_url=f'https://oauth.yandex.ru/authorize',
)

NOT_IMPLEMENTED = {'error': 'Not implemented'}
cache = {}


@app.route('/login')
def login():
    redirect_uri = url_for('authorize', _external=True)
    return oauth.yadisk.authorize_redirect(redirect_uri)


@app.route('/callback')
def authorize():
    token = oauth.yadisk.authorize_access_token()
    session['yadisk'] = token
    session['yadisk']['meta'] = {}
    return redirect('/')


@app.route('/', methods=['GET'])
def index():
    if session.get('yadisk'):
        # т.к не было указания, что возвращать с "/", я решил вернуть список эндпоинтов
        endpoints = [str(point) for point in app.url_map.iter_rules() if 'static' not in str(point)]
        return jsonify({'endpoints': endpoints})
    return redirect('/login')


@app.route('/calls', methods=['GET'])
def calls():
    if not session.get('yadisk'):
        return jsonify({'error': 'Token not found in session. Go to {} to get one'.format(request.base_url)})

    token = session['yadisk']['access_token']
    date_from=request.args.get('date_from')
    date_to=request.args.get('date_till')
    calls, calls_map = get_files(token, cache, date_from, date_to)
    session['calls'] = calls
    session['calls_map'] = calls_map
    return jsonify({'calls': calls})


@app.route('/recording')
def recording():
    if not session['calls_map']:
        return jsonify({'error': 'Calls not found. Go to /calls to get one.'})

    call_id = request.args.get('call_id')
    if not call_id:
        return jsonify({'error': 'Please specify call_id'})

    if call_id not in session['calls_map']:
        return jsonify({'error': 'call_id={} not found.'.format(call_id)})

    try:
        call_entry = session['calls_map'][call_id]

        r = requests.get(call_entry['url']) 
        strIO = BytesIO(r.content) # получаем файл
        return send_file(strIO, as_attachment=True, attachment_filename=call_entry['filename'])
    except requests.exceptions.MissingSchema:
        return jsonify({'error': 'Url for call_id={} is incorrect: {}'.format(call_id, call_entry['url'])})
    except Exception as e:
        return jsonify({'error': str(e)})
    

@app.route('/operators')
def operators():
    operators = [
		{
			"phone_number": "101",
			"name": "Иванов Иван",
		},
		{
			"phone_number": "102",
			"name": "Петрова Мария",
		},
        {
			"phone_number": "103",
			"name": "Васильева Анастасия",
		},
		{
			"phone_number": "104",
			"name": "Смирнов Михаил",
		},

	]

    return jsonify({'operators': operators})


if __name__ == "__main__":
    app.run(debug=True, ssl_context='adhoc')