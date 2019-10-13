Speechan application can be used by four endpoints:

* / : used for initial login to yandex disk. One need to specify /callback as a "Callback URI" in yandex disk app info
* /calls : used to get calls meta data. Takes 2 optional parameters in query string: date_from/date_till - unix timestamp of the lowest/highest call entry
* /recording : used to download the call audio file. Take 1 required parameter in query string: call_id - id of the call that needs to be downloaded
* /operators : used to get list of all operator codes and their names

### Setup:
1. `virtualenv env`
2. `env\scripts\activate`
3. `pip install -r requirements.txt`

### How to run:
`python app.py`

### Settings (`settings.py`)
* app_id - your yandex.disk app id
* app_pwd - your yandex.disk app password
* secret_key - flask app secret key