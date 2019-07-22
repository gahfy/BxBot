# Installation

## Modules

First, you need to install required Python modules:

```bash
pip3 install mysql.connector bigfloat requests websocket_client pytz
```

## Database

Then, you need to create a database to store data, and change values in `config.py` according to the database you just
created.

## Installation script

```bash
python3 install.py
```

### Script to run

Import data from webservices
```bash
python3 import.py
```