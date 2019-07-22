import mysql.connector
from config import *

conn = mysql.connector.connect(
    host=db_host,
    user=db_username,
    password=db_password,
    database=db_name,
    auth_plugin='mysql_native_password'
)
cursor = conn.cursor()