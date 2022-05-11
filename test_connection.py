# test_connection.py

import cx_Oracle
import config_geo3_transform

connection = None
try:
    connection = cx_Oracle.connect(
        config_geo3_transform.username,
        config_geo3_transform.password,
        config_geo3_transform.dsn,
        encoding=config_geo3_transform.encoding)
	
    mycursor = connection.cursor()

    # show the version of the Oracle Databasepip
    print("Connection successful")
    print("connection.version: ", connection.version)
    print(mycursor)
except cx_Oracle.Error as error:
    print(error)
finally:
    # release the connection
    if connection:
        connection.close()