import cx_Oracle
import config_geo3_transform

class oracleOperation():
	def openOracleConn(self):
		ConStr2Orcl = cx_Oracle.connect(config_geo3_transform.username,
        					config_geo3_transform.password,
        					config_geo3_transform.dsn,
        					encoding=config_geo3_transform.encoding)
		return ConStr2Orcl 


	def select(self, connection):
		mycursor = connection.cursor()

		sqlstr = 'select * from tdlp_parcels_test9e where rownum < 5'
		result = mycursor.execute(sqlstr)
		print('type of result', type(result))
		print('results:', result)
		# print('Number of rows returned: %d' % cursor.rowcount)
		rows = mycursor.fetchall()  
		for i in rows:
			print("rows:", i)  
		mycursor.close()



if __name__=='__main__':
	try:
		db = oracleOperation()
		connection = db.openOracleConn()
		print("Connection successful")
		print("connection.version: ", connection.version)
		
		db.select(connection)
	except cx_Oracle.Error as error:
    		print(error)
	finally:
    		# release the connection
    		if connection:
        		connection.close()
