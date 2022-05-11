import cx_Oracle
from configStr import config_geo3_transform
from configStr import config_CDTC
from configStr import config_POC
from configStr import config_ARCH
from configStr import config_WEEKC
import county


class OracleOperation:
	def openOracleConn(self):
		ConStr2Orcl = cx_Oracle.connect(config_geo3_transform.username, config_geo3_transform.password, config_geo3_transform.dsn, encoding=config_geo3_transform.encoding)
		return ConStr2Orcl

	def openOracleConn_CDTC(self):
		ConStr2OrclCDTC = cx_Oracle.connect(config_CDTC.username, config_CDTC.password, config_CDTC.dsn, encoding=config_CDTC.encoding)
		return ConStr2OrclCDTC

	def openOracleConn_POC(self):
		ConStr2OrclPOC = cx_Oracle.connect(config_POC.username, config_POC.password, config_POC.dsn, encoding=config_POC.encoding)
		return ConStr2OrclPOC

	def openOracleConn_ARCH(self):
		ConStr2OrclARCH = cx_Oracle.connect(config_ARCH.username, config_ARCH.password, config_ARCH.dsn, encoding=config_ARCH.encoding)
		return ConStr2OrclARCH

	def openOracleConn_WEEKC(self):
		ConStr2OrclWEEKC = cx_Oracle.connect(config_WEEKC.username, config_WEEKC.password, config_WEEKC.dsn, encoding=config_WEEKC.encoding)
		return ConStr2OrclWEEKC

	def selectOracle(self, sqlstr, connection):
		mycursor = connection.cursor()
		result = mycursor.execute(sqlstr)
		print('type of result', type(result))
		print('results:', result)
		# print('Number of rows returned: %d' % cursor.rowcount)
		rows = mycursor.fetchall()
		for i in rows:
			print("rows:", i)
		mycursor.close()

	def updateOracle(self, sqlstr, connection):
		mycursor = connection.cursor()
		result = mycursor.execute(sqlstr)
		connection.commit()
		print("Update succesful")


if __name__ == '__main__':
	try:
		db = OracleOperation()
		connection = db.openOracleConn()
		print("Connection successful", connection.version)

		connection_CDTC = db.openOracleConn_CDTC()
		print("connection.version: ", connection_CDTC.version)

		connection_POC = db.openOracleConn_POC()
		print("connection.version: ", connection_POC.version)

		connection_ARCH = db.openOracleConn_ARCH()
		print("connection.version: ", connection_ARCH.version)

		connection_WEEKC = db.openOracleConn_WEEKC()
		print("connection.version: ", connection_WEEKC.version)

	# sqlstr = "select * from tdlp_parcels_test9e where rownum < 5"
	# db.selectOracle(sqlstr, connection)

	# sqlstr2 = "Update tdlp_parcels_test9e set gid = 1"
	# db.updateOracle(sqlstr2, connection)

	except cx_Oracle.Error as error:
		print(error)

	finally:
		# release the connection
		if connection:
			connection.close()
			connection_CDTC.close()
			connection_POC.close()
			connection_ARCH.close()
			connection_WEEKC.close()
