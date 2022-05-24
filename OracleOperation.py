import cx_Oracle
from config_para import config_geo3_transform
from config_para import config_CDTC
from config_para import config_POC
from config_para import config_ARCH
from config_para import config_WEEKC
from config_para import config_county as cc
from sys import modules
from datetime import datetime
import getpass
from datetime import date
import subprocess
import time
from functools import wraps


class OracleOperation:

	def __init__(self):
		self.connection = self.open_oracle_conn()
		my_cursor = self.connection.cursor()
		print("Connection GEO3.version", self.connection.version)

		self.connection_cdtc = self.open_oracle_conn_cdtc()
		my_cursor_cdtc = self.connection_cdtc.cursor()
		print("connection CDTC.version: ", self.connection_cdtc.version)

		self.connection_poc = self.open_oracle_conn_poc()
		my_cursor_poc = self.connection_poc.cursor()
		print("connection POC.version: ", self.connection_poc.version)

		self.connection_arch = self.open_oracle_conn_arch()
		my_cursor_arch = self.connection_arch.cursor()
		print("connection ARCHIVE.version: ", self.connection_arch.version)

		self.connection_weekc = self.open_oracle_conn_weekc()
		my_cursor_weekc = self.connection_weekc.cursor()
		print("connection WEEKC.version: ", self.connection_weekc.version)

	def flog(self, loginfo):
		# with open("U:\FME_LIVE\FULL\LOGS\LPIS_CLEAN_LOG.txt", 'a') as f:
		with open("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/LOGS/LPIS_CLEAN_LOG.txt", 'a') as f: 				# in geo4
			username = getpass.getuser()
			curr_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			f.write(curr_date + " | " + username + " | " + loginfo + "\n")

			f.close()

	# def logger(self, fn):
	# 	@wraps(fn)
	# 	def wrapper(loginfo, *args, **kwargs):
	# 		results = fn(*args, **kwargs)
	# 		with open("U:\FME_LIVE\FULL\LOGS\LPIS_CLEAN_LOG.txt", 'a') as f:
	# 			strusername = getpass.getuser()
	# 			CurrDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	# 			f.write(CurrDate + " | " + strusername + " | " + loginfo + "\n")
	#
	# 			f.close()
	# 		return results
	# 	return wrapper

	def fmerun(self, path):
		# cmd = 'cmd.exe d:/start.bat'
		p = subprocess.Popen("cmd.exe /c" + path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		curr_line = p.stdout.readline()
		while curr_line != b'':
			# print(curr_line)
			curr_line = p.stdout.readline()
		p.wait()
		print(p.returncode)

	def open_oracle_conn(self):
		con_str_22 = cx_Oracle.connect(config_geo3_transform.username, config_geo3_transform.password, config_geo3_transform.dsn, encoding=config_geo3_transform.encoding)
		return con_str_22

	def open_oracle_conn_cdtc(self):
		con_str_22_cdtc = cx_Oracle.connect(config_CDTC.username, config_CDTC.password,
											config_CDTC.dsn, encoding=config_CDTC.encoding)
		return con_str_22_cdtc

	def open_oracle_conn_poc(self):
		con_str_22_poc = cx_Oracle.connect(config_POC.username, config_POC.password,
										config_POC.dsn, encoding=config_POC.encoding)
		return con_str_22_poc

	def open_oracle_conn_arch(self):
		con_str_22_arch = cx_Oracle.connect(config_ARCH.username, config_ARCH.password,
											config_ARCH.dsn, encoding=config_ARCH.encoding)
		return con_str_22_arch

	def open_oracle_conn_weekc(self):
		con_str_22_weekc = cx_Oracle.connect(config_WEEKC.username, config_WEEKC.password,
											 config_WEEKC.dsn, encoding=config_WEEKC.encoding)
		return con_str_22_weekc

	def select_oracle(self, sqlstr, connection):
		mycursor = connection.cursor()
		result = mycursor.execute(sqlstr)
		print('type of result', type(result))
		print('results:', result)
		# print('Number of rows returned: %d' % cursor.rowcount)
		rows = mycursor.fetchall()
		for i in rows:
			print("rows:", i)
		mycursor.close()

	#@logger
	def update_oracle(self, sqlstr, connection):
		mycursor = connection.cursor()
		mycursor.execute(sqlstr)
		connection.commit()
		print("Update succesful")

	def create_county_shape(self):
		array_index = 1
		diss_type = 6
		array_len = len(cc.arrayc)
		add_to_all_counties_table = True
		for i in cc.arrayc:
			if array_index == 1:
				self.update_oracle("TRUNCATE TABLE POC_TOWNLANDS_CURR_DIS_DEL", self.connection_cdtc)
				self.update_oracle("TRUNCATE TABLE POC_TOWNLANDS_CURR_DIS", self.connection_cdtc)
				self.update_oracle("TRUNCATE TABLE POC_TOWNLANDS_CURR", self.connection_cdtc)
				self.update_oracle("TRUNCATE TABLE POC_REAL_TOWNLANDS_CURR", self.connection_cdtc)

			# global County
			countyname = i
			self.update_oracle("TRUNCATE TABLE POC_CURR_COUNTY_RUN", self.connection_cdtc)
			self.update_oracle("INSERT INTO POC_CURR_COUNTY_RUN (COUNTY_ID) VALUES ('{}')".format(countyname), self.connection_cdtc)
			print("90")
			if countyname == 'O':
				diss_type = 5
			else:
				diss_type = 6

			self.create_townlands_from_weekc("SF_TEMP", "POC_TOWNLANDS_CURR", diss_type, countyname)
			print("97")
			self.create_townlands_from_weekc("SF_TEMP", "POC_REAL_TOWNLANDS_CURR", 5, countyname)

			self.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/1_DISSOLVE_TOWNLANDS_RERUN.bat")
			self.flog("Run 1_DISSOLVE_TOWNLANDS_RERUN.bat")

			if array_len == array_index:
				add_to_all_counties_table = False

			self.create_ded_dis_for_each_del_county(countyname, "POC_TOWNLANDS_CURR_DIS_", add_to_all_counties_table)

			array_index += 1

		self.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/1_DISSOLVE_TOWNLANDS_RERUN_SHP.bat")
		self.flog("Run 1_DISSOLVE_TOWNLANDS_RERUN_SHP.bat")

	def delete_sf_temp(self):
		try:
			self.update_oracle("DROP TABLE SF_TEMP PURGE", self.connection_weekc)
			self.flog("DROP TABLE SF_TEMP ")
		except:
			self.flog("SF_TEMP not exist")

	def create_townlands_from_weekc(self, tablename, twn_tab_create, dissolve_type, county):
		str1_1 = '"'
		str1 = "CREATE TABLE {} AS SELECT SPF_FEATURE_ID, SPF_FEATURE_LABEL, SPF_SPT_TYPE_ID, CREATE_DATE, UPDATE_DATE, START_DATE, " \
			   "END_DATE, GEOM, SPF_VER_NUM, SPF_AUDIT_ACTION, SPF_AUDIT_CREATE_DATE, SPF_AUDIT_CREATE_USER, SPF_AUDIT_DATE, SPF_AUDIT_USER, " \
			   "SPF_AUDIT_LOCATION, LABEL FROM (SELECT SUF.*, DBMS_LOB.SUBSTR(REPLACE(TRIM(REGEXP_SUBSTR (REGEXP_SUBSTR(SPF_FEATURE_ATTRIBUTES, " \
			   "'[^,]+', 1, 2), '[^:]+', 1, 2)),'{}'), 4000,1) AS LABEL FROM TDLP_SUPER_FEATURE SUF  WHERE SPF_SPT_TYPE_ID = {} AND END_DATE > sysdate) " \
			   "A WHERE SUBSTR(A.LABEL,1,1) = '{}'".format(tablename, str1_1, dissolve_type, county)
		str1log = "CREATE TABLE {}".format(tablename)

		v_meta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  " \
				 "MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(tablename)
		v_metalog = "CREATE META {}".format(tablename)

		spatial_index = "CREATE INDEX SPX_{} ON {}(GEOM)  INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('sdo_indx_dims=2')".format(
			tablename, tablename)
		spatial_indexlog = "CREATE INDEX SPX ON {}".format(tablename)

		str_dict = {str1: str1log, v_meta: v_metalog, spatial_index: spatial_indexlog}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection_weekc)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

		str1 = "INSERT INTO {} SELECT * FROM SF_TEMP@LPIS_VECTOR_WEEKC".format(twn_tab_create)
		str1log = "INSERT INTO TABLE {}".format(twn_tab_create)
		v_meta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  " \
				 "MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(twn_tab_create)
		v_metalog = "CREATE META {}".format(twn_tab_create)
		spatial_index = "CREATE INDEX SPX_{} ON {}(GEOM)  INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('sdo_indx_dims=2')".format(
			twn_tab_create, twn_tab_create)
		spatial_indexlog = "CREATE INDEX SPX ON {}".format(twn_tab_create)
		str_dict2 = {str1: str1log, v_meta: v_metalog, spatial_index: spatial_indexlog}

		for i in str_dict2.keys():
			try:
				self.update_oracle(i, self.connection_cdtc)
				self.flog(str(str_dict2[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict2[i])))
				pass

		self.update_oracle("DROP TABLE {} PURGE".format(tablename), self.connection_weekc)
		self.flog("DROP TABLE " + tablename)

	def create_ded_dis_for_each_del_county(self, county_dis, table_name, addcounty):
		twntabcreate = table_name + county_dis
		try:
			# Drop townlands local table
			print("171")
			self.update_oracle("DROP TABLE " + twntabcreate, self.connection_cdtc)
			self.flog("DROP TABLE {}".format(twntabcreate))

		finally:
			str1 = "CREATE TABLE " + twntabcreate + " AS SELECT * FROM POC_TOWNLANDS_CURR_DIS "
			self.update_oracle(str1, self.connection_cdtc)														# create local townlands table
			self.flog("CREATE TABLE " + twntabcreate)

			del_meta = "delete from USER_SDO_GEOM_METADATA where table_name = '{}'".format(twntabcreate)
			self.update_oracle(del_meta, self.connection_cdtc)
			v_meta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(
				twntabcreate)
			self.update_oracle(v_meta, self.connection_cdtc)
			self.flog("CREATE META " + twntabcreate)
			try:
				Spatialindex = "CREATE INDEX SPX_{} ON {}(GEOM)  INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('sdo_indx_dims=2')".format(
					twntabcreate, twntabcreate)
				self.update_oracle(Spatialindex, self.connection_cdtc)
				self.flog("CREATE INDEX SPX " + twntabcreate)
			finally:
				if addcounty is True:
					str1 = "INSERT INTO  POC_TOWNLANDS_CURR_DIS_DEL SELECT * FROM " + twntabcreate
					self.update_oracle(str1, self.connection_cdtc)

	def create_local_table(self, tablename):
		try:
			self.update_oracle("DROP TABLE {} PURGE".format(tablename), self.connection_cdtc)
			self.flog("DROP TABLE {}".format(tablename))
			str1 = "CREATE TABLE {} AS  SELECT  HERD_NO, SPH_SPS_HOLDING_ID, PARCEL_ID, LNU, AREA_ID, LNU_GROSS_AREA, " \
				   "CRP_TYPE, CALC_AREA, SUB_DIV_NO, SLU_CCM_ID, GEOM, VALIDATED, CHNG_NO, ALLOCATED_TO, VALIDATED_DATE, " \
				   "RE_VAL_ALLOC, RE_VAL_DATE, RE_VAL_RESULT, RE_VAL_COMMENT, PARC_STATUS, PARC_REVIEW, REVIEW_COMMENT, " \
				   "PARC_SPLIT, FORESTRY_CN, MD, MD_REASON, FAD, FAD_REASON, INACTIVE, INACTIVE_REASON, ORIG_PARC_ID, " \
				   "PARC_MERGE, RE_VAL_DROPDOWN, ENT_TOL_COMMENT, END_DATE, EXTRACT_COUNTY, CURRENT_TIMESTAMP AS EXTRACT_DATE FROM C##POC.POC_PROG_PARC_NEW".format(tablename)
			self.update_oracle(str1, self.connection_cdtc)
			self.flog("CREATE TABLE {}".format(tablename))

			v_meta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(
				tablename)
			# db.updateOracle(v_meta, connection_CDTC)
			self.flog("CREATE META " + tablename)

			Spatialindex = "CREATE INDEX SPX_POC_PROG_PARC_NEW_DEL ON {}(GEOM)  INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('sdo_indx_dims=2')".format(tablename)
			self.update_oracle(Spatialindex, self.connection_cdtc)
			self.flog("CREATE INDEX SPX " + tablename)

			vIndexParc = "CREATE INDEX PPPND_PARC_INX ON POC_PROG_PARC_NEW_DEL (PARCEL_ID)"
			self.update_oracle(vIndexParc, self.connection_cdtc)

			self.flog("01_COPY LIVE TABLES TO LPIS_CDTC TO USE AS BASE - COMPLETE")
		except Exception as e:
			self.flog("ERR - createLocalTable: {}".format(str(e)))

	def create_local_table_excl(self, tablename):
		try:
			#db.updateOracle("DROP TABLE {} PURGE".format(tablename), connection_CDTC)
			self.flog("DROP TABLE {}".format(tablename))
			str1 = "CREATE TABLE {} AS  SELECT LNU_PARCEL_ID, LNU, EXCLUSION_NUM, PERCENT_EXCL_IN_AREA, EFF_AREA, " \
				   "EXCL_TYPE, COUNTY, COUNTY_LETTER, Q__COL3, DESCRIPTION, M_AREA, VALIDATED_EX, CHNG_NO, ALLOCATED_TO, " \
				   "GEOM, VAL_RES, VAL_DESC, VALIDATED_DATE, M_AREA_NEW, REVIEW, REVIEW_COMMENT, FEAT_TYPE, RE_VAL_EX_DROPDOWN FROM C##POC.POC_PROG_EXCL_NEW".format(tablename)
			self.update_oracle(str1, self.connection_cdtc)
			self.flog("CREATE TABLE {}".format(tablename))

			Meta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(
				tablename)
			#db.updateOracle(Meta, connection_CDTC)
			self.flog("CREATE META " + tablename)

			Spatialindex = "CREATE INDEX SPX_POC_PROG_EXCL_NEW_DEL ON {}(GEOM) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('sdo_indx_dims=2')".format(tablename)
			self.update_oracle(Spatialindex, self.connection_cdtc)
			self.flog("CREATE INDEX SPX " + tablename)

			self.flog("01_COPY LIVE TABLES TO LPIS_CDTC TO USE AS BASE EXCL - COMPLETE")
		except Exception as e:
			self.flog("ERR - createLocalTable: {}".format(str(e)))

	def local_poc_parc(self, tablename):
		try:
			str1 = "TRUNCATE table C##LPIS_CDTC.POC_PROG_PARC_NEW_DEL"
			self.update_oracle(str1, self.connection_cdtc)
			self.flog("TRUNCATE table " + tablename)

			str2 = "INSERT INTO C##LPIS_CDTC.POC_PROG_PARC_NEW_DEL SELECT * FROM C##POC.POC_PROG_PARC_NEW"
			self.update_oracle(str2, self.connection_cdtc)
			self.flog("INSERT data into table " + tablename)

		except Exception as e:
			self.flog("ERR - create Local PARC POC Table: {}".format(str(e)))

	def local_poc_excl(self, tablename):
		try:
			str1 = "TRUNCATE table C##LPIS_CDTC.POC_PROG_EXCL_NEW_DEL"
			self.update_oracle(str1, self.connection_cdtc)
			self.flog("TRUNCATE table " + tablename)

			str2 = "INSERT INTO C##LPIS_CDTC.POC_PROG_EXCL_NEW_DEL SELECT * FROM C##POC.POC_PROG_EXCL_NEW"
			self.update_oracle(str2, self.connection_cdtc)
			self.flog("INSERT data into table " + tablename)

		except Exception as e:
			self.flog("ERR - create Local EXCL POC Table: {}".format(str(e)))

	def backup_del(self, curr_county, curr_date, dictCounty):
		try:
			currcountyname = dictCounty[curr_county]
			str1 = "CREATE TABLE C##LPIS_ARCHIVE.PPPN_DEL_{}_{} TABLESPACE IMAGERY_UPLOAD AS SELECT * FROM C##LPIS_CDTC.POC_PROG_PARC_NEW_DEL".format(currcountyname, curr_date)
			self.update_oracle(str1, self.connection_arch)
			self.flog("CREATE PPPN_DEL_{}_{}".format(currcountyname, curr_date))
		except Exception as e:
			self.flog("ERR - create POC PARC backup: {}".format(str(e)))
			print("ERR - create POC PARC backup: {}".format(str(e)))

	def backup_del_sf(self, curr_county, curr_date, dictCounty):
		try:
			currcountyname = dictCounty[curr_county]
			str1 = "CREATE TABLE C##LPIS_ARCHIVE.PPEN_DEL_{}_{} TABLESPACE IMAGERY_UPLOAD AS SELECT * FROM C##LPIS_CDTC.POC_PROG_PARC_NEW_DEL".format(currcountyname, curr_date)
			self.update_oracle(str1, self.connection_arch)
			self.flog("CREATE PPEN_DEL_{}_{}".format(currcountyname, curr_date))
		except Exception as e:
			self.flog("ERR - create POC EXCL backup: {}".format(str(e)))
			print("ERR - create POC EXCL backup: {}".format(str(e)))

	def create_curr_cnty_parc_view(self, viewname, dictCounty, vCurrCounty):
		try:
			str1 = "CREATE or replace VIEW {} AS  SELECT * FROM POC_PROG_PARC_NEW_DEL  " \
				   "WHERE LPAD(PARCEL_ID,1) in  ('{}')  OR LPAD(HERD_NO,1)  in  ('{}')  OR EXTRACT_COUNTY in '{}' " \
				   "OR FORESTRY_CN IN (select FORESTRY_CONTRACT_NO from C##LPIS_CDTC.FOR_CONTROL) " \
				   "OR PARCEL_ID IN (select LNU_PARCEL_ID from C##LPIS_CDTC.CONTROL_LIST) ".format(viewname, cc.vCurrCounty, cc.vCurrCounty, dictCounty[vCurrCounty])
			self.update_oracle(str1, self.connection_cdtc)
			self.flog("Create table/view: {}".format(viewname))

			v_meta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM_2157', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  " \
					 "MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(viewname)
			#db.updateOracle(v_meta, connection_CDTC)
			self.flog("CREATE META on: " + viewname)

		except Exception as e:
			self.flog("ERR - create Curr County View: {}".format(str(e)))
			print("ERR - create Curr County View: {}".format(str(e)))

	def create_weekc_on_cdtc(self, tablename, ref_db):
		try:
			str1 = "TRUNCATE TABLE {}".format(tablename)
			self.update_oracle(str1, self.connection_cdtc)
			self.flog("TRUNCATE TABLE: {}".format(tablename))

			str2 = "INSERT INTO {} select * FROM TDLP_PARCEL{} where end_date > sysdate AND LPS_PARCEL_LABEL NOT IN (SELECT LPS_PARCEL_LABEL FROM C##LPIS_CDTC.WHITELIST_PARC) ".format(tablename, ref_db)
			self.update_oracle(str2, self.connection_cdtc)
			self.flog("INSERT DATA INTO TABLE: {}".format(tablename))

			str3 = "TRUNCATE TABLE TDLP_SUB_FEATURE_WEEKC "
			self.update_oracle(str3, self.connection_cdtc)
			self.flog("TRUNCATE TABLE: {}".format(tablename))

			str4 = "INSERT INTO TDLP_SUB_FEATURE_WEEKC  select T.* FROM TDLP_SUB_FEATURE{} T where end_date > sysdate AND T.GEOM.sdo_gtype  IN (2003) ".format(ref_db)
			self.update_oracle(str4, self.connection_cdtc)
			self.flog("INSERT DATA INTO TABLE: TDLP_SUB_FEATURE_WEEKC ")

			#db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/0_0_EXPORT_Z_TDLP_SUBFEAT_WEEKC.bat")
			self.flog("EXPORT TDLP_SUB_FEATURE_WEEKC_shp - COMPLETE* ")

		except Exception as e:
			self.flog(str(e))

	def refresh_working_table(self):
		pass

	def refresh_pppn_base_table(self):
		try:
			del1 = "TRUNCATE TABLE POC_PROG_PARC_NEW_DEL2"
			self.update_oracle(del1, self.connection_cdtc)
			self.flog(del1)

			deliveredWS = "CREATE OR REPLACE VIEW V_DELIVERED_WS AS SELECT GID, HERD_NO, SPH_SPS_HOLDING_ID,PARCEL_ID, " \
						  "LNU FROM {} WHERE PARCEL_ID = 'WHITESP'".format(cc.DelLouth)
			deliveredforestry = "CREATE OR REPLACE VIEW V_DELIVERED_FORESTRY AS SELECT GID, HERD_NO, SPH_SPS_HOLDING_ID,PARCEL_ID, " \
						  "LNU FROM {} WHERE LNU IN (SELECT LNU FROM POC_PROG_PARC_NEW_DEL WHERE FORESTRY_CN <> 'X' and PARC_STATUS = 'X')".format(cc.DelLouth)

			for i in cc.arraydel:
				deliveredWS = deliveredWS + "UNION SELECT GID, HERD_NO, SPH_SPS_HOLDING_ID,PARCEL_ID, " \
										"LNU FROM {} WHERE PARCEL_ID = 'WHITESP'".format(i)

				deliveredforestry = deliveredforestry + "UNION SELECT GID, HERD_NO, SPH_SPS_HOLDING_ID,PARCEL_ID, " \
														"LNU FROM {} WHERE LNU IN (SELECT LNU FROM POC_PROG_PARC_NEW_DEL " \
														"WHERE FORESTRY_CN <> 'X' and PARC_STATUS = 'X')".format(i)

			self.update_oracle(deliveredWS, self.connection_cdtc)
			self.flog("CREATE OR REPLACE VIEW V_DELIVERED_WS")

			self.update_oracle(deliveredforestry, self.connection_cdtc)
			self.flog("CREATE OR REPLACE VIEW V_DELIVERED_FORESTRY")

			deliveredcountyshape = "CREATE OR REPLACE VIEW V_DELIVERED_COUNTIES AS SELECT SUBSTR(LABEL,1,1) AS COUNTY " \
								   "FROM C##LPIS_CDTC.POC_TOWNLANDS_CURR_DIS_DEL WHERE SUBSTR(LABEL,1,1) IN {}".format(cc.countyDelivered)

			self.update_oracle(deliveredcountyshape, self.connection_cdtc)
			self.flog("CREATE OR REPLACE VIEW V_DELIVERED_COUNTIES")
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def remove_delivered(self):
		try:
			str1 = "DELETE FROM POC_PROG_PARC_NEW_DEL2 WHERE TABLE_NAME IN ( 'TDLP_PARC_DEVC_WS', " \
				   "'TDLP_PARC_DEVC_WS EDIT','TDLPSUB_FEATURE_DEVC','TDLP_PARC_DEVC' ,'TDLP_SUB_FEATURE_DEVC' )"

			self.update_oracle(str1, self.connection_cdtc)
			self.flog("DELETE FROM POC_PROG_PARC_NEW_DEL2")
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def compare_del(self):
		try:
			str1 = "DROP TABLE C##LPIS_CDTC.DEL_DIFFERENCE_AREA"
			self.update_oracle(str1, self.connection_cdtc)
			self.flog("DROP TABLE DEL_DIFFERENCE_AREA")

			str2 = "CREATE TABLE C##lpis_CDTC.DEL_DIFFERENCE_AREA AS (select parcel_id, ROUND(sum((SDO_GEOM.SDO_AREA(geom, 0.005,'unit=hectare'))), 4) " \
				   "AS AREA FROM C##lpis_CDTC.POC_PROG_PARC_NEW_DEL a WHERE parcel_id IN (SELECT parcel_id FROM C##lpis_CDTC.POC_PROG_PARC_NEW_DEL2 " \
				   "WHERE PARCEL_ID <> 'WHITESP') GROUP BY parcel_id minus SELECT parcel_id, ROUND(SUM((SDO_GEOM.SDO_AREA(geom, 0.005,'unit=hectare'))), 4) " \
				   "AS AREA2 FROM C##lpis_CDTC.POC_PROG_PARC_NEW_DEL2 GROUP BY parcel_id) "
			self.update_oracle(str2, self.connection_cdtc)
			self.flog("CREATE TABLE DEL_DIFFERENCE_AREA")

			str3 = "UPDATE POC_PROG_PARC_NEW_DEL2 A SET GEOM = (SELECT GEOM FROM C##POC.POC_PROG_PARC_NEW C " \
				   "WHERE A.PARCEL_ID = C.PARCEL_ID AND A.HERD_NO = C.HERD_NO) WHERE PARCEL_ID IN (SELECT PARCEL_ID FROM DEL_DIFFERENCE_AREA) "
			self.update_oracle(str3, self.connection_cdtc)
			self.flog("UPDATE POC_PROG_PARC_NEW_DEL2")
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def validate_geom(self, tablename, geom):
		try:
			str1 = "UPDATE {} t SET t.{} = CASE WHEN t.{}.sdo_gtype < 2000 THEN MDSYS.SDO_MIGRATE.TO_CURRENT({}, " \
				   "MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', 456108.162283595, 723434.4002247971, 0.005), " \
				   "MDSYS.SDO_DIM_ELEMENT('Y', 759303.023763474, 835666.9073253961, 0.005))) " \
				   "WHEN MDSYS.SDO_GEOM.VALIDATE_GEOMETRY({},0.005) IN ('13349','13351') " \
				   "THEN MDSYS.SDO_GEOM.SDO_UNION({},{},0.005) " \
				   "WHEN MDSYS.SDO_GEOM.VALIDATE_GEOMETRY({},0.005) IN ('13356')  " \
				   "THEN MDSYS.SDO_UTIL.REMOVE_DUPLICATE_VERTICES({},0.005) " \
				   "WHEN MDSYS.SDO_GEOM.VALIDATE_GEOMETRY({},0.005) IN ('13366','13367') " \
				   "THEN MDSYS.SDO_UTIL.RECTIFY_GEOMETRY({},0.005) " \
				   "ELSE NVL(MDSYS.SDO_UTIL.RECTIFY_GEOMETRY({},0.005),{}) " \
				   "END " \
				   "WHERE MDSYS.SDO_GEOM.VALIDATE_GEOMETRY({},0.005) IN ('13349','13351','13356','13366','13367') " \
				   "AND t.{} IS NOT NULL".format(tablename, geom, geom, geom, geom, geom, geom, geom, geom, geom, geom, geom, geom, geom, geom)
			self.update_oracle(str1, self.connection)
			self.flog("UPDATE {} VALIDATE GEOMETRY".format(tablename))
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def delete_gtype_nonpoly(self, tablename, geom):
		try:
			str1 = "UPDATE {} T SET T.{} = EXTRACTPOLYGON(T.{}) WHERE T.{}.SDO_GTYPE <> '2003'".format(tablename, geom, geom, geom)
			self.update_oracle(str1, self.connection)
			self.flog("EXTRACT POLYGON FROM {} WHERE NOT 2003".format(tablename))

			str2 = "DELETE FROM {} T WHERE T.{}.SDO_GTYPE IN ('2001', '2002', '2006')".format(tablename, geom)
			self.update_oracle(str2, self.connection)
			self.flog("DELETE FROM {} 2001, 2002, 2006 GTYPES".format(tablename))
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def get_geom_valerror_count_less_multi(self, tablename, geomcol, gidcol):
		try:
			orcdv01 = "SELECT COUNT(*) FROM ( SELECT t.{}, t.{}, sdo_geom.validate_geometry_with_context(t.{}, 0.005) " \
					  "VALIDGEOM, t.{}.sdo_gtype gtype, t.{}.sdo_srid srid FROM {} t where ( " \
					  "(sdo_geom.validate_geometry_with_context(t.{}, 0.005) != 'TRUE') OR (t.{}.sdo_gtype NOT IN (2003)) " \
					  "OR (NVL(t.{}.sdo_srid,0) != 2157) ) ) where gtype NOT IN (2007)"\
				.format(gidcol, geomcol, geomcol, geomcol, geomcol, tablename, geomcol, geomcol, geomcol)

			resrows = self.connection.cursor().execute(orcdv01).fetchall()[0][0]
			return resrows

		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def oracle_spatial_geom(self, tablename, rowgeom, rowgid):
		try:
			valcount = 10000
			valcountpev= 0
			i = 0
			while valcount > 0:
				if i > 10:
					break
				self.validate_geom(tablename, rowgeom)
				self.delete_gtype_nonpoly(tablename, rowgeom)
				valcount = self.get_geom_valerror_count_less_multi(tablename, rowgeom,rowgid)
				i = i + 1
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def create_forestry_view(self, tablename):
		forestry_control = " AND (FORESTRY_CN IN (select FORESTRY_CONTRACT_NO from C##LPIS_CDTC.FOR_CONTROL))"
		str1 = "CREATE OR REPLACE VIEW {} AS  SELECT ROWNUM  AS GID,1 AS ACTIVE_PARCELS,'25-JUL-19' " \
			   "AS CREATE_DATE,'31-DEC-99' AS END_DATE,FAD AS FAD_PERCENT,FAD_REASON,  FORESTRY_CN,GEOM AS GEOM_2157,LNU AS LNU,MD AS MD_PERCENT," \
			   "MD_REASON AS MD_REASON,  INACTIVE_REASON AS NON_ACTIVE_REASON,PARCEL_ID AS PARCEL_ID,SPH_SPS_HOLDING_ID AS SPH_SPS_HOLDING_ID  " \
			   "from POC_PROG_PARC_NEW_DEL_CL  WHERE FORESTRY_CN <> 'X' and PARC_STATUS = 'X'  and VALIDATED = 'Y'  AND END_DATE IS NULL {}".format(tablename, forestry_control)

		str2 = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM_2157', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  " \
			   "MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(tablename)

		str_dict = {str1: "CREATE TABLE V_TRANSFORM_FORESTRY", str2: "CREATE META ON V_TRANSFORM_FORESTRY"}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection_cdtc)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

	def create_transform_parc_view(self, tablename):
		strtmp1 = "DROP TABLE {}".format(tablename)
		strtmp1_log = "DROP TABLE {}".format(tablename)
		strtmp2 = "DROP TABLE TBL_TRANSFORM_PARC_NEXT_TEMP"
		strtmp2_log = "DROP TABLE TBL_TRANSFORM_PARC_NEXT_TEMP"

		tempdict = {strtmp1: strtmp1_log, strtmp2:strtmp2_log}
		for i in tempdict.keys():
			try:
				self.update_oracle(i, self.connection_cdtc)
				self.flog(str(tempdict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(tempdict[i])))
				pass

		str1 = "CREATE TABLE TBL_TRANSFORM_PARC_NEXT_TEMP AS    SELECT lps_gid as GID,1 AS ACTIVE_PARCELS,TO_CHAR(SYSDATE,'DD-MON-YY') " \
			   "AS CREATE_DATE,'31-DEC-99' AS END_DATE,FAD AS FAD_PERCENT,    FAD_REASON,  HERD_NO as forestry_cn,LNU AS LNU,MD AS MD_PERCENT," \
			   "MD_REASON AS MD_REASON,  INACTIVE_REASON AS NON_ACTIVE_REASON,PARCEL_ID AS PARCEL_ID,   SPH_SPS_HOLDING_ID AS SPH_SPS_HOLDING_ID," \
			   "GEOM AS GEOM_2157     from poc_prog_parc_new_DEL_CL    where   (PARCEL_ID IN (SELECT PARCEL_ID FROM c##lpis_cdtc.V_CURR_COUNTY_PARCS) " \
			   "and PARC_STATUS = 'X' and  FORESTRY_CN = 'X' and END_DATE IS NULL and  PARCEL_ID NOT IN (select LNU_PARCEL_ID from POC_PROG_EXCL_NEW_DEL " \
			   "where VALIDATED_EX = 'N' and lnu_parcel_id is not null)    and RE_VAL_RESULT = 'Y'      and VALIDATED = 'Y')"
		str1log = "CREATE TABLE TBL_TRANSFORM_PARC_NEXT_TEMP"

		str2 = "insert into TBL_TRANSFORM_PARC_NEXT_TEMP SELECT lps_gid as GID,1 AS ACTIVE_PARCELS,TO_CHAR(SYSDATE,'DD-MON-YY') " \
			   "AS CREATE_DATE,'31-DEC-99' AS END_DATE,FAD AS FAD_PERCENT,      FAD_REASON,  HERD_NO as forestry_cn,LNU AS LNU,MD AS " \
			   "MD_PERCENT,MD_REASON AS MD_REASON,  INACTIVE_REASON AS NON_ACTIVE_REASON,PARCEL_ID AS PARCEL_ID, SPH_SPS_HOLDING_ID " \
			   "AS SPH_SPS_HOLDING_ID,GEOM AS GEOM_2157      from poc_prog_parc_new_DEL_CL      where PARCEL_ID = 'WHITESP'"
		str2log = "INSERT TABLE TEMP TBL_TRANSFORM_PARC_NEXT_TEMP"

		str3 = "insert into TBL_TRANSFORM_PARC_NEXT_TEMP SELECT GID + ( select max(lps_gid) from poc_prog_parc_new_DEL_CL) as GID, " \
			   "ACTIVE_PARCELS,CREATE_DATE,END_DATE,FAD_PERCENT,FAD_REASON, FORESTRY_CN,LNU,MD_PERCENT,MD_REASON,NON_ACTIVE_REASON," \
			   "PARCEL_ID,SPH_SPS_HOLDING_ID,GEOM_2157 from C##POC.FORESTRY_PARC_SHELLS_2157"
		str3log = "INSERT TABLE TEMP 2 TBL_TRANSFORM_PARC_NEXT_TEMP"

		str4 = "CREATE TABLE TBL_TRANSFORM_PARC_NEXT AS  SELECT  GID, 1 AS ACTIVE_PARCELS, TO_CHAR(SYSDATE,'DD-MON-YY') " \
			   "AS CREATE_DATE,'31-DEC-99' AS END_DATE,FAD_PERCENT, FAD_REASON,FORESTRY_CN,GEOM_2157,LNU AS LNU,MD_PERCENT," \
			   "MD_REASON AS MD_REASON,  NON_ACTIVE_REASON,PARCEL_ID AS PARCEL_ID,  SPH_SPS_HOLDING_ID AS SPH_SPS_HOLDING_ID from TBL_TRANSFORM_PARC_NEXT_TEMP"
		str4log = "CREATE TABLE TEMP TBL_TRANSFORM_PARC_NEXT"

		str5 = "DROP TABLE TBL_TRANSFORM_PARC_NEXT_TEMP"
		str5log = "DROP TABLE TBL_TRANSFORM_PARC_NEXT_TEMP {}".format(tablename)
		str6 = "CREATE INDEX TBL_TRANSFORM_PARC_NEXT_IDX1 ON TBL_TRANSFORM_PARC_NEXT (GID)"
		str6log = "CREATE INDEX TBL_TRANSFORM_PARC_NEXT_IDX1 ON TBL_TRANSFORM_PARC_NEXT (GID)".format(tablename)
		str7 = "CREATE INDEX TBL_TRANSFORM_PARC_NEXT_IDX2 ON TBL_TRANSFORM_PARC_NEXT (PARCEL_ID)"
		str7log = "CREATE INDEX TBL_TRANSFORM_PARC_NEXT_IDX1 ON TBL_TRANSFORM_PARC_NEXT (PARCEL_ID)".format(tablename)
		str8 = "CREATE INDEX TBL_TRANSFORM_PARC_NEXT_IDX3 ON TBL_TRANSFORM_PARC_NEXT (LNU)"
		str8log = "CREATE INDEX TBL_TRANSFORM_PARC_NEXT_IDX1 ON TBL_TRANSFORM_PARC_NEXT (LNU)".format(tablename)

		vmeta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM_2157', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  " \
			   "MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(tablename)
		vmeta_log = "CREATE META ON {}".format(tablename)

		vspa_index = "CREATE INDEX SPX_{} ON {}(GEOM_2157) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('sdo_indx_dims=2')".format(tablename, tablename)
		vspa_index_log = "CREATE SP_INDEX {}".format(tablename)

		str9 = "INSERT INTO TBL_TRANSFORM_PARC_NEXT  select (rownum  + (select max(gid)  FROM TBL_TRANSFORM_PARC_NEXT )) as " \
			   "GID2, ACTIVE_PARCELS, CREATE_DATE, END_DATE, FAD_PERCENT, FAD_REASON, FORESTRY_CN, GEOM_2157, LNU, MD_PERCENT, " \
			   "MD_REASON, NON_ACTIVE_REASON, PARCEL_ID, SPH_SPS_HOLDING_ID from TBL_TRANSFORM_PARC_NEXT  a  where a.rowid >= " \
			   "any (select b.rowid from TBL_TRANSFORM_PARC_NEXT b where a.gid = b.gid) AND GID = 0"
		str9log = "INSERT INTO TBL_TRANSFORM_PARC_NEXT {}".format(tablename)

		str10 = "DELETE FROM TBL_TRANSFORM_PARC_NEXT WHERE GID = 0"
		str10log = "DELETE FROM TBL_TRANSFORM_PARC_NEXT WHERE GID = 0 {}".format(tablename)
		str_dict = {str1: str1log, str2: str2log, str3: str3log, str4: str4log, str5: str5log, str6: str6log, str7: str7log,
				   str8: str8log, vmeta: vmeta_log, vspa_index: vspa_index_log, str9: str9log, str10: str10log}

		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection_cdtc)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

	def oracle_validation_1(self, tablename, ref_db):
		#str_comment = "UPDATE {} SET GID = GID + (SELECT  CEIL(((SELECT MAX(LPS_GID) FROM TDLP_PARCEL{}) / 100000)) * 100000 FROM DUAL)".format(tablename, ref_db)
		str1 = "update {} T set t.geom_2157 = SDO_UTIL.RECTIFY_GEOMETRY(T.geom_2157, 0.005) WHERE lpad(SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT(T.geom_2157, 0.005),4)<>'TRUE' ".format(tablename)   # Rectify geometry
		str1log = "UPDATE {} GEOM WITH CONTEXT".format(tablename)

		str2 = "UPDATE {} SET ACTIVE_PARCELS = 77 WHERE NON_ACTIVE_REASON IS NULL".format(tablename)								#update active parcels attribute
		str2log = "UPDATE {} SET ACTIVE_PARCELS = 77".format(tablename)

		str3 = "UPDATE {} SET ACTIVE_PARCELS = 78 WHERE NON_ACTIVE_REASON IS NOT NULL".format(tablename)							#update non-active parcels attribute
		str3log = "UPDATE {} SET ACTIVE_PARCELS = 78".format(tablename)

		str4 = "UPDATE {} SET ACTIVE_PARCELS = 78, NON_ACTIVE_REASON = 41 WHERE PARCEL_ID = 'WHITESP'".format(tablename)			#update non-active parcels whitespace to 78
		str4log = "UPDATE {} SET ACTIVE_PARCELS = 78 WHERE PARCEL_ID = 'WHITESP'".format(tablename)

		str5 = "UPDATE {} SET NON_ACTIVE_REASON = 44 WHERE NON_ACTIVE_REASON =36".format(tablename)									#update forestry non-active
		str5log = "UPDATE {} SET NON_ACTIVE_REASON = 44 forestry".format(tablename)

		str_dict = {str1: str1log, str2: str2log, str3: str3log, str4: str4log, str5: str5log}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

		self.validate_geom(tablename, "GEOM_2157")
		self.flog("3_ORACLE_VALIDATION - Complete")

	def create_curr_county_townlands(self, tablename, curr_county):
		try:
			array1Str = "('{}')".format(curr_county)
			str1 = 	"CREATE OR REPLACE VIEW V_CURR_COUNTY_TWNLS AS  SELECT * FROM {} WHERE SUBSTR(LABEL,1,1) IN {}".format(tablename, array1Str)
			self.update_oracle(str1, self.connection_cdtc)
			self.flog("create Curr County Townlands - create view current county townlands")
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def add_delivered(self, tablename, deli_table):
		str1 = " TRUNCATE TABLE {} ".format(tablename)							#Truncate tdlp_parcels_test4
		str1log = "TRUNCATE TABLE {}".format(tablename)
		str2 = "INSERT INTO {} (GID, HERD_NO, SPH_SPS_HOLDING_ID, PARCEL_ID, LNU, MD_PERCENT, MD_REASON, " \
			   "FAD_PERCENT, FAD_REASON, ACTIVE_PARCELS, NON_ACTIVE_REASON, CREATE_DATE, END_DATE, GEOM_2157) SELECT * FROM TDLP_PARC_REPAIRED4".format(tablename)
		str2log = "INSERT INTO TDLP_PARCELS_TEST4"
		str_dict = {str1: str1log, str2: str2log}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass


		str3 = "DROP TABLE POC_BEST_ORTHO_GRID_CURR PURGE"
		str3log = "DROP TABLE POC_BEST_ORTHO_GRID_CURR"
		array1Str = "('{}')".format(cc.vCurrCounty)
		twn_tab_Create = "POC_BEST_ORTHO_GRID_CURR"
		str4 = "CREATE TABLE C##LPIS_CDTC.POC_BEST_ORTHO_GRID_CURR AS  SELECT *  FROM C##LPIS_TRANSFORM.BEST_ORTHO_GRID_500 c " \
			   "WHERE SDO_ANYINTERACT(c.geom_2157, (SELECT SDO_AGGR_UNION(SDOAGGRTYPE(GEOM , 0.0005))  " \
			   "FROM C##LPIS_CDTC.POC_TOWNLANDS_CURR_DIS T WHERE SUBSTR(LABEL,1,1) IN {} )) = 'TRUE'".format(array1Str)
		str4log = "CREATE TABLE {}".format(twn_tab_Create)

		vmeta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM_2157', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  " \
			   "MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(twn_tab_Create)
		vmeta_log = "CREATE META ON {}".format(twn_tab_Create)

		vspa_index = "CREATE INDEX SPX_{} ON {}(GEOM_2157) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('sdo_indx_dims=2')".format(twn_tab_Create, twn_tab_Create)
		vspa_index_log = "CREATE SP_INDEX {}".format(twn_tab_Create)

		str_dict2 = {str3: str3log, str4: str4log, vmeta: vmeta_log, vspa_index: vspa_index_log}
		for i in str_dict2.keys():
			try:
				self.update_oracle(i, self.connection_cdtc)
				self.flog(str(str_dict2[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict2[i])))
				pass

	def add_to_test9(self, tablename, delitab):
		str1 = "TRUNCATE TABLE {} ".format(delitab)
		str1log = "TRUNCATE TABLE {} ".format(delitab)

		str2 = " INSERT INTO {} (GID, HERD_NO, SPH_SPS_HOLDING_ID, PARCEL_ID, LNU, MD_PERCENT, MD_REASON, " \
			   "FAD_PERCENT, FAD_REASON, ACTIVE_PARCELS, NON_ACTIVE_REASON, CREATE_DATE, END_DATE, GEOM_2157)  " \
			   "SELECT GID, HERD_NO, SPH_SPS_HOLDING_ID, PARCEL_ID, LNU, MD_PERCENT, MD_REASON, FAD_PERCENT, FAD_REASON, " \
			   "ACTIVE_PARCELS, NON_ACTIVE_REASON, CREATE_DATE, END_DATE, GEOM_2157 FROM {}".format(delitab, tablename)
		str2log = "INSERT INTO {}".format(delitab)

		str_dict = {str1: str1log, str2: str2log}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass


	def replace_duplicate_ws(self,tablename, geom_col):
		str1 = "INSERT INTO  {} select  (rownum + (select max(gid)  FROM {})) as GID2,HERD_NO, SPH_SPS_HOLDING_ID, " \
			   "PARCEL_ID, LNU, MD_PERCENT, MD_REASON, FAD_PERCENT, FAD_REASON, ACTIVE_PARCELS, NON_ACTIVE_REASON, " \
			   "CREATE_DATE, END_DATE, GEOM_2157 from  {}  a where   a.rowid > any (select b.rowid from {} " \
			   "b where a.gid = b.gid )  AND PARCEL_ID = 'WHITESP' ".format(tablename, tablename, tablename, tablename)
		str1log = "INSERT INTO {} NEXT GID FOR DUPLICATE WS".format(tablename)

		str2 = " DELETE FROM {} a WHERE a.rowid > any (SELECT b.rowi FROM {} b WHERE a.gid = b.gid) AND PARCEL_ID = 'WHITESP' ".format(tablename, tablename)
		str2log = "DELETE DUPLICATE ID'S {}".format(tablename)

		str_dict = {str1: str1log, str2: str2log}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

		self.oracle_spatial_geom(tablename, geom_col, "GID")

	def recombine_split_polys(self, tablename):
		str1 = " CREATE TABLE TEMP_INSERT AS  SELECT GID, HERD_NO, SPH_SPS_HOLDING_ID, PARCEL_ID, LNU, MD_PERCENT, MD_REASON, FAD_PERCENT, " \
			   "AD_REASON, ACTIVE_PARCELS, NON_ACTIVE_REASON, CREATE_DATE, END_DATE, SDO_AGGR_UNION(SDOAGGRTYPE(C.GEOM_2157, 0.0005)) AS " \
			   "GEOM_2157 FROM {} C WHERE GID IN (SELECT GID FROM ( SELECT GID, parcel_id, COUNT(*) FROM TDLP_PARCELS_TEST9 A GROUP BY GID," \
			   "parcel_id HAVING COUNT(*)>1 ORDER BY parcel_id )) GROUP BY GID, HERD_NO, SPH_SPS_HOLDING_ID, PARCEL_ID, LNU, MD_PERCENT, " \
			   "MD_REASON, FAD_PERCENT, FAD_REASON, ACTIVE_PARCELS, NON_ACTIVE_REASON, CREATE_DATE, END_DATE".format(tablename)
		str1log = "CREATE TABLE TEMP_INSERT"

		str2 = "DELETE FROM TDLP_PARCELS_TEST9 WHERE GID IN (SELECT GID FROM (SELECT GID, parcel_id, COUNT(*) FROM {} A " \
			   "GROUP BY GID,parcel_id HAVING COUNT(*)>1 ORDER BY parcel_id ))".format(tablename)
		str2log = "DELETE FROM TDLP_PARCELS_TEST9"

		str3 = "INSERT INTO {} SELECT * FROM TEMP_INSERT".format(tablename)
		str3log = "INSERT FROM TEMP_INSERT INTO {}".format(tablename)

		str4 = "DROP TABLE TEMP_INSERT"
		str4log = "DROP TABLE TEMP_INSERT"

		str_dict = {str1: str1log, str2: str2log, str3: str3log, str4: str4log}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

	def skip_table(self, tablename, table2name):

		str1 = "TRUNCATE TABLE {}".format(table2name)
		str1log = "TRUNCATE TABLE {}".format(table2name)
		str2 = "INSERT INTO {} SELECT * FROM {}".format(table2name, tablename)
		str2log = "INSERT INTO {} SELECT * FROM {}".format(table2name, tablename)

		str_dict = {str1: str1log, str2: str2log}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

	def update_md_non_active(self, tablename):
		try:
			str1 = " UPDATE {} SET MD_PERCENT = 0, MD_REASON = 0 WHERE nvl(md_percent,0) >= 100 AND ACTIVE_PARCELS = 78".format(tablename)
			str1log = "updateMD0NonActive - MD_PERCENT 0 where non active "
			self.update_oracle(str1, self.connection)
			self.flog(str1log)
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def update_fad_gross_non_active(self, tablename):
		try:
			str1 = " UPDATE {} SET FAD_REASON = 0, FAD_PERCENT = 0 WHERE nvl(fad_percent,0) != 0 AND  nvl(fad_percent,0) >= ROUND(SDO_GEOM.SDO_AREA(GEOM_2157, 0.005,'unit=hectare'),2) ".format(tablename)
			str1log = "updateFADGrGrossNonActive - fad_percent 0 where fad > gross "
			self.update_oracle(str1, self.connection)
			self.flog(str1log)
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def test9e_from9e1(self, tablename):

		str1 = "TRUNCATE TABLE {}".format(tablename)
		str1log = "TRUNCATE TABLE {}".format(tablename)
		str2 = "INSERT INTO {} SELECT * FROM TDLP_PARCELS_TEST9E1".format(tablename)
		str2log = "INSERT INTO {} SELECT * FROM TDLP_PARCELS_TEST9E1".format(tablename)
		str_dict = {str1: str1log, str2: str2log}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

	def replace_duplicate_ws_only(self, tablename):
		str1 = " INSERT INTO {} select  (rownum + (select max(gid)  FROM {} )) as GID2,HERD_NO, SPH_SPS_HOLDING_ID, PARCEL_ID, LNU, " \
			"MD_PERCENT, MD_REASON, FAD_PERCENT, FAD_REASON, ACTIVE_PARCELS, NON_ACTIVE_REASON, CREATE_DATE, END_DATE, GEOM_2157 " \
			"from {}  a  where a.rowid > any (select b.rowid from {} b where a.gid = b.gid) AND PARCEL_ID = 'WHITESP' ".format(tablename, tablename, tablename, tablename)
		str1log = "INSERT INTO {} NEXT GID FOR DUPLICATE WS".format(tablename)
		str2 = "DELETE FROM {} a WHERE a.rowid > any (SELECT b.rowid FROM {} b WHERE a.gid = b.gid) AND PARCEL_ID = 'WHITESP' ".format(tablename, tablename)
		str2log = "DELETE DUPLICATE ID'S {}".format(tablename)
		str_dict = {str1: str1log, str2: str2log}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

	def insert_ws(self):
		try:
			str1 = "INSERT INTO c##lpis_transform.TDLP_PARCELS_TEST9E SELECT (rownum + (SELECT MAX(gid)  FROM  " \
				"c##lpis_transform.TDLP_PARCELS_TEST9E )) as GID2,NULL, 0, 'WHITESP', 0, 0, 0, 0, 0, 78, 41, " \
				"SYSDATE, '31-DEC-99', GEOM FROM c##lpis_CDTC.CH55_NO_GAPS_TOWNLANDS_TMP WHERE AREA_TWNLAND_GAP > 0.001 "
			str1log = "INSERT WS INTO TDLP_PARCELS_TEST9E"
			self.update_oracle(str1, self.connection)
			self.flog(str1log)
		except Exception as e:
			self.flog(str(e))

	def validate_subfeatures_oracle(self, tablename):
		try:
			valcount = 100000
			valcountpev= 0
			i = 0
			while valcount > 0:
				if i > 10:
					break
				else:
					self.validate_geom(tablename, "GEOM_2157")
					self.delete_gtype_nonpoly(tablename, "GEOM_2157")
					valcount = self.get_geom_valerror_count_less_multi(tablename, "GEOM_2157", "GID")
					i = i + 1
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def delete_efa_subfeatures(self, tablename):
		try:
			str1 = "DELETE FROM {} WHERE SUBFEAT_TYPE IN (33,34,35)".format(tablename)
			self.update_oracle(str1, self.connection)
			self.flog("deleteEFASubFeatures - Complete ")
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def replace_duplicate_subfeatures(self, tablename):
		str1 = "INSERT INTO {} select  (rownum + (select max(gid)  FROM {})) as GID2, ORIG_ID, SUBFEAT_TYPE, " \
			   "SUBFEAT_EFF_PERCENT, SUBFEAT_START_DATE, SUBFEAT_END_DATE, LNU, GEOM_2157 from {} a " \
			   "where a.rowid >  any (select b.rowid from {}  b where a.gid = b.gid) ".format(tablename, tablename, tablename, tablename)

		str1log = "INSERT INTO {} NEXT GID FOR DUPLICATE WS".format(tablename)

		str2 = "DELETE FROM {} a WHERE a.rowid > any (SELECT b.rowid FROM {} b WHERE a.GID = b.GID)".format(tablename, tablename)
		str2log = "DELETE DUPLICATE ID'S {}".format(tablename)
		str_dict = {str1: str1log, str2: str2log}
		for i in str_dict.keys():
			try:
				self.update_oracle(i, self.connection)
				self.flog(str(str_dict[i]))
			except Exception as e:
				print(e)
				self.flog(" ****************  Error: {} ****************".format(str(str_dict[i])))
				pass

	def update_area_fenced_off(self, tablename):
		try:
			str1 = "UPDATE {} SET SUBFEAT_TYPE = 151 WHERE SUBFEAT_TYPE = 65".format(tablename)
			self.update_oracle(str1, self.connection)
			self.flog("updateAreaFencedOff - subfeat type 65 -> 151 area fenced off ")
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def delete_sf_from_whitespace(self, tablename):
		try:
			str1= " DELETE FROM {} WHERE GID IN (SELECT GID FROM TDLP_SUB_FEAT_REPAIRED_3_TMP )".format(tablename)
			self.update_oracle(str1, self.connection)
			self.flog("DELETE SF features in Whitespace {}".format(tablename))
		except Exception as e:
			print(e)
			self.flog(" ****************  Error: {} ****************".format(str(e)))

	def check_fme_run(self,numcode):
		try:
			str1 = " SELECT WS_NAME FROM FME_WORKSPACES WHERE WS_NUM = {}".format(numcode)
			fmecodes = self.connection_cdtc.cursor().execute(str1).fetchall()
			if fmecodes:
				print(fmecodes)
				if fmecodes:
					print("FME: {} {} successful to run!".format(fmecodes[0], numcode))
					self.flog("FME: {} {} successful to run!".format(fmecodes[0], numcode))
				else:
					print("FME: {} failed to run!".format(numcode))
					self.flog("FME: {} failed to run!".format(numcode))
			else:
				print("FME: {} failed to run!".format(numcode))
				self.flog("FME: {} failed to run!".format(numcode))
		except Exception as e:
			print(e)
			self.flog(e)
