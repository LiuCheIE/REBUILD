import cx_Oracle
from config_para import config_county as cc
import time
import OracleOperation as orac


if __name__ == '__main__':
	try:
		db = orac.OracleOperation()
		db.flog("---------------------START----------------------")
		dictCounty = dict()
		cc.countyCodeDict(dictCounty)
		print(dictCounty)

		# db.delete_sf_temp()
		# db.create_county_shape()								   #Create shapes for current and delivered counties by dissolving townlands/deds
		#
		# db.create_local_table("POC_PROG_PARC_NEW_DEL")   		#Createa copy of the poc parcel table on LPIS_CDTC
		# db.create_local_table_excl("POC_PROG_EXCL_NEW_DEL")       #Createa copy of the poc excl tables on LPIS_CDTC
		#
		# db.backup_del(cc.vCurrCounty, cc.CurrDate, dictCounty)					#Createa backup of the poc PARC table on ARCHIVE
		# db.backup_del_sf(cc.vCurrCounty, cc.CurrDate, dictCounty)					#Createa backup of the poc EXCL table on ARCHIVE
		#
		# db.create_curr_cnty_parc_view("V_CURR_COUNTY_PARCS", dictCounty, cc.vCurrCounty)		#CREATE A VIEW OF CURRENT COUNTY FROM CURR DISOLVE +  FORESTRY    V_CURR_COUNTY_PARCS
		#
		# db.create_weekc_on_cdtc("TDLP_PARCEL_WEEKC", cc.ref_db)			#Create TDLP_PARCEL_WEEKC table on geo3 to use as base for delivered data. Which to use determined from above. Also create TDLP_SUB_FEATURE_WEEKC
		#
		# db.refresh_pppn_base_table()
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/0_1_A_SYNC_DEVC_AND_TO_DEL.bat")
		# db.check_fme_run(905)
		#
		# db.remove_delivered()
		#
		# db.compare_del()
		#
		# db.oracle_spatial_geom("C##LPIS_CDTC.POC_PROG_PARC_NEW_DEL2", "GEOM", "ROW_GID")
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/0_1_FILTER_FROM_PPPND_CREATE_PPPEN_COMB_AND_PPPN_PARC_NX_AND_VCLEAN_SHP_RE_RUN.bat")
		# db.check_fme_run(910)
		#
		# db.flog("PRE VCLEAN - START")
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/3_PYTHON\Python_Scripts\VClean3_PRE_BAT.bat")
		# db.flog("PRE VCLEAN - COMPLETE")
		#
		# db.flog("START SLEEP")
		# time.sleep(120)
		# db.flog("END SLEEP")
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/0_5_VCLEAN_TO_ORACLE_RE_RUN.bat")
		# db.check_fme_run(950)
		#
		# db.create_forestry_view("V_TRANSFORM_FORESTRY")
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/1_DISSOLVE_FOREST_SHELLS.bat")
		# db.check_fme_run(1)
		#
		# db.create_transform_parc_view("TBL_TRANSFORM_PARC_NEXT")
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/2_PARC_SLIVER_DETECT_RE_RUN.bat")
		# db.check_fme_run(2)
		#
		# db.oracle_validation_1("TDLP_PARC_REPAIRED", cc.ref_db)
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/3_PARC_REPAIRED_TO_SHP_RERUN.bat")
		# db.check_fme_run(3)
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/3_PYTHON/Python_Scripts/VClean3_BAT.bat")
		# db.flog("VCLEAN - COMPLETE")
		#
		# db.flog("START SLEEP")
		# time.sleep(120)
		# db.flog("END SLEEP")
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/4__VCLEAN_SHP_ORCSP_RERUN.bat")															#Cleaned shp data to Oracle TDLP_PARC_REPAIRED2
		# db.check_fme_run(4)
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/5_BREAK_REPAIR_TO_INTERSECT_AND_NON_INTERSECT_DELIVERED - Copy_RERUN.bat")				#Copy, also create shpfile for us in python bat
		# db.flog("5_BREAK_REPAIR_TO_INTERSECT_AND_NON_INTERSECT_DELIVERED -    Complete")
		# db.check_fme_run(5)
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/5_Z1_SNAP_TO_DELIVERED_FME.bat")														#This replaces python/grass method below which can crash on certain polys
		# db.flog("5_Z1_SNAP_TO_DELIVERED_FME -    Complete")
		# db.check_fme_run(501)
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/6_IMPORT_SNAPPED.bat")										#6_IMPORT_SNAPPED
		# db.flog("6_IMPORT_SNAPPED -    Complete")
		# db.check_fme_run(6)
		#
		# db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/7_COMBINE_SNAPPED_AND_NON_INTERSECT.bat")					#7_COMBINE_SNAPPED_AND_NON_INTERSECT   [Creates tclp_parc_repaired4]
		# db.flog("7_COMBINE_SNAPPED_AND_NON_INTERSECT -    Complete")
		# db.check_fme_run(7)
		#
		# db.create_curr_county_townlands("POC_REAL_TOWNLANDS_CURR", cc.vCurrCounty)
		#
		# db.add_delivered("TDLP_PARC_REPAIRED4", "TDLP_PARCELS_TEST9E")
		# db.flog("92_INSERT_DELIVERED_INCL_WS_INTO_PARCS - Complete")

		if db.connection:
			db.connection.close()
			db.connection_cdtc.close()
			db.connection_poc.close()
			db.connection_arch.close()
			db.connection_weekc.close()
			db.flog("-----------------------DONE--------------------------")
			print("-----------------------DONE--------------------------")
	except cx_Oracle.Error as error:
		print(error)