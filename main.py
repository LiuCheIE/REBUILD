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

		db.delete_sf_temp()
		db.create_county_shape()								   #Create shapes for current and delivered counties by dissolving townlands/deds

		db.create_local_table("POC_PROG_PARC_NEW_DEL")   		#Createa copy of the poc parcel table on LPIS_CDTC
		db.create_local_table_excl("POC_PROG_EXCL_NEW_DEL")       #Createa copy of the poc excl tables on LPIS_CDTC

		db.backup_del(cc.vCurrCounty, cc.CurrDate, dictCounty)					#Createa backup of the poc PARC table on ARCHIVE
		db.backup_del_sf(cc.vCurrCounty, cc.CurrDate, dictCounty)					#Createa backup of the poc EXCL table on ARCHIVE

		db.create_curr_cnty_parc_view("V_CURR_COUNTY_PARCS", dictCounty, cc.vCurrCounty)		#CREATE A VIEW OF CURRENT COUNTY FROM CURR DISOLVE +  FORESTRY    V_CURR_COUNTY_PARCS

		db.create_weekc_on_cdtc("TDLP_PARCEL_WEEKC", cc.ref_db)			#Create TDLP_PARCEL_WEEKC table on geo3 to use as base for delivered data. Which to use determined from above. Also create TDLP_SUB_FEATURE_WEEKC

		db.refresh_pppn_base_table()
		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/0_1_A_SYNC_DEVC_AND_TO_DEL.bat")
		db.check_fme_run(905)

		db.remove_delivered()

		db.compare_del()

		db.oracle_spatial_geom("C##LPIS_CDTC.POC_PROG_PARC_NEW_DEL2", "GEOM", "ROW_GID")
		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/0_1_FILTER_FROM_PPPND_CREATE_PPPEN_COMB_AND_PPPN_PARC_NX_AND_VCLEAN_SHP_RE_RUN.bat")
		db.check_fme_run(910)

		db.flog("PRE VCLEAN - START")
		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/3_PYTHON/Python_Scripts/VClean3_PRE_BAT.bat")
		db.flog("PRE VCLEAN - COMPLETE")

		db.flog("START SLEEP")
		time.sleep(120)
		db.flog("END SLEEP")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/0_5_VCLEAN_TO_ORACLE_RE_RUN.bat")
		db.check_fme_run(950)

		db.create_forestry_view("V_TRANSFORM_FORESTRY")
		#
		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/1_DISSOLVE_FOREST_SHELLS.bat")
		db.check_fme_run(1)

		db.create_transform_parc_view("TBL_TRANSFORM_PARC_NEXT")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/2_PARC_SLIVER_DETECT_RE_RUN.bat")
		db.check_fme_run(2)

		db.oracle_validation_1("TDLP_PARC_REPAIRED", cc.ref_db)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/3_PARC_REPAIRED_TO_SHP_RERUN.bat")
		db.check_fme_run(3)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/3_PYTHON/Python_Scripts/VClean3_BAT.bat")
		db.flog("VCLEAN - COMPLETE")

		db.flog("START SLEEP")
		time.sleep(120)
		db.flog("END SLEEP")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/4__VCLEAN_SHP_ORCSP_RERUN.bat")															#Cleaned shp data to Oracle TDLP_PARC_REPAIRED2
		db.check_fme_run(4)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/5_BREAK_REPAIR_TO_INTERSECT_AND_NON_INTERSECT_DELIVERED - Copy_RERUN.bat")				#Copy, also create shpfile for us in python bat
		db.flog("5_BREAK_REPAIR_TO_INTERSECT_AND_NON_INTERSECT_DELIVERED -    Complete")
		db.check_fme_run(5)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/5_Z1_SNAP_TO_DELIVERED_FME.bat")														#This replaces python/grass method below which can crash on certain polys
		db.flog("5_Z1_SNAP_TO_DELIVERED_FME -    Complete")
		db.check_fme_run(501)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/6_IMPORT_SNAPPED.bat")										#6_IMPORT_SNAPPED
		db.flog("6_IMPORT_SNAPPED -    Complete")
		db.check_fme_run(6)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/7_COMBINE_SNAPPED_AND_NON_INTERSECT.bat")					#7_COMBINE_SNAPPED_AND_NON_INTERSECT   [Creates tclp_parc_repaired4]
		db.flog("7_COMBINE_SNAPPED_AND_NON_INTERSECT -    Complete")
		db.check_fme_run(7)

		db.create_curr_county_townlands("POC_REAL_TOWNLANDS_CURR", cc.vCurrCounty)

		db.add_delivered("TDLP_PARC_REPAIRED4", "TDLP_PARCELS_TEST9E")
		db.flog("92_INSERT_DELIVERED_INCL_WS_INTO_PARCS - Complete")

		db.add_to_test9("TDLP_PARCELS_TEST4", "TDLP_PARCELS_TEST9")

		db.replace_duplicate_ws("TDLP_PARCELS_TEST9", "GEOM_2157")
		db.recombine_split_polys("TDLP_PARCELS_TEST9")
		db.flog("Q_REVALIDATE - COMPLETE")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/R_DEAGG.bat")
		db.flog("R_DEAGG -    Complete")
		db.check_fme_run(25)

		db.replace_duplicate_ws("TDLP_PARCELS_TEST9A", "GEOM_2157")
		db.flog("S_REMOVE_DUPL - COMPLETE")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/T_REMOVE_WS_OLAPS.bat")
		db.flog("T_REMOVE_WS_OLAPS -    Complete")
		db.check_fme_run(27)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/V_FIND_MULTI_DEAGG.bat")
		db.flog("V_FIND_MULTI_DEAGG -    Complete")
		db.check_fme_run(29)

		db.replace_duplicate_ws("TDLP_PARCELS_TEST9C", "GEOM_2157")
		db.flog("W_REMOVE_DUPL - COMPLETE")

		db.skip_table("TDLP_PARCELS_TEST9C", "TDLP_PARCELS_TEST9E")

		db.update_md_non_active("TDLP_PARCELS_TEST9E")
		db.update_fad_gross_non_active("TDLP_PARCELS_TEST9E")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/CH55_NO_GAPS_TOWNLAND_FME.bat")
		db.flog("CH55_NO_GAPS_TOWNLAND_FME -    Complete")
		db.check_fme_run(701)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/CH55_NO_GAPS_DED_FME_FIX_TEMP.bat")
		db.flog("CH55_NO_GAPS_DED_FME_FIX_TEMP -    Complete")
		db.check_fme_run(703)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z4_SNAP_WS_TO_WEEKC.bat")
		db.flog("Z4_SNAP_WS_TO_WEEKC")

		db.test9e_from9e1("TDLP_PARCELS_TEST9E")
		db.flog("test9efrom9e1")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/CH55_NO_GAPS_TOWNLAND_FME.bat")
		db.flog("CH55_NO_GAPS_TOWNLAND_FME -    Complete")
		db.check_fme_run(701)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/CH55_NO_GAPS_DED_FME_FIX_TEMP.bat")
		db.flog("CH55_NO_GAPS_DED_FME_FIX_TEMP -    Complete")
		db.check_fme_run(703)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z3_A_REMOVE_TWNLAND_GAPS_WS.bat")
		db.flog("Z3_A_REMOVE_TWNLAND_GAPS_WS")

		db.test9e_from9e1("TDLP_PARCELS_TEST9E")
		db.flog("test9efrom9e1")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z4_Z1_ORACLE_TO_TEST9E_SHP_PLUS_WEEKC2.bat")
		db.flog("Z4_Z1_ORACLE_TO_TEST9E_SHP_PLUS_WEEKC2")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z5_FILL_GAPS_TEST9ESHP_TO_ORCL.bat")
		db.flog("Z5_FILL_GAPS_TEST9ESHP_TO_ORCL")

		db.test9e_from9e1("TDLP_PARCELS_TEST9E")
		db.flog("test9efrom9e1")

		db.replace_duplicate_ws_only("TDLP_PARCELS_TEST9E")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/CH55_NO_GAPS_TOWNLAND_FME.bat")
		db.flog("CH55_NO_GAPS_TOWNLAND_FME -    Complete")
		db.check_fme_run(701)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/CH55_NO_GAPS_DED_FME_FIX_TEMP.bat")
		db.flog("CH55_NO_GAPS_DED_FME_FIX_TEMP -    Complete")
		db.check_fme_run(703)

		db.insert_ws()     # 491 rows in VB.net

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/CH55_NO_GAPS_TOWNLAND_FME.bat")
		db.flog("CH55_NO_GAPS_TOWNLAND_FME -    Complete")
		db.check_fme_run(701)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/CH55_NO_GAPS_DED_FME_FIX_TEMP.bat")
		db.flog("CH55_NO_GAPS_DED_FME_FIX_TEMP -    Complete")
		db.check_fme_run(703)

		db.replace_duplicate_ws("TDLP_PARCELS_TEST9C", "GEOM_2157")
		db.flog("replaceDuplicateWS + ORACLEGEOMVALIDATION")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z4_Z1_ORACLE_TO_TEST9E_SHP_PLUS_WEEKC2.bat")
		db.flog("Z4_Z1_ORACLE_TO_TEST9E_SHP_PLUS_WEEKC2")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z5_SUBFEAT_OLAPS_NEW.bat")
		db.flog("Z5_SUBFEAT_OLAPS_NEW -     Complete")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z5_SUBFEAT_OLAPS_NEW.bat")
		db.flog("Z5_SUBFEAT_OLAPS_NEW -     Complete")
		db.check_fme_run(51)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z6_EXCL_SLIVER_DETECT_ERAD2a.bat")
		db.flog("Z6_EXCL_SLIVER_DETECT_ERAD2a -    Complete")
		db.check_fme_run(52)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z7_EXPORT_FOR_SNAP.bat")
		db.flog("Z7_EXPORT_FOR_SNAP -    Complete")
		db.check_fme_run(53)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z7_SNAP_TO_PARCS_FME.bat")
		db.flog("Z7_SNAP_TO_PARCS_FME -    Complete")
		db.check_fme_run(54)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/3_PYTHON/Python_Scripts/VClean_SF_BAT.bat")
		db.flog("VCLEAN_SF - COMPLETE")

		db.flog("START SLEEP")
		time.sleep(120)
		db.flog("END SLEEP")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z8__VCLEAN_SF_SHP_ORCL.bat")
		db.flog("Z8__VCLEAN_SF_SHP_ORCL -    Complete")
		db.check_fme_run(56)

		db.validate_subfeatures_oracle("TDLP_SUB_FEAT_REPAIRED_2")
		db.flog("Z81_ORACLE_VALIDATION_SF - COMPLETE")

		db.delete_efa_subfeatures("TDLP_SUB_FEAT_REPAIRED_2")
		db.flog("Z82_DELETE EFA_SF_TYPE_33_34_35 - COMPLETE")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z84_RECLIP_SUBFEAT.bat")
		db.flog("Z84_RECLIP_SUBFEAT -    Complete")
		db.check_fme_run(59)

		db.validate_subfeatures_oracle("TDLP_SUB_FEAT_REPAIRED_3")
		db.flog("Z86_FINAL FIXES_SF - A - COMPLETE")

		db.replace_duplicate_subfeatures("TDLP_SUB_FEAT_REPAIRED_3")
		db.flog("Z86_FINAL FIXES_SF - B - COMPLETE")

		db.update_area_fenced_off("TDLP_SUB_FEAT_REPAIRED_3")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z87_REMOVE THOSE IN WHITESP.bat")
		db.flog("Z87_REMOVE THOSE IN WHITESP -    Complete")
		db.check_fme_run(63)

		db.delete_sf_from_whitespace("TDLP_SUB_FEAT_REPAIRED_3")
		db.flog("Z88_DELETE_SF_IN_WHITESP - COMPLETE")

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z89_CREATE_TBL_FORESTRY_WEEKC_IN_V_TRANSFORM_FOR.bat")
		db.flog("Z89_CREATE_TBL_FORESTRY_WEEKC_IN_V_TRANSFORM_FOR -    Complete")
		db.check_fme_run(65)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z891_CREATE_TBL_FORESTRY_SEHLL_WEEKC_IN_POC_SHELL_FOR.bat")
		db.flog("Z891_CREATE_TBL_FORESTRY_SEHLL_WEEKC_IN_POC_SHELL_FOR -    Complete")
		db.check_fme_run(66)

		db.fmerun("//sdbahgeo2/GISDEV/TRANSFORMED_PARCELS/FME_LIVE/FULL/Process/Z911_CLEAN_SPIKE_ISSUE.bat")
		db.flog("Z911_CLEAN_SPIKE_ISSUE -    Complete")

		db.test9e_from9e1("TDLP_PARCELS_TEST9E")
		db.flog("test9efrom9e1")


		if db.connection:
			db.connection.close()
			db.connection_cdtc.close()
			db.connection_poc.close()
			db.connection_arch.close()
			db.connection_weekc.close()
			db.flog("--------------------------DONE--------------------------")
			print("--------------------------DONE--------------------------")
	except cx_Oracle.Error as error:
		print(error)