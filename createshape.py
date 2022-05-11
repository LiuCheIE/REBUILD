import main
import county

def createCountyShape(self):
		Disstype = 6
		Arraylen = len(arrayc)
		addToAllCountiesTable = True
		for i in arrayc:
			if ArrayIndex  == 1:
				db.updateOracle("TRUNCATE TABLE POC_TOWNLANDS_CURR_DIS_DEL", connection_CDTC)
				db.updateOracle("TRUNCATE TABLE POC_TOWNLANDS_CURR_DIS", connection_CDTC)
				db.updateOracle("TRUNCATE TABLE POC_TOWNLANDS_CURR", connection_CDTC)
				db.updateOracle("TRUNCATE TABLE POC_REAL_TOWNLANDS_CURR", connection_CDTC)

			County = i
			db.updateOracle("TRUNCATE TABLE POC_CURR_COUNTY_RUN", connection_CDTC)
			db.updateOracle('INSERT INTO POC_CURR_COUNTY_RUN (COUNTY_ID) VALUES ("{}")'.format(i), connection_CDTC)
			
			if County == 'O':
				Disstype = 5
			else if:
				Disstype = 6

			createTownlandsFromWeekc("SF_TEMP", "POC_TOWNLANDS_CURR", DissType)
			createTownlandsFromWeekc("SF_TEMP", "POC_REAL_TOWNLANDS_CURR", 5)
			
			### run FME 1_DISSOLVE_TOWNLANDS_RERUN.bat

			if Arraylen = ArrayIndex:
				addToAllCountiesTable = False
			
			createDEDDisForEachDelCounty(County, "POC_TOWNLANDS_CURR_DIS_", addToAllCountiesTable)

			ArrayIndex += 1
		
		## run FME \1_DISSOLVE_TOWNLANDS_RERUN_SHP.bat"


def createTownlandsFromWeekc(self, varTab, TwnTabCreate, DissolveType):

	db.updateOrclTable("DROP TABLE {}".format(varTab), connection_WEEKC)

	str2 = ""
	str1 = 'CREATE TABLE {} AS SELECT SPF_FEATURE_ID, SPF_FEATURE_LABEL, SPF_SPT_TYPE_ID, CREATE_DATE, UPDATE_DATE, START_DATE, END_DATE, GEOM,  SPF_VER_NUM, SPF_AUDIT_ACTION, SPF_AUDIT_CREATE_DATE, SPF_AUDIT_CREATE_USER, SPF_AUDIT_DATE, SPF_AUDIT_USER, SPF_AUDIT_LOCATION, LABEL FROM (SELECT SUF.*, DBMS_LOB.SUBSTR(REPLACE(TRIM(REGEXP_SUBSTR (REGEXP_SUBSTR(SPF_FEATURE_ATTRIBUTES, \'[^,]+\', 1, 2), \'[^:]+\', 1, 2)),"{}"), 4000,1) AS LABEL FROM TDLP_SUPER_FEATURE SUF  WHERE SPF_SPT_TYPE_ID = {} AND END_DATE > sysdate) A WHERE SUBSTR(A.LABEL,1,1) = "{}"'.format(varTab, str2, DissolveType, i)
	
	db.updateOrclTable(str1, connection_WEEKC)
	
	Meta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(varTab)
	db.updateOrclTable(Meta, connection_WEEKC)

	Spatialindex = "CREATE INDEX SPX_{} ON {}(GEOM)  INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('sdo_indx_dims=2')".format(varTab, varTab)
	db.updateOrclTable(Spatialindex, connection_WEEKC)

	
	str1 = "INSERT INTO {} SELECT * FROM SF_TEMP@LPIS_VECTOR_WEEKC".format(TwnTabCreate)
	db.updateOrclTable(str1, connection_CDTC)
	
	Meta = "INSERT INTO USER_SDO_GEOM_METADATA VALUES ('{}', 'GEOM', MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',418829.965,786046.9273,0.005),  MDSYS.SDO_DIM_ELEMENT('Y',511786.6808,964701.5937,0.005)), 2157)".format(TwnTabCreate)
	db.updateOrclTable(Meta, connection_CDTC)

	Spatialindex = "CREATE INDEX SPX_{} ON {}(GEOM)  INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS('sdo_indx_dims=2')".format(TwnTabCreate, TwnTabCreate)
	db.updateOrclTable(Spatialindex, connection_CDTC)

	
	updateOrclTable("DROP TABLE {}".format(varTab), connection_WEEKC))
	
	

	
