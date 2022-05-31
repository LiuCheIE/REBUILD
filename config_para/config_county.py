from datetime import date


array_country = ["O", "Q", "R", "B", "N", "L", "F", "I", "X", "U", "A", "Z", "K", "S", "T", "J", "P", "M", "E", "D", "H", "W", "Y", "V"]
v_county = "V"
v_curr_county = "V"
county_delivered = ("O", "Q", "R", "B", "N", "L", "F", "I", "X", "U", "A", "Z", "K", "S", "T", "J", "P", "M", "E", "D", "H", "W", "Y")

array1 = {"V"}
array_index = 1
sample_herd = ""
vOrControlList = " OR PARCEL_ID IN (select LNU_PARCEL_ID from C##LPIS_CDTC.CONTROL_LIST) "
vForestryControl = " AND FORESTRY_CN IN (select FORESTRY_CONTRACT_NO from C##LPIS_CDTC.FOR_CONTROL) "

ref_db = "@LPIS_VECTOR_WEEKC"

firstRun = True

if firstRun is True:
    ref_db = "@LPIS_VECTOR_LRP"

del_louth = "C##LPIS_TRANSFORM.TDLP_PARCELS"
del_meath = "C##LPIS_TRANSFORM.TDLP_PARCELS_MEATH2"
del_monaghan = "C##LPIS_TRANSFORM.TDLP_PARCELS_MONAGHAN2"
del_cavan = "C##LPIS_TRANSFORM.TDLP_PARCELS_CAVAN2"
del_longford = "C##LPIS_TRANSFORM.TDLP_PARCELS_LONGFORD2"
del_leitrim = "C##LPIS_TRANSFORM.TDLP_PARCELS_LEITRIM2"
del_miniImp = "C##LPIS_TRANSFORM.TDLP_PARCELS_Lpis_6698"
del_dublin = "C##LPIS_TRANSFORM.TDLP_PARCELS_DUBLIN2"
del_kildare = "C##LPIS_TRANSFORM.TDLP_PARCELS_KILDARE2"
del_westmeath = "C##LPIS_TRANSFORM.TDLP_PARCELS_WESTMEATH2"
del_sligo = "C##LPIS_TRANSFORM.TDLP_PARCELS_SLIGO2"
del_carlow = "C##LPIS_TRANSFORM.TDLP_PARCELS_CARLOW2"
del_offaly = "C##LPIS_TRANSFORM.TDLP_PARCELS_OFFALY2"
del_laois = "C##LPIS_TRANSFORM.TDLP_PARCELS_LAOIS2"
del_wicklow = "C##LPIS_TRANSFORM.TDLP_PARCELS_WICKLOW2"
del_roscommon = "C##LPIS_TRANSFORM.TDLP_PARCELS_ROSCOMMON2"
del_kilkenny = "C##LPIS_TRANSFORM.TDLP_PARCELS_KILKENNY2"
del_mayo = "C##LPIS_TRANSFORM.TDLP_PARCELS_MAYO2"
del_limerick = "C##LPIS_TRANSFORM.TDLP_PARCELS_LIMERICK2"
del_donegal = "C##LPIS_TRANSFORM.TDLP_PARCELS_DONEGAL2"
del_cork = "C##LPIS_TRANSFORM.TDLP_PARCELS_CORK"
del_kerry = "C##LPIS_TRANSFORM.TDLP_PARCELS_KERRY"
del_waterford = "C##LPIS_TRANSFORM.TDLP_PARCELS_WATERFORD"
del_wexford = "C##LPIS_TRANSFORM.TDLP_PARCELS_WEXFORD"
curr_date = date.today().strftime("%Y%m%d")

arraydel = [del_meath, del_monaghan, del_cavan, del_longford, del_leitrim, del_miniImp, del_dublin, del_kildare,
            del_westmeath, del_sligo, del_carlow, del_offaly, del_laois, del_wicklow, del_roscommon, del_kilkenny, del_mayo,
            del_limerick, del_donegal, del_cork, del_kerry, del_waterford, del_wexford]


def county_code_dict(dict_county):
    dict_county["A"] = "CARLOW"
    dict_county["B"] = "CAVAN"
    dict_county["C"] = "CLARE"
    dict_county["D"] = "CORK"
    dict_county["E"] = "DONEGAL"

    dict_county["F"] = "DUBLIN"
    dict_county["G"] = "GALWAY"
    dict_county["H"] = "KERRY"
    dict_county["I"] = "KILDARE"
    dict_county["J"] = "KILKENNY"

    dict_county["K"] = "LAOIS"
    dict_county["L"] = "LEITRIM"
    dict_county["M"] = "LIMERICK"
    dict_county["N"] = "LONGFORD"
    dict_county["O"] = "LOUTH"

    dict_county["P"] = "MAYO"
    dict_county["Q"] = "MEATH"
    dict_county["R"] = "MONAGHAN"
    dict_county["S"] = "OFFALY"
    dict_county["T"] = "ROSCOMMON"

    dict_county["U"] = "SLIGO"
    dict_county["V"] = "TIPPERARY"
    dict_county["W"] = "WATERFORD"
    dict_county["X"] = "WESTMEATH"
    dict_county["Y"] = "WEXFORD"
    dict_county["Z"] = "WICKLOW"

    return dict_county
