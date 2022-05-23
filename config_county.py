from sys import modules
from datetime import datetime
import getpass
from datetime import date
import subprocess
import time
from functools import wraps


arrayc = ["O", "Q", "R", "B", "N", "L", "F", "I", "X", "U", "A", "Z", "K", "S", "T", "J", "P", "M", "E", "D", "H", "W",
          "Y", "V"]
VCounty = "V"
vCurrCounty = "V"
countyDelivered = (
"O", "Q", "R", "B", "N", "L", "F", "I", "X", "U", "A", "Z", "K", "S", "T", "J", "P", "M", "E", "D", "H", "W", "Y")

array1 = {"V"}
ArrayIndex = 1
vSampleHerd = ""
vOrControlList = " OR PARCEL_ID IN (select LNU_PARCEL_ID from C##LPIS_CDTC.CONTROL_LIST) "
vForestryControl = " AND FORESTRY_CN IN (select FORESTRY_CONTRACT_NO from C##LPIS_CDTC.FOR_CONTROL) "

ref_db = "@LPIS_VECTOR_WEEKC"

firstRun = True

if firstRun is True:
    ref_db = "@LPIS_VECTOR_LRP"

DelLouth = "C##LPIS_TRANSFORM.TDLP_PARCELS"
DelMeath = "C##LPIS_TRANSFORM.TDLP_PARCELS_MEATH2"
DelMonaghan = "C##LPIS_TRANSFORM.TDLP_PARCELS_MONAGHAN2"
DelCavan = "C##LPIS_TRANSFORM.TDLP_PARCELS_CAVAN2"
DelLongford = "C##LPIS_TRANSFORM.TDLP_PARCELS_LONGFORD2"
DelLeitrim = "C##LPIS_TRANSFORM.TDLP_PARCELS_LEITRIM2"
DelMiniImp = "C##LPIS_TRANSFORM.TDLP_PARCELS_Lpis_6698"
DelDublin = "C##LPIS_TRANSFORM.TDLP_PARCELS_DUBLIN2"
DelKildare = "C##LPIS_TRANSFORM.TDLP_PARCELS_KILDARE2"
DelWestmeath = "C##LPIS_TRANSFORM.TDLP_PARCELS_WESTMEATH2"
DelSligo = "C##LPIS_TRANSFORM.TDLP_PARCELS_SLIGO2"
DelCarlow = "C##LPIS_TRANSFORM.TDLP_PARCELS_CARLOW2"
DelOffaly = "C##LPIS_TRANSFORM.TDLP_PARCELS_OFFALY2"
DelLaois = "C##LPIS_TRANSFORM.TDLP_PARCELS_LAOIS2"
DelWicklow = "C##LPIS_TRANSFORM.TDLP_PARCELS_WICKLOW2"
DelRoscommon = "C##LPIS_TRANSFORM.TDLP_PARCELS_ROSCOMMON2"
DelKilkenny = "C##LPIS_TRANSFORM.TDLP_PARCELS_KILKENNY2"
DelMayo = "C##LPIS_TRANSFORM.TDLP_PARCELS_MAYO2"
DelLimerick = "C##LPIS_TRANSFORM.TDLP_PARCELS_LIMERICK2"
DelDonegal = "C##LPIS_TRANSFORM.TDLP_PARCELS_DONEGAL2"
DelCork = "C##LPIS_TRANSFORM.TDLP_PARCELS_CORK"
DelKerry = "C##LPIS_TRANSFORM.TDLP_PARCELS_KERRY"
DelWaterford = "C##LPIS_TRANSFORM.TDLP_PARCELS_WATERFORD"
DelWexford = "C##LPIS_TRANSFORM.TDLP_PARCELS_WEXFORD"
CurrDate = date.today().strftime("%Y%m%d")

arraydel = [DelMeath, DelMonaghan, DelCavan, DelLongford, DelLeitrim, DelMiniImp, DelDublin, DelKildare,
            DelWestmeath, DelSligo, DelCarlow, DelOffaly, DelLaois, DelWicklow, DelRoscommon, DelKilkenny, DelMayo,
            DelLimerick, DelDonegal, DelCork, DelKerry, DelWaterford, DelWexford]


def countyCodeDict(dictCounty):
    dictCounty["A"] = "CARLOW"
    dictCounty["B"] = "CAVAN"
    dictCounty["C"] = "CLARE"
    dictCounty["D"] = "CORK"
    dictCounty["E"] = "DONEGAL"

    dictCounty["F"] = "DUBLIN"
    dictCounty["G"] = "GALWAY"
    dictCounty["H"] = "KERRY"
    dictCounty["I"] = "KILDARE"
    dictCounty["J"] = "KILKENNY"

    dictCounty["K"] = "LAOIS"
    dictCounty["L"] = "LEITRIM"
    dictCounty["M"] = "LIMERICK"
    dictCounty["N"] = "LONGFORD"
    dictCounty["O"] = "LOUTH"

    dictCounty["P"] = "MAYO"
    dictCounty["Q"] = "MEATH"
    dictCounty["R"] = "MONAGHAN"
    dictCounty["S"] = "OFFALY"
    dictCounty["T"] = "ROSCOMMON"

    dictCounty["U"] = "SLIGO"
    dictCounty["V"] = "TIPPERARY"
    dictCounty["W"] = "WATERFORD"
    dictCounty["X"] = "WESTMEATH"
    dictCounty["Y"] = "WEXFORD"
    dictCounty["Z"] = "WICKLOW"

    return dictCounty