#국토교통부 데이터 백업

#ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable = 'kt_realty_gov_code_info'
#ConstRealEstateTable_GOV.MolitOfficetelRealRentMasterTable = 'kt_realty_molit_officetel_real_rent_master'

# ConstRealEstateTable_GOV.MolitTownHouseRealTradeMasterTable = 'kt_realty_molit_townhouse_real_trade_master'
# ConstRealEstateTable_GOV.MolitTownHouseRealTradeCancelTable = 'kt_realty_molit_townhouse_real_trade_master_cancel'
# ConstRealEstateTable_GOV.MolitTownHouseRealRentMasterTable = 'kt_realty_molit_townhouse_real_rent_master'




import sys
sys.path.append("D:/PythonProjects/airstock")


import urllib.request
import json
import pymysql
import datetime

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV


from Lib.RDB import pyMysqlConnector

from Init.Functions.Logs import GetLogDef
from Lib.GeoDataModule import GeoDataModule

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
import time
import random
import pymysql
import datetime
import traceback , inspect
import logging
import logging.handlers

from Lib.CustomException import QuitException
import requests


def main():

    try:
        #사용변수 초기화
        nSequence = 0

        # print( nBaseStartDate, nBaseEndDate)
        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        #국토교통부 매매/임대차 데이터 백업
        strProcessType = '035301'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'

        stToday = DateTime.today()
        DAYCARE_AUTH_KEY = '528c41f06c8146b09cae6b37431f7a66'
        nInsertedCount = 0
        nUpdateCount = 0

        GOVMoltyAddressSequence = 0

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            QuitException.QuitException(GetLogDef.lineno(__file__)+ 'strResult => '+ strResult)  # 예외를 발생시킴

        if strResult == '10':
            QuitException.QuitException(GetLogDef.lineno(__file__)+ 'It is currently in operation. => '+ strResult)  # 예외를 발생시킴

        if strResult == '20':
            GOVMoltyAddressSequence = str(rstResult.get('data_3'))

        nProcessedMasterSequence = rstResult.get('data_1')

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        nMasterBaseSeq = nProcessedMasterSequence
        nMasterSeq = nProcessedMasterSequence

        # MolitTownHouseRealTradeMasterTable = 'kt_realty_molit_townhouse_real_trade_master'
        # MolitTownHouseRealTradeBackupTable = 'kt_realty_molit_townhouse_real_trade_backup'
        # MolitTownHouseRealRentMasterTable = 'kt_realty_molit_townhouse_real_rent_master'
        # MolitTownHouseRealRentBackupTable = 'kt_realty_molit_townhouse_real_rent_backup'


        strDEALYMD = str(stToday.year).zfill(4) + str(stToday.month).zfill(2) + "00"

        nbaseDate = stToday - TimeDelta(days=365*5)
        strBackupDay = str(nbaseDate.strftime("%Y0000"))

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno),
              "strBackupDay < ", strBackupDay)

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), DateTime.today().strftime('%Y-%m-%d %H:%M:%S'))

        ########################################################################################################################

        # sqlBackupTable  = " INSERT INTO " + ConstRealEstateTable_GOV.MolitTownHouseRealTradeBackupTable
        # sqlBackupTable += " (SELECT * FROM "+ ConstRealEstateTable_GOV.MolitTownHouseRealTradeMasterTable +" "
        # sqlBackupTable += " WHERE DEAL_YMD < %s "
        # sqlBackupTable += " )"
        # cursorRealEstate.execute(sqlBackupTable, (strBackupDay))
        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), DateTime.today().strftime('%Y-%m-%d %H:%M:%S'))

        # sqlDeleteTable  = " DELETE FROM " + ConstRealEstateTable_GOV.MolitTownHouseRealTradeMasterTable
        # sqlDeleteTable += " WHERE DEAL_YMD < %s "
        # cursorRealEstate.execute(sqlDeleteTable, (strBackupDay))
        # ResRealEstateConnection.commit()
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno),
        #       DateTime.today().strftime('%Y-%m-%d %H:%M:%S'))


        # sqlBackupTable  = " INSERT INTO " + ConstRealEstateTable_GOV.MolitTownHouseRealRentBackupTable
        # sqlBackupTable += " (SELECT * FROM "+ ConstRealEstateTable_GOV.MolitTownHouseRealRentMasterTable +" "
        # sqlBackupTable += " WHERE DEAL_YMD < %s "
        # sqlBackupTable += " )"
        # cursorRealEstate.execute(sqlBackupTable, (strBackupDay))
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), DateTime.today().strftime('%Y-%m-%d %H:%M:%S'))
        #
        # sqlDeleteTable  = " DELETE FROM " + ConstRealEstateTable_GOV.MolitTownHouseRealRentMasterTable
        # sqlDeleteTable += " WHERE DEAL_YMD < %s "
        # cursorRealEstate.execute(sqlDeleteTable, (strBackupDay))
        # ResRealEstateConnection.commit()
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno),
        #       DateTime.today().strftime('%Y-%m-%d %H:%M:%S'))

########################################################################################################################

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), DateTime.today().strftime('%Y-%m-%d %H:%M:%S'))

        # ConstRealEstateTable_GOV.MolitRealTradeMasterTable = 'kt_realty_molit_townhouse_real_trade_master'
        # ConstRealEstateTable_GOV.MolitRealTradeBackupTable = 'kt_realty_molit_townhouse_real_trade_backup'
        # ConstRealEstateTable_GOV.MolitRealRentMasterTable = 'kt_realty_molit_townhouse_real_rent_master'
        # ConstRealEstateTable_GOV.MolitRealRentBackupTable = 'kt_realty_molit_townhouse_real_rent_backup'

        MolitRealTradeMasterTable = 'kt_realty_molit_real_trade_master'
        MolitRealTradeBackupTable = 'kt_realty_molit_real_trade_backup'
        MolitRealRentBackupTable = 'kt_realty_molit_real_rent_backup'
        MolitRealRentMasterTable = 'kt_realty_molit_real_rent_master'


        sqlSelectTable = " SELECT * FROM "+ ConstRealEstateTable_GOV.MolitRealTradeMasterTable +" "
        sqlSelectTable += " WHERE DEAL_YMD < %s "
        sqlSelectTable += " ORDER BY DEAL_YMD ASC"
        sqlSelectTable += " LIMIT 100000"

        cursorRealEstate.execute(sqlSelectTable, (strBackupDay))
        print(GetLogDef.lineno(__file__), "SELECT =>", sqlSelectTable, (strBackupDay))
        rstMasterDatas = cursorRealEstate.fetchall()
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "rstMasterDatas => ",
        #       rstMasterDatas)
        for rstMasterData in rstMasterDatas:
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "rstMasterData => ", rstMasterData)

            strMasterSeq = str(rstMasterData.get('seq'))
            strUniqueKey = str(rstMasterData.get('unique_key'))

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strMasterSeq => ", strMasterSeq)

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strUniqueKey => ",
                  strUniqueKey)


            sqlSelectBackupTable = " SELECT * FROM " + ConstRealEstateTable_GOV.MolitRealTradeBackupTable + " "
            sqlSelectBackupTable += " WHERE unique_key = %s"
            cursorRealEstate.execute(sqlSelectBackupTable, (strUniqueKey))
            nBackupCount = int(cursorRealEstate.rowcount)
            if nBackupCount > 0:
                continue

            sqlBackupTable  = " INSERT INTO " + ConstRealEstateTable_GOV.MolitRealTradeBackupTable
            sqlBackupTable += " (SELECT * FROM "+ ConstRealEstateTable_GOV.MolitRealTradeMasterTable +" "
            sqlBackupTable += " WHERE seq = %s"
            sqlBackupTable += " )"
            cursorRealEstate.execute(sqlBackupTable, (strMasterSeq))
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), cursorRealEstate.lastrowid)

            nInsertedId = cursorRealEstate.lastrowid
            if nInsertedId < 1:
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                        inspect.getframeinfo(inspect.currentframe()).lineno),
                      cursorRealEstate.lastrowid)
                break

            # sqlDeleteTable  = " DELETE FROM " + ConstRealEstateTable_GOV.MolitRealTradeMasterTable
            # sqlDeleteTable += " WHERE seq = %s"
            # cursorRealEstate.execute(sqlDeleteTable, (strMasterSeq))
            #
            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                         inspect.getframeinfo(inspect.currentframe()).lineno),
            #       sqlDeleteTable , strMasterSeq )
            ResRealEstateConnection.commit()

        # sqlBackupTable  = " INSERT INTO " + ConstRealEstateTable_GOV.MolitRealRentBackupTable
        # sqlBackupTable += " (SELECT * FROM "+ ConstRealEstateTable_GOV.MolitRealRentMasterTable +" "
        # sqlBackupTable += " WHERE DEAL_YMD < %s "
        # sqlBackupTable += " )"
        # cursorRealEstate.execute(sqlBackupTable, (strBackupDay))
        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), DateTime.today().strftime('%Y-%m-%d %H:%M:%S'))
        #
        # sqlDeleteTable  = " DELETE FROM " + ConstRealEstateTable_GOV.MolitRealRentMasterTable
        # sqlDeleteTable += " WHERE DEAL_YMD < %s "
        # cursorRealEstate.execute(sqlDeleteTable, (strBackupDay))
        # ResRealEstateConnection.commit()
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno),
        #       DateTime.today().strftime('%Y-%m-%d %H:%M:%S'))

        ########################################################################################################################




        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_3'] = GOVMoltyAddressSequence
        dictSwitchData['data_4'] = stToday
        dictSwitchData['data_5'] = nUpdateCount
        dictSwitchData['data_6'] = nInsertedCount
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "[END strAdminName]]================== ", strProcessType)



    except Exception as e:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "Error Exception")

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), e)

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), type(e))
        err_msg = traceback.format_exc()
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), err_msg)

        # Switch 오류 업데이트
        dictSeoulSwitch = {}
        dictSeoulSwitch['seq'] = nSequence
        dictSeoulSwitch['state'] = 20
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "dictSeoulSwitch >", dictSeoulSwitch)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        dictSwitchData['data_3'] = GOVMoltyAddressSequence
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "[END strAdminName]]================== ", strProcessType)


    else:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno),"========================= SUCCESS END")

    finally:

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno),"Finally END")

        # print('nTotalCount => ', nTotalCount)
        # print('nTradeCount => ', nTradeCount)
        # print('nRentCount => ', nRentCount)
        # print('nLookIntoCount => ', nLookIntoCount)
        # print('nNoLookCount => ', nNoLookCount)

