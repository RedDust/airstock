# This is a sample Python script.
# 매일 09시 실행
import os
import sys
import json
import time
import random
import pymysql
import datetime
sys.path.append("D:/PythonProjects/airstock")




#from Helper import basic_fnc

from Realty.Auction.Const import ConstRealEstateTable_AUC
from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from Init.DefConstant import ConstRealEstateTable
from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.CustomException import InspectionException , QuitException
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
import traceback, inspect


def main():

    return False

    try:

        #서울 부동산 실거래가 데이터 - 매매
        strProcessType = 'X021000'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'
        nLoop = 0
        nMasterSeq = 0
        issue_number = '00'
        auction_code = '00'
        nInsertedCount = 0
        nUpdateCount = 0

        dtNow = DateTime.today()

        strBaseYYYY = str(dtNow.year).zfill(4)
        strBaseMM = str(dtNow.month).zfill(2)
        strBaseDD = str(dtNow.day).zfill(2)
        strBaseHH = str(dtNow.hour).zfill(2)
        strBaseII = str(dtNow.minute).zfill(2)
        strBaseSS = str(dtNow.second).zfill(2)



        LogPath = 'CourtAuction/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " [CRONTAB START : " + strNowTime + "]=====================================")


        # print( nBaseStartDate, nBaseEndDate)
        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        # 스위치 데이터 조회 type(000200) result (10:정기점검)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2('000200')
        strResult = rstResult.get('result')
        if strResult is False:
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strResult : " + str(strResult) + "]")
            quit(GetLogDef.lineno(__file__)+ 'strResult => '+ str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            process_start_date = rstResult.get('process_start_date').strftime('%Y-%m-%d %H:%M:%S')
            last_date = rstResult.get('last_date').strftime('%Y-%m-%d %H:%M:%S')
            dtRegNow = DateTime.today()
            process_start_date_obj = DateTime.strptime(process_start_date, '%Y-%m-%d %H:%M:%S')
            last_date_obj = DateTime.strptime(last_date, '%Y-%m-%d %H:%M:%S')


            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [process_start_date_obj : " + str(process_start_date_obj) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [dtRegNow : " + str(dtRegNow) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [last_date_obj : " + str(last_date_obj) + "]")

            if (process_start_date_obj <= dtRegNow) and (dtRegNow <= last_date_obj):
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [InspectionException : ]")
                raise InspectionException.InspectionException("500")


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strResult : " + str(strResult) + "]")
            quit(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) +  'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strResult : " + str(strResult) + "]")
            quit(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = KuIndex
        dictSwitchData['data_2'] = arrCityPlace
        dictSwitchData['data_3'] = targetRow
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        print(GetLogDef.lineno(__file__), "CourtAuctionDataTable ===================================> ")
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " [CourtAuctionDataTable")
        # qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " LIMIT 10"
        qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable
        cursorRealEstate.execute(qrySelectNaverMobileMaster)
        rstMasterDatas = cursorRealEstate.fetchall()

        for MasterDataList in rstMasterDatas:
            nMasterSeq = str(MasterDataList.get('seq'))
            issue_number = str(MasterDataList.get('issue_number'))
            auction_seq = str(MasterDataList.get('auction_seq'))
            auction_day = str(MasterDataList.get('auction_day'))
            auction_code = str(MasterDataList.get('auction_code'))
            address_data = str(MasterDataList.get('address_data'))
            CityKey = str(MasterDataList.get('sido_code'))
            strSiGuCode = str(MasterDataList.get('sigu_code'))
            strRoadName =  str(MasterDataList.get('road_name'))
            unique_value2 = str(MasterDataList.get('unique_value2'))

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [nMasterSeq : " + str(nMasterSeq) + "]")

            nLoop = nLoop + 1

            qrySelectNaverMobileMaster = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionBackupTable
            qrySelectNaverMobileMaster += " WHERE unique_value2 = %s "
            cursorRealEstate.execute(qrySelectNaverMobileMaster, (unique_value2))
            rstBackupDatas = cursorRealEstate.fetchone()
            nStatisticsCount = int(cursorRealEstate.rowcount)

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [nMasterSeq : " + str(nMasterSeq) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [rstBackupDatas : " + str(rstBackupDatas) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [qrySelectNaverMobileMaster : " + str(qrySelectNaverMobileMaster) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [auction_code : " + str(auction_code) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [auction_seq : " + str(auction_seq) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [auction_day : " + str(auction_day) + "]")



            if nStatisticsCount < 1:
                qryInsertCourtAuctionBackup = " INSERT INTO " + ConstRealEstateTable_AUC.CourtAuctionBackupTable
                qryInsertCourtAuctionBackup += " (SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable
                qryInsertCourtAuctionBackup += " WHERE unique_value2 = %s "
                qryInsertCourtAuctionBackup += ")"
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [qryInsertCourtAuctionBackup : " + str(qryInsertCourtAuctionBackup) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [unique_value2 : " + str(unique_value2) + "]")

                cursorRealEstate.execute(qryInsertCourtAuctionBackup, (unique_value2))

            else:

                qryUpdateCourtAuctionBackup = "UPDATE " + ConstRealEstateTable_AUC.CourtAuctionBackupTable
                qryUpdateCourtAuctionBackup += " SET modify_date=NOW() "
                qryUpdateCourtAuctionBackup += " , sido_code= %s "
                qryUpdateCourtAuctionBackup += " , sigu_code=%s "
                qryUpdateCourtAuctionBackup += " , road_name=%s "
                qryUpdateCourtAuctionBackup += " WHERE unique_value2 = %s "
                # qryUpdateCourtAuctionBackup += " WHERE auction_code = %s AND auction_seq = %s AND auction_day =%s "
                print(GetLogDef.lineno(__file__), "UPDATE =>", unique_value2)
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [qryUpdateCourtAuctionBackup : " + str(qryUpdateCourtAuctionBackup) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [CityKey : " + str(CityKey) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [strSiGuCode : " + str(strSiGuCode) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [strRoadName : " + str(strRoadName) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [unique_value2 : " + str(unique_value2) + "]")
                cursorRealEstate.execute(qryUpdateCourtAuctionBackup, (CityKey, strSiGuCode , strRoadName , unique_value2))

            qryDeleteCourtAuctionData = " DELETE FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable
            qryDeleteCourtAuctionData += " WHERE seq = %s LIMIT 1"
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [qryDeleteCourtAuctionData : " + str(qryDeleteCourtAuctionData) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [nMasterSeq : " + str(nMasterSeq) + "]")
            cursorRealEstate.execute(qryDeleteCourtAuctionData, nMasterSeq)

            # if(rstBackupDatas.get > 0):
            #     print(GetLogDef.lineno(__file__), "MasterDataList.seq => ", nMasterSeq)
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [nLoop : " + str(nLoop) + "]")
            # ResRealEstateConnection.rollback()
            ResRealEstateConnection.commit()

        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " [CourtAuctionCompleteTable]========================>")

        qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable
        cursorRealEstate.execute(qrySelectNaverMobileMaster)
        rstMasterDatas = cursorRealEstate.fetchall()

        for MasterDataList in rstMasterDatas:
            nMasterSeq = str(MasterDataList.get('seq'))
            issue_number = str(MasterDataList.get('issue_number'))
            auction_seq = str(MasterDataList.get('auction_seq'))
            auction_day = str(MasterDataList.get('auction_day'))
            auction_code = str(MasterDataList.get('auction_code'))
            address_data = str(MasterDataList.get('address_data'))
            CityKey = str(MasterDataList.get('sido_code'))
            strSiGuCode = str(MasterDataList.get('sigu_code'))
            strRoadName = str(MasterDataList.get('road_name'))

            unique_value2 = str(MasterDataList.get('unique_value2'))

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [MasterDataList : " + str(nMasterSeq) + "]")

            nLoop = nLoop + 1

            qrySelectNaverMobileMaster = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionBackupTable
            qrySelectNaverMobileMaster += " WHERE unique_value2 = %s "
            cursorRealEstate.execute(qrySelectNaverMobileMaster, (unique_value2))
            print(GetLogDef.lineno(__file__), "SELECT =>", qrySelectNaverMobileMaster, (unique_value2))
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [qrySelectNaverMobileMaster : " + str(qrySelectNaverMobileMaster) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [unique_value2 : " + str(unique_value2) + "]")

            rstBackupDatas = cursorRealEstate.fetchone()
            nStatisticsCount = int(cursorRealEstate.rowcount)

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [nMasterSeq : " + str(nMasterSeq) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [rstBackupDatas : " + str(rstBackupDatas) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [qrySelectNaverMobileMaster : " + str(qrySelectNaverMobileMaster) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [nStatisticsCount : " + str(nStatisticsCount) + "]")


            if nStatisticsCount < 1:
                qryInsertCourtAuctionBackup = " INSERT INTO " + ConstRealEstateTable_AUC.CourtAuctionBackupTable
                qryInsertCourtAuctionBackup += " (SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable
                qryInsertCourtAuctionBackup += " WHERE unique_value2 = %s "
                qryInsertCourtAuctionBackup += ")"
                print(GetLogDef.lineno(__file__), "INSERT =>", qryInsertCourtAuctionBackup, (unique_value2))
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [qryInsertCourtAuctionBackup : " + str(qryInsertCourtAuctionBackup) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [unique_value2 : " + str(unique_value2) + "]")

                cursorRealEstate.execute(qryInsertCourtAuctionBackup, (unique_value2))

            else:

                qryUpdateCourtAuctionBackup = "UPDATE " + ConstRealEstateTable_AUC.CourtAuctionBackupTable
                qryUpdateCourtAuctionBackup += " SET modify_date=NOW() "
                qryUpdateCourtAuctionBackup += " , sido_code= %s "
                qryUpdateCourtAuctionBackup += " , sigu_code=%s "
                qryUpdateCourtAuctionBackup += " , road_name=%s "
                qryUpdateCourtAuctionBackup += " WHERE unique_value2 = %s "
                # qryUpdateCourtAuctionBackup += " WHERE auction_code = %s AND auction_seq = %s AND auction_day =%s "
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [unique_value2 : " + str(unique_value2) + "]")

                cursorRealEstate.execute(qryUpdateCourtAuctionBackup, (CityKey, strSiGuCode,strRoadName , unique_value2))

            qryDeleteCourtAuctionData = " DELETE FROM " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable
            qryDeleteCourtAuctionData += " WHERE seq = %s LIMIT 1"
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [qryDeleteCourtAuctionData : " + str(qryDeleteCourtAuctionData) + "]")

            print(GetLogDef.lineno(__file__), "DELETE =>", qryDeleteCourtAuctionData, nMasterSeq)
            cursorRealEstate.execute(qryDeleteCourtAuctionData, nMasterSeq)

            # if(rstBackupDatas.get > 0):
            #     print(GetLogDef.lineno(__file__), "MasterDataList.seq => ", nMasterSeq)
            print("================For Loop", nLoop)
            # ResRealEstateConnection.rollback()
            ResRealEstateConnection.commit()

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_3'] = nLoop
            dictSwitchData['data_4'] = nMasterSeq
            dictSwitchData['data_5'] = auction_code
            dictSwitchData['data_6'] = issue_number
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_3'] = nLoop
        dictSwitchData['data_4'] = nMasterSeq
        dictSwitchData['data_5'] = auction_code
        dictSwitchData['data_6'] = issue_number
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except QuitException as e:

        print("QuitException Exception")
        print("========================================================")
        pass

    except InspectionException as e:

        print("InspectionException Exception")
        print("========================================================")
        pass

    except Exception as e:

        ResRealEstateConnection.rollback()



        print("========================================================")
        logging.info(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + " [Exception :=====================================")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        dictSwitchData['data_3'] = nLoop
        dictSwitchData['data_4'] = nMasterSeq
        if strProcessType is not None:
            dictSwitchData['data_1'] = nMasterSeq

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + "err_msg>"  + str(err_msg))
        logging.info(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + "e>"  + str(e))

    else:
        print("========================================================")
        logging.info(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + " [SUCCESS :=====================================")

    finally:
        ResRealEstateConnection.close()
        logging.info(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + " [Finally END :=====================================")
