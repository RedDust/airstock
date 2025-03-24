import os
import subprocess
import sys
import time
sys.path.append("D:/PythonProjects/airstock")
from multiprocessing import Process
from threading import Thread


from datetime import datetime as DateTime, timedelta as TimeDelta
from multiprocessing import Process
from Lib.CustomException.InspectionException import InspectionException
from Lib.CustomException.QuitException import QuitException
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.LIB.Functions.Table import ManufactureItemTable as MIT
from Realty.Auction.Const import ConstRealEstateTable_AUC
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Realty.Auction.AuctionLib.AuctionSwitchDBLib import GetSwitchData

from Lib.RDB import pyMysqlConnector
import pymysql
import traceback



dtNow = DateTime.today()

strBaseYYYY = str(dtNow.year).zfill(4)
strBaseMM = str(dtNow.month).zfill(2)
strBaseDD = str(dtNow.day).zfill(2)
strBaseHH = str(dtNow.hour).zfill(2)
strBaseII = str(dtNow.minute).zfill(2)
strBaseSS = str(dtNow.second).zfill(2)

intWeekDay = dtNow.weekday()
strNowDate = strBaseYYYY +"-"+ strBaseMM +"-"+ strBaseDD
strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS


print("[DAEMON START : "+strNowDate+" "+strNowTime+"]=====================================")


def beforeProcessRun(strProcessType,dtProcessStart,rstSwitchDatas):

    try:

        if len(strProcessType) != 6:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strProcessType => " + strProcessType)  # 예외를 발생시킴

        if dtProcessStart is None:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " dtProcessStart => None ")  # 예외를 발생시킴

        if rstSwitchDatas is None:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstResult => None ")  # 예외를 발생시킴


        dtProcessStart = DateTime.today()
        strBaseYYYY = str(dtProcessStart.year).zfill(4)
        strBaseMM = str(dtProcessStart.month).zfill(2)
        strBaseDD = str(dtProcessStart.day).zfill(2)
        strBaseHH = str(dtProcessStart.hour).zfill(2)
        strBaseII = str(dtProcessStart.minute).zfill(2)
        strBaseSS = str(dtProcessStart.second).zfill(2)
        strLoopNowDate = strBaseYYYY + "-" + strBaseMM + "-" + strBaseDD
        strLoopNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS
        strLoopDateTime = strLoopNowDate + " " + strLoopNowTime

        # print("strProcessType =>", type(strProcessType), strProcessType)
        # print("dtProcessStart =>", type(dtProcessStart), dtProcessStart)
        # print("rstSwitchDatas =>", type(rstSwitchDatas), rstSwitchDatas)

        logging.info(
            SLog.Ins(Isp.getframeinfo,
                     Isp.currentframe()) + "strProcessType => " + str(type(strProcessType)) + str(strProcessType))

        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strLoopNowTime => " + str(
                type(strLoopNowTime)) + str(strLoopNowTime))

        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "rstSwitchDatas => " + str(
                type(rstSwitchDatas)) + str(rstSwitchDatas))

        strStartTime = rstSwitchDatas.get('start_time')
        before_switchs = rstSwitchDatas.get('before_switchs')
        last_date = rstSwitchDatas.get('last_date')

        if strLoopNowTime < strStartTime:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstResult => None ")  # 예외를 발생시킴

        listBeforeSwitchs = []
        if len(before_switchs) > 0:
            listBeforeSwitchs = str(before_switchs).split("|")
            for listBeforeSwitch in listBeforeSwitchs:

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "listBeforeSwitch >> " + str(
                        listBeforeSwitch))

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "last_date >> " + str(
                        last_date))

                rstBeforeListResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(listBeforeSwitch)
                strListResult = rstBeforeListResult.get('result')
                if strListResult is False:
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                strBeforeResult = rstBeforeListResult.get('result')
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strBeforeResult >> " + str(type(strBeforeResult)) + str(
                        strBeforeResult))
                if strBeforeResult != '00':
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                strBeforeTodayWork = rstBeforeListResult.get('today_work')
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strBeforeTodayWork >> " + str(type(strBeforeTodayWork)) + str(
                        strBeforeTodayWork))

                if strBeforeTodayWork != '1':
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Error QuitException]=======")
        boolResult = False

    except Exception as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Error Exception]===========")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(e))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
        err_msg = str(traceback.format_exc())
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))
        boolResult = False

    else:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[ELSE SUCCESS]=======")

        boolResult = True

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]======")

        return boolResult




def main():

    strProcessType = '001200'
    strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
    LogPath = strAddLogPath + '/' + strProcessType

    setLogger = ULF.setLogFileV2(logging, LogPath)
    logging.info(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + "[DAEMON START : " + strNowDate + " " + strNowTime + "]")

    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

    sqlSelectSwitch = GetSwitchData()
    cursorRealEstate.execute(sqlSelectSwitch)

    rstAuctionSwitchs = cursorRealEstate.fetchall()

    # print("rstSiDoLists =>", rstAuctionSwitchs, rstAuctionSwitchs)

    dictGetProcessData = dict()
    for rstAuctionSwitch in rstAuctionSwitchs:
        # print("rstAuctionSwitch =>", rstAuctionSwitch)

        strSwitchType = rstAuctionSwitch.get('switch_type')

        # print("switch_type =>", rstAuctionSwitch.get('switch_type'))
        # print("before_switchs =>", rstAuctionSwitch.get('before_switchs'))
        # print("state =>", rstAuctionSwitch.get('state'))
        # print("last_date =>", rstAuctionSwitch.get('last_date'))
        # print("working_time =>", rstAuctionSwitch.get('working_time'))
        # print("result =>", rstAuctionSwitch.get('result'))
        # print("start_time =>", rstAuctionSwitch.get('start_time'))

        dictGetProcessData[strSwitchType] = dict()
        dictGetProcessData[strSwitchType]['switch_type'] = rstAuctionSwitch.get('switch_type')
        dictGetProcessData[strSwitchType]['before_switchs'] = rstAuctionSwitch.get('before_switchs')
        dictGetProcessData[strSwitchType]['last_date'] = rstAuctionSwitch.get('last_date')
        dictGetProcessData[strSwitchType]['result'] = rstAuctionSwitch.get('result')
        dictGetProcessData[strSwitchType]['process_sequence'] = rstAuctionSwitch.get('process_sequence')
        dictGetProcessData[strSwitchType]['start_time'] = rstAuctionSwitch.get('start_time')

    while True:

        try:

            dtProcessStart = DateTime.today()
            strBaseYYYY = str(dtProcessStart.year).zfill(4)
            strBaseMM = str(dtProcessStart.month).zfill(2)
            strBaseDD = str(dtProcessStart.day).zfill(2)
            strBaseHH = str(dtProcessStart.hour).zfill(2)
            strBaseII = str(dtProcessStart.minute).zfill(2)
            strBaseSS = str(dtProcessStart.second).zfill(2)
            strLoopNowDate = strBaseYYYY + "-" + strBaseMM + "-" + strBaseDD
            strLoopNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS
            strLoopDateTime = strLoopNowDate + " " + strLoopNowTime

            data_1 = "00"
            data_2 = "00"
            data_3 = "00"
            data_4 = "00"
            data_5 = "00"
            data_6 = "00"

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + "WHILE LOOP START  : " + strLoopDateTime +"]")

            # 스위치 데이터 조회 경매 정기점검
            rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2('000200')
            strResult = rstResult.get('result')
            if strResult is False:
                if strResult is False:
                    raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

            if strResult == '10':
                process_start_date = rstResult.get('process_start_date').strftime('%Y-%m-%d %H:%M:%S')
                last_date = rstResult.get('last_date').strftime('%Y-%m-%d %H:%M:%S')
                dtRegNow = DateTime.today()
                process_start_date_obj = DateTime.strptime(process_start_date, '%Y-%m-%d %H:%M:%S')
                last_date_obj = DateTime.strptime(last_date, '%Y-%m-%d %H:%M:%S')

                if (process_start_date_obj <= dtRegNow) and (dtRegNow <= last_date_obj):
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "process_start_date >> " + process_start_date)
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "dtRegNow >> " + dtRegNow)
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "last_date >> " + last_date)
                    raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴



            # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
            rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
            strResult = rstResult.get('result')
            if strResult is False:
                raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

            if strResult == '10':
                raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + strResult)  # 예외를 발생시킴

            if strResult == '20':
                raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + strResult)  # 예외를 발생시킴

            if strResult == '30':
                raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + strResult)  # 예외를 발생시킴

            if strResult == '40':
                raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + strResult)  # 예외를 발생시킴

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = data_1
            dictSwitchData['data_2'] = data_2
            dictSwitchData['data_3'] = data_3
            dictSwitchData['data_4'] = data_4
            dictSwitchData['data_5'] = data_5
            dictSwitchData['data_6'] = data_6
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


            #프로세서 순서대로 실행
            for strGetProcessDataKey, strGetProcessDataValue in dictGetProcessData.items():

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "==================================")

                # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
                rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strGetProcessDataKey)
                strResult = rstResult.get('result')
                if strResult is False:
                    raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                if strResult == '10':
                    continue

                if strResult == '20':
                    raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                if strResult == '40':
                    raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "Working >> " + str(strGetProcessDataKey))

                boolResult = beforeProcessRun(strGetProcessDataKey, dtProcessStart, rstResult)
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "boolResult >> " + str(boolResult))

                if boolResult == True:
                    print("실행 => ", strGetProcessDataKey)





                    # logging.info(
                    #     SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "listBeforeSwitchs.list >> " + str(listBeforeSwitchs))
                    #
                    # logging.info(
                    #     SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "before_switchs.len >> " + str(len(before_switchs)))
                    #
                    # logging.info(
                    #     SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "before_switchs >> " + before_switchs)











            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '00'
            dictSwitchData['data_1'] = data_1
            dictSwitchData['data_2'] = data_2
            dictSwitchData['data_3'] = data_3
            dictSwitchData['data_4'] = data_4
            dictSwitchData['data_5'] = data_5
            dictSwitchData['data_6'] = data_6
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


        except Exception as e:

            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Error Exception]===========================")
            print("Exception e=>", e)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(e))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
            err_msg = str(traceback.format_exc())
            print("Exception err_msg=>", err_msg)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '20'

            if data_1 is not None:
                dictSwitchData['data_1'] = data_1

            if data_2 is not None:
                dictSwitchData['data_2'] = data_2

            if err_msg is not None:
                dictSwitchData['data_3'] = err_msg

            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


        else:
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + "[ELSE SUCCESS]======================================")

        finally:
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + "[Finally END]======")

            logging.info(SLog.Ins(Isp.getframeinfo,
                                              Isp.currentframe()) + "WHILE LOOP END  : " + strLoopDateTime + "]")

            break
            time.sleep(10)









    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_1'] = data_1
    dictSwitchData['data_2'] = data_2
    dictSwitchData['data_3'] = data_3
    dictSwitchData['data_4'] = data_4
    dictSwitchData['data_5'] = data_5
    dictSwitchData['data_6'] = data_6
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    time.sleep(3)

    logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "TRY END  : " + strLoopDateTime + "]")




    dtProcessEnd = DateTime.today()
    strBaseYYYY = str(dtProcessEnd.year).zfill(4)
    strBaseMM = str(dtProcessEnd.month).zfill(2)
    strBaseDD = str(dtProcessEnd.day).zfill(2)
    strBaseHH = str(dtProcessEnd.hour).zfill(2)
    strBaseII = str(dtProcessEnd.minute).zfill(2)
    strBaseSS = str(dtProcessEnd.second).zfill(2)
    strMainNowDate = strBaseYYYY + "-" + strBaseMM + "-" + strBaseDD
    strMainNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS
    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[DAEMON END   : " + strMainNowDate + " " + strMainNowTime + "]")



if __name__ == '__main__':
    main()

print("[DAEMON END : "+strNowDate+" "+strNowTime+"]=====================================")