import os
import subprocess
import sys
import time
sys.path.append("D:/PythonProjects/airstock")
from multiprocessing import Process
from threading import Thread


from datetime import datetime as DateTime, timedelta as TimeDelta
import multiprocessing
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


def beforeProcessRun(strProcessType,rstSwitchDatas):

    try:

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[beforeProcessRun START]------------------------------")
        logging.info(f"%s [%s] %s", *listLogData)


        if len(strProcessType) != 6:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strProcessType => " + strProcessType)  # 예외를 발생시킴

        if rstSwitchDatas is None:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstResult => None ")  # 예외를 발생시킴

        # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) ,rstSwitchDatas)


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

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[strProcessType]")
        logging.info(f"%s [%s] %s", *listLogData)


        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strLoopNowTime => " + str(
                type(strLoopNowTime)) + str(strLoopNowTime))

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[strLoopNowTime]")
        listLogData.append(str(strLoopNowTime))
        logging.info(f"%s [%s] %s %s", *listLogData)

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[rstSwitchDatas]")
        listLogData.append(str(rstSwitchDatas))
        logging.info(f"%s [%s] %s %s", *listLogData)

        strStartTime = rstSwitchDatas.get('start_time')
        before_switchs = rstSwitchDatas.get('before_switchs')
        last_date = rstSwitchDatas.get('last_date')


        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[strStartTime]")
        listLogData.append(str(strStartTime))
        logging.info(f"%s [%s] %s %s", *listLogData)


        if strLoopNowTime < strStartTime:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstResult => None ")  # 예외를 발생시킴

        listBeforeSwitchs = []
        if len(before_switchs) > 0:
            listBeforeSwitchs = str(before_switchs).split("|")

            for listBeforeSwitch in listBeforeSwitchs:

                listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                listLogData.append(strProcessType)
                listLogData.append("listBeforeSwitch=>")
                listLogData.append(listBeforeSwitch)
                logging.info(f"%s [%s] %s %s", *listLogData)

                listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                listLogData.append(strProcessType)
                listLogData.append("[last_date]")
                listLogData.append(str(last_date))
                logging.info(f"%s [%s] %s %s", *listLogData)

                rstBeforeListResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(listBeforeSwitch)
                strListResult = rstBeforeListResult.get('result')
                if strListResult is False:
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                strBeforeResult = rstBeforeListResult.get('result')

                listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                listLogData.append(strProcessType)
                listLogData.append("[strBeforeResult]")
                listLogData.append(str(strBeforeResult))
                logging.info(f"%s [%s] %s %s", *listLogData)


                if strBeforeResult != '00':
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                strBeforeTodayWork = rstBeforeListResult.get('today_work')

                listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                listLogData.append(strProcessType)
                listLogData.append("[strBeforeTodayWork]")
                listLogData.append(str(strBeforeTodayWork))
                logging.info(f"%s [%s] %s %s", *listLogData)

                if strBeforeTodayWork != '1':
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


    except QuitException as e:

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[beforeProcessRun QuitException]------------------------------")
        logging.info(f"%s [%s] %s", *listLogData)


        boolResult = False

    except Exception as e:

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[beforeProcessRun Exception]------------------------------")
        logging.info(f"%s [%s] %s", *listLogData)


        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[e]")
        listLogData.append(str(type(e)))
        listLogData.append(str(e))
        logging.info(f"%s [%s] %s %s %s", *listLogData)

        err_msg = str(traceback.format_exc())

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[err_msg]")
        listLogData.append(str(type(err_msg)))
        listLogData.append(str(err_msg))
        logging.info(f"%s [%s] %s %s %s", *listLogData)

        boolResult = False

    else:
        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[beforeProcessRun SUCCESS]------------------------------")
        logging.info(f"%s [%s] %s", *listLogData)

        boolResult = True

    finally:

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strProcessType)
        listLogData.append("[beforeProcessRun END]------------------------------")
        logging.info(f"%s [%s] %s", *listLogData)

        return boolResult




def main():

    from Realty.Auction import test_daemon as StockDeamonTest



    try:

        strCronProcessType = '001200'

        LogPath = 'CourtAuction/' + strCronProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append("[CRONTAB START]============================================================")
        logging.info(f"%s %s", *listLogData)

        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strCronProcessType)
        strResult = rstResult.get('result')
        data_1 = ''
        if strResult is False:
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'It is currently in operation. => ' + str(
                strResult))  # 예외를 발생시킴

        if strResult == '20':
            data_1 = strAddressSiguSequence = str(rstResult.get('data_1'))

        if strResult == '40':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + '경매 서비스 점검 ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strCronProcessType, True, dictSwitchData)


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
            data_1 += strSwitchType + "|"
            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append(strSwitchType)
            listLogData.append("============================================================")
            logging.info(f"%s[%s] %s", *listLogData)

            dictGetProcessData = dict()
            dictGetProcessData['switch_type'] = rstAuctionSwitch.get('switch_type')
            dictGetProcessData['before_switchs'] = rstAuctionSwitch.get('before_switchs')
            dictGetProcessData['last_date'] = rstAuctionSwitch.get('last_date')
            dictGetProcessData['result'] = rstAuctionSwitch.get('result')
            dictGetProcessData['process_sequence'] = rstAuctionSwitch.get('process_sequence')
            dictGetProcessData['start_time'] = rstAuctionSwitch.get('start_time')
            dictGetProcessData['today_work'] = rstAuctionSwitch.get('today_work')


            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("strSwitchType >> ")
            listLogData.append(strSwitchType)
            logging.info(f"%s %s %s ", *listLogData)
            #금일 동작 확인
            boolContinue = False

            if dictGetProcessData['today_work'] == '1':
                boolContinue = True


            boolResult = beforeProcessRun(strSwitchType, dictGetProcessData)
            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("boolResult >> ")
            listLogData.append(boolResult)
            logging.info(f"%s %s %s ", *listLogData)
            if boolResult != True:
                boolContinue = True



            if boolContinue == True:
                listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                listLogData.append(strSwitchType)
                listLogData.append("continue >> ")
                logging.info(f"%s[%s] %s ", *listLogData)
                continue

            print("실행 => ", strSwitchType)

            intLoop = 0

            # while True:

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "--->" + str(intLoop) + " START ")

            # logging.info("[" + SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "]---------------------------------" + str(intLoop) + " => " + str(bbbb))
            # slave_thread():
            # th1 = Thread(target=StockDeamonTest.main, args=(str(intLoop)))

            listProcess = []

            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("strSwitchType >> ")
            listLogData.append(strSwitchType)
            logging.info(f"%s %s %s ", *listLogData)

            if strSwitchType == '021000':
                # [022000] 법원 경매 부동산 물건 수집(매각)
                from Realty.Auction import get_1_acution_data
                th1 = multiprocessing.Process(name="Sub Process", target=get_1_acution_data.main, daemon=False)

                logging.info("[" + SLog.Ins(Isp.getframeinfo,
                                            Isp.currentframe()) + "]---------------------------------" + str(
                    intLoop) + " => " + str(th1))


                # th1.daemon = True
                th1.start()
                listProcess.append(th1)

            if strSwitchType == '022000':
                # [022000] 법원 경매 부동산 물건 수집(예정)
                from Realty.Auction import get_1_auction_data_planned
                th1 = multiprocessing.Process(name="Sub Process", target=get_1_auction_data_planned.main, daemon=False)

                logging.info("[" + SLog.Ins(Isp.getframeinfo,
                                            Isp.currentframe()) + "]---------------------------------" + str(
                    intLoop) + " => " + str(th1))


                # th1.daemon = True
                th1.start()
                listProcess.append(th1)

            if strSwitchType == '023000':
                # [023000] 법원경매 물건수집 - (결과)
                from Realty.Auction import get_1_auction_data_complete
                th1 = multiprocessing.Process(name="Sub Process", target=get_1_auction_data_complete.main, daemon=False)

                logging.info("[" + SLog.Ins(Isp.getframeinfo,
                                            Isp.currentframe()) + "]---------------------------------" + str(
                    intLoop) + " => " + str(th1))

                # th1.daemon = True
                th1.start()
                listProcess.append(th1)

            if strSwitchType == '021100':
                # [021100] 법원 경매 부동산 물건 수집(매각) SPOOL 처리
                from Realty.Auction import get_1_acution_data_2_spool_distribution
                th1 = multiprocessing.Process(name="Sub Process", target=get_1_acution_data_2_spool_distribution.main, daemon=False)

                logging.info("[" + SLog.Ins(Isp.getframeinfo,
                                            Isp.currentframe()) + "]---------------------------------" + str(
                    intLoop) + " => " + str(th1))

                # th1.daemon = True
                th1.start()
                listProcess.append(th1)

            if strSwitchType == '021120':
                # [021120] 법원 경매 부동산 물건 수집(매각) GEO 데이터 UPDATE
                from Realty.Auction import get_1_auction_data_3_master_geodata
                th1 = multiprocessing.Process(name="021200 Process", target=get_1_auction_data_3_master_geodata.main, daemon=False)

                logging.info("[" + SLog.Ins(Isp.getframeinfo,
                                            Isp.currentframe()) + "]---------------------------------" + str(
                    intLoop) + " => " + str(th1))

                # th1.daemon = True
                th1.start()
                listProcess.append(th1)




            if strSwitchType == '021110':
                # [021110] 법원 경매 부동산 물건 수집(매각) SPOOL 백업
                from Realty.Auction import backup_auction_data_spool
                th1 = multiprocessing.Process(name="Sub Process", target=backup_auction_data_spool.main, daemon=False)

                logging.info("[" + SLog.Ins(Isp.getframeinfo,
                                            Isp.currentframe()) + "]---------------------------------" + str(
                    intLoop) + " => " + str(th1))

                # th1.daemon = True
                th1.start()
                listProcess.append(th1)







            if len(listProcess) > 1:
                logging.info("[" + SLog.Ins(Isp.getframeinfo,
                                            Isp.currentframe()) + "]---------------------------------" + str(
                    intLoop) + " => " + str(th1))

            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("listProcess >> ")
            listLogData.append(listProcess)
            logging.info(f"%s %s %s ", *listLogData)

            # for p in listProcess:
            #     p.join()  # 프로세스가 모두 종료될 때까지 대기

            intLoop += 1
            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = data_1
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strCronProcessType, False, dictSwitchData)



        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strCronProcessType, False, dictSwitchData)



    except Exception as e:

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strCronProcessType)
        listLogData.append("[CRONTAB Exception]------------------------------")
        logging.info(f"%s [%s] %s", *listLogData)

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strCronProcessType)
        listLogData.append("[e]")
        listLogData.append(str(type(e)))
        listLogData.append(str(e))
        logging.info(f"%s [%s] %s %s %s", *listLogData)

        err_msg = str(traceback.format_exc())

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strCronProcessType)
        listLogData.append("[err_msg]")
        listLogData.append(str(type(err_msg)))
        listLogData.append(str(err_msg))
        logging.info(f"%s [%s] %s %s %s", *listLogData)

        boolResult = False

        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strCronProcessType, False, dictSwitchData)


    else:
        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strCronProcessType)
        listLogData.append("[CRONTAB SUCCESS]------------------------------")
        logging.info(f"%s [%s] %s", *listLogData)

        boolResult = True

    finally:

        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
        listLogData.append(strCronProcessType)
        listLogData.append("[CRONTAB END]------------------------------")
        logging.info(f"%s [%s] %s", *listLogData)

        return boolResult


if __name__ == '__main__':
    main()

print("[DAEMON END : "+strNowDate+" "+strNowTime+"]=====================================")
