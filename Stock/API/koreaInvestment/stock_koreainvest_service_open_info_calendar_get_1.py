import sys,os
sys.path.append("D:/PythonProjects/airstock")
sys.path.append("D:/PythonProjects/airstock/Stock")

import html
import time
import json
import pymysql
import traceback
import re
import sys, inspect as Isp, logging, logging.handlers
import pandas as pd
import urllib.request
import pandas as pd

from Stock.API.koreaInvestment.Lib import kis_auth as ka, kis_domstk as kb
from datetime import datetime, timedelta
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup
from datetime import datetime as Datetime, timedelta , date
from Stock.LIB.Functions.Switch import StockSwitchTable
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.CONFIG import ConstTableName

def main():

    try:

        # 토큰 발급
        ka.auth()


        dtNow = datetime.today()

        strBaseYYYY = str(dtNow.year).zfill(4)
        strBaseMM = str(dtNow.month).zfill(2)
        strBaseDD = str(dtNow.day).zfill(2)
        strBaseHH = str(dtNow.hour).zfill(2)
        strBaseII = str(dtNow.minute).zfill(2)
        strBaseSS = str(dtNow.second).zfill(2)

        strProcessType = '020101'

        LogPath = 'Stock/CronLog_' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()

        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = 'Stock/'+strAddLogPath+'/'+ strProcessType

        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        strSequence='0'

        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " [CRONTAB START : " + strNowTime + "]=====================================")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = StockSwitchTable.SwitchResultSelectV2(logging, strProcessType)
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

        if strResult == '20':
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strResult : " + str(strResult) + "]")
            strSequence = rstResult.get('data_2')
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strSequence : " + str(strSequence) + "]")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = strNowDate
        dictSwitchData['data_2'] = strSequence
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'a', dictSwitchData)

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)


        for nLoop in range(0, 31):
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + "["+str(nLoop)+"]---------------------------------------------------- ")

            nbaseDate = dtNow + timedelta(days=nLoop)
            dtProcessDay = int(nbaseDate.strftime("%Y%m%d"))

            strBaseYYYY = str(nbaseDate.year).zfill(4)
            strBaseMM = str(nbaseDate.month).zfill(2)
            strBaseDD = str(nbaseDate.day).zfill(2)
            strbaseWeek = str(nbaseDate.isocalendar().week)
            strbaseWeekDay = str(nbaseDate.isocalendar().weekday)

            strBaseYYYYMM = strBaseYYYY + strBaseMM
            strBaseYYYYMMDD = strBaseYYYYMM+strBaseDD

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [dtProcessDay : " + str(dtProcessDay) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strbaseWeekDay : " + str(strbaseWeekDay) + "]")
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strbaseWeek : " + str(strbaseWeek) + "]")



            dBaseIssueDatetime = date(int(strBaseYYYY), int(strBaseMM), int(strBaseDD))

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [dBaseIssueDatetime : " + str(dBaseIssueDatetime) + "]")

            # [국내주식] 업종/기타 > 국내휴장일조회
            rt_data = kb.get_quotations_ch_holiday(dt=strBaseYYYYMMDD)

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [rt_data :"+str(rt_data)+"]")

            cOpenFlag = rt_data['opnd_yn']
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [cOpenFlag :"+str(cOpenFlag)+"]")


            sqlSelectCalendar = "SELECT * FROM " + ConstTableName.KoreaInvestOpenCalendarTable + " WHERE "
            sqlSelectCalendar += " YYYYMMDD = %s "
            cursorStockFriends.execute(sqlSelectCalendar, ( strBaseYYYYMMDD ) )

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strBaseYYYYMMDD :"+str(strBaseYYYYMMDD)+"]")

            intSelectCount = cursorStockFriends.rowcount

            if intSelectCount < 1:

                sqlInsertCalendar = " INSERT INTO " + ConstTableName.KoreaInvestOpenCalendarTable + " SET "
                sqlInsertCalendar += " YYYYMMDD = %s "
                sqlInsertCalendar += ", YYYY = %s "
                sqlInsertCalendar += ", YYYYMM = %s "
                sqlInsertCalendar += ", YYYYWEEK = %s "
                sqlInsertCalendar += ", YYYYWEEKDay = %s "
                sqlInsertCalendar += ", open_flag = %s "

                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [sqlInsertCalendar :" + str(sqlInsertCalendar) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [strBaseYYYYMMDD :" + str(strBaseYYYYMMDD) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [strBaseYYYY :" + str(strBaseYYYY) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [strBaseYYYYMM :" + str(strBaseYYYYMM) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [strbaseWeek :" + str(strbaseWeek) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [strbaseWeekDay :" + str(strbaseWeekDay) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + " [cOpenFlag :" + str(cOpenFlag) + "]")

                cursorStockFriends.execute(sqlInsertCalendar, (strBaseYYYYMMDD,strBaseYYYY,strBaseYYYYMM,strbaseWeek,strbaseWeekDay,cOpenFlag))

            elif intSelectCount > 0:

                rstDBCareldarData = cursorStockFriends.fetchone()

                strDBOpenFlag = str(rstDBCareldarData.get('open_flag'))
                if strDBOpenFlag != cOpenFlag:
                    sqlModifyCalendar = " UPDATE " + ConstTableName.KoreaInvestOpenCalendarTable + " SET "
                    sqlModifyCalendar += " open_flag = %s "
                    sqlModifyCalendar += " , modify_date = NOW() "
                    sqlModifyCalendar += " WHERE YYYYMMDD = %s "
                    cursorStockFriends.execute(sqlModifyCalendar, ( cOpenFlag, strBaseYYYYMMDD) )

            else:
                raise Exception(SLog.Ins(Isp.getframeinfo,Isp.currentframe()))

            ResStockFriendsConnection.commit()
            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = strNowDate
            dictSwitchData['data_2'] = strSequence
            StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'b', dictSwitchData)
            time.sleep(1)


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

    except Exception as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Error Exception")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(e))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
        err_msg = str(traceback.format_exc())
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        dictSwitchData['data_1'] = strNowDate
        dictSwitchData['data_2'] = strSequence
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

    else:

        print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "========================= SUCCESS END")

    finally:
        print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "Finally END")


if __name__ == '__main__':
    main()