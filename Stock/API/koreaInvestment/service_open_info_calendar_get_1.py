import time
from Stock.API.koreaInvestment.Lib import kis_auth as ka, kis_domstk as kb
import sys, inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
import pandas as pd
from datetime import datetime, timedelta
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.CONFIG import ConstTableName

from Stock.LIB.SeleniumModule.Windows import Chrome, Firefox
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup
import html

import urllib.request
import json
import pymysql
import traceback
import pandas as pd
from datetime import datetime as Datetime, timedelta , date
import re



def main():

    try:

        dtNow = datetime.today()

        strBaseYYYY = str(dtNow.year).zfill(4)
        strBaseMM = str(dtNow.month).zfill(2)
        strBaseDD = str(dtNow.day).zfill(2)
        strBaseHH = str(dtNow.hour).zfill(2)
        strBaseII = str(dtNow.minute).zfill(2)
        strBaseSS = str(dtNow.second).zfill(2)

        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS
        strProcessType = '020101'

        LogPath = 'Stock/CronLog_' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()


        logging.info("[CRONTAB START : " + strNowTime + "]=====================================")

        # 토큰 발급
        ka.auth()

        dtNow = Datetime.today()

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        for nLoop in range(0, 31):
            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "---------------------------------------------------- ")

            nbaseDate = dtNow + timedelta(days=nLoop)
            dtProcessDay = int(nbaseDate.strftime("%Y%m%d"))

            strBaseYYYY = str(nbaseDate.year).zfill(4)
            strBaseMM = str(nbaseDate.month).zfill(2)
            strBaseDD = str(nbaseDate.day).zfill(2)
            strbaseWeek = str(nbaseDate.isocalendar().week)
            strbaseWeekDay = str(nbaseDate.isocalendar().weekday)

            strBaseYYYYMM = strBaseYYYY + strBaseMM
            strBaseYYYYMMDD = strBaseYYYYMM+strBaseDD

            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "dtProcessDay > ", dtProcessDay)
            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "nbaseWeekDay > ", strbaseWeekDay)
            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "nbaseWeek > ", strbaseWeek)


            dBaseIssueDatetime = date(int(strBaseYYYY), int(strBaseMM), int(strBaseDD))

            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "dBaseIssueDatetime > " , dBaseIssueDatetime)

            # [국내주식] 업종/기타 > 국내휴장일조회
            rt_data = kb.get_quotations_ch_holiday(dt=strBaseYYYYMMDD)

            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "rt_data >> ", len(rt_data),
                  type(rt_data),rt_data)

            cOpenFlag = rt_data['opnd_yn']

            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "cOpenFlag >> ", len(cOpenFlag),
                  type(cOpenFlag),cOpenFlag)


            sqlSelectCalendar = "SELECT * FROM " + ConstTableName.KoreaInvestOpenCalendarTable + " WHERE "
            sqlSelectCalendar += " YYYYMMDD = %s "
            cursorStockFriends.execute(sqlSelectCalendar, ( strBaseYYYYMMDD ) )

            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "strBaseYYYYMMDD > ", strBaseYYYYMMDD)

            intSelectCount = cursorStockFriends.rowcount

            if intSelectCount < 1:

                sqlInsertCalendar = " INSERT INTO " + ConstTableName.KoreaInvestOpenCalendarTable + " SET "
                sqlInsertCalendar += " YYYYMMDD = %s "
                sqlInsertCalendar += ", YYYY = %s "
                sqlInsertCalendar += ", YYYYMM = %s "
                sqlInsertCalendar += ", YYYYWEEK = %s "
                sqlInsertCalendar += ", YYYYWEEKDay = %s "
                sqlInsertCalendar += ", open_flag = %s "

                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "sqlInsertCalendar > ", sqlInsertCalendar)

                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "strBaseYYYYMMDD > ", strBaseYYYYMMDD,strBaseYYYY,strBaseYYYYMM,strbaseWeek,strbaseWeekDay,cOpenFlag)


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
            time.sleep(1)

    except Exception as e:

        print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "Error Exception")
        print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), e)
        print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), type(e))
        err_msg = traceback.format_exc()
        print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), err_msg)



    else:

        print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "========================= SUCCESS END")

    finally:
        print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "Finally END")


if __name__ == '__main__':
    main()