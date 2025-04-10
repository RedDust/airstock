#set_0_auction_daily_reset.py

import os
import subprocess
import sys
import time
sys.path.append("D:/PythonProjects/airstock")
from multiprocessing import Process
from threading import Thread
from datetime import datetime as DateTime, timedelta as TimeDelta
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.LIB.Functions.Table import ManufactureItemTable as MIT
from Lib.RDB import pyMysqlConnector
import pymysql
import traceback
from Realty.Auction.AuctionLib.AuctionSwitchDBLib import GetSwitchData

def main():

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



        strProcessType = '001210'
        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = strAddLogPath + '/' + strProcessType

        setLogger = ULF.setLogFileV2(logging, LogPath)

        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[SET START : " + strLoopDateTime + "]")



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
            dictGetProcessData[strSwitchType]['today_work'] = rstAuctionSwitch.get('today_work')

            if dictGetProcessData[strSwitchType]['today_work'] == '0':
                print("83 Continue")
                continue

            sqlUpdateSwitch  = " UPDATE kt_realty_swtich SET "
            sqlUpdateSwitch += " today_work = '0' "
            sqlUpdateSwitch += " , last_date = NOW() "
            sqlUpdateSwitch += " WHERE switch_type = %s "
            cursorRealEstate.execute(sqlUpdateSwitch, (strSwitchType))

            print("UPDATE SET 0 ")



        print("dictGetProcessData => " , dictGetProcessData)


    except Exception as e:

        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Error Exception]===========================")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(e))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
        err_msg = str(traceback.format_exc())
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))


    else:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[ELSE SUCCESS]======================================")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]======")


    logging.info(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + "[SET END : " + strLoopDateTime + "]")


if __name__ == '__main__':
    main()
