
import os
import sys
import pymysql
import time
import datetime
import traceback
import inspect as Isp, logging, logging.handlers

from Stock.LIB.RDB import pyMysqlConnector
from Stock.CONFIG import ConstTableName
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF


def SwitchResultUpdate(SwitchData):


    try:

        SwitchData = str(SwitchData)
        dtNowDateTime = datetime.datetime.now()

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchData]" + str(SwitchData))
        if SwitchData=='00' or SwitchData=='10' or SwitchData=='20':
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchData]" + str(SwitchData))
        else:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchData Not Allow]")

        # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        if SwitchData=='10':
            sqlUpdateSwitch = "UPDATE " + ConstTableName.SwitchTable + " SET " \
                                                                                              "result = %s" \
                                                                                              ",process_start_date=NOW() " \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  type='00'"

            print("[", str(datetime.datetime.now()), "]", sqlUpdateSwitch, SwitchData)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[sqlUpdateSwitch]" + str(sqlUpdateSwitch))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchData]" + str(SwitchData))

        else:
            sqlUpdateSwitch = "UPDATE " + ConstTableName.SwitchTable + " SET " \
                                                                                              "result = %s" \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  type='00'"
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[sqlUpdateSwitch]" + str(sqlUpdateSwitch))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchData]" + str(SwitchData))

        cursorStockFriends.execute(sqlUpdateSwitch, SwitchData)

    except Exception as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchTable Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[sqlUpdateSwitch]" + str(sqlUpdateSwitch))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e]" + str(e))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[ERROR]========================================================")
        ResStockFriendsConnection.rollback()
        return False

    else:
        ResStockFriendsConnection.commit()
        return True





# type 00:네이버 부동산 물건 수집 , 10:네이버 물건 테이블 분산 , 20 : 법원경매 물건 수집
# startFlag 가 있으면 start_date 기록 1: start_data 기록 , 그외 기록하지 않음
# dictUpdateData = 업데이트 내용
def SwitchResultSelectV2(strType):

    try:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchData]" + str(strType))
        # DB 연결
        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)


        #type 가  str 이 아니면 오류 방지
        if type(strType) is not str:
            strType = str(strType)

        # 스위치 데이터 조회 (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        qrySelectNaverMobileMaster = "SELECT * FROM " + ConstTableName.SwitchTable + "  WHERE switch_type=%s"

        cursorStockFriends.execute(qrySelectNaverMobileMaster, strType)
        results = cursorStockFriends.fetchone()

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[results]" + str(results))

        return results

    except Exception as e:

        print("SwitchResultSelectV2 Error Exception")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchResultSelectV2 Error Exception]" + str(e))
        err_msg = str(traceback.format_exc())
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))
        return False

    finally:
        # DB Close
        ResStockFriendsConnection.close()



# type 00:네이버 부동산 물건 수집 , 10:네이버 물건 테이블 분산 , 20 : 법원경매 물건 수집
# startFlag 가 있으면 start_date 기록 True: start_data 기록 , 그외 기록하지 않음
# dictUpdateData = 업데이트 내용
def SwitchResultUpdateV2(strType,startFlag,dictUpdateData=dict()):

    try:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchData]" + str(strType))
        #startFlag 가  str 이 아니면 오류 방지
        if type(strType) is not str:
            strType = str(strType)


        #startFlag 가  str 이 아니면 오류 방지
        if type(startFlag) is not bool:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[startFlag Dict Is Wrong]" + str(strType))

        if type(dictUpdateData) is not dict:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[startFlag Dict Is Wrong]" + str(strType))

        # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        sqlUpdateSwitch = "UPDATE " + ConstTableName.SwitchTable + " SET "

        if startFlag==True:
            sqlUpdateSwitch = sqlUpdateSwitch + ' process_start_date=NOW() , '

        for strKey, strUpdateData in dictUpdateData.items():

            if type(strUpdateData) is not str:
                strUpdateData = str(strUpdateData)

            sqlUpdateSwitch = sqlUpdateSwitch + strKey + "='"+strUpdateData+"', "

        sqlUpdateSwitch = sqlUpdateSwitch + ' last_date=NOW() '
        sqlUpdateSwitch = sqlUpdateSwitch + " WHERE  switch_type=%s"

        cursorStockFriends.execute(sqlUpdateSwitch , strType)


    except Exception as e:

        print("SwitchResultSelectV2 Error Exception")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SwitchResultSelectV2 Error Exception]" + str(e))
        err_msg = str(traceback.format_exc())
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))
        ResStockFriendsConnection.rollback()
        return False
    else:
        ResStockFriendsConnection.commit()
        return True
    finally:
        ResStockFriendsConnection.close()
