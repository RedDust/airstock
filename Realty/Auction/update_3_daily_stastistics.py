import requests


# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import logging
import logging.handlers
import inspect
import traceback
import re

from datetime import datetime as DateTime, date as DateTimeDate , timedelta as TimeDelta
sys.path.append("D:/PythonProjects/airstock")

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Realty.Auction.Const import ConstRealEstateTable_AUC
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Realty.Auction.Const import AuctionCourtInfo
from Init.DefConstant import ConstRealEstateTable
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.CryptoModule import AesCrypto
from Realty.Auction.AuctionLib import MakeAuctionUniqueKey
from Lib.CustomException.QuitException import QuitException



def main():

    try:
        print(GetLogDef.lineno(__file__), "============================================================")
        print(GetLogDef.lineno(__file__), "법원경매 통계 데이터 작성")

        strProcessType = '020020'
        KuIndex = DBKuIndex = '00'
        CityKey = DBCityKey = '00'
        targetRow = '00'
        strAuctionUniqueNumber = '00'
        strAuctionSeq   =   '0'
        jsonIssueNumber = '0'

        dtNow = DateTime.today()
        # 전날 통계 데이터 오류인경우
        # dtNow = dtNow - TimeDelta(days=1)

        print(dtNow.year, type(dtNow.year))
        print(dtNow.month, type(dtNow.month))
        print(dtNow.day, type(dtNow.day))
        print(dtNow, type(dtNow))

        logFileName = str(dtNow.year) + str(dtNow.month) + str(dtNow.day).zfill(2) + ".log"

        dBaseIssueDatetime = DateTimeDate(int(dtNow.year), int(dtNow.month), int(dtNow.day))
        strBaseYYYY = str(dBaseIssueDatetime.year).zfill(4)
        strBaseMM = str(dBaseIssueDatetime.month).zfill(2)
        strBaseDD = str(dBaseIssueDatetime.day).zfill(2)
        strBaseYYYYWEEK = str(dBaseIssueDatetime.isocalendar().year) + str(dBaseIssueDatetime.isocalendar().week).zfill(2)
        strBaseYYYYWEEKDAY = dBaseIssueDatetime.isocalendar().weekday



        strBaseYYYYMM = strBaseYYYY + strBaseMM
        strBaseYYYYMMDD = strBaseYYYYMM + strBaseDD
        strBaseYYYYMMDDHHIISS = strBaseYYYYMMDD

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')

        streamingHandler = logging.StreamHandler()
        streamingHandler.setFormatter(formatter)

        # RotatingFileHandler
        log_max_size = 10 * 1024 * 1024  ## 10MB
        log_file_count = 20

        # RotatingFileHandler
        timeFileHandler = logging.handlers.TimedRotatingFileHandler(
            filename='D:/PythonProjects/airstock/Shell/logs/'+strProcessType+ '_get_auction_' + logFileName,
            when='midnight',
            interval=1,
            encoding='utf-8'
        )
        timeFileHandler.setFormatter(formatter)
        logger.addHandler(streamingHandler)
        logger.addHandler(timeFileHandler)


        # 스위치 데이터 조회 type(000200) result (10:정기점검)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2('000200')
        strResult = rstResult.get('result')
        if strResult is False:
            quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴

        if strResult == '10':
            process_start_date = rstResult.get('process_start_date').strftime('%Y-%m-%d %H:%M:%S')
            last_date = rstResult.get('last_date').strftime('%Y-%m-%d %H:%M:%S')
            dtRegNow = DateTime.today()
            process_start_date_obj = DateTime.strptime(process_start_date, '%Y-%m-%d %H:%M:%S')
            last_date_obj = DateTime.strptime(last_date, '%Y-%m-%d %H:%M:%S')

            if (process_start_date_obj <= dtRegNow) and (dtRegNow <= last_date_obj):
                print("process_start_date >> ", process_start_date)
                print("dtRegNow >> ", dtRegNow)
                print("last_date >> ", last_date)
                quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')

        if strResult is False:
            raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                          inspect.getframeinfo(inspect.currentframe()).lineno),
                        'strResult => ', strResult)  # 예외를 발생시킴

        elif strResult == '10':
            raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                              inspect.getframeinfo(inspect.currentframe()).lineno),
                            'strResult => ', strResult)  # 예외를 발생시킴

        elif strResult == '20' or strResult == '30':
            DBKuIndex = rstResult.get('data_1')
            DBCityKey = rstResult.get('data_2')
            targetRow = rstResult.get('data_3')




        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = DBKuIndex
        dictSwitchData['data_2'] = DBCityKey
        dictSwitchData['data_3'] = targetRow
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[CRONTAB START]==================================================================")


        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[ count_A_1 , count_A_1 START ]==================================================================")
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        if strResult != '00':

            if DBKuIndex == 'CourtAuctionDataTable':
                sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable + "  "
                sqlCourtAuctionSelect += "  WHERE seq > " + DBCityKey
            else:
                sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " WHERE 1 != 1  "

        else:
            sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable + "  "



        print("sqlCourtAuctionSelect > ",sqlCourtAuctionSelect)

        # sqlCourtAuctionSelect += " LIMIT 100"
        cursorRealEstate.execute(sqlCourtAuctionSelect)
        rstBackupLists = cursorRealEstate.fetchall()
        nLoopCount = 0

        KuIndex = 'CourtAuctionDataTable'

        count_B_1 = 0
        count_B_2 = 0

        sqlDeleteStatistics  = " DELETE FROM " + ConstRealEstateTable_AUC.CourtAuctionDailyStatistics
        sqlDeleteStatistics += " WHERE DATE_FORMAT(reg_date,'%Y-%m-%d') = DATE_FORMAT(NOW(),'%Y-%m-%d') "
        cursorRealEstate.execute(sqlDeleteStatistics)


        for rstBackupList in rstBackupLists:
            CityKey = iSequence = rstBackupList.get('seq')
            strAuctionUniqueNumber = rstBackupList.get('unique_value2')
            jsonIssueNumber = rstBackupList.get('issue_number_text')
            strAuctionSeq = rstBackupList.get('auction_code')
            strListState = str(rstBackupList.get('state'))


            strBuildTypeText = rstBackupList.get('build_type_text')
            strBuildTypeText = re.sub(r"[^가-힣]", "", strBuildTypeText)
            strBuildTypeTextKeyword = AuctionCourtInfo.dictBuildTypeReverseKeyWord[strBuildTypeText]

            strCourtName = rstBackupList.get('court_name')
            strCourtName = str(re.sub(r"[^가-힣]", "", strCourtName))

            strSidoCode = str(rstBackupList.get('sido_code'))
            strSiguCode = str(rstBackupList.get('sigu_code'))
            strListAuctionType = strAuctionType = str(rstBackupList.get('auction_type'))
            dtListAuctionDay = targetRow = str(rstBackupList.get('auction_day'))

            strAuctionDayformat = '%Y-%m-%d %H:%M:%S'

            print("182>>>>>" , dtListAuctionDay, type(dtListAuctionDay))

            objAuctionDay = DateTime.strptime(dtListAuctionDay, strAuctionDayformat)
            strListAuctionDayYYYY = str(objAuctionDay.year).zfill(4)
            strListAuctionDayMM = str(objAuctionDay.month).zfill(2)
            strListAuctionDayDD = str(objAuctionDay.day).zfill(2)

            strListAuctionDayYYYY = strListAuctionDayYYYY
            strListAuctionDayYYYYMM  = strListAuctionDayYYYY + strListAuctionDayMM
            strListAuctionDayYYYYMMDD = strListAuctionDayYYYYMM + strListAuctionDayDD

            strListAuctionDayYYYYWEEK = str(objAuctionDay.isocalendar().year) + str(objAuctionDay.isocalendar().week).zfill(2)
            strListAuctionDayYYYYWEEKDAY = objAuctionDay.isocalendar().weekday



            # if len(str(strBuildTypeTextKeyword)) < 2:
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "["+KuIndex+":SELECT]==================================================================")

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[YYYYMMDD : " +strBaseYYYYMMDD+ "]"+"[court_name : " +strCourtName+ "]"+"[sido_code : " +strSidoCode+ "]"+"[sigu_code : " +strSiguCode+ "]"+"[build_type : " +strBuildTypeTextKeyword+ "]")

            sqlSelectStatistics = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDailyStatistics
            sqlSelectStatistics += " WHERE YYYYMMDD = %s AND court_name = %s AND sido_code = %s"
            sqlSelectStatistics += " AND sigu_code = %s AND build_type = %s "
            cursorRealEstate.execute(sqlSelectStatistics, (strBaseYYYYMMDD, strCourtName, strSidoCode, strSiguCode, strBuildTypeTextKeyword))
            nStatisticsCount = int(cursorRealEstate.rowcount)
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[nStatisticsCount = "+str(nStatisticsCount)+"]"+"[strAuctionType = "+str(strAuctionType)+"]")

            # 새로등록된
            if strListState == '00':
                # 진행중인 물건
                if strAuctionType == '10':
                    strFieldsName = "count_A_1"
                    strFieldBName = 'count_B_1'
                # 진행 예정 물건
                elif strAuctionType == '20':
                    strFieldsName = "count_A_2"
                    strFieldBName = 'count_B_2'
                #오류난 것들
                else:
                    strFieldsName = "count_A_5"
                    strFieldBName = 'count_B_5'

            else:
                # 이전에 등록된 진행중인 물건
                if strAuctionType == '10':
                    strFieldsName = "count_A_1"
                    strFieldBName = ''
                # 이전에 등록된 진행 예정 물건
                elif strAuctionType == '20':
                    strFieldsName = "count_A_2"
                    strFieldBName = ''
                #오류난 것들
                else:
                    strFieldsName = "count_A_5"
                    strFieldBName = ''

            #오늘 매각 진행 물건
            strFieldCName = ''

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[strListAuctionDayYYYYMMDD : " +strListAuctionDayYYYYMMDD+ "]"+"[strListAuctionDayYYYYMMDD : " +str(type(strListAuctionDayYYYYMMDD))+ "]"+"[strBaseYYYYMMDD : " +strBaseYYYYMMDD+ "]"+"[strBaseYYYYMMDD : " +str(type(strBaseYYYYMMDD))+ "]"+"[strListState : " +strListState+ "]")


            if strListAuctionDayYYYYMMDD == strBaseYYYYMMDD:
                strFieldCName = 'count_C_1'



            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[YYYYMMDD : " + strBaseYYYYMMDD + "]" + "[court_name : " + strCourtName + "]" + "[sido_code : " + strSidoCode + "]" + "[sigu_code : " + strSiguCode + "]" + "[build_type : " + strBuildTypeTextKeyword + "]")

            if nStatisticsCount < 1:
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[INSERT]==================================================================")


                sqlInsertStatistics  = " INSERT INTO " + ConstRealEstateTable_AUC.CourtAuctionDailyStatistics + " SET "
                sqlInsertStatistics += " YYYY = %s , YYYYMM = %s, YYYYMMDD = %s , YYYYWEEK = %s , YYYYWEEKDAY = %s , "
                sqlInsertStatistics += " court_name = %s, sido_code = %s, sigu_code = %s , build_type = %s, "

                if strFieldBName != '':
                    sqlInsertStatistics += strFieldBName + "  = 1 ,"

                if strFieldCName != '':
                    sqlInsertStatistics += " count_C_1 = 1 ,"

                sqlInsertStatistics += strFieldsName + " = 1"
                # cursorRealEstate.execute(sqlInsertStatistics, (strBaseYYYYMMDD, strCourtName, strSidoCode, strSiguCode, strBuildTypeTextKeyword ))
                cursorRealEstate.execute(sqlInsertStatistics, (strBaseYYYY, strBaseYYYYMM, strBaseYYYYMMDD, strBaseYYYYWEEK, strBaseYYYYWEEKDAY, strCourtName, strSidoCode, strSiguCode, strBuildTypeTextKeyword))
                intAffectedCount = cursorRealEstate.rowcount


            else:
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[UPDATE]==================================================================")

                sqlInsertStatistics  = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionDailyStatistics + " SET "

                if strFieldBName != '':
                    sqlInsertStatistics += strFieldBName + " = "+strFieldBName+" + 1 , "

                if strFieldCName != '':
                    sqlInsertStatistics += " count_C_1 = count_C_1 + 1 ,"

                sqlInsertStatistics += " "+strFieldsName+" = "+strFieldsName+" + 1"

                sqlInsertStatistics += " WHERE YYYYMMDD = %s AND court_name = %s AND sido_code = %s"
                sqlInsertStatistics += " AND sigu_code = %s AND build_type = %s "
                cursorRealEstate.execute(sqlInsertStatistics, (strBaseYYYYMMDD, strCourtName, strSidoCode, strSiguCode, strBuildTypeTextKeyword))
                intAffectedCount = cursorRealEstate.rowcount

            if intAffectedCount < 1:
                    ResRealEstateConnection.rollback()
                    raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                          inspect.getframeinfo(inspect.currentframe()).lineno),
                                        'intAffectedCount => ' + str(intAffectedCount))  # 예외를 발생시킴

            ResRealEstateConnection.commit()

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = KuIndex
            dictSwitchData['data_2'] = CityKey
            dictSwitchData['data_3'] = targetRow
            dictSwitchData['data_4'] = strAuctionUniqueNumber
            dictSwitchData['data_5'] = strAuctionSeq
            dictSwitchData['data_6'] = jsonIssueNumber
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            print(iSequence, nStatisticsCount,  nLoopCount, strBuildTypeText, ">", strBuildTypeTextKeyword , "("+str(len(strBuildTypeTextKeyword))+")")
            print(strCourtName)
            nLoopCount += 1



        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[ count_A_1 , count_A_1 END ]==================================================================")













        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[ count_A_3 , count_A_4 , , count_A_5 START ]==================================================================")

        nbaseDate = dtNow - TimeDelta(days=1)
        dtAuctionDay = str(nbaseDate.strftime("%Y-%m-%d 00:00:00"))




        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        if strResult != '00':
            if DBKuIndex == 'CourtAuctionCompleteTable':
                sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable + "  "
                sqlCourtAuctionSelect += "  WHERE seq > " + DBCityKey

            else:
                # sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable
                sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable + " WHERE auction_day >= '"+dtAuctionDay+"' "

        else:
            # sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable
            sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable + " WHERE auction_day >= '"+dtAuctionDay+"' "


        print("sqlCourtAuctionSelect > ", sqlCourtAuctionSelect)
        # sqlCourtAuctionSelect += " LIMIT 100"
        cursorRealEstate.execute(sqlCourtAuctionSelect)
        rstBackupLists = cursorRealEstate.fetchall()
        nLoopCount = 0
        KuIndex = 'CourtAuctionCompleteTable'


        for rstBackupList in rstBackupLists:
            CityKey = iSequence = rstBackupList.get('seq')
            strAuctionUniqueNumber = rstBackupList.get('unique_value2')
            jsonIssueNumber = rstBackupList.get('issue_number_text')
            strAuctionSeq = rstBackupList.get('auction_code')
            strListState = str(rstBackupList.get('state'))

            dtListAuctionDay = targetRow = str(rstBackupList.get('auction_day'))
            strAuctionDayformat = '%Y-%m-%d %H:%M:%S'
            objAuctionDay = DateTime.strptime(dtListAuctionDay, strAuctionDayformat)

            strBiddingInfo = rstBackupList.get('bidding_info')
            strCourtName = rstBackupList.get('court_name')
            strCourtName = str(re.sub(r"[^가-힣]", "", strCourtName))
            strSidoCode = str(rstBackupList.get('sido_code'))
            strSiguCode = str(rstBackupList.get('sigu_code'))
            strAuctionType = str(rstBackupList.get('auction_type'))

            strBiddingInfoText = rstBackupList.get('bidding_info')
            strBiddingInfoText = re.sub(r"[^가-힣]", "", strBiddingInfoText)

            strBuildTypeText = rstBackupList.get('build_type_text')
            strBuildTypeText = re.sub(r"[^가-힣]", "", strBuildTypeText)
            strBuildTypeTextKeyword = AuctionCourtInfo.dictBuildTypeReverseKeyWord[strBuildTypeText]

            strListAuctionDayYYYY = str(objAuctionDay.year).zfill(4)
            strListAuctionDayMM = str(objAuctionDay.month).zfill(2)
            strListAuctionDayDD = str(objAuctionDay.day).zfill(2)

            strListAuctionDayYYYYMM  = strListAuctionDayYYYY + strListAuctionDayMM
            strListAuctionDayYYYYMMDD = strListAuctionDayYYYYMM + strListAuctionDayDD

            strListAuctionDayYYYYWEEK = str(objAuctionDay.isocalendar().year) + str(objAuctionDay.isocalendar().week).zfill(2)
            strListAuctionDayYYYYWEEKDAY = objAuctionDay.isocalendar().weekday


            # 새로등록된
            if strListState == '00':
                # 진행중인 물건
                if strBiddingInfoText == '유찰':
                    strFieldsName = "count_A_3"
                    strFieldBName = 'count_B_3'
                # 진행 예정 물건
                elif strBiddingInfoText == '매각':
                    strFieldsName = "count_A_4"
                    strFieldBName = 'count_B_4'
                #오류난 것들
                else:
                    strFieldsName = "count_A_5"
                    strFieldBName = 'count_B_5'
            else:
                if strBiddingInfoText == '유찰':
                    strFieldsName = "count_A_3"
                    strFieldBName = ''
                elif strBiddingInfoText == '매각':
                    strFieldsName = "count_A_4"
                    strFieldBName = ''
                else:
                    strFieldsName = "count_A_5"
                    strFieldBName = ''


            # if len(str(strBuildTypeTextKeyword)) < 2:
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "["+KuIndex+":SELECT]==================================================================")

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[YYYYMMDD : " +strListAuctionDayYYYYMMDD+ "]"+"[court_name : " +strCourtName+ "]"+"[sido_code : " +strSidoCode+ "]"+"[sigu_code : " +strSiguCode+ "]"+"[build_type : " +strBuildTypeTextKeyword+ "]")

            sqlSelectStatistics = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDailyStatistics
            sqlSelectStatistics += " WHERE YYYYMMDD = %s AND court_name = %s AND sido_code = %s"
            sqlSelectStatistics += " AND sigu_code = %s AND build_type = %s "

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "sqlSelectStatistics > " + sqlSelectStatistics)

            cursorRealEstate.execute(sqlSelectStatistics, (strListAuctionDayYYYYMMDD, strCourtName, strSidoCode, strSiguCode, strBuildTypeTextKeyword))
            nStatisticsCount = int(cursorRealEstate.rowcount)

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[nStatisticsCount = "+str(nStatisticsCount)+"]"+"[strAuctionType = "+str(strAuctionType)+"]")

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "strListAuctionDayYYYYMMDD > " + strListAuctionDayYYYYMMDD)
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "strCourtName > " + strCourtName)
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "strSidoCode > " + strSidoCode)
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "strSiguCode > " + strSiguCode)
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "strBuildTypeTextKeyword > " + strBuildTypeTextKeyword)

            if nStatisticsCount < 1:
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[INSERT]==================================================================")

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[YYYYMMDD : " + strListAuctionDayYYYYMMDD + "]" + "[court_name : " + strCourtName + "]" + "[sido_code : " + strSidoCode + "]" + "[sigu_code : " + strSiguCode + "]" + "[build_type : " + strBuildTypeTextKeyword + "]")


                sqlInsertStatistics  = " INSERT INTO " + ConstRealEstateTable_AUC.CourtAuctionDailyStatistics + " SET "
                sqlInsertStatistics += " YYYY = %s , YYYYMM = %s, YYYYMMDD = %s , YYYYWEEK = %s , YYYYWEEKDAY = %s , "
                sqlInsertStatistics += " court_name = %s, sido_code = %s, sigu_code = %s , build_type = %s, "

                sqlInsertStatistics += strFieldsName+" = 1"

                cursorRealEstate.execute(sqlInsertStatistics, (strListAuctionDayYYYY,strListAuctionDayYYYYMM,strListAuctionDayYYYYMMDD,strListAuctionDayYYYYWEEK,strListAuctionDayYYYYWEEKDAY, strCourtName, strSidoCode, strSiguCode, strBuildTypeTextKeyword ))
                intAffectedCount = cursorRealEstate.rowcount


            else:
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[UPDATE]==================================================================")

                sqlInsertStatistics  = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionDailyStatistics + " SET "

                sqlInsertStatistics += " "+strFieldsName+" = "+strFieldsName+" + 1"

                sqlInsertStatistics += " WHERE YYYYMMDD = %s AND court_name = %s AND sido_code = %s"
                sqlInsertStatistics += " AND sigu_code = %s AND build_type = %s "

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "sqlInsertStatistics > " + sqlInsertStatistics )

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "strListAuctionDayYYYYMMDD > " + strListAuctionDayYYYYMMDD )
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "strCourtName > " + strCourtName)
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "strSidoCode > " + strSidoCode)
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "strSiguCode > " + strSiguCode)
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "strBuildTypeTextKeyword > " + strBuildTypeTextKeyword)


                cursorRealEstate.execute(sqlInsertStatistics, (strListAuctionDayYYYYMMDD, strCourtName, strSidoCode, strSiguCode, strBuildTypeTextKeyword))
                intAffectedCount = cursorRealEstate.rowcount

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "intAffectedCount > " + str(intAffectedCount))



            if intAffectedCount < 1:
                ResRealEstateConnection.rollback()
                raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                  inspect.getframeinfo(inspect.currentframe()).lineno)+
                                'intAffectedCount => ' + str(intAffectedCount))  # 예외를 발생시킴




            print(iSequence, strCourtName,  nLoopCount, strListAuctionDayYYYYMMDD, ">", strBiddingInfoText ,"[" ,nStatisticsCount,"]")
            print(strCourtName)

            ResRealEstateConnection.commit()
            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = KuIndex
            dictSwitchData['data_2'] = CityKey
            dictSwitchData['data_3'] = targetRow
            dictSwitchData['data_4'] = strAuctionUniqueNumber
            dictSwitchData['data_5'] = strAuctionSeq
            dictSwitchData['data_6'] = jsonIssueNumber
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            nLoopCount += 1

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[ count_A_3 , count_A_4 , count_A_5 END ]==================================================================")









        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = KuIndex
        dictSwitchData['data_2'] = CityKey
        dictSwitchData['data_3'] = targetRow
        dictSwitchData['data_4'] = strAuctionUniqueNumber
        dictSwitchData['data_5'] = strAuctionSeq
        dictSwitchData['data_6'] = jsonIssueNumber
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)



    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "Error Exception")
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     str(e))
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     str(err_msg))

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "Error Exception")
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     str(e))
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     str(err_msg))

    else:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[SUCCESS END]==================================================================")

    finally:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[CRONTAB END]==================================================================")

if __name__ == '__main__':
    main()