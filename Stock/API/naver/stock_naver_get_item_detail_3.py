# 금융위원회_주식시세정보
# 2024-11-25 커밋
#https://www.data.go.kr/iim/api/selectAPIAcountView.do
#https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo

import sys ,os
import time

sys.path.append("D:/PythonProjects/airstock")

import urllib.request, requests
import json, re
import pymysql
import traceback
import xml, html
import xml.etree.ElementTree as ET

from Realty.Government.Init import init_conf
from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName

from Stock.CONFIG import ConstTableName
from Stock.LIB.SeleniumModule.Windows import Chrome, Firefox
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup
from Lib.CustomException.QuitException import QuitException

def main():

    from Stock.LIB.Functions.Switch import StockSwitchTable
    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF

    try:

        dtNow = DateTime.today()

        strBaseYYYY = str(dtNow.year).zfill(4)
        strBaseMM = str(dtNow.month).zfill(2)
        strBaseDD = str(dtNow.day).zfill(2)
        strBaseHH = str(dtNow.hour).zfill(2)
        strBaseII = str(dtNow.minute).zfill(2)
        strBaseSS = str(dtNow.second).zfill(2)

        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

        # 초기값
        strProcessType = '010103'
        strDBSequence = '0'
        strDBSectorsName = '00'
        intItemLoop = 0
        listUpdateColumn=''
        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = 'Stock/'+strAddLogPath+'/'+ strProcessType

        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[CRONTAB START : =====================================]")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = StockSwitchTable.SwitchResultSelectV2(logging, strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        if strResult == '10':
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        if strResult == '20':
            strDBSequence = str(rstResult.get('data_2'))

        if strResult == '40':
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'a', dictSwitchData)

        # DB 연결
        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        sqlSelectItems = " SELECT * FROM " +ConstTableName.NaverStockItemTable
        sqlSelectItems += " WHERE seq > %s "
        # sqlSelectItems += " AND item_code = '138580' "
        sqlSelectItems += " AND state = '00' "
        sqlSelectItems += " ORDER BY seq ASC "
        # sqlSelectItems += " LIMIT 1 "

        cursorStockFriends.execute(sqlSelectItems, (strDBSequence))

        intAffectedCount = cursorStockFriends.rowcount
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                intAffectedCount) + "]")

        if intAffectedCount > 0:
            rstSelectDatas = cursorStockFriends.fetchall()
            if rstSelectDatas == None:
                raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

            # 파이어폭스 셀리니움 드라이버
            driver = Firefox.defFireBoxDrive()
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstSelectDatas >> " + str(rstSelectDatas))

            for rstSelectData in rstSelectDatas:
                strDBSequence = str(rstSelectData.get('seq'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " inDBSequence=> ["+str(intItemLoop)+"][" + str(strDBSequence) + "]")

                strDBSectorsName = rstSelectData.get('item_name')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSectorsName=> ["+str(intItemLoop)+"][" + str(strDBSectorsName) + "]")

                strDBItemCode = rstSelectData.get('item_code')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(intItemLoop) + "][" + str(strDBItemCode) + "]")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstSelectData=> [" + str(rstSelectData) + "][" + str(rstSelectData) + "]")

                RealtyCallUrl = 'https://finance.naver.com/item/main.naver?code='+strDBItemCode
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[RealtyCallUrl]" + str(RealtyCallUrl))

                strDateYYYYMMDD = str(dtNow.year).zfill(4) + str(dtNow.month).zfill(2) + str(dtNow.day).zfill(2)
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateYYYYMMDD]" + str(strDateYYYYMMDD))

                strDateHHIISS = str(dtNow.hour).zfill(2) + str(dtNow.minute).zfill(2) + str(dtNow.second).zfill(2)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateHHIISS]" + str(strDateHHIISS))

                #[1.크롤링]###################################################################################
                strResult = driver.get(RealtyCallUrl)  # 크롤링할 사이트 호출
                htmlSource = driver.page_source  # page_source 얻기

                soup = BeautifulSoup(htmlSource, "html.parser")  # get html
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[soup][" + str(soup)+"]")
                # [2. 코스피 / 코스닥 / 코넥스 조회 ]###################################################################################
                rstMatgetImageElement = soup.select_one('#middle > div.h_company > div.wrap_company > div > img')
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstMatgetImageElement][" + str(rstMatgetImageElement)+"]")

                if rstMatgetImageElement == None:
                    rstMatgetImage= soup.select_one('body > div')

                    sqlUpdateStopItems = " UPDATE " + ConstTableName.NaverStockItemTable + " SET "
                    sqlUpdateStopItems += " state = '99' "
                    sqlUpdateStopItems += " , stop_date = '"+strBaseYYYY +"-"+ strBaseMM +"-"+ strBaseDD+"' "
                    sqlUpdateStopItems += " WHERE seq = %s "
                    cursorStockFriends.execute(sqlUpdateStopItems, (strDBSequence))
                    ResStockFriendsConnection.commit()
                    intItemLoop += 1
                    break

                strMarketName = rstMatgetImageElement.get("alt")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strMarketName][" + str(
                    strMarketName) + "]")
                strMarketCode = 'ETC'
                if strMarketName == '코스닥':
                    strMarketCode = 'KOSDAQ'
                elif strMarketName == '코넥스':
                    strMarketCode = 'KONEX'
                elif strMarketName == '코스피':
                    strMarketCode = 'KOSPI'

                # [3. 마켓정보 조회 ]###################################################################################


                rstMainElement = soup.select_one('div > #tab_con1')
                # logging.info(
                #     SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstContentElement][" + str(rstMainElement) + "]")
                #
                rstMainTBodyElements = rstMainElement.select("tbody")
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstMainTBodyElements][" + str(intItemLoop) + "][" + str(rstMainTBodyElements) + "]")

                dictDetailDatas = dict()
                for rstMainTBodyElement in rstMainTBodyElements:
                    # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstMainTBodyElement][" + str(
                    #     intItemLoop) + "][" + str(rstMainTBodyElement) + "]")

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstMainTBodyElement][==========================================]")

                    rstMainTBodyTrElements = rstMainTBodyElement.select("tr")
                    for rstMainTBodyTrElement in rstMainTBodyTrElements:
                        # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstMainTBodyTrElement][" + str(rstMainTBodyTrElement) + "]")
                        strTitles = rstMainTBodyTrElement.select_one('th').getText().strip().replace(" ", "")
                        # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTitle][" + str(
                        #     strTitles) + "]")

                        strTdContents = rstMainTBodyTrElement.select_one('td').getText().strip()
                        # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTdContents][" + str(
                        #     strTdContents) + "]")

                        listTitles = strTitles.split("l")
                        # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[listTitles][" + str(
                        #     listTitles) + "]")

                        listContents = strTdContents.split("l")
                        # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[listContents][" + str(
                        #     listContents) + "]")

                        dictDataList = dict(zip(listTitles, listContents))
                        # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dictDataList][" + str(
                        #         dictDataList) + "]")

                        for strDataKey, strDataValue in dictDataList.items():
                            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dictDataList][" + str(
                            #     strDataKey) + "][" + str(strDataValue) + "]")

                            if strDataKey == '시가총액':
                                dictDetailDatas['market_cap'] = re.sub('[^0-9]', '', str(strDataValue))+"00000000"
                            if strDataKey == '상장주식수':
                                dictDetailDatas['market_volume'] = re.sub('[^0-9]', '', str(strDataValue)).zfill(1)
                            if strDataKey == '액면가':
                                dictDetailDatas['unit_price'] = re.sub('[^0-9]', '', str(strDataValue)).zfill(1)
                            if strDataKey == 'PER':
                                dictDetailDatas['PER'] = re.sub('[^0-9.]', '', str(strDataValue)).zfill(1)
                            if str(strDataKey).startswith('EPS(20') == True:
                                dictDetailDatas['EPS'] = re.sub('[^0-9]', '', str(strDataValue)).zfill(1)
                            if strDataKey == 'PBR':
                                dictDetailDatas['PBR'] = re.sub('[^0-9.]', '', str(strDataValue)).zfill(1)
                            if str(strDataKey).startswith('외국인소진율') == True:
                                dictDetailDatas['foreign_rate'] = re.sub('[^0-9.]', '', str(strDataValue)).zfill(1)
                            if str(strDataKey).startswith('배당수익률') == True:
                                dictDetailDatas['div_yield'] = re.sub('[^0-9.]', '', str(strDataValue)).zfill(1)

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dictDetailDatas][" + str(
                        dictDetailDatas) + "]")

                    if 'market_cap' not in dictDetailDatas:
                        dictDetailDatas['market_cap'] = 0
                    if 'market_volume' not in dictDetailDatas:
                        dictDetailDatas['market_volume'] = 0
                    if 'unit_price' not in dictDetailDatas:
                        dictDetailDatas['unit_price'] = 0
                    if 'PER' not in dictDetailDatas:
                        dictDetailDatas['PER'] = 0
                    if 'EPS' not in dictDetailDatas:
                        dictDetailDatas['EPS'] = 0
                    if 'PBR' not in dictDetailDatas:
                        dictDetailDatas['PBR'] = 0
                    if 'div_yield' not in dictDetailDatas:
                        dictDetailDatas['div_yield'] = 0
                    if 'foreign_rate' not in dictDetailDatas:
                        dictDetailDatas['foreign_rate'] = 0

                listUpdateColumn = [
                    strMarketCode,
                    dictDetailDatas['market_cap'],
                    dictDetailDatas['market_volume'],
                    dictDetailDatas['unit_price'],
                    dictDetailDatas['PER'],
                    dictDetailDatas['EPS'],
                    dictDetailDatas['PBR'],
                    dictDetailDatas['div_yield'],
                    dictDetailDatas['foreign_rate'],
                    strDBSequence
                ]
                sqlUpdateItems = " UPDATE " + ConstTableName.NaverStockItemTable + " SET "
                sqlUpdateItems += " market_code = %s "
                sqlUpdateItems += " , market_cap = %s "
                sqlUpdateItems += " , market_volume = %s "
                sqlUpdateItems += " , unit_price = %s "
                sqlUpdateItems += " , PER = %s "
                sqlUpdateItems += " , EPS = %s "
                sqlUpdateItems += " , PBR = %s "
                sqlUpdateItems += " , div_yield = %s "
                sqlUpdateItems += " , foreign_rate = %s "
                sqlUpdateItems += " , processing_state = '01' "
                sqlUpdateItems += " , modify_date = NOW() "
                sqlUpdateItems += " WHERE seq = %s "

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[sqlUpdateItems][" + str(
                    sqlUpdateItems) + "]")

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[listUpdateColumn][" + str(
                    listUpdateColumn) + "]")

                cursorStockFriends.execute(sqlUpdateItems, (listUpdateColumn))
                ResStockFriendsConnection.commit()
                intItemLoop+= 1

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = strNowDate
                dictSwitchData['data_2'] = strDBSequence
                dictSwitchData['data_3'] = strDBSectorsName
                dictSwitchData['data_4'] = intItemLoop
                StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'b', dictSwitchData)
                time.sleep(5)

            driver.quit()  # 크롬 브라우저 닫기
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[listUpdateColumn][" + str(
                listUpdateColumn) + "]")


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
        dictSwitchData['data_2'] = strDBSequence
        dictSwitchData['data_3'] = strDBSectorsName
        dictSwitchData['data_4'] = intItemLoop
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)
        # driver.quit()  # 크롬 브라우저 닫기
    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())  + "[ELSE]========================================================")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]========================================================")


if __name__ == '__main__':
    main()