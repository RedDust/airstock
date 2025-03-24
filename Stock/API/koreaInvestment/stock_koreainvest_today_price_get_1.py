# 금융위원회_주식시세정보
# 2024-11-25 커밋
#https://www.data.go.kr/iim/api/selectAPIAcountView.do
#https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo

import sys ,os
import time

sys.path.append("D:/PythonProjects/airstock")


import urllib.request, requests
import json
import pymysql
import traceback
import xml
import xml.etree.ElementTree as ET
import pandas as pd

from Realty.Government.Init import init_conf
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName

from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Lib.CustomException.QuitException import QuitException

def main():

    from Stock.LIB.Functions.Switch import StockSwitchTable
    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
    from Stock.LIB.Functions.Table import ManufactureItemTable as MIT
    import Stock.API.koreaInvestment.Lib.kis_auth as ka
    import Stock.API.koreaInvestment.Lib.kis_domstk as kb

    # 토큰 발급
    ka.auth()

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
        strNowHHMM = strBaseHH + strBaseII

        # strNowDate = '20241227'

        # 초기값
        strProcessType = '020103'
        strDBSequence = '0'
        strDBSectorsName = ''
        intItemLoop = 0


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

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        sqlSelectNaverItemLists = " SELECT * FROM " +ConstTableName.NaverStockItemTable +" WHERE state != '99' "
        sqlSelectNaverItemLists += " AND seq > %s "
        sqlSelectNaverItemLists += " AND market_code in ('KOSDAQ' , 'KOSPI', 'ETC') "
        sqlSelectNaverItemLists += " AND sectors_code not in ( '277' , '280' , '25' ) "
        sqlSelectNaverItemLists += " ORDER BY seq ASC "
        # sqlSelectNaverItemLists += " LIMIT 1 "

        cursorStockFriends.execute(sqlSelectNaverItemLists, (strDBSequence))

        intAffectedCount = cursorStockFriends.rowcount
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                intAffectedCount) + "]")

        if intAffectedCount > 0:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                intAffectedCount) + "]")

            rstSelectDatas = cursorStockFriends.fetchall()
            if rstSelectDatas == None:
                raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstSelectDatas >> " + str(rstSelectDatas))
            for rstSelectData in rstSelectDatas:
                strDBSequence = str(int(rstSelectData.get('seq')))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSequence=> ["+str(intItemLoop)+"][" + str(strDBSequence) + "]")

                strDBSectorsName = rstSelectData.get('item_name')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSectorsName=> ["+str(intItemLoop)+"][" + str(strDBSectorsName) + "]")

                strDBItemCode = rstSelectData.get('item_code')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(intItemLoop) + "][" + str(strDBItemCode) + "]")

                strDBCountryCode = str(rstSelectData.get('country_code'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBCountryCode=> [" + str(intItemLoop) + "][" + str(strDBCountryCode) + "]")

                strDBUnitPrice = str(int(rstSelectData.get('unit_price')))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBUnitPrice=> [" + str(intItemLoop) + "][" + str(strDBUnitPrice) + "]")

                strDBMultiplication = str(int(rstSelectData.get('multiplication')))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBMultiplication=> [" + str(intItemLoop) + "][" + str(strDBMultiplication) + "]")

                strMarketCap = str(int(rstSelectData.get('market_cap')))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strMarketCap=> [" + str(intItemLoop) + "][" + str(strMarketCap) + "]")

                strMarketVolume = str(int(rstSelectData.get('market_volume')))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strMarketVolume=> [" + str(intItemLoop) + "][" + str(strMarketVolume) + "]")

                strDBState = str(rstSelectData.get('state'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBState=> [" + str(intItemLoop) + "][" + str(strDBState) + "]")

                strItemTableName = MIT.GetItemTableName(strDBItemCode, strDBCountryCode)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemTableName=> [" + str(len(
                    strItemTableName)) + "][" + str(
                    strItemTableName) + "]")

                if len(strItemTableName) < 1:
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                # 테이블 없으면 제작
                bCreateResult = MIT.CheckExistItemTable(strItemTableName, ResStockFriendsConnection)
                if bCreateResult != True:
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                # [국내주식] 기본시세 > 주식현재가 일자별 (종목번호 6자리 + 기간분류코드)
                # 기간분류코드 	D : (일)최근 30거래일  W : (주)최근 30주   M : (월)최근 30개월
                # 수정주가기준이며 수정주가미반영 기준을 원하시면 인자값 adj_prc_code="2" 추가
                # rt_data = kb.get_inquire_daily_price(itm_no=strDBItemCode, period_code="D")

                # [국내주식] 기본시세 > 주식현재가 시세 (종목번호 6자리)
                rt_data = kb.get_inquire_price(itm_no=strDBItemCode)
                # print(rt_data)  # 현재가, 전일대비

                dictRtDatas = pd.DataFrame(rt_data)
                dictRootValues = dictRtDatas.to_dict()

                print("dictRootValues => " , dictRootValues)

                dictKoreaInvestDatas = dict()


                for dictkey, dictValue in dictRootValues.items():
                    dictKoreaInvestDatas[dictkey] = dictValue[0]

                print("dictKoreaInvestDatas => ", dictKoreaInvestDatas)


                strStartPrice = str(dictKoreaInvestDatas['stck_oprc'])  # 주식 시가
                strMaxPrice = str(dictKoreaInvestDatas['stck_hgpr'])    # 주식 최고가
                strMinPrice = str(dictKoreaInvestDatas['stck_lwpr'])    # 주식 최저가
                strNowPrice = str(dictKoreaInvestDatas['stck_prpr'])    # 주식 하한가
                strTradeAmount = str(dictKoreaInvestDatas['acml_tr_pbmn']) #누적 거래대금
                strTradeVolume = str(dictKoreaInvestDatas['acml_vol']) #누적 거래량
                strChangePrice = str(dictKoreaInvestDatas['prdy_vrss'])  # 변동 금액
                strChangeRate = str(dictKoreaInvestDatas['prdy_ctrt'])  # 변동 금액

                strChangeFlag = 'EVEN'
                if int(strChangePrice) < 0:
                    strChangeFlag = 'FALL'
                elif int(strChangePrice) > 0:
                    strChangeFlag = 'RISE'


                listAverage = []
                listAverage.append('5')
                listAverage.append('10')
                listAverage.append('20')
                listAverage.append('30')
                listAverage.append('60')
                listAverage.append('120')

                dictAverage = dict()
                for strAverage in listAverage:
                    dictAverage[strAverage] = dict()
                    intAverage = int(strAverage)
                    intCalAverage = intAverage - 1
                    strCalAverage = str(intCalAverage)

                    sqlSelectAverage = " SELECT round(avg(now_price)) as avg_price , "
                    sqlSelectAverage += " MAX(now_price) as max_price , "
                    sqlSelectAverage += " MIN(now_price) as min_price , "
                    sqlSelectAverage += " round(avg(trade_volume)) as avg_volume , "
                    sqlSelectAverage += " MAX(trade_volume) as max_volume , "
                    sqlSelectAverage += " MIN(trade_volume) as min_volume , "
                    sqlSelectAverage += " count(*) as cnt  "
                    sqlSelectAverage += " FROM (select now_price , trade_volume from " + strItemTableName + " WHERE state='00' "
                    sqlSelectAverage += " AND YYYYMMDD < %s "
                    sqlSelectAverage += " ORDER BY YYYYMMDD DESC LIMIT " + strCalAverage + " ) as sub "

                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " sqlSelectAverage=> [" + str(
                            strDBSectorsName) + "][" + str(sqlSelectAverage) + "][" + str(strNowDate) + "]")

                    cursorStockFriends.execute(sqlSelectAverage, (strNowDate))
                    rstSelectAverage = cursorStockFriends.fetchone()
                    strDBCount = rstSelectAverage.get('cnt')
                    intDBCount = int(strDBCount)
                    if intDBCount != intCalAverage:
                        continue

                    intDBAvgPrice  = int(rstSelectAverage.get('avg_price'))
                    temp1 = float(intDBAvgPrice * intCalAverage)
                    temp2 = float(temp1 + int(strNowPrice))
                    intDBAvgPrice = int(round(temp2 / (intAverage)))

                    intDBMaxPrice = round(rstSelectAverage.get('max_price'))
                    temp1 = float(intDBMaxPrice * intCalAverage)
                    temp2 = float(temp1 + int(strNowPrice))
                    intDBMaxPrice = int(round(temp2 / (intAverage)))

                    intDBMinPrice = round(rstSelectAverage.get('min_price'))
                    temp1 = float(intDBMinPrice * intCalAverage)
                    temp2 = float(temp1 + int(strNowPrice))
                    intDBMinPrice = int(round(temp2 / (intAverage)))


                    intDBAvgVolume = round(rstSelectAverage.get('avg_volume'))
                    temp1 = float(intDBAvgVolume * intCalAverage)
                    temp2 = float(temp1 + int(strNowPrice))
                    intDBAvgVolume = int(round(temp2 / (intAverage)))

                    intDBMaxVolume = round(rstSelectAverage.get('max_volume'))
                    temp1 = float(intDBMaxVolume * intCalAverage)
                    temp2 = float(temp1 + int(strNowPrice))
                    intDBMaxVolume = int(round(temp2 / (intAverage)))

                    intDBMinVolume = round(rstSelectAverage.get('min_volume'))
                    temp1 = float(intDBMinVolume * intCalAverage)
                    temp2 = float(temp1 + int(strNowPrice))
                    intDBMinVolume = int(round(temp2 / (intAverage)))

                    dictAverage[strAverage]['avg_price'] = str(intDBAvgPrice)
                    dictAverage[strAverage]['max_price'] = str(intDBMaxPrice)
                    dictAverage[strAverage]['min_price'] = str(intDBMinPrice)
                    dictAverage[strAverage]['avg_volume'] = str(intDBAvgVolume)
                    dictAverage[strAverage]['max_volume'] = str(intDBMaxVolume)
                    dictAverage[strAverage]['min_volume'] = str(intDBMinVolume)

                print("dictAverage", type(dictAverage), dictAverage)
                strJsonAverage = json.dumps(dictAverage)


                sqlSelectNaverItems = " SELECT * FROM " + strItemTableName + "  "
                sqlSelectNaverItems += " WHERE  YYYYMMDD = %s "
                sqlSelectNaverItems += " AND  HHII = '1630' "
                cursorStockFriends.execute(sqlSelectNaverItems, (strNowDate))
                intSelectedCount = cursorStockFriends.rowcount

                if intSelectedCount < 1:
                    print("INSERT")
                    dictInsertItemDatas = dict()
                    dictInsertItemDatas['YYYYMMDD'] = strNowDate
                    dictInsertItemDatas['daily_seq'] = '0'
                    dictInsertItemDatas['unit_price'] = strDBUnitPrice
                    dictInsertItemDatas['multiplication'] = strDBMultiplication
                    dictInsertItemDatas['start_price'] = strStartPrice
                    dictInsertItemDatas['now_price'] = strNowPrice
                    dictInsertItemDatas['max_price'] = strMaxPrice
                    dictInsertItemDatas['min_price'] = strMinPrice
                    dictInsertItemDatas['trade_volume'] = strTradeVolume
                    dictInsertItemDatas['trade_amount'] = strTradeAmount
                    dictInsertItemDatas['total_volume'] = strMarketVolume
                    dictInsertItemDatas['total_amount'] = strMarketCap
                    dictInsertItemDatas['change_flag'] = strChangeFlag
                    dictInsertItemDatas['change_price'] = strChangePrice
                    dictInsertItemDatas['change_rate'] = strChangeRate
                    dictInsertItemDatas['average_info'] = strJsonAverage
                    dictInsertItemDatas['investment_flag'] = '001'
                    dictInsertItemDatas['state'] = strDBState

                    listFieldValues = list()
                    sqlInsertItemTable = " INSERT INTO " + strItemTableName + " SET  "
                    sqlInsertItemTable += " HHII = '1630' "

                    for dictInsertItemKey, dictInsertItemValue in dictInsertItemDatas.items():
                        sqlInsertItemTable += ", " + dictInsertItemKey + " = %s"
                        listFieldValues.append(dictInsertItemValue)

                    print("sqlInsertItemTable ==> " , sqlInsertItemTable )
                    print("listFieldValues ==> ", listFieldValues)

                    cursorStockFriends.execute(sqlInsertItemTable, listFieldValues)

                else:
                    print("UPDATE")
                    rstSelectItems = cursorStockFriends.fetchone()
                    strItemSecquence = str(rstSelectItems.get('seq'))

                    dictUpdateItemDatas = dict()
                    dictUpdateItemDatas['unit_price'] = strDBUnitPrice
                    dictUpdateItemDatas['multiplication'] = strDBMultiplication
                    dictUpdateItemDatas['start_price'] = strStartPrice
                    dictUpdateItemDatas['now_price'] = strNowPrice
                    dictUpdateItemDatas['max_price'] = strMaxPrice
                    dictUpdateItemDatas['min_price'] = strMinPrice
                    dictUpdateItemDatas['trade_volume'] = strTradeVolume
                    dictUpdateItemDatas['trade_amount'] = strTradeAmount
                    dictUpdateItemDatas['total_volume'] = strMarketVolume
                    dictUpdateItemDatas['total_amount'] = strMarketCap
                    dictUpdateItemDatas['change_flag'] = strChangeFlag
                    dictUpdateItemDatas['change_price'] = strChangePrice
                    dictUpdateItemDatas['change_rate'] = strChangeRate
                    dictUpdateItemDatas['average_info'] = strJsonAverage
                    dictUpdateItemDatas['state'] = strDBState

                    listFieldValues = list()
                    sqlUpdateItem = " UPDATE " + strItemTableName + " SET modify_date = NOW()  "

                    for dictUpdateItemKey, dictUpdateItemValue in dictUpdateItemDatas.items():
                        sqlUpdateItem += ", " + dictUpdateItemKey + " = %s"
                        listFieldValues.append(dictUpdateItemValue)
                    sqlUpdateItem += " WHERE seq = %s"

                    print("sqlUpdateItem ==> " , sqlUpdateItem )
                    print("listFieldValues ==> ", listFieldValues)

                    listFieldValues.append(strItemSecquence)
                    cursorStockFriends.execute(sqlUpdateItem, listFieldValues)
                print("ResStockFriendsConnection.commit() " )

                sqlUpdateNaverItemLists = "UPDATE " + ConstTableName.NaverStockItemTable + " SET average_info = %s "
                sqlUpdateNaverItemLists += " WHERE seq = %s "

                print("sqlUpdateNaverItemLists ==> ", sqlUpdateNaverItemLists)
                print("strJsonAverage ==> ", strJsonAverage)
                print("strDBSequence ==> ", strDBSequence)

                cursorStockFriends.execute(sqlUpdateNaverItemLists, (strJsonAverage , strDBSequence))
                ResStockFriendsConnection.commit()


                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = strNowDate
                dictSwitchData['data_2'] = strDBSequence
                dictSwitchData['data_3'] = strDBSectorsName
                dictSwitchData['data_4'] = intItemLoop
                StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'b', dictSwitchData)
                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "============================time.sleep(1) ")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " ============================time.sleep(1)")

                if (intItemLoop % 50) == 19:
                    time.sleep(1)

                intItemLoop += 1


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

    else:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[ELSE]========================================================")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]========================================================")


if __name__ == '__main__':
    main()