# -*- coding: utf-8 -*-
"""
Created on Tue Feb 15 07:56:54 2022
"""
#kis_api module 을 찾을 수 없다는 에러가 나는 경우 sys.path에 kis_api.py 가 있는 폴더를 추가해준다.
import Stock.API.koreaInvestment.Lib.kis_auth as ka
import Stock.API.koreaInvestment.Lib.kis_domstk as kb

import pandas as pd

import sys
try:
    #토큰 발급
    ka.auth()


    #
    # [국내주식] 기본시세 > 주식현재가 시세 (종목번호 6자리)

    rt_data = kb.get_inquire_price(itm_no="071050")
    print(rt_data.stck_prpr+ " " + rt_data.prdy_vrss)    # 현재가, 전일대비
    print(rt_data)

    # requests 모듈 설치 필요 (pip install requests)
    import requests
    import json


    body = {
                "grant_type": "client_credentials",
                "appkey": "PSstlhMrIUX68wEhro9D0QIXwCYtTWm5SRyA",
                "appsecret": "h4fvpOjIt6Wg57EfmAxtBORHTXfvLcFmsWIEeBiIZqoAkvvjbXpphlkfQ9QN9irVvc87UDoLzpi67v75RT8yqynsRpJCtnr+WNYjb00MJk3Nq4s21VcshLu1SeiCReXAsJUen5IkK+4T72j/BkxVfuww0UYytPQ6ZBhI3Afik5YXbxd7C34="
                }



    PATH = "oauth2/tokenP"

    URL_BASE = 'https://openapi.koreainvestment.com:9443'

    URL = f"{URL_BASE}/{PATH}"

    headers = {"content-type":"application/json"}


    res = requests.post(URL, headers=headers , data=json.dumps(body))


    rescode = res.status_code
    if rescode == 200:
        print(res.headers)
        print(str(rescode) , " | " , res.text)
    else:
        print("Error Code : " + str(rescode) + " | " + res.text)

    responseContents = res.text  # page_source 얻기

    print("responseContents >>" , responseContents)

    #
    # url = 'https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/order-cash'
    # body = {
    #     "CANO": "67897190",
    #     "ACNT_PRDT_CD": "계좌상품코드",
    #     "PDNO": "상품번호",
    #     "ORD_DVSN": "주문구분",
    #     "ORD_QTY": "주문수량",
    #     "ORD_UNPR": "주문단가",
    #     "CTAC_TLNO": "연락전화번호"
    # }
    # headers = {
    #     "Content-Type": "application/json",
    #     "authorization": "Bearer {TOKEN}",
    #     "appKey": "{Client_ID}",
    #     "appSecret": "{Client_Secret}",
    #     "personalSeckey": "{personalSeckey}",
    #     "tr_id": "TTTC0802U",
    #     "tr_cont": " ",
    #     "custtype": "법인(B), 개인(P)",
    #     "seq_no": "법인(01), 개인( )",
    #     "mac_address": "{Mac_address}",
    #     "phone_num": "P01011112222",
    #     "ip_addr": "{IP_addr}",
    #     "hashkey": "{Hash값}",
    #     "gt_uid": "{Global UID}"
    # }
    #
    # res = requests.post(url, data=json.dumps(body), headers=headers)
    # rescode = res.status_code
    # if rescode == 200:
    #     print(res.headers)
    #     print(str(rescode) + " | " + res.text)
    # else:
    #     print("Error Code : " + str(rescode) + " | " + res.text)





    from Stock.API.koreaInvestment.Lib import kis_auth as ka, kis_domstk as kb

    import inspect
    import logging
    import logging.handlers
    from Init.Functions.Logs import GetLogDef

    import pandas as pd
    from datetime import datetime
    from datetime import timedelta
    import sys

    # 토큰 발급
    ka.auth()
    strStockCode = "311960"



    # # [국내주식] 주문/계좌 > 주식잔고조회 (보유종목리스트)
    # rt_data = kb.get_inquire_balance_lst()
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno), "rt_data >> " , type(rt_data), rt_data)    # 현재가, 전일대비
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno), "==================================================================>>")    # 현재가, 전일대비
    #
    #
    # dictRtDatas = pd.DataFrame(rt_data)
    # dictValues = dictRtDatas.to_dict()
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                         inspect.getframeinfo(inspect.currentframe()).lineno), "dictValue >> ",  len(dictValues), type(dictValues),
    #        dictValues)  # 현재가, 전일대비
    # #
    # for key in dictValues:
    #
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                             inspect.getframeinfo(inspect.currentframe()).lineno), "key >> ", type(key),
    #           key)  # 현재가, 전일대비
    #
    #     listValue = dictValues[key]
    #
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
    #           key , listValue)  # 현재가, 전일대비
    #



    #

    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno), "==================================================================>>")    # 현재가, 전일대비
    # [국내주식] 기본시세 > 주식현재가 투자자 (종목번호 6자리)
    rt_data = kb.get_inquire_time_itemconclusion(output_dv='3',itm_no=strStockCode,inqr_hour='154500')

    dictRtDatas = pd.DataFrame(rt_data)
    dictValues = dictRtDatas.to_dict()
    #
    for key in dictValues:

        listValue = dictValues[key]
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
              key , listValue)  # 현재가, 전일대비




    from Stock.CONFIG import ConstTableName

    from Stock.LIB.SeleniumModule.Windows import Chrome, Firefox
    from Stock.LIB.RDB import pyMysqlConnector
    from bs4 import BeautifulSoup
    import html

    import urllib.request
    import json
    import pymysql
    import traceback
    import time , datetime
    import re


    # # # DB 연결
    # ResRealEstateConnection = pyMysqlConnector.StockFriendsConnection()
    # cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


    # # [국내주식] 업종/기타 > 국내휴장일조회
    # rt_data = kb.get_quotations_ch_holiday(dt="20241116")
    #
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                         inspect.getframeinfo(inspect.currentframe()).lineno), "rt_data >> ", len(rt_data),
    #       type(rt_data),rt_data)

    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno),"============================================================================================>>")    # 현재가, 전일대비
    #
    # rt_data = kb.get_inquire_daily_itemchartprice(itm_no=strStockCode)
    #
    # dictRtDatas = pd.DataFrame(rt_data)
    # dictValues = dictRtDatas.to_dict()
    # #
    # for key in dictValues:
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                 inspect.getframeinfo(inspect.currentframe()).lineno), "key >> ", type(key),
    #               key)  # 현재가, 전일대비
    #
    #     listValue = dictValues[key]
    #
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
    #           key , listValue)  # 현재가, 전일대비


    #
    #
    #
    #
    #
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno),"============================================================================================>>")    # 현재가, 전일대비
    #
    # rt_data = kb.get_inquire_daily_itemchartprice(output_dv="3", itm_no=strStockCode)
    #
    # dictRtDatas = pd.DataFrame(rt_data)
    # dictValues = dictRtDatas.to_dict()
    # #
    # for key in dictValues:
    #     # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #     #                             inspect.getframeinfo(inspect.currentframe()).lineno), "key >> ", type(key),
    #     #           key)  # 현재가, 전일대비
    #
    #     listValue = dictValues[key]
    #
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
    #           key , listValue)  # 현재가, 전일대비
    #
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno),"============================================================================================>>")    # 현재가, 전일대비
    #
    # rt_data = kb.get_quotations_ch_holiday(dt="20241116")
    #
    # dictRtDatas = pd.DataFrame(rt_data)
    # dictValues = dictRtDatas.to_dict()
    # #
    # for key in dictValues:
    #     # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #     #                             inspect.getframeinfo(inspect.currentframe()).lineno), "key >> ", type(key),
    #     #           key)  # 현재가, 전일대비
    #
    #     listValue = dictValues[key]
    #
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
    #           key , listValue)  # 현재가, 전일대비
    #
    #
    #
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno),"============================================================================================>>")    # 현재가, 전일대비
    #
    # rt_data = kb.get_quotations_ch_holiday(dt="20241117")
    #
    # dictRtDatas = pd.DataFrame(rt_data)
    # dictValues = dictRtDatas.to_dict()
    # #
    # for key in dictValues:
    #     # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #     #                             inspect.getframeinfo(inspect.currentframe()).lineno), "key >> ", type(key),
    #     #           key)  # 현재가, 전일대비
    #
    #     listValue = dictValues[key]
    #
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
    #           key , listValue)  # 현재가, 전일대비
    #
    #
    #
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno),"============================================================================================>>")    # 현재가, 전일대비
    #
    # rt_data = kb.get_quotations_ch_holiday(dt="20241118")
    #
    # dictRtDatas = pd.DataFrame(rt_data)
    # dictValues = dictRtDatas.to_dict()
    # #
    # for key in dictValues:
    #     # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #     #                             inspect.getframeinfo(inspect.currentframe()).lineno), "key >> ", type(key),
    #     #           key)  # 현재가, 전일대비
    #
    #     listValue = dictValues[key]
    #
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
    #           key , listValue)  # 현재가, 전일대비
    #
    #
    #
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno),"============================================================================================>>")    # 현재가, 전일대비
    #
    # rt_data = kb.get_inquire_daily_price_2( itm_no=strStockCode)
    #
    # dictRtDatas = pd.DataFrame(rt_data)
    # dictValues = dictRtDatas.to_dict()
    # #
    # for key in dictValues:
    #     # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #     #                             inspect.getframeinfo(inspect.currentframe()).lineno), "key >> ", type(key),
    #     #           key)  # 현재가, 전일대비
    #
    #     listValue = dictValues[key]
    #
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ",  len(listValue), type(listValue),
    #           key , listValue)  # 현재가, 전일대비
    #







    #
    # # [국내주식] 기본시세 > 주식현재가 시세 (종목번호 6자리)
    # rt_data = kb.get_inquire_price(itm_no=strStockCode)
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno),"rt_data >> " , type(rt_data), rt_data)    # 현재가, 전일대비
    #
    #
    # dictRtDatas = pd.DataFrame(rt_data)
    #
    # for key  in dictRtDatas:
    #     # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #     #                         inspect.getframeinfo(inspect.currentframe()).lineno), "key >> ", type(key),
    #     #       key)  # 현재가, 전일대비
    #
    #     listValue = dictRtDatas[key].tolist()
    #
    #     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                             inspect.getframeinfo(inspect.currentframe()).lineno), "listValue >> ", type(listValue),len(listValue) ,
    #           key , listValue)  # 현재가, 전일대비
    #
    # print("==================================================================>>")    # 현재가, 전일대비
    # # [국내주식] 기본시세 > 주식현재가 호가 (종목번호 6자리)
    # rt_data = kb.get_inquire_asking_price_exp_ccn(itm_no=strStockCode)
    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
    #                                        inspect.getframeinfo(inspect.currentframe()).lineno),"rt_data >> " , type(rt_data), rt_data)    # 현재가, 전일대비



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