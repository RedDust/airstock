# 서울 일별 실거래가 데이터 수집 프로그램
# 2023-01-30 커밋
# 취소 데이터 때문에 2년전 데이터 까지 조회
# https://data.seoul.go.kr/dataList/OA-21275/S/1/datasetView.do


import sys
sys.path.append("D:/PythonProjects/airstock")
#https://data.seoul.go.kr/dataList/OA-21275/S/1/datasetView.do


import urllib.request
import json
import pymysql
import datetime
import time
import pandas as pd
import requests

from Realty.Government.Init import init_conf
from Lib.RDB import pyMysqlConnector

from SeoulLib.RDB import LibSeoulRealTradeSwitch

from Init.Functions.Logs import GetLogDef
from bs4 import BeautifulSoup
from Realty.Government.Const import ConstRealEstateTable_GOV

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
import xml.etree.ElementTree as elemTree
from bs4 import BeautifulSoup as Soup
try:

    #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
    #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
    #거래 신고 30일 + 취소 신고 +30일
    stToday = DateTime.today()

    nInsertedCount = 0
    nUpdateCount = 0

    #서울 부동산 실거래가 데이터 - 매매
    strProcessType = '994000'
    KuIndex = '00'
    arrCityPlace = '00'
    targetRow = '00'

    # # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
    # url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/tbLnOpendataRtmsV/"+str(nStartNumber)+"/"+str(nEndNumber)+"/"+strProcessYear

    # 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'

    url = 'http://apis.data.go.kr/1613000/BldRgstService_v2/getBrTitleInfo'
    params = {
                'serviceKey': init_conf.KoreaAuthorizationKey,
                'sigunguCd': '11530',
                'bjdongCd': '10200',
                'platGbCd': '0',
                'bun': '0110',
                'ji': '0000',
                'startDate': '',
                'endDate': '',
                'numOfRows': '10',
                'pageNo': '1'
              }

    params = {
                'serviceKey': init_conf.KoreaAuthorizationKey,
                'sigunguCd': '11230',
                'bjdongCd': '10900',
                'platGbCd': '0',
                'bun': '0375',
                'ji': '0000',
                'startDate': '',
                'endDate': '',
                'numOfRows': '10',
                'pageNo': '1'
              }


    response = requests.get(url, params=params)
    html = response.text  # page_source 얻기
    print(GetLogDef.lineno(__file__), html)

    responseXml = elemTree.fromstring(html)

    # rstMainElements = soup.select_one('#contents > div.table_contents > form:nth-child(1) > table > tbody')

    print(GetLogDef.lineno(__file__), responseXml)

    header = responseXml.findall('header')
    print(GetLogDef.lineno(__file__), "header", len(header), header)

    list = responseXml.findall('body/items')
    # list = Soup(html, 'xml')

    # print(list.header.get_text())

    # responseXml = BeautifulSoup(html)
    #
    print(GetLogDef.lineno(__file__), "list" , type(list), list)



    #
    for items in list:
        for item in items:
            print(GetLogDef.lineno(__file__), '====================================================')
            print(GetLogDef.lineno(__file__), 'archArea => ', item.find('archArea').text)
            print(GetLogDef.lineno(__file__), 'atchBldArea => ', item.find('atchBldArea').text)
            print(GetLogDef.lineno(__file__), 'atchBldCnt => ', item.find('atchBldCnt').text)
            print(GetLogDef.lineno(__file__), 'bcRat => ', item.find('bcRat').text)
            print(GetLogDef.lineno(__file__), 'bjdongCd => ', item.find('bjdongCd').text)
            print(GetLogDef.lineno(__file__), 'bldNm => ', item.find('bldNm').text)
            print(GetLogDef.lineno(__file__), 'block => ', item.find('block').text)
            print(GetLogDef.lineno(__file__), 'bun => ', item.find('bun').text)
            print(GetLogDef.lineno(__file__), 'bylotCnt => ', item.find('bylotCnt').text)
            print(GetLogDef.lineno(__file__), 'crtnDay => ', item.find('crtnDay').text)
            print(GetLogDef.lineno(__file__), 'dongNm => ', item.find('dongNm').text)
            print(GetLogDef.lineno(__file__), 'emgenUseElvtCnt => ', item.find('emgenUseElvtCnt').text)
            print(GetLogDef.lineno(__file__), 'engrEpi => ', item.find('engrEpi').text)
            print(GetLogDef.lineno(__file__), 'engrGrade => ', item.find('engrGrade').text)
            print(GetLogDef.lineno(__file__), 'engrRat => ', item.find('engrRat').text)
            print(GetLogDef.lineno(__file__), 'etcPurps => ', item.find('etcPurps').text)
            print(GetLogDef.lineno(__file__), 'etcRoof => ', item.find('etcRoof').text)
            print(GetLogDef.lineno(__file__), 'etcStrct => ', item.find('etcStrct').text)
            print(GetLogDef.lineno(__file__), 'fmlyCnt => ', item.find('fmlyCnt').text)
            print(GetLogDef.lineno(__file__), 'gnBldCert => ', item.find('gnBldCert').text)
            print(GetLogDef.lineno(__file__), 'gnBldGrade => ', item.find('gnBldGrade').text)
            print(GetLogDef.lineno(__file__), 'grndFlrCnt => ', item.find('grndFlrCnt').text)
            print(GetLogDef.lineno(__file__), 'heit => ', item.find('heit').text)
            print(GetLogDef.lineno(__file__), 'hhldCnt => ', item.find('hhldCnt').text)
            print(GetLogDef.lineno(__file__), 'hoCnt => ', item.find('hoCnt').text)
            print(GetLogDef.lineno(__file__), 'indrAutoArea => ', item.find('indrAutoArea').text)
            print(GetLogDef.lineno(__file__), 'indrAutoUtcnt => ', item.find('indrAutoUtcnt').text)
            print(GetLogDef.lineno(__file__), 'indrMechArea => ', item.find('indrMechArea').text)
            print(GetLogDef.lineno(__file__), 'indrMechUtcnt => ', item.find('indrMechUtcnt').text)
            print(GetLogDef.lineno(__file__), 'itgBldCert => ', item.find('itgBldCert').text)
            print(GetLogDef.lineno(__file__), 'itgBldGrade => ', item.find('itgBldGrade').text)
            print(GetLogDef.lineno(__file__), 'ji => ', item.find('ji').text)
            print(GetLogDef.lineno(__file__), 'lot => ', item.find('lot').text)
            print(GetLogDef.lineno(__file__), 'mainAtchGbCd => ', item.find('mainAtchGbCd').text)
            print(GetLogDef.lineno(__file__), 'mainAtchGbCdNm => ', item.find('mainAtchGbCdNm').text)
            print(GetLogDef.lineno(__file__), 'mainPurpsCd => ', item.find('mainPurpsCd').text)
            print(GetLogDef.lineno(__file__), 'mainPurpsCdNm => ', item.find('mainPurpsCdNm').text)
            print(GetLogDef.lineno(__file__), 'mgmBldrgstPk => ', item.find('mgmBldrgstPk').text)
            print(GetLogDef.lineno(__file__), 'naBjdongCd => ', item.find('naBjdongCd').text)
            print(GetLogDef.lineno(__file__), 'naMainBun => ', item.find('naMainBun').text)
            print(GetLogDef.lineno(__file__), 'naRoadCd => ', item.find('naRoadCd').text)
            print(GetLogDef.lineno(__file__), 'naSubBun => ', item.find('naSubBun').text)
            print(GetLogDef.lineno(__file__), 'naUgrndCd => ', item.find('naUgrndCd').text)
            print(GetLogDef.lineno(__file__), 'newPlatPlc => ', item.find('newPlatPlc').text)
            print(GetLogDef.lineno(__file__), 'oudrAutoArea => ', item.find('oudrAutoArea').text)
            print(GetLogDef.lineno(__file__), 'oudrAutoUtcnt => ', item.find('oudrAutoUtcnt').text)
            print(GetLogDef.lineno(__file__), 'oudrMechArea => ', item.find('oudrMechArea').text)
            print(GetLogDef.lineno(__file__), 'oudrMechUtcnt => ', item.find('oudrMechUtcnt').text)
            print(GetLogDef.lineno(__file__), 'platArea => ', item.find('platArea').text)
            print(GetLogDef.lineno(__file__), 'platGbCd => ', item.find('platGbCd').text)
            print(GetLogDef.lineno(__file__), 'platPlc => ', item.find('platPlc').text)
            print(GetLogDef.lineno(__file__), 'pmsDay => ', item.find('pmsDay').text)
            print(GetLogDef.lineno(__file__), 'pmsnoGbCd => ', item.find('pmsnoGbCd').text)
            print(GetLogDef.lineno(__file__), 'pmsnoGbCdNm => ', item.find('pmsnoGbCdNm').text)
            print(GetLogDef.lineno(__file__), 'pmsnoKikCd => ', item.find('pmsnoKikCd').text)
            print(GetLogDef.lineno(__file__), 'pmsnoKikCdNm => ', item.find('pmsnoKikCdNm').text)
            print(GetLogDef.lineno(__file__), 'pmsnoYear => ', item.find('pmsnoYear').text)
            print(GetLogDef.lineno(__file__), 'regstrGbCd => ', item.find('regstrGbCd').text)
            print(GetLogDef.lineno(__file__), 'regstrGbCdNm => ', item.find('regstrGbCdNm').text)
            print(GetLogDef.lineno(__file__), 'regstrKindCd => ', item.find('regstrKindCd').text)
            print(GetLogDef.lineno(__file__), 'regstrKindCdNm => ', item.find('regstrKindCdNm').text)
            print(GetLogDef.lineno(__file__), 'rideUseElvtCnt => ', item.find('rideUseElvtCnt').text)
            print(GetLogDef.lineno(__file__), 'rnum => ', item.find('rnum').text)
            print(GetLogDef.lineno(__file__), 'roofCd => ', item.find('roofCd').text)
            print(GetLogDef.lineno(__file__), 'roofCdNm => ', item.find('roofCdNm').text)
            print(GetLogDef.lineno(__file__), 'rserthqkAblty => ', item.find('rserthqkAblty').text)
            print(GetLogDef.lineno(__file__), 'rserthqkDsgnApplyYn => ', item.find('rserthqkDsgnApplyYn').text)
            print(GetLogDef.lineno(__file__), 'sigunguCd => ', item.find('sigunguCd').text)
            print(GetLogDef.lineno(__file__), 'splotNm => ', item.find('splotNm').text)
            print(GetLogDef.lineno(__file__), 'stcnsDay => ', item.find('stcnsDay').text)
            print(GetLogDef.lineno(__file__), 'strctCd => ', item.find('strctCd').text)
            print(GetLogDef.lineno(__file__), 'strctCdNm => ', item.find('strctCdNm').text)
            print(GetLogDef.lineno(__file__), 'totArea => ', item.find('totArea').text)
            print(GetLogDef.lineno(__file__), 'totDongTotArea => ', item.find('totDongTotArea').text)
            print(GetLogDef.lineno(__file__), 'ugrndFlrCnt => ', item.find('ugrndFlrCnt').text)
            print(GetLogDef.lineno(__file__), 'useAprDay => ', item.find('useAprDay').text)
            print(GetLogDef.lineno(__file__), 'vlRat => ', item.find('vlRat').text)
            print(GetLogDef.lineno(__file__), 'vlRatEstmTotArea => ', item.find('vlRatEstmTotArea').text)



except Exception as e:
    print(GetLogDef.lineno(__file__), "Error Exception")
    print(GetLogDef.lineno(__file__), e)
    print(GetLogDef.lineno(__file__), type(e))

    # # Switch 오류 업데이트
    # dictSeoulSwitch = {}
    # dictSeoulSwitch['seq'] = dtProcessDay
    # dictSeoulSwitch['state'] = 20
    # print(GetLogDef.lineno(__file__), "dtProcessDay >", type(dtProcessDay), dictSeoulSwitch)
    # print(GetLogDef.lineno(__file__), "dictSeoulSwitch >", type(dtProcessDay), dictSeoulSwitch)
    # bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulUpdate(dictSeoulSwitch)
    # print(GetLogDef.lineno(__file__), "bSwitchUpdateResult >", bSwitchUpdateResult)
    #
    # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    # dictSwitchData = dict()
    # dictSwitchData['result'] = '30'
    # if dtProcessDay is not None:
    #     dictSwitchData['data_1'] = dtProcessDay
    #
    # if strProcessDay is not None:
    #     dictSwitchData['data_2'] = strProcessDay
    #
    # if nStartNumber is not None:
    #     dictSwitchData['data_3'] = nStartNumber
    #
    # if nEndNumber is not None:
    #     dictSwitchData['data_3'] = nEndNumber

    # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

else:
    print(GetLogDef.lineno(__file__), 'Inserted => ', nInsertedCount, ' , Updated => ', nUpdateCount)
    print(GetLogDef.lineno(__file__), "========================= SUCCESS END")
finally:
    print(GetLogDef.lineno(__file__), "Finally END")


