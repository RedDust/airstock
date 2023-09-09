# 크롬 버전이 너무 자주 업데이트를 해서
# 업데이트 할때 마다 확인해준다.
# https://googlechromelabs.github.io/chrome-for-testing/

# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import datetime
sys.path.append("D:/PythonProjects/airstock")

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Init.DefConstant import ConstRealEstateTable
from Init.DefConstant import ConstSectorInfo
from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.SeleniumModule.Windows import Chrome


try:

    switchAtclNo='0000000'
    switchAtclCfmYmd=''
    nTotalInsertedCount = 0

    # 크롬 셀리니움 드라이버
    driver = Chrome.defChromeDrive()

    RealtyCallUrl = "https://m.land.naver.com/cluster/ajax/articleList?itemId=&mapKey=&lgeo=&showR0=&rletTpCd=APT:OPST:VL:JGC:JWJT:DDDGG:SGJT:HOJT:JGB:SG:SMS:GJCG:GM:TJ:APTHGJ&tradTpCd=A1:B1:B2:B3&z=12&lat=37.574493&lon=127.039765&btm=37.4111879&lft=126.7005621&top=37.7374408&rgt=127.3789679&totCnt=12213&cortarNo=1123000000&sort=dates&page=1"

    strResult = driver.get(RealtyCallUrl)  # 크롤링할 사이트 호출
    html = driver.page_source  # page_source 얻기
    soup = BeautifulSoup(html, "html.parser")  # get html

    print(html)
    print(soup)


except Exception as e:
    print("Error Exception")
    print(e)
    print(type(e))
else:

    print("SUCCESS")
    print("========================================================")

finally:
    driver.quit()    # 크롬 브라우저 닫기
    print("Finally END")



