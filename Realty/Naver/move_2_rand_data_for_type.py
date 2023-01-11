# This is a sample Python script.
import os
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
from selenium import webdriver    # 라이브러리에서 사용하는 모듈만 호출
from Init.DefConstant import ConstRealEstateTable
from selenium import webdriver    # 라이브러리에서 사용하는 모듈만 호출

from Init.DefConstant import ConstRealEstateTable
from Init.DefConstant import ConstSectorInfo
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.RDB import LibNaverMobileMasterSwitchTable

try:

    date_1 = DateTime.today()
    end_date = date_1 - TimeDelta(days=2)
    nBaseAtclCfmYmd = str(end_date.strftime('2023-01-01 00:00:00'))


    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
    qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + "  WHERE  reg_date >= %s "
    cursorRealEstate.execute(qrySelectNaverMobileMaster, nBaseAtclCfmYmd)
    rstMasterDatas = cursorRealEstate.fetchall()

    # print(results)

    for MasterDataList in rstMasterDatas:

        strTypeCode = MasterDataList.get('rletTpCd')
        nMasterSeq  = MasterDataList.get('seq')
        strNewPartTable = ConstRealEstateTable.NaverMobileMasterTable + "_" + strTypeCode

        try:
            qryCheckTable = "SELECT count(*) FROM information_schema.tables WHERE TABLE_SCHEMA='"+ConstRealEstateTable.RealtyDatabase+"' AND TABLE_NAME='"+strNewPartTable+"'"
            cursorRealEstate.execute(qryCheckTable)
            results = cursorRealEstate.rowcount
            if results < 1:
                strNewPartTable = ConstRealEstateTable.NaverMobileMasterTable + "_ETC"


            #Insert
            qryInsertTypeTable = "INSERT INTO "+strNewPartTable+" SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + " WHERE seq = %s"
            cursorRealEstate.execute(qryInsertTypeTable, nMasterSeq)
            ResRealEstateConnection.commit()

        except Exception as e:
            print(e)
        #try-except End

    #for MasterDataList in rstMasterDatas END

    ResRealEstateConnection.close()

except Exception as e:
    print("Error Exception")
    print(e)
    print(type(e))

    # print(sqlInsertNaverMobileMaster)
    # LibNaverMobileMasterSwitchTable.SwitchResultUpdate("20")
else:
    print("========================================================")
    # LibNaverMobileMasterSwitchTable.SwitchResultUpdate("00")
finally:
    print("Finally END")


