# This is a sample Python script.
# 매일 23시 실행
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

from Init.DefConstant import ConstRealEstateTable
from datetime import datetime as DateTime, timedelta as TimeDelta

try:

    date_1 = DateTime.today()
    end_date = date_1 - TimeDelta(days=1)
    nBaseStartDate = str(end_date.strftime('%Y-%m-%d 00:00:00'))
    nBaseEndDate = str(date_1.strftime('%Y-%m-%d 00:00:00'))

    print( nBaseStartDate, nBaseEndDate)
    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
    #qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + "  WHERE  reg_date > %s  AND reg_date < %s"
    #어제 데이터 부터 전체 처리
    qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + "  WHERE  reg_date > %s "
    cursorRealEstate.execute(qrySelectNaverMobileMaster, nBaseStartDate)
    rstMasterDatas = cursorRealEstate.fetchall()

    print(qrySelectNaverMobileMaster, nBaseStartDate, nBaseEndDate)

    nLoop = 0
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


