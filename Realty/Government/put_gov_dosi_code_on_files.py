#https://www.code.go.kr/index.do
#법정동 코드 INSERT (주기적으로 해야함)
#fields (법정동코드,법정동명,폐지여부)
#kt_realty_gov_code_info Table 작성
#www.code.go.kr 의 자료를 파일로 가져와서

import unicodedata

from datetime import datetime as DateTime, date as DateTimeDate , timedelta as TimeDelta
from Lib.GeoDataModule import GeoDataModule
from Init.Functions.Logs import GetLogDef
from Init.DefConstant import ConstRealEstateTable
import inspect, pymysql, logging
import requests
import traceback
from Helper.basic_fnc import OnlyKorean
from Lib.RDB import pyMysqlConnector




# from Realty.Government.Const

#Realty/Government/Const/ConstRealEstateTable_GOV.py

import Realty.Auction.AuctionLib.AuctionDataDecode as AuctionDataDecode

import Realty.Government.MolitLib.GetRoadNameJuso as GetRoadNameJuso




try:

    # 상수 설정
    strProcessType = '020103'
    KuIndex = '00'
    CityKey = '00'
    targetRow = '00'
    ConsIntProcessCount = int(100)
    ConsStrAuthKey = 'U01TX0FVVEgyMDI0MDMxNTExMTQxNzExNDYwMTU='

    # 로그 설정
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    dtNow = DateTime.today()
    logFileName = str(dtNow.year) + str(dtNow.month).zfill(2) + str(dtNow.day).zfill(2) + ".log"
    formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')
    streamingHandler = logging.StreamHandler()
    streamingHandler.setFormatter(formatter)

    # RotatingFileHandler
    log_max_size = 10 * 1024 * 1024  ## 10MB
    log_file_count = 20

    rotatingFileHandler = logging.handlers.RotatingFileHandler(
        filename='D:/PythonProjects/airstock/Shell/logs/test_put_gov_dosi_code' + logFileName,
        maxBytes=log_max_size,
        backupCount=log_file_count
    )

    rotatingFileHandler.setFormatter(formatter)
    # RotatingFileHandler
    timeFileHandler = logging.handlers.TimedRotatingFileHandler(
        filename='D:/PythonProjects/airstock/Shell/logs/test_put_gov_dosi_code' + logFileName,
        when='midnight',
        interval=1,
        encoding='utf-8'
    )
    timeFileHandler.setFormatter(formatter)
    logger.addHandler(streamingHandler)
    logger.addHandler(timeFileHandler)

    filePath = 'Realty/Government/TempFiles/Code1.txt'

    filePath = "D:/PythonProjects/airstock/Realty/Government/TempFiles/Code1.txt"

    file = open(filePath,mode='r',buffering=-1,encoding='euc-kr',errors=None,newline=None)

    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) , "  ====file >> " , type(file) , file)


    listLines = file.readlines()


    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


    for listLine in listLines:

        listLineDatas = listLine.split("\t")

        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                            inspect.getframeinfo(inspect.currentframe()).lineno) , "  ====listLineDatas >> " , type(listLineDatas) , listLineDatas)

        strLawFullCode = str(listLineDatas[0])
        strLawFullName = str(listLineDatas[1])
        strStateText = OnlyKorean(listLineDatas[2])

        if strLawFullCode == '법정동코드':
            continue

        if len(strLawFullCode) != 10:
            raise Exception("strLawFullCode>" + strLawFullCode)

        strStateCode = '00'

        strSidoCode = strLawFullCode[0:2]
        strSiguCode = strLawFullCode[2:5]
        strDongMyunCode = strLawFullCode[5:10]

        listLawFullNames = strLawFullName.split(" ")
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====listLawFullNames >> ",
        #       len(listLawFullNames), listLawFullNames)

        strSidoName=strSiguName=strDongMyunName=''

        if len(listLawFullNames) > 0:
            strSidoName = listLawFullNames[0]
        if len(listLawFullNames) > 1:
            strSiguName = listLawFullNames[1]
        if len(listLawFullNames) > 2:
            strDongMyunName = listLawFullNames[2]

        strStateText = unicodedata.normalize('NFC', strStateText)

        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====strStateText >> ",
        #       type(strStateText), len(strStateText), strStateText)

        if strStateText == '존재':
            strStateCode = '00'
        elif strStateText == '폐지':
            strStateCode = '01'
        else:
            strStateCode = '99'

        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====strStateCode >> ",
        #       type(strStateCode), strStateCode)

        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====strSidoCode >> ",
        #       type(strSidoCode), strSidoCode)
        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====strSiguCode >> ",
        #       type(strSiguCode), strSiguCode)
        #
        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====strDongMyunCode >> ",
        #       type(strDongMyunCode), strDongMyunCode)
        #
        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====strSidoName >> ",
        #       type(strSidoName), strSidoName)
        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====strSiguName >> ",
        #       type(strSiguName), strSiguName)
        #
        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====strDongMyunName >> ",
        #       type(strDongMyunName), strDongMyunName)
        #
        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "  ====strState >> ",
        #       type(strStateCode), strStateCode)


        sqlSelectGovCode = " SELECT * FROM "+ConstRealEstateTable.GovAddressInfoTable+" WHERE law_full_cd = %s LIMIT 1"

        cursorRealEstate.execute(sqlSelectGovCode, ( strLawFullCode ))
        intSelectResult = cursorRealEstate.rowcount
        # print("intSelectResult > " , intSelectResult)

        if intSelectResult > 0:
            rstSelectData = cursorRealEstate.fetchone()
            strGovCodeSequence = str(rstSelectData.get('seq'))
            strGovCodeState = str(rstSelectData.get('state'))

            if strGovCodeState != strStateCode:
                sqlUpdateGovCode  = " UPDATE "+ConstRealEstateTable.GovAddressInfoTable+" SET "
                sqlUpdateGovCode += " state = %s "
                sqlUpdateGovCode += " WHERE seq = %s "
                cursorRealEstate.execute(sqlInsertGovCode, (strStateCode,strGovCodeSequence))
            cursorRealEstate.execute(sqlInsertGovCode, ( strLawFullCode,strLawFullName,strSidoCode,strSidoName,strSiguCode,strSiguName,strDongMyunCode,strDongMyunName,strStateCode))
            print(GetLogDef.lineno(__file__), "UPDATE => ", sqlInsertGovCode)
            ResRealEstateConnection.commit()

        else:

            #INSERT
            sqlInsertGovCode  = " INSERT INTO "+ConstRealEstateTable.GovAddressInfoTable+" SET "
            sqlInsertGovCode += " law_full_cd = %s ,  "
            sqlInsertGovCode += " law_full_name = %s ,  "
            sqlInsertGovCode += " sido_code = %s ,  "
            sqlInsertGovCode += " sido_name = %s ,  "
            sqlInsertGovCode += " sigu_code = %s ,  "
            sqlInsertGovCode += " sigu_name = %s ,  "
            sqlInsertGovCode += " dongmyun_code = %s ,  "
            sqlInsertGovCode += " dongmyun_name = %s ,  "
            sqlInsertGovCode += " state = %s ,  "
            sqlInsertGovCode += " modify_date = NOW() ,  "
            sqlInsertGovCode += " reg_date = NOW()   "
            cursorRealEstate.execute(sqlInsertGovCode, ( strLawFullCode,strLawFullName,strSidoCode,strSidoName,strSiguCode,strSiguName,strDongMyunCode,strDongMyunName,strStateCode))
            print(GetLogDef.lineno(__file__), "INSERT => ", sqlInsertGovCode)
            ResRealEstateConnection.commit()



except Exception as e:

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    err_msg = traceback.format_exc()

    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 "Error Exception")
    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 str(e))
    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 str(err_msg))


else:
    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 "[SUCCESS END]==================================================================")

finally:
    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 "[PROCESS END]==================================================================")
