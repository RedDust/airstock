

import time
from urllib import parse
import urllib.request
import inspect
import json
import traceback
import re
import pymysql
from Init.Functions.Logs import GetLogDef
from datetime import datetime as DateTime, date as DateTimeDate , timedelta as TimeDelta
from Lib.GeoDataModule import GeoDataModule
import Realty.Auction.Const.AuctionCourtInfo as AuctionCourtInfo
import Realty.Auction.AuctionLib.AuctionDataDecode as AuctionDataDecode
from Init.Functions.Logs import GetLogDef
from Realty.Auction.Const import ConstRealEstateTable_AUC
from Lib.RDB import pyMysqlConnector

def CustomiseAddressText(strTextAddress):
    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF

    listResult = list()
    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "strTextAddress", len(strTextAddress), type(strTextAddress), strTextAddress)

    # strStripFieldName = str(strTextAddress).removeprefix("[\"").removesuffix("\"]")
    # print("listStripFieldName" , len(strStripFieldName), strStripFieldName)

    strStripFieldName = re.sub(r"[^\uAC00-\uD7A30-9a-zA-Z-,\s]", "", strTextAddress)
    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) , "listStripFieldName" , len(strStripFieldName), type(strTextAddress), strStripFieldName)

    strStripFieldName = strStripFieldName.strip('사용본거지')


    listStripFieldNames = str(strStripFieldName).split(",")

    for listStripFieldName in listStripFieldNames:

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "listStripFieldName",
              len(listStripFieldName), type(listStripFieldName), listStripFieldName)

        strAddress = listStripFieldName.strip(" ")

        for dictCityPlaceKey, dictCityPlaceValue in AuctionCourtInfo.dictCityPlace.items():
            # print("dictCityPlaceKey", len(dictCityPlaceKey), dictCityPlaceKey)
            # print("dictCityPlaceValue", len(dictCityPlaceValue), dictCityPlaceValue)
           if str(strAddress).startswith(dictCityPlaceValue) == True:

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) , "strAddress", len(strAddress), strAddress)

                listResult.append(strAddress)


    return listResult

# juso.go.kr 데이터 조회
# kt_realty_address_conversion Table Insert

def GetJusoApiForAddress(logging,strIssueNumber, strDecodeCostomiseKeyword):

    try:

        strReturn = ''
        strLongitude = str(0)
        strLatitude = str(0)


        ConsStrAuthKey = 'U01TX0FVVEgyMDI0MDMxNTExMTQxNzExNDYwMTU='
        strApiAddress = 'https://business.juso.go.kr/addrlink/addrLinkApi.do'

        strAddressKeyword = GetLogDef.stripSpecharsForText(strDecodeCostomiseKeyword)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno)+"strIssueNumber", len(strIssueNumber), strIssueNumber)

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno)+"AddressKeyWord", len(strAddressKeyword), strAddressKeyword)

        listExtractionAddressTexts = CustomiseAddressText(strDecodeCostomiseKeyword)

        print("===================listExtractionAddressTexts",listExtractionAddressTexts)


        #DB 연결 선언
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        #필드 테이블 컬럼 타입 조회 int 인경우는 예외처리 하기 위한 준비
        qrySelectSeoulTableColumns = "show columns from " + ConstRealEstateTable_AUC.AddressConversionTable
        cursorRealEstate.execute(qrySelectSeoulTableColumns)
        SelectColumnLists = cursorRealEstate.fetchall()
        dictSeoulColumnInfoData = {}

        for SelectColumnList in SelectColumnLists:
            strFieldName = str(SelectColumnList.get('Field'))
            strColumnType = str(SelectColumnList.get('Type'))
            dictSeoulColumnInfoData[strFieldName] = {}
            dictSeoulColumnInfoData[strFieldName]['name'] = strFieldName
            dictSeoulColumnInfoData[strFieldName]['type'] = strColumnType

        for listExtractionAddressText in listExtractionAddressTexts:

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno)+" listExtractionAddressText ==>>", len(listExtractionAddressText), listExtractionAddressText)


            sqlCourtAuctionSelect  = " SELECT * FROM " + ConstRealEstateTable_AUC.AddressConversionTable
            sqlCourtAuctionSelect += " WHERE address_keyword = %s "

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno)+"strIssueNumber", len(strIssueNumber), strIssueNumber)

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno)+"strAddressKeyWord", len(listExtractionAddressText), listExtractionAddressText)


            cursorRealEstate.execute(sqlCourtAuctionSelect,(listExtractionAddressText))
            intMasterResultCount = cursorRealEstate.rowcount
            print("nMasterResultCount", type(intMasterResultCount), intMasterResultCount)
            intKeywordSeq = 0

            if intMasterResultCount > 0:

                rstAddressConversionData = cursorRealEstate.fetchone()
                strLoadAddress = str(rstAddressConversionData.get('roadAddrPart1'))

            else:

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[listExtractionAddressTexts] => " , type(listExtractionAddressText) , listExtractionAddressText)


                strEncodeCostomiseKeyword = parse.quote(listExtractionAddressText)


                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[strDecodeCostomiseAddress] => " + str(listExtractionAddressText))

                strDataListUrl = strApiAddress + "?currentPage=1&countPerPage=10"
                strDataListUrl += "&keyword=" + strEncodeCostomiseKeyword + ""
                strDataListUrl += "&confmKey=" + ConsStrAuthKey + "&hstryYn=Y&resultType=json"

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) , "strDataListUrl", len(strDataListUrl), strDataListUrl)

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[strDataListUrl] => " + str(strDataListUrl))

                ObjDataListResponse = urllib.request.urlopen(strDataListUrl)
                JsonDataList = ObjDataListResponse.read().decode("utf-8")

                # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
                DictJsonObject = json.loads(JsonDataList)

                ListResultJusos = (DictJsonObject['results']['juso'])
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) , "ListResultJusos", len(ListResultJusos), ListResultJusos)

                dictGetURLResultCommon = (DictJsonObject['results']['common'])
                intResultTotalCount = int(dictGetURLResultCommon['totalCount'])
                strResultErrorCode = str(dictGetURLResultCommon['errorCode'])

                if len(ListResultJusos) != 1:
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno), len(ListResultJusos),
                          ListResultJusos)

                    continue

                intKeywordSeq=0
                for listResultJuso in ListResultJusos:  # 조회되는 주소록
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno), len(listResultJuso),
                          listResultJuso)

                    sqlAddressTableInsert = "INSERT INTO " + ConstRealEstateTable_AUC.AddressConversionTable + " SET "
                    sqlAddressTableInsert += " keyword='" + strIssueNumber + "', "
                    sqlAddressTableInsert += " keyword_seq='" + str(intKeywordSeq) + "', "
                    sqlAddressTableInsert += " address_keyword='" + str(listExtractionAddressText) + "', "



                for dictResultJusoKey, dictResultJusoValue in listResultJuso.items():

                    # Non-strings are converted to strings.
                    if type(dictResultJusoValue) == 'int' and dictResultJusoValue == '':
                        dictResultJusoValue = '0'

                    if dictResultJusoKey == 'detBdNmList':
                        dictResultJusoValue = AuctionDataDecode.trim_msg(dictResultJusoValue, 500, 'utf-8')

                    dictResultJusoValue = dictResultJusoValue.strip()
                    sqlAddressTableInsert += " " + dictResultJusoKey + "='" + dictResultJusoValue + "', "

                sqlAddressTableInsert += " result_code = '" + strResultErrorCode + "', "
                sqlAddressTableInsert += " longitude = '" + strLongitude + "', "
                sqlAddressTableInsert += " latitude = '" + strLatitude + "', "
                sqlAddressTableInsert += " geo_point = ST_GeomFromText('POINT(" + strLongitude + " " + strLatitude + ")'), "
                sqlAddressTableInsert += " modify_date = now(), "
                sqlAddressTableInsert += " reg_date = now() "
                print("sqlAddressTableInsert => ", sqlAddressTableInsert)
                cursorRealEstate.execute(sqlAddressTableInsert)
                nConversionSequence = cursorRealEstate.lastrowid
                intKeywordSeq += 1
                ResRealEstateConnection.commit()

                strLoadAddress = ListResultJusos[0]['roadAddrPart1'].strip()
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) , "strLoadAddress========>", type(strLoadAddress), strLoadAddress)




    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)


        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "Error Exception")
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     str(e))


        strReturn = ''

    else:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[GetJusoApiForAddress]")

        strReturn = strLoadAddress
    finally:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[CRONTAB END]==================================================================")

        return strReturn





def GetDictConversionAddress(log , strIssueNumber, strDecodeCostomiseKeyword):

    import inspect as Isp
    from Init.Functions.Logs import GetLogDef as SLog

    try:

        strReturn = ''
        strLongitude = str(0)
        strLatitude = str(0)
        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strDecodeCostomiseKeyword =>[" + str(strDecodeCostomiseKeyword) +"]" + "[" + str(type(strDecodeCostomiseKeyword)) + "]")

        ConsStrAuthKey = 'U01TX0FVVEgyMDI0MDMxNTExMTQxNzExNDYwMTU='
        strApiAddress = 'https://business.juso.go.kr/addrlink/addrLinkApi.do'

        strAddressKeyword = GetLogDef.stripSpecharsForText(strDecodeCostomiseKeyword)
        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strIssueNumber =>[" + str(
            strIssueNumber) + "]" + "[" + str(type(strIssueNumber)) + "]")
        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strAddressKeyword =>[" + str(
            strAddressKeyword) + "]" + "[" + str(type(strAddressKeyword)) + "]")

        listExtractionAddressTexts = CustomiseAddressText(strDecodeCostomiseKeyword)
        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "listExtractionAddressTexts =>[" + str(
            listExtractionAddressTexts) + "]" + "[" + str(type(listExtractionAddressTexts)) + "]")

        #DB 연결 선언
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "listExtractionAddressTexts =>[" + str(
            listExtractionAddressTexts) + "]" + "[" + str(type(listExtractionAddressTexts)) + "]")

        sqlSelectAddressTable =  " SELECT * FROM " + ConstRealEstateTable_AUC.AddressConversionTable
        sqlSelectAddressTable += " WHERE keyword= %s "
        sqlSelectAddressTable += " LIMIT 1 "

        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strIssueNumber =>[" + str(
            strIssueNumber) + "]" + "[" + str(type(strIssueNumber)) + "]")


        cursorRealEstate.execute(sqlSelectAddressTable, (strIssueNumber))
        intMasterResultCount = cursorRealEstate.rowcount
        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "intMasterResultCount =>[" + str(
            intMasterResultCount) + "]" + "[" + str(type(intMasterResultCount)) + "]")

        if intMasterResultCount > 0:

            rstFieldsList = cursorRealEstate.fetchone()

            log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "rstFieldsList =>[" + str(
                rstFieldsList) + "]" + "[" + str(type(rstFieldsList)) + "]")


        else:

            log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "intMasterResultCount =>[" + str(
                intMasterResultCount) + "]" + "[" + str(type(intMasterResultCount)) + "]")

            listExtractionAddressText = listExtractionAddressTexts[0]

            log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "listExtractionAddressText =>[" + str(
                listExtractionAddressText) + "]" + "[" + str(type(listExtractionAddressText)) + "]")


            listStripFieldNames = str(listExtractionAddressText)

            intLoopCount = len(listStripFieldNames)

            log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "intLoopCount =>[" + str(
                intLoopCount) + "]" + "[" + str(type(intLoopCount)) + "]")


            intTemp = 0
            strExtractionAddressText = listStripFieldNames

            while intTemp < intLoopCount:

                log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strExtractionAddressText =>[" + str(
                    strExtractionAddressText) + "]" + "[" + str(type(strExtractionAddressText)) + "]")

                strEncodeCostomiseKeyword = parse.quote(strExtractionAddressText)

                strDataListUrl = strApiAddress + "?currentPage=1&countPerPage=1"
                strDataListUrl += "&keyword=" + strEncodeCostomiseKeyword + ""
                strDataListUrl += "&confmKey=" + ConsStrAuthKey + "&hstryYn=Y&resultType=json"

                log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strDataListUrl =>[" + str(
                    strDataListUrl) + "]" + "[" + str(type(strDataListUrl)) + "]")

                ObjDataListResponse = urllib.request.urlopen(strDataListUrl)
                JsonDataList = ObjDataListResponse.read().decode("utf-8")

                # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
                DictJsonObject = json.loads(JsonDataList)

                ListResultJusos = (DictJsonObject['results']['juso'])
                log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "ListResultJusos =>[" + str(
                    ListResultJusos) + "]" + "[" + str(type(ListResultJusos)) + "]")


                dictGetURLResultCommon = (DictJsonObject['results']['common'])
                log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "dictGetURLResultCommon =>[" + str(
                    dictGetURLResultCommon) + "]" + "[" + str(type(dictGetURLResultCommon)) + "]")

                intResultTotalCount = int(dictGetURLResultCommon['totalCount'])
                strResultErrorCode = str(dictGetURLResultCommon['errorCode'])

                if intResultTotalCount < 1:

                    strExtractionAddressText = strExtractionAddressText[:-1]
                    intTemp += 1
                    time.sleep(0.3)

                else:
                    intKeywordSeq = 0
                    rstFieldsList = ListResultJusos[0]
                    log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "rstFieldsList =>[" + str(
                        rstFieldsList) + "]" + "[" + str(type(rstFieldsList)) + "]")

                    break




    except Exception as e:

        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Error Exception]")
        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "e =>[" + str(
            e) + "]" + "[" + str(type(e)) + "]")
        err_msg = traceback.format_exc()
        log.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        strReturn = ''

    else:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()

        strReturn = rstFieldsList

    finally:
        return strReturn



