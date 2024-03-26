#경매 주소 데이터 확인 2번째 개발
#1번 개발이 오류가 발생하여 수정하기보다는 가독성을 위해 처음부터 다시 개발함
#https://business.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=10&keyword=%EC%84%B8%EC%A2%85%EB%A1%9C&confmKey=devU01TX0FVVEgyMDI0MDMwNzE5MzIxNjExNDU3NTk=&firstSort=road
#https://business.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=10&keyword=강남대로12길&confmKey=devU01TX0FVVEgyMDI0MDMwNzE5MzIxNjExNDU3NTk=&resultType=json
#https://www.juso.go.kr/addrlink/addrLinkApi.do
#https://business.juso.go.kr/addrlink/addrLinkApi.do
# 인증키 U01TX0FVVEgyMDI0MDMxNTExMTQxNzExNDYwMTU=
# 서비스 용도	개발 ( 사용기간 : 2024-03-07 ~ 2024-03-14 )

import sys
sys.path.append("D:/PythonProjects/airstock")

import urllib.request
import json
import pymysql
import datetime, time
from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from Lib.RDB import pyMysqlConnector


from Init.Functions.Logs import GetLogDef
from Realty.Auction.Const import ConstRealEstateTable_AUC

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
import math, random
import inspect
import traceback
import pymysql
import logging
import logging.handlers
from urllib import parse



def trim_msg(str_msg, int_max_len=80, encoding='euc-kr'):
    try:
        return str_msg.encode(encoding)[:int_max_len].decode(encoding)
    except UnicodeDecodeError:
        try:
            return str_msg.encode(encoding)[:int_max_len-1].decode(encoding)
        except UnicodeDecodeError:
            return str_msg.encode(encoding)[:int_max_len-2].decode(encoding)




def main():

    try:
        print(GetLogDef.lineno(__file__), "============================================================")

        # 상수 설정
        strProcessType = '020030'
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
            filename='D:/PythonProjects/airstock/Shell/logs/Cron_' + strProcessType + '_get_auction_address_' + logFileName,
            maxBytes=log_max_size,
            backupCount=log_file_count
        )

        rotatingFileHandler.setFormatter(formatter)
        # RotatingFileHandler
        timeFileHandler = logging.handlers.TimedRotatingFileHandler(
            filename='D:/PythonProjects/airstock/Shell/logs/Cron_' + strProcessType + '_get_auction_address_' + logFileName,
            when='midnight',
            interval=1,
            encoding='utf-8'
        )
        timeFileHandler.setFormatter(formatter)
        logger.addHandler(streamingHandler)
        logger.addHandler(timeFileHandler)


        #중복방지 필터 설정

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')

        if strResult is False:

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[=============>strResult is False ]=> " + str(strResult))
            quit("100")

        if strResult == '10':
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[=============>strResult == '10']=> " + str(strResult))
            quit("101")

        if strResult == '30':
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[=============>strResult == '30']=> " + str(strResult))
            quit("102")

        DBKuIndex = rstResult.get('data_1')
        DBCityKey = rstResult.get('data_2')
        targetRow = rstResult.get('data_3')

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = DBKuIndex
        dictSwitchData['data_2'] = DBCityKey
        dictSwitchData['data_3'] = targetRow
        dictSwitchData['data_4'] = '0'
        dictSwitchData['data_5'] = '0'
        dictSwitchData['data_6'] = '0'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


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


        sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " WHERE text_address != '' "
        # sqlCourtAuctionSelect += " AND build_type_text NOT LIKE '%대지%' "
        # sqlCourtAuctionSelect += " AND seq > " + DBKuIndex
        # sqlCourtAuctionSelect += " AND seq=2103704 "
        # sqlCourtAuctionSelect += " LIMIT 1 "

        cursorRealEstate.execute(sqlCourtAuctionSelect)
        nMasterResultCount = cursorRealEstate.rowcount
        rstAddressLists = cursorRealEstate.fetchall()

        intInsertCount = 0
        intUpdateCount = 0
        intMasterProcessCount = 0

        for rstAddressList in rstAddressLists:

            # 키워드 설정 영역 START

            intMasterProcessCount += 1
            strTextAddress = str(rstAddressList.get('address_data_text'))
            strTextAddressField = str(rstAddressList.get('text_address'))
            strAuctionMasterSecquence = str(rstAddressList.get('seq'))
            strLongitude = str(rstAddressList.get('longitude'))
            strLatitude = str(rstAddressList.get('latitude'))
            strAuctionBuildTypeText = str(rstAddressList.get('build_type_text'))
            strStripFieldName = str(strTextAddress).removeprefix("[\"").removesuffix("\"]")
            strLoadAddress = ''

            strDecodeCostomiseKeyword = str(rstAddressList.get('text_address').strip())

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[=============>strAuctionMasterSecquence ] => " + str(strAuctionMasterSecquence))

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[=============>strDecodeCostomiseKeyword ] => " + str(strDecodeCostomiseKeyword))


            sqlAddressTableSelect = " SELECT * FROM " + ConstRealEstateTable_AUC.AddressConversionTable + " WHERE "
            sqlAddressTableSelect += " keyword=%s LIMIT 1"
            cursorRealEstate.execute(sqlAddressTableSelect, str(strDecodeCostomiseKeyword))
            intPreviewResultCount = cursorRealEstate.rowcount




            if intPreviewResultCount > 0:   #주소테이블에 주소가 있으면
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[intPreviewResultCount > 0 : 이미 주소록에 등록 되어 있음 ] => " + str(intPreviewResultCount))

                rstAddressConversionData = cursorRealEstate.fetchone()
                strAddressConversionroadAddrPart1 = str(rstAddressConversionData.get('roadAddrPart1'))

                sqlCourtAuctionUpdate = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " SET "
                sqlCourtAuctionUpdate += " address_keyword = %s "
                sqlCourtAuctionUpdate += " , road_name = %s "
                sqlCourtAuctionUpdate += " WHERE seq = %s"

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[strAddressConversionroadAddrPart1] => " + str(strAddressConversionroadAddrPart1))
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[strAuctionMasterSecquence] => " + str(strAuctionMasterSecquence))
                cursorRealEstate.execute(sqlCourtAuctionUpdate, ( str(strDecodeCostomiseKeyword), strAddressConversionroadAddrPart1, strAuctionMasterSecquence))
                ResRealEstateConnection.commit()
                continue

            else:  #주소테이블에 주소가 없으면
                strEncodeCostomiseKeyword = parse.quote(strDecodeCostomiseKeyword)
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[strDecodeCostomiseAddress] => " + str(strDecodeCostomiseKeyword))

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[intPreviewResultCount] => " + str(intPreviewResultCount))


                strCountURL = "https://business.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=1" \
                      "&keyword=" + strEncodeCostomiseKeyword + "" \
                      "&confmKey=" + str(ConsStrAuthKey) + "&hstryYn=Y&resultType=json"
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[strCountURL] => " + str(strCountURL))

                # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
                responseCountURL = urllib.request.urlopen(strCountURL)
                jsonCountResponse = responseCountURL.read().decode("utf-8")
                dictCountResponse = json.loads(jsonCountResponse)

                dictGetURLResultCommon = (dictCountResponse['results']['common'])
                intResultTotalCount = int(dictGetURLResultCommon['totalCount'])
                strResultErrorCode = str(dictGetURLResultCommon['errorCode'])

                if intResultTotalCount < 1:  #키워드로  API 주소가 존재 하지 않는다.
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[strResultTotalCount < 1 :] => " + str(dictGetURLResultCommon))

                    intLandFindString = strAuctionBuildTypeText.find("대지")
                    intMountFindString = strAuctionBuildTypeText.find("임야")
                    intFarmFindString = strAuctionBuildTypeText.find("전답")
                    intCarFindString = strAuctionBuildTypeText.find("자동차")
                    intMachineFindString = strAuctionBuildTypeText.find("중기")

                    #대지,임야,전답,자동차,중기 의 경우는 주소가 없음
                    if intLandFindString > 0 or intMountFindString > 0 or intFarmFindString > 0 or intCarFindString > 0 or intMachineFindString > 0:

                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[대지는 도로명 주소가 없어요.intLandFindString => " + str(intLandFindString) + "][intMountFindString => " + str(intMountFindString) + "][intFarmFindString => " + str(intFarmFindString) + "][intCarFindString => " + str(intCarFindString) + "]][intMachineFindString => " + str(intMachineFindString) + "]")
                        continue

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[strDecodeCostomiseKeyword] => " + str(strDecodeCostomiseKeyword))

                    strNewDecodeCostomiseKeyword = strDecodeCostomiseKeyword

                    while True: # 루프를 돌리며 키워드 조회 될때 까지 글자를 줄여 가며 조회 한다.

                        strNewDecodeCostomiseKeyword = strNewDecodeCostomiseKeyword[0:-1]
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strNewDecodeCostomiseAddress] => len " + str(len(strNewDecodeCostomiseKeyword)) + ":" +str(strNewDecodeCostomiseKeyword))


                        if len(strNewDecodeCostomiseKeyword) < 6:  # 조회 단어가 5 글자 보다 작아지면 루프를 중단한다.(키워드 줄임 포기)
                            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                                         "[strNewDecodeCostomiseKeyword] => " + str(strNewDecodeCostomiseKeyword))
                            #조회 키워드 설정
                            strDecodeCostomiseKeyword = strNewDecodeCostomiseKeyword
                            break


                        strNewEncodeCostomiseKeyword = parse.quote(strNewDecodeCostomiseKeyword)

                        strModifyUrl = "https://business.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=1" \
                                       "&keyword=" + strNewEncodeCostomiseKeyword + "" \
                                       "&confmKey=" + str(ConsStrAuthKey) + "&hstryYn=Y&resultType=json"

                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strModifyUrl] => " + str(strModifyUrl))

                        ModifyResponse = urllib.request.urlopen(strModifyUrl)
                        jsonModifyResponse = ModifyResponse.read().decode("utf-8")
                        dictModifyResponse = json.loads(jsonModifyResponse)

                        dictModifydictGetURLResult = (dictModifyResponse['results']['common'])
                        strModifyCurrentPage = str(dictModifydictGetURLResult['currentPage'])
                        intModifyResultTotalCount = int(dictModifydictGetURLResult['totalCount'])
                        strModifyResultErrorCode = str(dictModifydictGetURLResult['errorCode'])

                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[dictModifydictGetURLResult] => " + str(dictModifydictGetURLResult))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strNewDecodeCostomiseKeyword] => " + str(strNewDecodeCostomiseKeyword))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strModifyCurrentPage] => " + str(strModifyCurrentPage))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[intModifyResultTotalCount] => " + str(intModifyResultTotalCount))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strModifyResultErrorCode] => " + str(strModifyResultErrorCode))

                        if intModifyResultTotalCount < 1:   # 주소 줄여서 API 조회 했는데도 없으면 다시 처음으로 보낸다.(하나 더 줄인다.) )
                            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                                         "[Continue] => " + str(strNewDecodeCostomiseKeyword))
                            continue


                        strDecodeCostomiseKeyword = strNewDecodeCostomiseKeyword
                        intResultTotalCount = intModifyResultTotalCount
                        strResultErrorCode = strModifyResultErrorCode
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strDecodeCostomiseKeyword] => " + str(strDecodeCostomiseKeyword))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[break] => " + str(strDecodeCostomiseKeyword))
                        break

                else:  #키워드로  API 주소가 확인된다.
                    if len(strDecodeCostomiseKeyword) < 6:
                        quit("349")
            # 키워드 설정 영역 END =========================================================================


            # 조회 키워드 strDecodeCostomiseKeyword
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[조회 키워드] => " + str(strDecodeCostomiseKeyword))


            # 키워드 조회 영역 START=========================================================================
            strEncodeCostomiseKeyword = parse.quote(strDecodeCostomiseKeyword)
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[strDecodeCostomiseKeyword] => " + str(strDecodeCostomiseKeyword))


            intLoopCount = math.ceil(intResultTotalCount / ConsIntProcessCount)
            intKeywordSeq = 0
            for i in range(intLoopCount):

                intPageNumber = int(i+1)


                DataListUrl  = "https://business.juso.go.kr/addrlink/addrLinkApi.do?currentPage="+str(intPageNumber)
                DataListUrl += "&countPerPage="+str(ConsIntProcessCount)+"&keyword=" + strEncodeCostomiseKeyword + ""
                DataListUrl += "&confmKey=" + str(ConsStrAuthKey) + "&hstryYn=Y&resultType=json"

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[DataListUrl] => " + str(DataListUrl))

                # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
                ObjDataListResponse = urllib.request.urlopen(DataListUrl)
                JsonDataList = ObjDataListResponse.read().decode("utf-8")

                # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
                DictJsonObject = json.loads(JsonDataList)
                ListResultJusos = (DictJsonObject['results']['juso'])
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[ListResultJusos] => " + str(ListResultJusos))

                intDictResultJusoLoop = 0
                strLoadAddress = ''


                for dictResultJuso in ListResultJusos: #조회되는 주소록
                    intDictResultJusoLoop += 1
                    intKeywordSeq += 1
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[dictResultJuso] => " + str(dictResultJuso))


                    strLoadAddress = dictResultJuso['roadAddrPart1'].strip()
                    print("strLoadAddress========>",type(strLoadAddress) ,  strLoadAddress)

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[intDictResultJusoLoop] => " + str(intDictResultJusoLoop))

                    #주소가 이미 등록 되어 있는 주소인지 확인
                    sqlAddressTableSelect = " SELECT * FROM "+ ConstRealEstateTable_AUC.AddressConversionTable + " WHERE "
                    sqlAddressTableSelect += " jibunAddr=%s "

                    cursorRealEstate.execute(sqlAddressTableSelect, str(dictResultJuso['jibunAddr'].strip()))
                    rstMasterDatas = cursorRealEstate.fetchone()
                    nResultCount = cursorRealEstate.rowcount

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[nResultCount] => " + str(nResultCount))
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[rstMasterDatas] => " + str(rstMasterDatas))

                    if nResultCount > 0: #주소테이블에 이미 등록 되어 있는 주소인 경우 - modify_date UPDATE
                        nConversionSequence = str(rstMasterDatas.get('seq'))
                        # UPDATE
                        qryInfoUpdate = " UPDATE " + ConstRealEstateTable_AUC.AddressConversionTable + " SET "
                        qryInfoUpdate += " modify_date=now()"
                        qryInfoUpdate += " WHERE seq='" + nConversionSequence + "' "

                        print("qryInfoUpdate", qryInfoUpdate, type(qryInfoUpdate))
                        cursorRealEstate.execute(qryInfoUpdate)
                        intUpdateCount += 1

                    else:  #주소테이블에 없는 주소인 경우 INSERT

                        sqlAddressTableInsert = "INSERT INTO " + ConstRealEstateTable_AUC.AddressConversionTable + " SET "
                        sqlAddressTableInsert += " keyword='" + strDecodeCostomiseKeyword + "', "
                        sqlAddressTableInsert += " keyword_seq='" + str(intKeywordSeq) + "', "



                        for dictResultJusoKey, dictResultJusoValue  in dictResultJuso.items():

                            # Non-strings are converted to strings.
                            if type(dictResultJusoValue) == 'int' and dictResultJusoValue =='':
                                dictResultJusoValue = '0'

                            if dictResultJusoKey == 'detBdNmList':
                                dictResultJusoValue = trim_msg(dictResultJusoValue, 500, 'utf-8')


                            dictResultJusoValue = dictResultJusoValue.strip()
                            sqlAddressTableInsert += " " + dictResultJusoKey + "='" + dictResultJusoValue + "', "

                        sqlAddressTableInsert += " result_code = '"+strResultErrorCode+"', "
                        sqlAddressTableInsert += " longitude = '"+strLongitude+"', "
                        sqlAddressTableInsert += " latitude = '"+strLatitude+"', "
                        sqlAddressTableInsert += " geo_point = ST_GeomFromText('POINT(" + strLongitude + " " + strLatitude + ")'), "
                        sqlAddressTableInsert += " modify_date = now(), "
                        sqlAddressTableInsert += " reg_date = now() "
                        print("sqlAddressTableInsert => ", sqlAddressTableInsert)
                        cursorRealEstate.execute(sqlAddressTableInsert)
                        nConversionSequence = cursorRealEstate.lastrowid
                        intInsertCount += 1

                        print("intInsertCount => ", type(intInsertCount), intInsertCount)
                        print("intUpdateCount => ", type(intUpdateCount), intUpdateCount)
                        print("nMasterResultCount => ", type(nMasterResultCount), nMasterResultCount)
                        print("strAuctionMasterSecquence => ", type(strAuctionMasterSecquence),
                              strAuctionMasterSecquence)
                        print("strDecodeCostomiseAddress => ", type(strDecodeCostomiseKeyword),
                              strDecodeCostomiseKeyword)

                    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                    dictSwitchData = dict()
                    dictSwitchData['result'] = '10'
                    dictSwitchData['data_1'] = strAuctionMasterSecquence # 경매 테이블에서 조회된 키워드
                    dictSwitchData['data_2'] = strDecodeCostomiseKeyword # 처리된 키워드
                    dictSwitchData['data_3'] = nConversionSequence     # 주소록 테이블에서 처리된 seq
                    dictSwitchData['data_4'] = nMasterResultCount     # 경매테이블에서 조회된 건수
                    dictSwitchData['data_5'] = intMasterProcessCount  # 경매테이블에서 처리된 건수
                    dictSwitchData['data_6'] = intInsertCount         # 주소록에서 새로 추가된 건수
                    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


            sqlCourtAuctionUpdate = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " SET "
            sqlCourtAuctionUpdate += " address_keyword = %s "
            sqlCourtAuctionUpdate += ",  road_name = %s "
            sqlCourtAuctionUpdate += " WHERE seq = %s"

            print("sqlCourtAuctionUpdate => ", type(sqlCourtAuctionUpdate), sqlCourtAuctionUpdate)
            print("strLoadAddress => ",  type(strLoadAddress), strLoadAddress)
            print("strAuctionMasterSecquence => ", type(strAuctionMasterSecquence), strAuctionMasterSecquence)
            print("nConversionSequence => ", type(nConversionSequence), nConversionSequence)

            cursorRealEstate.execute(sqlCourtAuctionUpdate , (str(strDecodeCostomiseKeyword),strLoadAddress, strAuctionMasterSecquence))
            ResRealEstateConnection.commit()

            # 테스트 딜레이 추가
            if int(strAuctionMasterSecquence) % 1000 == 0:
                nRandomSec = random.randint(1, 1)
                time.sleep(nRandomSec)


            # print(GetLogDef.lineno(__file__), "===================================================")

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'

        if strAuctionMasterSecquence is not None:
            dictSwitchData['data_1'] = strAuctionMasterSecquence

        if strDecodeCostomiseKeyword is not None:
            dictSwitchData['data_2'] = strDecodeCostomiseKeyword

        if nConversionSequence is not None:
            dictSwitchData['data_3'] = nConversionSequence

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

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[SUCCESS END]==================================================================")

    finally:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[CRONTAB END]==================================================================")


