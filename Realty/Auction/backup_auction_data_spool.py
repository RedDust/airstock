#
#

import requests

# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import logging
import logging.handlers
import inspect
import traceback
import re

import datetime

sys.path.append("D:/PythonProjects/airstock")

# from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Realty.Auction.Const import ConstRealEstateTable_AUC
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Realty.Auction.Const import AuctionCourtInfo
from Init.DefConstant import ConstRealEstateTable
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.CryptoModule import AesCrypto
from Realty.Auction.AuctionLib import MakeAuctionUniqueKey
import Realty.Auction.AuctionLib.AuctionDataDecode as AuctionDataDecode
import Realty.Government.MolitLib.GetRoadNameJuso as GetRoadNameJuso
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Lib.CustomException.QuitException import QuitException
from Realty.Auction.AuctionLib import AuctionMakeRequestHeader
from shapely.geometry import Point


def main():
    try:

        # https://curlconverter.com/ <- 프로그램 컨버터

        # 물건상세 검색
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf"

        # 매각예정물건
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveMgakPlanMulSrch.laf"

        # 매각결과
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMgakGyulgwaMulList.laf"

        strProcessType = '021110'

        data_1 = '00'
        data_2 = '00'
        data_3 = '0'
        data_4 = '0'
        data_5 = '00'
        data_6 = '00'

        dtNow = DateTime.today()
        dtTimeBefore1Min = DateTime.today() - TimeDelta(seconds=5)
        strTimeStamp = str(dtTimeBefore1Min.timestamp()).replace(".", "")[0:13]
        # print(dtNow.hour)
        # print(dtNow.minute)
        print("strTimeStamp => " , strTimeStamp)

        LogPath = 'CourtAuction/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        strAddressSiguSequence='0'

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+
                     "[CRONTAB START]============================================================")


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        data_1 = strAddressSiguSequence = str(rstResult.get('data_1'))
        if strResult is False:
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'It is currently in operation. => ' + str(
                strResult))  # 예외를 발생시킴

        if strResult == '20':
            data_1 = strAddressSiguSequence = str(rstResult.get('data_1'))

        if strResult == '40':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + '경매 서비스 점검 ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


        # 초기 값
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        # aaaa  = '{"docid": "B0004102024013000312011", "boCd": "B000410", "saNo": "20240130003120", "maemulSer": "1", "mokmulSer": "1", "srnSaNo": "2024타경3120", "jpDeptCd": "1013", "jinstatCd": "0002100001", "mulStatcd": "01", "mulJinYn": "Y", "maemulUtilCd": "01", "mulBigo": "집합건축물대장상 다동 503호이며, 현칭 다동 503호임. rn감정서에 따르면 정비구역<도시 및 주거환경정비법>으로 기재되어 있는바, 정비사업단계, 조합원의 자격 등 정비사업 관련하여 모든 사항은 낙찰자가 확인하여 매각절차에 참여하기 바람", "gamevalAmt": "278000000", "minmaePrice": "278000000", "yuchalCnt": "1", "maeAmt": "0", "inqCnt": "10", "gwansMulRegCnt": "1", "remaeordDay": "", "ipchalGbncd": "000331", "maeGiil": "20250212", "maegyuljGiil": "20250219", "maeHh1": "1000", "maeHh2": "", "maeHh3": "", "maeHh4": "", "notifyMinmaePrice1": "194600000", "notifyMinmaePrice2": "0", "notifyMinmaePrice3": "0", "notifyMinmaePrice4": "0", "notifyMinmaePriceRate1": "70", "notifyMinmaePriceRate2": "70", "maeGiilCnt": "1", "ipgiganFday": "", "ipgiganTday": "", "maePlace": "입찰전용법정(256호법정)", "spJogCd": "", "mokGbncd": "03", "jongCd": "000", "stopsaGbncd": "00", "daepyoSidoCd": "26", "daepyoSiguCd": "260", "daepyoDongCd": "101", "daepyoRdCd": "00", "hjguSido": "부산광역시", "hjguSigu": "동래구", "hjguDong": "명장동", "hjguRd": "", "daepyoLotno": "497-30", "buldNm": "", "buldList": "5층503호", "areaList": "", "jimokList": "", "lclsUtilCd": "20000", "mclsUtilCd": "20100", "sclsUtilCd": "20104", "jejosaNm": "", "fuelKindcd": "", "bsgFormCd": "", "carNm": "", "carYrtype": "0", "xCordi": "500491", "yCordi": "290769", "cordiLvl": "1", "bgPlaceSidoCd": "", "bgPlaceSiguCd": "", "bgPlaceDongCd": "", "bgPlaceRdCd": "", "bgPlaceLotno": "", "bgPlaceSido": "", "bgPlaceSigu": "", "bgPlaceDong": "", "bgPlaceRd": "", "srchHjguBgFlg": "", "pjbBuldList": "철근콩크리트조rn71.91㎡", "minArea": "71", "maxArea": "71", "groupmaemulser": "B000410202401300031201", "bocdsano": "B0004102024013000312", "dupSaNo": "", "byungSaNo": "", "srchLclsUtilCd": "20000", "srchMclsUtilCd": "20100", "srchSclsUtilCd": "20104", "srchHjguSidoCd": "26", "srchHjguSiguCd": "26260", "srchHjguDongCd": "26260101", "srchHjguRdCd": "2626010100", "srchHjguLotno": "497-30", "jiwonNm": "부산지방법원", "jpDeptNm": "경매13계", "tel": "(051)590-1831(구내:1831)", "maejibun": "", "wgs84Xcordi": "129", "wgs84Ycordi": "35", "rd1Cd": "26", "rd2Cd": "260", "rd3Rd4Cd": "3130024", "rd1Nm": "부산광역시", "rd2Nm": "동래구", "rdEubMyun": "", "rdNm": "명서로", "buldNo": "137", "rdAddrSub": "", "addrGbncd": "R", "bgPlaceRdAllAddr": "부산광역시 동래구 명서로 00137-00000", "bgPlaceAddrGbncd": "R", "srchRd1Cd": "26", "srchRd2Cd": "26260", "srchRd3Rd4Cd": "262603130024", "alias": "budongsanmok", "dummyField": "", "dspslUsgNm": "아파트", "convAddr": "[집합건물 철근콩크리트조rn71.91㎡]", "printSt": "부산광역시 동래구 명서로 137 5층503호 ", "printCsNo": "부산지방법원<br/>2024타경3120", "colMerge": "202401300031201"}'
        #
        # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), aaaa)
        #
        # dictDataRow = json.loads(aaaa)
        #
        # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), type(dictDataRow) , dictDataRow)
        #
        # quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴
        # #

        # qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionSpoolTable
        # qrySelectSeoulTradeMaster += " WHERE state='00' "
        # qrySelectSeoulTradeMaster += " AND seq >= %s "
        # qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
        # qrySelectSeoulTradeMaster += " LIMIT 50000 "
        # cursorRealEstate.execute(qrySelectSeoulTradeMaster,(strAddressSiguSequence))
        #


        qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionSpoolTable
        qrySelectSeoulTradeMaster += " WHERE state='10' "
        qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
        qrySelectSeoulTradeMaster += " LIMIT 10 "
        cursorRealEstate.execute(qrySelectSeoulTradeMaster)
        rstSpoolDatas = cursorRealEstate.fetchall()

        print("rstSiDoLists =>", qrySelectSeoulTradeMaster)

        intProcessLoop = 0
        for rstSpoolData in rstSpoolDatas:
            # print("----------------------------------------------- =>")

            # CityKey = str(rstSiDoList.get('sido_code'))
            # strSiGuCode = str(rstSiDoList.get('sigu_code'))
            data_1 = strAddressSiguSequence = str(rstSpoolData.get('seq'))
            data_2 = strUniqueKey = str(rstSpoolData.get('unique_key'))
            data_3 = dtRegDate = str(rstSpoolData.get('reg_date'))
            data_4 = strSidoName = str(rstSpoolData.get('sido_name'))
            data_5 = strSiguName = str(rstSpoolData.get('sigu_name'))


            # 정규 표현식으로 쌍따옴표 안의 작은따옴표를 대시로 변경
            # new_text = re.sub(r'"(.*?)\'(.*?)"', lambda x: '"' + x.group(1) + '-' + x.group(2) + '"', text)
            # print("new_text=>" , new_text)
            # 결과 출력

            strTableForYYYY = dtRegDate[0:4]

            print("ConstRealEstateTable_AUC.CourtAuctionProgressingMasterTable => strTableForYYYY ", strTableForYYYY)

            # BAKCUP TABLE 지정
            strTargetNowYearTable = ConstRealEstateTable_AUC.CourtAuctionSpoolBackupTable+"_"+strTableForYYYY
            sqlSelectTable = " SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            sqlSelectTable += " WHERE TABLE_SCHEMA = 'kt_real_estate_auction' "
            sqlSelectTable += " AND TABLE_NAME =  %s "
            cursorRealEstate.execute(sqlSelectTable,(strTargetNowYearTable))
            strSelectedCount = int(cursorRealEstate.rowcount)
            print("NowYearTable strSelectedCount => ", strSelectedCount, strTargetNowYearTable)

            if strSelectedCount < 1:
                strTargetNowYearTable = ConstRealEstateTable_AUC.CourtAuctionSpoolBackupTable

            # BAKCUP TABLE 지정
            print("179 strSelectedCount => ", strSelectedCount, strTargetNowYearTable)

            # BACKUP TABLE
            sqlSelectBackUpTable = " SELECT * FROM "+strTargetNowYearTable
            sqlSelectBackUpTable += " WHERE seq = %s "
            cursorRealEstate.execute(sqlSelectBackUpTable, (strAddressSiguSequence))
            intSelectedCount = cursorRealEstate.rowcount
            if intSelectedCount > 0:

                qryUpdateAuctionSpoolMaster = "DELETE FROM " + ConstRealEstateTable_AUC.CourtAuctionSpoolTable + "  "
                qryUpdateAuctionSpoolMaster += " WHERE seq = %s  "

                print("qryUpdateAuctionSpoolMaster => ", strSelectedCount, qryUpdateAuctionSpoolMaster , strAddressSiguSequence)

                cursorRealEstate.execute(qryUpdateAuctionSpoolMaster, (strAddressSiguSequence))
                ResRealEstateConnection.commit()

                continue

            sqlSelectMasterTable = " INSERT INTO  " +strTargetNowYearTable
            sqlSelectMasterTable += " (SELECT * FROM "+ConstRealEstateTable_AUC.CourtAuctionSpoolTable+" WHERE seq = %s AND state = '10') "
            cursorRealEstate.execute(sqlSelectMasterTable, (strAddressSiguSequence))


            qryUpdateAuctionSpoolMaster = "DELETE FROM " + ConstRealEstateTable_AUC.CourtAuctionSpoolTable + "  "
            qryUpdateAuctionSpoolMaster += " WHERE seq = %s  "
            cursorRealEstate.execute(qryUpdateAuctionSpoolMaster, (strAddressSiguSequence))

            print("ConstRealEstateTable_AUC.CourtAuctionProgressingMasterTable => PASS ", strUniqueKey)

            # 변경 사항 커밋
            ResRealEstateConnection.commit()

            intProcessLoop += 1
            data_6 = str(intProcessLoop)

            dtTimeDifference = DateTime.now() - TimeDelta(hours=dtNow.hour, minutes=dtNow.minute, seconds=dtNow.second)
            dtWorkingTime = str(dtTimeDifference.strftime('%H:%M:%S'))

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = data_1
            dictSwitchData['data_2'] = data_2
            dictSwitchData['data_3'] = data_3
            dictSwitchData['data_4'] = data_4
            dictSwitchData['data_5'] = data_5
            dictSwitchData['data_6'] = data_6
            dictSwitchData['working_time'] = dtWorkingTime
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            time.sleep(0.005)
        print("for rstSiDoList in rstSiDoLists: END")

        dtTimeDifference = DateTime.now() - TimeDelta(hours=dtNow.hour, minutes=dtNow.minute, seconds=dtNow.second)
        dtWorkingTime = str(dtTimeDifference.strftime('%H:%M:%S'))

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = data_1
        dictSwitchData['data_2'] = data_2
        dictSwitchData['data_3'] = data_3
        dictSwitchData['data_4'] = data_4
        dictSwitchData['data_5'] = data_5
        dictSwitchData['data_6'] = data_6
        dictSwitchData['today_work'] = '1'
        dictSwitchData['working_time'] = dtWorkingTime
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    except Exception as e:

        ResRealEstateConnection.rollback()

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1

        if data_2 is not None:
            dictSwitchData['data_2'] = data_2

        if data_3 is not None:
            dictSwitchData['data_3'] = data_3

        if data_4 is not None:
            dictSwitchData['data_4'] = data_4

        if data_5 is not None:
            dictSwitchData['data_5'] = data_5

        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                     "[=============[SUCCESS END]")


    finally:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                     "[=============[CRONTAB END]")
        cursorRealEstate.close()  # cursor를 닫아줘야합니다.
        return True

if __name__ == '__main__':
    main()
