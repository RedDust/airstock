# from datetime import datetime

from datetime import datetime as DateTime, date as DateTimeDate , timedelta as TimeDelta
from Lib.GeoDataModule import GeoDataModule
from Init.Functions.Logs import GetLogDef
import inspect, pymysql, logging
import requests
import traceback
from Realty.Auction.Const import ConstRealEstateTable_AUC
from Lib.RDB import pyMysqlConnector



import Realty.Auction.AuctionLib.AuctionDataDecode as AuctionDataDecode

import Realty.Government.MolitLib.GetRoadNameJuso as GetRoadNameJuso

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
    filename='D:/PythonProjects/airstock/Shell/logs/test_get_auction_address_' + logFileName,
    maxBytes=log_max_size,
    backupCount=log_file_count
)

rotatingFileHandler.setFormatter(formatter)
# RotatingFileHandler
timeFileHandler = logging.handlers.TimedRotatingFileHandler(
    filename='D:/PythonProjects/airstock/Shell/logs/test_get_auction_address_' + logFileName,
    when='midnight',
    interval=1,
    encoding='utf-8'
)
timeFileHandler.setFormatter(formatter)
logger.addHandler(streamingHandler)
logger.addHandler(timeFileHandler)
















#
#
# # DB 연결 선언
# ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
# cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
#
# issue_number_text = '["서울중앙지방법원", "2023타경113800"]'
#
# sqlCourtAuctionSelect = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable
# sqlCourtAuctionSelect += " ORDER BY seq ASC "
# sqlCourtAuctionSelect += " LIMIT 1 "
#
#
#
# cursorRealEstate.execute(sqlCourtAuctionSelect)
# nMasterResultCount = cursorRealEstate.rowcount
# rstAddressLists = cursorRealEstate.fetchall()
#
# for rstAddressList in rstAddressLists:
#
#     issue_number_text = str(rstAddressList.get('issue_number_text'))
#     strTextAddress = str(rstAddressList.get('address_data_text'))
#
#
#     strIssueNumber = AuctionDataDecode.DecodeIssueNumber(issue_number_text)
#
#     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                             inspect.getframeinfo(inspect.currentframe()).lineno), len(strIssueNumber),
#           strIssueNumber)
#
#     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                             inspect.getframeinfo(inspect.currentframe()).lineno), len(strTextAddress),
#           strTextAddress)
#
#
#     Return = GetRoadNameJuso.GetJusoApiForAddress(logging,strIssueNumber, strTextAddress)
#
#
#     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                             inspect.getframeinfo(inspect.currentframe()).lineno), len(Return),
#           Return)


# strAddress = '["서울특별시 강북구  인수봉로42길 36, 에이동 제4층 제402호 (수유동,초원빌리지)"]'
# strAddress = '["서울특별시 도봉구 쌍문동  716-39", " 서울특별시 도봉구  도봉로137길 9"]'
#
# # ReturnRoadAddressText = GetRoadNameJuso.CustomiseAddressText('["서울특별시 강북구 인수봉로61길12-11,3층302호(수유동,예움하우스)"]')
# strIssueNumber = '["서울북부지방법원", "2020타경6405", "2020타경24652020타경57852020타경6481(중복)"]'
#
#
# listExtractionAddressTexts = GetRoadNameJuso.CustomiseAddressText(strAddress)
# if len(listExtractionAddressTexts) < 1:
#     # 주소가 없어서 오류
#     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                             inspect.getframeinfo(inspect.currentframe()).lineno), len(listExtractionAddressTexts),
#           listExtractionAddressTexts)
#
# else:
#     print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                         inspect.getframeinfo(inspect.currentframe()).lineno), len(listExtractionAddressTexts), listExtractionAddressTexts)
#
#     for listExtractionAddressText in listExtractionAddressTexts:
#         print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                                 inspect.getframeinfo(inspect.currentframe()).lineno), len(listExtractionAddressText),
#               listExtractionAddressText)
#
#         ReturnRoadAddress = GetRoadNameJuso.GetJusoApiForAddress(strIssueNumber, listExtractionAddressText)
#         print("==============================================")
#
#         print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                                 inspect.getframeinfo(inspect.currentframe()).lineno), len(ReturnRoadAddress),
#               ReturnRoadAddress)
#
#

#
# ReturnRoadAddress  = GetRoadNameJuso.GetJusoApiForAddress('서울특별시 관악구 신림동 87-92 어반팰리스')
#
# print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                         inspect.getframeinfo(inspect.currentframe()).lineno), len(ReturnRoadAddress), ReturnRoadAddress)
#
# # date_str = '2023-11-23 00:00:00'
# # date_format = '%Y-%m-%d %H:%M:%S'
#
# date_obj = DateTime.strptime(date_str, date_format)
#
# dtNow = DateTime.today()
# print(dtNow.year, type(dtNow.year))
# print(dtNow.month, type(dtNow.month))
# print(dtNow.day, type(dtNow.day))
# print(dtNow, type(dtNow))
#
# logFileName = str(dtNow.year) + str(dtNow.month).zfill(2) + str(dtNow.day).zfill(2) + ".log"
#
# print(logFileName, type(logFileName))
# print("=============================================================")
# dBaseIssueDatetime = DateTimeDate(2023, 4, 11)
#
# strAuctionDayformat = '%Y-%m-%d %H:%M:%S'
# objAuctionDay = DateTime.strptime(date_str, strAuctionDayformat)
#
# print(objAuctionDay.year, type(objAuctionDay.year))
# print(objAuctionDay.month, type(objAuctionDay.month))
# print(objAuctionDay.day, type(objAuctionDay.day))
# print(objAuctionDay, type(objAuctionDay))
#
#
#
# strBaseYYYY = str(dBaseIssueDatetime.year).zfill(4)
# strBaseMM = str(dBaseIssueDatetime.month).zfill(2)
# strBaseDD = str(dBaseIssueDatetime.day).zfill(2)
#
# strTradeDBMasterSGG_NM = '강서구'
# strTradeDBMasterBJDONG_NM = '화곡동'
# strTradeDBMasterBONBEON = str('0159').lstrip("0")
# strTradeDBMasterBUBEON = str('0024').lstrip("0")
#
#
# strDOROJUSO = "서울특별시 "
# strDOROJUSO += strTradeDBMasterSGG_NM + " "
# strDOROJUSO += strTradeDBMasterBJDONG_NM + " "
# strDOROJUSO += strTradeDBMasterBONBEON + "-"
# strDOROJUSO += strTradeDBMasterBUBEON + " "
#
# resultsDict = GeoDataModule.getJusoData(strDOROJUSO)
# print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
#                                        inspect.getframeinfo(inspect.currentframe()).lineno), str(resultsDict['jibunAddr'].strip()))
#
# # resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
#
# print("roadAddrPart1" , resultsDict['roadAddrPart1'])
# print("jibunAddr" , resultsDict['jibunAddr'])
# print("siNm" , resultsDict['siNm'])
# print("sggNm" , resultsDict['sggNm'])
# print("emdNm" , resultsDict['emdNm'])
# print("lnbrMnnm" , resultsDict['lnbrMnnm'])
# print("lnbrSlno" , resultsDict['lnbrSlno'])


