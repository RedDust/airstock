
import urllib.request, requests
import json
import pymysql
import traceback
import xml
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime as DateTime, timedelta as TimeDelta

from Lib.CustomException.QuitException import QuitException
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog


def makeCookies(strSidoCode):
    try:

        print("========================= makeCookies START")

        dtTimeBefore1Min = DateTime.today() - TimeDelta(seconds=5)
        strTimeStamp = str(dtTimeBefore1Min.timestamp()).replace(".", "")[0:13]

        cookies = {
            'mapGuide': 'Y',
            'mvmPlaceSiguCd': '',
            'roadPlaceSidoCd': '',
            'roadPlaceSiguCd': '',
            'vowelSel': '35207_45207',
            'realVowel': '35207_45207',
            'rd1Cd': '',
            'rd2Cd': '',
            '358': 'Y',
            '365': 'Y',
            'mvmPlaceSidoCd': '',
            'mvJiwonNm': '',
            'page': 'default40',
            'daepyoSiguCd': '',
            'daepyoSidoCd': strSidoCode,
            '375': 'Y',
            'realJiwonNm': '',
            'WMONID': 'M9mPk-A5Xh-',
            'SID': '',
            'cortAuctnLgnMbr': '',
            'wcCookieV2': '211.108.74.37_T_'+strTimeStamp+'_WC',
            'JSESSIONID': 'Amu67hPlSVwkCvgJUSympSqaIKni9wx_eABOatNHzn6OIbGnVM0D!-1743341647',
            'lastAccess': strTimeStamp,
        }


    except Exception as e:

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")

        cookies = False
    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException =>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")

        cookies = False

    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END =>")



    finally:
        print("Finally END")
        return cookies






def makeHeader():
    try:
        print("========================= makeHeader START")

        headers = {
            'Accept': 'application/json',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://www.courtauction.go.kr',
            'Referer': 'https://www.courtauction.go.kr/pgj/index.on?w2xPath=/pgj/ui/pgj100/PGJ151F00.xml',
            'SC-Userid': 'SYSTEM',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'submissionid': 'mf_wfm_mainFrame_sbm_selectGdsDtlSrch',
        }



    except Exception as e:

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")

        headers = False

    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException =>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")

        headers = False

    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END =>")

    finally:
        print("Finally END")
        return headers


def makePlannedHeader():
    try:
        print("========================= makePlannedHeader START")

        headers = {
            'Accept': 'application/json',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://www.courtauction.go.kr',
            'Referer': 'https://www.courtauction.go.kr/pgj/index.on?w2xPath=/pgj/ui/pgj100/PGJ151F00.xml',
            'SC-Userid': 'SYSTEM',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'submissionid': 'mf_wfm_mainFrame_sbm_selectGdsDtlSrch',
        }



    except Exception as e:

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")

        headers = False

    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException =>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")

        headers = False

    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END =>")

    finally:
        print("Finally END")
        return headers


def makeCompleteHeader():
    try:
        print("========================= makeCompleteHeader START")

        headers = {
            'Accept': 'application/json',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://www.courtauction.go.kr',
            'Referer': 'https://www.courtauction.go.kr/pgj/index.on?w2xPath=/pgj/ui/pgj100/PGJ158M00.xml',
            'SC-Userid': 'SYSTEM',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'submissionid': 'mf_wfm_mainFrame_sbm_selectDspslRsltSrch',
        }



    except Exception as e:

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")

        headers = False

    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException =>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")

        headers = False

    else:
        print("========================= makeCompleteHeader SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END =>")

    finally:
        print("makeCompleteHeader Finally END")
        return headers




def makeJsonData(strSidoCode, pageNo):
    try:
        print("========================= makeJsonData START")
        paging = 40
        dtNow = DateTime.today()

        dtBidBgngYmd = dtNow - TimeDelta(days=1)
        bidBgngYmd = dtBidBgngYmd.strftime("%Y%m%d")
        dtBidEndYmd = dtNow + TimeDelta(days=60)
        bidEndYmd = dtBidEndYmd.strftime("%Y%m%d")

        startRowNo = ((pageNo-1) * paging) + 1

        intbfPageNo = str(pageNo - 1)

        json_data = {
            'dma_pageInfo': {
                'pageNo': pageNo,
                'pageSize': paging,
                'bfPageNo': intbfPageNo,
                'startRowNo': startRowNo,
                'totalCnt': '1892',
                'totalYn': 'Y',
                'groupTotalCount': 1526,
            },
            'dma_srchGdsDtlSrchInfo': {
                'rletDspslSpcCondCd': '',
                'bidDvsCd': '',
                'mvprpRletDvsCd': '00031R',
                'cortAuctnSrchCondCd': '0004601',
                'rprsAdongSdCd': strSidoCode,
                'rprsAdongSggCd': '',
                'rprsAdongEmdCd': '',
                'rdnmSdCd': '',
                'rdnmSggCd': '',
                'rdnmNo': '',
                'mvprpDspslPlcAdongSdCd': '',
                'mvprpDspslPlcAdongSggCd': '',
                'mvprpDspslPlcAdongEmdCd': '',
                'rdDspslPlcAdongSdCd': '',
                'rdDspslPlcAdongSggCd': '',
                'rdDspslPlcAdongEmdCd': '',
                'cortOfcCd': 'B000210',
                'jdbnCd': '',
                'execrOfcDvsCd': '',
                'lclDspslGdsLstUsgCd': '',
                'mclDspslGdsLstUsgCd': '',
                'sclDspslGdsLstUsgCd': '',
                'cortAuctnMbrsId': '',
                'aeeEvlAmtMin': '',
                'aeeEvlAmtMax': '',
                'lwsDspslPrcRateMin': '',
                'lwsDspslPrcRateMax': '',
                'flbdNcntMin': '',
                'flbdNcntMax': '',
                'objctArDtsMin': '',
                'objctArDtsMax': '',
                'mvprpArtclKndCd': '',
                'mvprpArtclNm': '',
                'mvprpAtchmPlcTypCd': '',
                'notifyLoc': 'on',
                'lafjOrderBy': '',
                'pgmId': 'PGJ151F01',
                'csNo': '',
                'cortStDvs': '2',
                'statNum': 1,
                'bidBgngYmd': bidBgngYmd,
                'bidEndYmd': bidEndYmd,
                'dspslDxdyYmd': '',
                'fstDspslHm': '',
                'scndDspslHm': '',
                'thrdDspslHm': '',
                'fothDspslHm': '',
                'dspslPlcNm': '',
                'lwsDspslPrcMin': '',
                'lwsDspslPrcMax': '',
                'grbxTypCd': '',
                'gdsVendNm': '',
                'fuelKndCd': '',
                'carMdyrMax': '',
                'carMdyrMin': '',
                'carMdlNm': '',
            },
        }




    except Exception as e:

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")

        json_data = False

    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException =>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")

        json_data = False

    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END =>")



    finally:
        print("Finally END")
        return json_data



def makePlannedCookies(strSidoCode):
    try:

        print("========================= makePlannedCookies START")

        dtTimeBefore1Min = DateTime.today() - TimeDelta(seconds=5)
        strTimeStamp = str(dtTimeBefore1Min.timestamp()).replace(".", "")[0:13]

        cookies = {
            'mapGuide': 'Y',
            'mvmPlaceSiguCd': '',
            'roadPlaceSidoCd': '',
            'roadPlaceSiguCd': '',
            'vowelSel': '35207_45207',
            'realVowel': '35207_45207',
            'rd1Cd': '',
            'rd2Cd': '',
            '358': 'Y',
            '365': 'Y',
            'mvmPlaceSidoCd': '',
            'mvJiwonNm': '',
            'page': 'default40',
            'daepyoSiguCd': '',
            'daepyoSidoCd': strSidoCode,
            '375': 'Y',
            'realJiwonNm': '',
            'WMONID': 'M9mPk-A5Xh-',
            'SID': '',
            'cortAuctnLgnMbr': '',
            'wcCookieV2': '211.108.74.37_T_'+strTimeStamp+'_WC',
            'JSESSIONID': 'Amu67hPlSVwkCvgJUSympSqaIKni9wx_eABOatNHzn6OIbGnVM0D!-1743341647',
            'lastAccess': strTimeStamp,
        }


    except Exception as e:

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")

        cookies = False
    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException =>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")

        cookies = False

    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END =>")



    finally:
        print("Finally END")
        return cookies




def makeCompleteCookies(strSidoCode):
    try:

        print("========================= makePlannedCookies START")

        dtTimeBefore1Min = DateTime.today() - TimeDelta(seconds=5)
        strTimeStamp = str(dtTimeBefore1Min.timestamp()).replace(".", "")[0:13]

        cookies = {
            'pageCnt': '40',
            'mvmPlaceSiguCd': '',
            'roadPlaceSidoCd': '',
            'roadPlaceSiguCd': '',
            'vowelSel': '35207_45207',
            'realVowel': '35207_45207',
            'rd1Cd': '',
            'rd2Cd': '',
            '358': 'Y',
            '365': 'Y',
            'mvmPlaceSidoCd': '',
            'mvJiwonNm': '%3F%3F%3F%EC%A4%3F%3F%EC%A7%3F%B0%A9%EB%B2%3F%3F',
            'page': 'default40',
            'daepyoSiguCd': '',
            'daepyoSidoCd': strSidoCode,
            '375': 'Y',
            'realJiwonNm': '%BC%AD%BF%EF%BA%CF%BA%CE%C1%F6%B9%E6%B9%FD%BF%F8',
            'WMONID': 'M9mPk-A5Xh-',
            'SID': '',
            'cortAuctnLgnMbr': '',
            'lastAccess': strTimeStamp,
            'globalDebug': 'false',
            'JSESSIONID': 'hMPgGId9tezPwNSh1qMRMYVGAEFdVKnqdwraq5nPRSEaTipgiiqD!1903052769',
            'wcCookieV2': '211.108.74.37_T_'+strTimeStamp+'_WC',
        }


    except Exception as e:

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")

        cookies = False
    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException =>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")

        cookies = False

    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END =>")



    finally:
        print("Finally END")
        return cookies




def makePlannedJsonData(strSidoCode, pageNo):
    try:
        print("========================= makePlannedJsonData START")
        paging = 40
        dtNow = DateTime.today()

        dtBidBgngYmd = dtNow + TimeDelta(days=15)
        bidBgngYmd = dtBidBgngYmd.strftime("%Y%m%d")
        dtBidEndYmd = dtNow + TimeDelta(days=60)
        bidEndYmd = dtBidEndYmd.strftime("%Y%m%d")

        startRowNo = ((pageNo-1) * paging) + 1

        intbfPageNo = str(pageNo - 1)



        json_data = {
            'dma_pageInfo': {
                'pageNo': pageNo,
                'pageSize': paging,
                'bfPageNo': intbfPageNo,
                'startRowNo': startRowNo,
                'totalCnt': '5000',
                'totalYn': 'Y',
                'groupTotalCount': 1824,
            },
            'dma_srchGdsDtlSrchInfo': {
                'rletDspslSpcCondCd': '',
                'bidDvsCd': '',
                'mvprpRletDvsCd': '00031R',
                'cortAuctnSrchCondCd': '0004601',
                'rprsAdongSdCd': strSidoCode,
                'rprsAdongSggCd': '',
                'rprsAdongEmdCd': '',
                'rdnmSdCd': '',
                'rdnmSggCd': '',
                'rdnmNo': '',
                'mvprpDspslPlcAdongSdCd': '',
                'mvprpDspslPlcAdongSggCd': '',
                'mvprpDspslPlcAdongEmdCd': '',
                'rdDspslPlcAdongSdCd': '',
                'rdDspslPlcAdongSggCd': '',
                'rdDspslPlcAdongEmdCd': '',
                'cortOfcCd': 'B000210',
                'jdbnCd': '',
                'execrOfcDvsCd': '',
                'lclDspslGdsLstUsgCd': '',
                'mclDspslGdsLstUsgCd': '',
                'sclDspslGdsLstUsgCd': '',
                'cortAuctnMbrsId': '',
                'aeeEvlAmtMin': '',
                'aeeEvlAmtMax': '',
                'lwsDspslPrcRateMin': '',
                'lwsDspslPrcRateMax': '',
                'flbdNcntMin': '',
                'flbdNcntMax': '',
                'objctArDtsMin': '',
                'objctArDtsMax': '',
                'mvprpArtclKndCd': '',
                'mvprpArtclNm': '',
                'mvprpAtchmPlcTypCd': '',
                'notifyLoc': 'on',
                'lafjOrderBy': '',
                'pgmId': 'PGJ151F01',
                'csNo': '',
                'cortStDvs': '2',
                'statNum': 1,
                'bidBgngYmd': bidBgngYmd,
                'bidEndYmd': bidEndYmd,
                'dspslDxdyYmd': '',
                'fstDspslHm': '',
                'scndDspslHm': '',
                'thrdDspslHm': '',
                'fothDspslHm': '',
                'dspslPlcNm': '',
                'lwsDspslPrcMin': '',
                'lwsDspslPrcMax': '',
                'grbxTypCd': '',
                'gdsVendNm': '',
                'fuelKndCd': '',
                'carMdyrMax': '',
                'carMdyrMin': '',
                'carMdlNm': '',
            },
        }




    except Exception as e:

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")

        json_data = False

    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException =>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")

        json_data = False

    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END =>")



    finally:
        print("Finally END")
        return json_data





def makeCompleteJsonData(strSidoCode, pageNo):
    try:
        print("========================= makePlannedJsonData START")
        paging = 40
        dtNow = DateTime.today()

        dtBidBgngYmd = dtNow + TimeDelta(days=15)
        bidBgngYmd = dtBidBgngYmd.strftime("%Y%m%d")
        dtBidEndYmd = dtNow + TimeDelta(days=60)
        bidEndYmd = dtBidEndYmd.strftime("%Y%m%d")

        startRowNo = ((pageNo-1) * paging) + 1

        intbfPageNo = str(pageNo - 1)



        json_data = {
            'dma_pageInfo': {
                'pageNo': pageNo,
                'pageSize': paging,
                'bfPageNo': intbfPageNo,
                'startRowNo': startRowNo,
                'totalCnt': '5000',
                'totalYn': 'Y',
                'groupTotalCount': 1824,
            },
            'dma_srchGdsDtlSrchInfo': {
                'statNum': '3',
                'pgmId': 'PGJ158M01',
                'cortStDvs': '2',
                'cortOfcCd': 'B000210',
                'jdbnCd': '',
                'csNo': '',
                'rprsAdongSdCd': strSidoCode,
                'rprsAdongSggCd': '',
                'rprsAdongEmdCd': '',
                'rdnmSdCd': '',
                'rdnmSggCd': '',
                'rdnmNo': '',
                'auctnGdsStatCd': '',
                'lclDspslGdsLstUsgCd': '',
                'mclDspslGdsLstUsgCd': '',
                'sclDspslGdsLstUsgCd': '',
                'dspslAmtMin': '',
                'dspslAmtMax': '',
                'aeeEvlAmtMin': '',
                'aeeEvlAmtMax': '',
                'flbdNcntMin': '',
                'flbdNcntMax': '',
                'lafjOrderBy': '',
            },


        }




    except Exception as e:

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")

        json_data = False

    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException =>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")

        json_data = False

    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END =>")



    finally:
        print("Finally END")
        return json_data










def makePlannedJsonData2(strSidoCode, pageNo):
    json_data = {
        'dma_srchGdsDtlSrchInfo': {
            'statNum': '1',
            'cortAuctnMbrsId': '',
            'pgmId': 'PGJ157M01',
            'lafjOrderBy': '',
            'bidDvsCd': '',
            'cortAuctnSrchCondCd': '0004602',
            'cortStDvs': '2',
            'cortOfcCd': 'B000210',
            'jdbnCd': '',
            'csNo': '',
            'rprsAdongSdCd': strSidoCode,
            'rprsAdongSggCd': '',
            'rprsAdongEmdCd': '',
            'rdnmSdCd': '',
            'rdnmSggCd': '',
            'rdnmNo': '',
            'lclDspslGdsLstUsgCd': '',
            'mclDspslGdsLstUsgCd': '',
            'sclDspslGdsLstUsgCd': '',
            'aeeEvlAmtMin': '',
            'aeeEvlAmtMax': '',
            'rletLwsDspslPrcMin': '',
            'rletLwsDspslPrcMax': '',
            'lwsDspslPrcRateMin': '',
            'lwsDspslPrcRateMax': '',
            'flbdNcntMin': '',
            'flbdNcntMax': '',
            'objctArDtsMin': '',
            'objctArDtsMax': '',
            'bidBgngYmd': '20250215',
            'bidEndYmd': '20250401',
        },
        'dma_pageInfo': {
            'pageNo': pageNo,
            'pageSize': '40',
            'bfPageNo': 1,
            'startRowNo': 2761,
            'totalCnt': '2794',
            'totalYn': 'N',
            'groupTotalCount': 1824,
        },
    }

    return json_data