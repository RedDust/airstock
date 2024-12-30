
import requests
import traceback
import logging
import logging.handlers
import inspect

from typing import Dict, Union, Optional
from urllib import parse
from Realty.Government.Init import init_conf
import urllib.request
import json
import pymysql

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.CustomException.QuitException import QuitException
from datetime import datetime as DateTime, timedelta as TimeDelta

def getJusoData( TextAddress):

    try:

        print(GetLogDef.lineno(__file__) , "getJusoData START====================================")

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) , "TextAddress=>" , TextAddress)
        strNewEncodeCostomiseKeyword = parse.quote(TextAddress)

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) , "TextAddress=>" , strNewEncodeCostomiseKeyword)


        strModifyUrl  = "https://business.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=1"
        strModifyUrl += "&keyword=" + strNewEncodeCostomiseKeyword + ""
        strModifyUrl += "&confmKey=" + str(init_conf.strJusoConsAuthKey) + "&hstryYn=Y&resultType=json"

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[strModifyUrl] => " + str(strModifyUrl))

        ModifyResponse = urllib.request.urlopen(strModifyUrl)

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), ModifyResponse)

        jsonModifyResponse = ModifyResponse.read().decode("utf-8")
        dictModifyResponse = json.loads(jsonModifyResponse)

        dictModifydictGetURLResult = (dictModifyResponse['results']['common'])
        strModifyCurrentPage = str(dictModifydictGetURLResult['currentPage'])
        intModifyResultTotalCount = int(dictModifydictGetURLResult['totalCount'])
        strModifyResultErrorCode = str(dictModifydictGetURLResult['errorCode'])

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), dictModifyResponse)

        # if intModifyResultTotalCount > 1:
        #     raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno) + "intModifyResultTotalCount > " + str(intModifyResultTotalCount) )

        ListResultJusos = (dictModifyResponse['results']['juso'])
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[ListResultJusos] => " + str(ListResultJusos))


        returnDict = dict()
        returnDictLoop = 0
        for ListResultJuso in ListResultJusos:
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), ListResultJuso)
            returnDict[returnDictLoop] = ListResultJuso
            returnDictLoop += 1

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) ,
                     "[returnDict[0]] => " , str(returnDict[0]))
        return returnDict[0]

    except Exception as e:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno),
              "getJusoData Exception======================================")
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), e, type(e))
        err_msg = traceback.format_exc()
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), err_msg)
        return False
    else:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno),
              "getJusoData SUCCESS ============================================================")

    finally:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getJusoData END")


def getNaverGeoData(TextAddress):


    try:

        print(GetLogDef.lineno(__file__) , "getNaverGeoData START====================================")

        returnDict = dict()

        strJiBunAddress = ''
        strLongitude = '000.00000000'  # 127
        strLatitude = '000.00000000'  # 37

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverGeoData TextAddress 26 ", type(TextAddress), len(TextAddress), TextAddress)

        if len(TextAddress) < 10:
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "TextAddress is None")

            raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno) + "TextAddress is None" )


        strTextAddress = (TextAddress)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverGeoData strTextAddress 32 >> ", strTextAddress)

        NAVER_API_URL: str = 'https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode'
        NAVER_API_HEADERS: Dict[str, str] = {
            'X-NCP-APIGW-API-KEY-ID': 'idgprv275x',
            'X-NCP-APIGW-API-KEY': 'Ux9SxxEJiynvKt2CwKgfEOfXn6tA3ksyoVa4rAUs'
        }

        response = requests.get(
            NAVER_API_URL,
            params={'query': strTextAddress},
            headers=NAVER_API_HEADERS
        )

        results = response.json()
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "results", results)
        print("$$$$$$$$$$$$$$$$$$$$$$$>>" , results , results['status'])
        if results['status'] != 'OK':

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "results 50 >>>", results)

            # return False
            raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno) + " > " + str(results['status']) ) # 예외를 발생시킴

        dictMeta = results['meta']
        if dictMeta['count'] < 1:
            raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno) + " > " + str(dictMeta['count']))  # 예외를 발생시킴

        dictAddresses = results['addresses']
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "dictAddresses 50 >>>", dictAddresses)


        listFirstAddress = dictAddresses[0]
        strJiBunAddress = listFirstAddress['jibunAddress']
        strRoadAddress= listFirstAddress['roadAddress']
        strLongitude = listFirstAddress['x']  # 127
        strLatitude = listFirstAddress['y']  # 37


        returnDict['address_name'] = strJiBunAddress
        returnDict['road_name'] = strRoadAddress
        returnDict['x'] = strLongitude
        returnDict['y'] = strLatitude

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "returnDict 500 >>>", returnDict)

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverGeoData SUCCESS ============================================================")

        return returnDict

    except QuitException as e:

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverGeoData QuitException======================================")
        err_msg = traceback.format_exc()
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), err_msg)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), e)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno),type(e))
        return False

    except Exception as e:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverGeoData Exception======================================")
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), e, type(e))
        err_msg = traceback.format_exc()
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), err_msg)
        return False
    else:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverGeoData SUCCESS ============================================================")

    finally:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverGeoData END")


def getNaverReverseGeoData(logging,lng,  lat):

    try:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverReverseGeoData START====================================")

        returnDict = dict()
        strLongitude = '000.00000000'  # 127
        strLatitude = '000.00000000'  # 37
        dictAddressReult = dict()

        strJiBunAddress = ''
        strLongitude = '000.00000000'  # 127
        strLatitude = '000.00000000'  # 37

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverReverseGeoData lat >> ", type(lat), len(lat), lat)

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[getNaverReverseGeoData.lat] => " + str(lat))
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[getNaverReverseGeoData.lng] => " + str(lng))


        if len(lat) < 5:
            raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno))

        if len(lng) < 5:
            raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno))

        strTextAddress = (lng + "," + lat)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverReverseGeoData strTextAddress >> ", strTextAddress)
        # strTextAddress = "127.029478,37.509811"
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverReverseGeoData strTextAddress >> ", strTextAddress)

        NAVER_API_URL: str = 'https://naveropenapi.apigw.ntruss.com/map-reversegeocode/v2/gc'
        NAVER_API_HEADERS: Dict[str, str] = {
            'X-NCP-APIGW-API-KEY-ID': 'idgprv275x',
            'X-NCP-APIGW-API-KEY': 'Ux9SxxEJiynvKt2CwKgfEOfXn6tA3ksyoVa4rAUs'
        }


        response = requests.get(
            NAVER_API_URL,
            params={
                    'request': 'coordsToaddr',
                    'coords': strTextAddress,
                    'sourcecrs': 'epsg:4326',
                    'output': 'json',
                    'orders': 'addr'
                    },
            headers=NAVER_API_HEADERS
        )

        results = response.json()

        # print(results)
        #
        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "results", results)

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[getNaverReverseGeoData.results] => " + str(results))

        #{'status': {'code': 0, 'name': 'ok', 'message': 'done'},

        if results['status']['name'] != 'ok':
            # return False
            raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno) + "getNaverReverseGeoData  > " + results['status'] ) # 예외를 발생시킴


        listGeoDecodeResults = results['results']
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[listGeoDecodeResults] => " +str(len(listGeoDecodeResults)) +  str(listGeoDecodeResults))

        # print("dictMeta" , type(listGeoDecodeResults) , listGeoDecodeResults)

        if len(listGeoDecodeResults) != 1:
            raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) + "getNaverReverseGeoData  > " + str(len(listGeoDecodeResults)) )

        else:

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverReverseGeoData > " , type(listGeoDecodeResults) , listGeoDecodeResults)



            for listGeoDecodeResult in listGeoDecodeResults:


                dictAddressesRegions = listGeoDecodeResult['region']
                dictAddressesLands  = listGeoDecodeResult['land']

                if 'name' not in dictAddressesLands:
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(
                                                       inspect.currentframe()).lineno) + "[getNaverReverseGeoData]> name" )
                    dictAddressesLands['name'] = ''


                if 'number2' not in dictAddressesLands:
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(
                                                       inspect.currentframe()).lineno) + "[getNaverReverseGeoData]> number2")
                    dictAddressesLands['number2'] = ''



                if 'area1' not in dictAddressesRegions:
                    raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                      inspect.getframeinfo(inspect.currentframe()).lineno) + "[getNaverReverseGeoData]>area1 ")

                if 'area2' not in dictAddressesRegions:
                    raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                      inspect.getframeinfo(inspect.currentframe()).lineno) + "[getNaverReverseGeoData]> area2")

                if 'area3' not in dictAddressesRegions:
                    raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                      inspect.getframeinfo(inspect.currentframe()).lineno) + "[getNaverReverseGeoData]> area3")

                if 'number1' not in dictAddressesLands:
                    raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                      inspect.getframeinfo(inspect.currentframe()).lineno) + "[getNaverReverseGeoData]> number1")


                dictAddressReult['latitude'] = lat
                dictAddressReult['longitude'] = lng
                dictAddressReult['dosi_name'] = str(dictAddressesRegions['area1']['name'])#서울특별시
                dictAddressReult['sigu_name'] = str(dictAddressesRegions['area2']['name'])#강남구
                dictAddressReult['dong_name'] = str(dictAddressesRegions['area3']['name'])#논현동
                dictAddressReult['bone_bun'] = str(dictAddressesLands['number1'])#157
                dictAddressReult['bu_bun']  = str(dictAddressesLands['number2'])#1
                dictAddressReult['road_name'] = str(dictAddressesLands['name'])#도로명주소



    except Exception as e:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno)+ "getNaverReverseGeoData Exception======================================")
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno) + str(e) )
        err_msg = traceback.format_exc()
        logging.info(err_msg)
        return False
    else:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno) + "getNaverReverseGeoData SUCCESS ============================================================")
        return dictAddressReult
    finally:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getNaverReverseGeoData END")


def getKakaoGeoData(TextAddress):
    #
    #
    #
    # dtNow = DateTime.today()
    # # print(dtNow.hour)
    # # print(dtNow.minute)
    # # print(dtNow)
    #
    # logFileName = str(dtNow.year) + str(dtNow.month) + str(dtNow.day) + ".log"
    #
    # logger = logging.getLogger()
    # logger.setLevel(logging.DEBUG)
    #
    # formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')
    #
    # streamingHandler = logging.StreamHandler()
    # streamingHandler.setFormatter(formatter)
    #
    # # RotatingFileHandler
    # log_max_size = 10 * 1024 * 1024  ## 10MB
    # log_file_count = 20
    # rotatingFileHandler = logging.handlers.RotatingFileHandler(
    #     filename='D:/PythonProjects/airstock/Shell/logs/020101_' + logFileName,
    #     maxBytes=log_max_size,
    #     backupCount=log_file_count
    # )
    # rotatingFileHandler.setFormatter(formatter)
    # # RotatingFileHandler
    # timeFileHandler = logging.handlers.TimedRotatingFileHandler(
    #     filename='D:/PythonProjects/airstock/Shell/logs/020101_' + logFileName,
    #     when='midnight',
    #     interval=1,
    #     encoding='utf-8'
    # )
    # timeFileHandler.setFormatter(formatter)
    # logger.addHandler(streamingHandler)
    # logger.addHandler(timeFileHandler)



    # dtNow = DateTime.today()
    # # print(dtNow.hour)
    # # print(dtNow.minute)
    # # print(dtNow)
    #
    # logFileName = str(dtNow.year) + str(dtNow.month) + str(dtNow.day) + ".log"
    #
    # logger = logging.getLogger()
    # logger.setLevel(logging.DEBUG)
    #
    # formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')
    #
    # streamingHandler = logging.StreamHandler()
    # streamingHandler.setFormatter(formatter)
    #
    # # RotatingFileHandler
    # log_max_size = 10 * 1024 * 1024  ## 10MB
    # log_file_count = 20
    # rotatingFileHandler = logging.handlers.RotatingFileHandler(
    #     filename='D:/PythonProjects/airstock/Shell/logs/020101_' + logFileName,
    #     maxBytes=log_max_size,
    #     backupCount=log_file_count
    # )
    # rotatingFileHandler.setFormatter(formatter)
    # # RotatingFileHandler
    # timeFileHandler = logging.handlers.TimedRotatingFileHandler(
    #     filename='D:/PythonProjects/airstock/Shell/logs/020101_' + logFileName,
    #     when='midnight',
    #     interval=1,
    #     encoding='utf-8'
    # )
    # timeFileHandler.setFormatter(formatter)
    # logger.addHandler(streamingHandler)
    # logger.addHandler(timeFileHandler)


    try:

        print("getKakaoGeoData START====================================")

        returnDict = dict()

        strJiBunAddress = ''
        strLongitude = '000.00000000'  # 127
        strLatitude = '000.00000000'  # 37
        nProcessStep = 13

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getKakaoGeoData TextAddress 100>> ", type(TextAddress), len(TextAddress), TextAddress)


        if len(TextAddress) < 10:
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "TextAddress is None")
            raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno))

        strTextAddress = (TextAddress)

        url = 'https://dapi.kakao.com/v2/local/search/address.json'
        rest_api_key = "e68350f56ddf2d5c7bf17964098301d9"

        header = {'Authorization': 'KakaoAK ' + rest_api_key}

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "getKakaoGeoData strTextAddress 112>>", type(strTextAddress), strTextAddress)
        params = dict(query=strTextAddress, analyze_type='exact')

        response = requests.get(
            "https://dapi.kakao.com/v2/local/search/address.json",
            params=params,
            headers=header
        )
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), response)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "================================================================")
        kakaoResults = response.json()
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), kakaoResults)
        listKakaoDocuments = kakaoResults['documents']

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "listKakaoDocuments>", type(listKakaoDocuments), len(listKakaoDocuments),
              listKakaoDocuments)

        if len(listKakaoDocuments) < 1:
            return False
            # raise QuitException.QuitException(GetLogDef.lineno(__file__))


        listDocuments = listKakaoDocuments[0]['address']
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "listDocuments>", type(listDocuments), listDocuments)

        strJiBunAddress = listDocuments['address_name']
        strLongitude = listDocuments['x']
        strLatitude = listDocuments['y']


        returnDict['address_name'] = strJiBunAddress
        returnDict['x'] = strLongitude
        returnDict['y'] = strLatitude

        return returnDict

    except QuitException as e:

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "QuitException")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))

        return False

    except Exception as e:

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "Error Exception")
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), e, type(e))
        err_msg = traceback.format_exc()
        print(err_msg)
        return False

    else:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "============================================================")

    finally:
        print("getKakaoGeoData END")