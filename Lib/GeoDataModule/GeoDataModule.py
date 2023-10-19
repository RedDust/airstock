
import requests
import traceback

from typing import Dict, Union, Optional

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.CustomException.QuitException import QuitException




def getNaverGeoData(TextAddress):

    try:
        print("getNaverGeoData START====================================")

        returnDict = dict()

        strJiBunAddress = ''
        strLongitude = '000.00000000'  # 127
        strLatitude = '000.00000000'  # 37

        print(GetLogDef.lineno(__file__), "getNaverGeoData TextAddress 26 >> ", type(TextAddress), len(TextAddress), TextAddress)
        if len(TextAddress) < 10:
            print(GetLogDef.lineno(__file__), "TextAddress is None")
            QuitException(GetLogDef.lineno(__file__))

        strTextAddress = (TextAddress)
        print(GetLogDef.lineno(__file__), "getNaverGeoData strTextAddress 32 >> ", strTextAddress)

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
        print(GetLogDef.lineno(__file__), "results", results)
        print("$$$$$$$$$$$$$$$$$$$$$$$>>" , results , results['status'])
        if results['status'] != 'OK':

            print(GetLogDef.lineno(__file__), "results 50 >>>", results)

            # return False
            raise QuitException(GetLogDef.lineno(__file__) + " > " + results['status'] ) # 예외를 발생시킴

        dictMeta = results['meta']
        if dictMeta['count'] < 1:
            raise QuitException(GetLogDef.lineno(__file__) + " > " + dictMeta['count'])  # 예외를 발생시킴

        dictAddresses = results['addresses']
        print(GetLogDef.lineno(__file__), "dictAddresses 50 >>>", dictAddresses)


        listFirstAddress = dictAddresses[0]
        strJiBunAddress = listFirstAddress['jibunAddress']
        strLongitude = listFirstAddress['x']  # 127
        strLatitude = listFirstAddress['y']  # 37

        returnDict['address_name'] = strJiBunAddress
        returnDict['x'] = strLongitude
        returnDict['y'] = strLatitude

        print(GetLogDef.lineno(__file__), "returnDict 500 >>>", returnDict)

        print(GetLogDef.lineno(__file__), "getNaverGeoData SUCCESS ============================================================")
        return returnDict

    except QuitException as e:

        print(GetLogDef.lineno(__file__), "getNaverGeoData QuitException======================================")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))
        return False

    except Exception as e:
        print(GetLogDef.lineno(__file__), "getNaverGeoData Exception======================================")
        print(GetLogDef.lineno(__file__), e, type(e))
        err_msg = traceback.format_exc()
        print(err_msg)
        return False
    else:
        print(GetLogDef.lineno(__file__), "getNaverGeoData SUCCESS ============================================================")

    finally:
        print("getNaverGeoData END")

def getKakaoGeoData(TextAddress):


    try:

        print("getKakaoGeoData START====================================")

        returnDict = dict()

        strJiBunAddress = ''
        strLongitude = '000.00000000'  # 127
        strLatitude = '000.00000000'  # 37
        nProcessStep = 13

        print(GetLogDef.lineno(__file__), "getKakaoGeoData TextAddress 100>> ", type(TextAddress), len(TextAddress), TextAddress)
        if len(TextAddress) < 10:
            print(GetLogDef.lineno(__file__), "TextAddress is None")
            raise QuitException(GetLogDef.lineno(__file__))

        strTextAddress = (TextAddress)

        url = 'https://dapi.kakao.com/v2/local/search/address.json'
        rest_api_key = "e68350f56ddf2d5c7bf17964098301d9"

        header = {'Authorization': 'KakaoAK ' + rest_api_key}

        print(GetLogDef.lineno(__file__), "getKakaoGeoData strTextAddress 112>>", type(strTextAddress), strTextAddress)
        params = dict(query=strTextAddress, analyze_type='exact')

        response = requests.get(
            "https://dapi.kakao.com/v2/local/search/address.json",
            params=params,
            headers=header
        )
        print(GetLogDef.lineno(__file__), response)
        print(GetLogDef.lineno(__file__), "================================================================")
        kakaoResults = response.json()
        print(GetLogDef.lineno(__file__), kakaoResults)
        listKakaoDocuments = kakaoResults['documents']

        print(GetLogDef.lineno(__file__), "listKakaoDocuments>", type(listKakaoDocuments), len(listKakaoDocuments),
              listKakaoDocuments)

        if len(listKakaoDocuments) < 1:
            return False
            # raise QuitException.QuitException(GetLogDef.lineno(__file__))


        listDocuments = listKakaoDocuments[0]['address']
        print(GetLogDef.lineno(__file__), "listDocuments>", type(listDocuments), listDocuments)

        strJiBunAddress = listDocuments['address_name']
        strLongitude = listDocuments['x']
        strLatitude = listDocuments['y']


        returnDict['address_name'] = strJiBunAddress
        returnDict['x'] = strLongitude
        returnDict['y'] = strLatitude

        return returnDict

    except QuitException as e:

        print(GetLogDef.lineno(__file__), "QuitException")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))

        return False

    except Exception as e:

        print(GetLogDef.lineno(__file__), "Error Exception")
        print(GetLogDef.lineno(__file__), e, type(e))
        err_msg = traceback.format_exc()
        print(err_msg)
        return False

    else:
        print(GetLogDef.lineno(__file__), "============================================================")

    finally:
        print("getKakaoGeoData END")