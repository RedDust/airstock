#해당프로그램은 사용하지 않습니다.

quit("PROCESS END")

import get_1_seoul_bus_data_with_thread_child
import math
import traceback
import datetime
import urllib.request
import json
from multiprocessing import Process

from Init.Functions.Logs import GetLogDef
from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.CustomException import QuitException


if __name__ == '__main__':


    print( "START======================================================================================================")

    # nTotal = 46061
    dtToday = DateTime.today()
    nBaseProcessDate = 0

    end_date = dtToday - TimeDelta(days=nBaseProcessDate)
    nBaseDate = int(end_date.strftime('%Y%m%d'))
    nStartNumber = 1
    nEndNumber = 2

    GTest = 0

    dtToday = datetime.datetime(2021, 12, 31, 00, 00, 00)
    end_date = dtToday - TimeDelta(days=nBaseProcessDate)
    nFinalDate = 20201231
    nBaseDate = int(end_date.strftime('%Y%m%d'))
    print(GetLogDef.lineno(__file__), type(end_date), end_date )


    # url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/CardBusStatisticsServiceNew/" + str(nStartNumber) + "/" + str(nEndNumber) + "/" + str(nBaseDate)
    # print(GetLogDef.lineno(__file__), "url > ", url)
    #
    # # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
    # response = urllib.request.urlopen(url)
    # json_str = response.read().decode("utf-8")
    #
    # # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
    # json_object = json.loads(json_str)
    #
    # print(GetLogDef.lineno(__file__), "json_object.get('RESULT')  > ", json_object.get('RESULT'))
    #
    # # 조회된 데이터가 "있으면" json_object.get('RESULT') => None
    # if json_object.get('RESULT') != None:
    #     print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('CODE'))
    #     print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('MESSAGE'))
    #     Exception(GetLogDef.lineno(__file__))  # 예외를 발생시킴
    #
    # bMore = json_object.get('CardBusStatisticsServiceNew')
    # if bMore is None:
    #     Exception(GetLogDef.lineno(__file__))  # 예외를 발생시킴
    #
    # nTotalCount = bMore.get('list_total_count')
    # print("nTotalCount > ", nTotalCount)

    nTotal = 46061

    nProcessingPerCount = 1000

    nLoopCount = math.ceil(nTotal / nProcessingPerCount)



    print(GetLogDef.lineno(), "nTotal >", type(nTotal), nTotal)
    print(GetLogDef.lineno(), "nProcessingPerCount >", type(nProcessingPerCount), nProcessingPerCount)
    print(GetLogDef.lineno(), "nLoopCount >", type(nLoopCount), nLoopCount)

    # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
    numbers = [5, 10, 15, 20, 25]

    procs = []
    for nLoop in range(0, nLoopCount):
        # print(GetLogDef.lineno(), "nLoop >", type(nLoop), nLoop)

        nStartCount = 1 + (nLoop * nProcessingPerCount)
        nEndCount = ((nLoop+1) * nProcessingPerCount)


        # print(GetLogDef.lineno(), "nStartCount >", type(nStartCount), nStartCount)
        # print(GetLogDef.lineno(), "nEndCount >", type(nEndCount), nEndCount)

        #thread 생성(각 숫자를 5회 출력)

        # locals()['Th_{}'.format(nLoop)] = nLoop


        Multiprocess = Process(target=get_1_seoul_bus_data_with_thread_child.test_thread, args=(nStartCount, nEndCount))
        # thread = threading.Thread(target=get_1_seoul_bus_data_with_thread_child.test_thread, args=(nStartCount, nEndCount))
        # # print(GetLogDef.lineno(), "[", nLoop,"]===============================================================================")
        procs.append(Multiprocess)
        Multiprocess.start()

        print(procs)

    for Multiprocess in procs:
        Multiprocess.join()


    print("END======================================================================================================")

