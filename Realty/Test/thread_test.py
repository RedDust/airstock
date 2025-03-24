quit("500")

import threading
import thread_test_child
import math

from Init.Functions.Logs import GetLogDef

# # 첫번째 실행할 쓰레드 함수
# def first_thread(num):
#     print("first_thread function START")
#     for i in range(num):
#         thread_test_child.test_thread(num)
#     print("first_thread function END")
#
# # 두번째 실행할 쓰레드 함수
# def second_thread(num):
#     print("second_thread function START")
#     for i in range(num):
#         thread_test_child.test_thread(num)
#     print("second_thread function END")
#





nTotal = 46061





nProcessingPerCount = 1000

nLoopCount = math.ceil(nTotal / nProcessingPerCount)
print(GetLogDef.lineno(), "nTotal >", type(nTotal), nTotal)
print(GetLogDef.lineno(), "nProcessingPerCount >", type(nProcessingPerCount), nProcessingPerCount)
print(GetLogDef.lineno(), "nLoopCount >", type(nLoopCount), nLoopCount)


for nLoop in range(0, nLoopCount):
    print(GetLogDef.lineno(), "nLoop >", type(nLoop), nLoop)

    nStartCount = 1 + (nLoop * nProcessingPerCount)
    nEndCount = ((nLoop+1) * nProcessingPerCount)


    # print(GetLogDef.lineno(), "nStartCount >", type(nStartCount), nStartCount)
    # print(GetLogDef.lineno(), "nEndCount >", type(nEndCount), nEndCount)

    #thread 생성(각 숫자를 5회 출력)

    thread = threading.Thread(target=thread_test_child.test_thread, args=(nStartCount ,nEndCount))
    # print(GetLogDef.lineno(), "thread >", type(thread), thread)
    print(GetLogDef.lineno(), "[", nLoop,"]===============================================================================")

    thread.start()


    # thread2 = threading.Thread(target=thread_test_child.test_thread, args=(12,))
    # thread3 = threading.Thread(target=thread_test_child.test_thread, args=(12,))
    # thread4 = threading.Thread(target=thread_test_child.test_thread, args=(12,))
    # thread5 = threading.Thread(target=thread_test_child.test_thread, args=(12,))
    # thread6 = threading.Thread(target=thread_test_child.test_thread, args=(12,))
    # thread7 = threading.Thread(target=thread_test_child.test_thread, args=(12,))
    # thread8 = threading.Thread(target=thread_test_child.test_thread, args=(12,))


    # thread 시작
    # thread1.start()
    # thread2.start()
    # thread3.start()
    # thread4.start()
    # thread5.start()
    # thread6.start()
    # thread7.start()
    # thread8.start()




# thread1.end()
# thread2.end()