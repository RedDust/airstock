import os
import time
import datetime
import math
from datetime import datetime as DateTime, timedelta as TimeDelta
from multiprocessing import Process
import thread_test_child

if __name__ == '__main__':
    numbers = [5, 10, 15, 20, 25]
    procs = []


    print("START["+str(DateTime.today()) +"]=======================================================")

    for index, number in enumerate(numbers):
        # proc = Process(target=doubler, args=(number,))
        proc = Process(target=thread_test_child.test_thread, args=(number, numbers))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()



    print("END["+str(DateTime.today())+"]=======================================================")