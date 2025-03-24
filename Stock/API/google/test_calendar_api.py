import os
import logging
import logging.handlers
import multiprocessing
from threading import Thread
from random import choice, random
import time
import platform
import inspect as Isp
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import MultiLogClass as MLC
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
import Stock.API.koreaInvestment.stock_deamon_test as StockDeamonTest

'''
Class Log performs logger configuration, creation, multiprocess listener.
'''


'''
The code below tests the multiprocess logging.
Main process and child process produce log messasge. (put message into queue.) 
(random choice in variable LEVEL, MESSAGES)
Listener process produced by main process consume log message. (write log message in stdout and file)
'''

def worker(queue):
    # multi process log producer start
    logger = MLC.Log().config_queue_log(queue, 'mp')
    name = multiprocessing.current_process().name
    print('Worker started: %s' % name)
    for i in range(10):
        time.sleep(random())
        level = logging.INFO
        message = 'Random message #1'
        logger.log(level, f"{name} - {message}")
    print('Worker finished: %s' % name)
    # multi process log producer end


def main():
    strProcessType = "000004"
    queue = multiprocessing.Queue(-1)
    listener = MLC.Log()

    strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]

    print("strAddLogPath=>" , strAddLogPath)

    strLogRootDirectory = 'D:/PythonProjects/airstock/Shell/logs/'
    strLogRootDirectory += 'Stock/' + '/' + strAddLogPath

    listener.listener_start(strLogRootDirectory, 'listener', queue)  # log consumer thread start

    w = multiprocessing.Process(target=StockDeamonTest.main, args=(queue,))
    w.start()

    # main process log producer start
    logger1 = MLC.Log().config_queue_log(queue, 'mp')
    name = multiprocessing.current_process().name
    print('Worker started: %s' % name)
    logger1.log("20고재춘입니다.0000")
    print('Worker finished: %s' % name)
    # main process log producer end
    w.join()
    listener.listener_end(queue)  # log consumer thread end




if __name__ == '__main__':
    main()