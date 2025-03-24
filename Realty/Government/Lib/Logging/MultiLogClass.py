import logging
import logging.handlers
import multiprocessing
from threading import Thread
from random import choice, random
import time
import platform
import os


def setLogFile(dtNow,logging,strFilePath):

    logFileName = str(dtNow.year).zfill(4)  + str(dtNow.month).zfill(2) + str(dtNow.day).zfill(2)
    logFileName += "_"
    logFileName += str(dtNow.hour).zfill(2) + str(dtNow.minute).zfill(2) + str(dtNow.second).zfill(2) + ".log"

    strLogRootDirectory = 'D:/PythonProjects/airstock/Shell/logs/'


    logger = logging.getLogger()
    if len(logger.handlers) > 0:
        return logger

    logger.setLevel(logging.DEBUG)

    listFilePaths = str(strFilePath).split('/')

    strPopCode = listFilePaths.pop()
    strFilePaths = strLogRootDirectory + '/'.join(listFilePaths)

    if not os.path.exists(strFilePaths):
        os.makedirs(strFilePaths)


    formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')

    streamingHandler = logging.StreamHandler()
    streamingHandler.setFormatter(formatter)

    # RotatingFileHandler
    log_max_size = 100 * 1024 * 1024  ## 10MB
    log_file_count = 20
    rotatingFileHandler = logging.handlers.RotatingFileHandler(
        filename='D:/PythonProjects/airstock/Shell/logs/'+strFilePath+'_'+logFileName,
        maxBytes=log_max_size,
        backupCount=log_file_count
    )

    rotatingFileHandler.setFormatter(formatter)
    # RotatingFileHandler
    timeFileHandler = logging.handlers.TimedRotatingFileHandler(
        filename='D:/PythonProjects/airstock/Shell/logs/'+strFilePath+'_'+logFileName,
        when='midnight',
        interval=1,
        encoding='utf-8'
    )
    timeFileHandler.setFormatter(formatter)
    logger.addHandler(streamingHandler)
    logger.addHandler(timeFileHandler)

    return logger

class Log():
    def __init__(self):
        self.th = None

    def get_logger(self, name):
        return logging.getLogger(name)

    def listener_start(self, file_path, name, queue):
        self.th = Thread(target=self._proc_log_queue, args=(file_path, name, queue))
        self.th.start()

    def listener_end(self, queue):
        queue.put(None)
        self.th.join()
        print('log listener end...')

    def _proc_log_queue(self, file_path, name, queue):
        self.config_log(file_path, name)
        logger = self.get_logger(name)
        while True:
            try:
                record = queue.get()
                if record is None:
                    break
                logger.handle(record)
            except Exception:
                import sys, traceback
                print('listener problem', file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

    def config_queue_log(self, queue, name):
        '''
        if you use multiprocess logging,
        call this in multiprocess as logging producer.
        logging consumer function is [self.listener_start] and [self.listener_end]
        it returns logger and you can use this logger to log
        '''
        qh = logging.handlers.QueueHandler(queue)
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(qh)
        return logger

    def config_log(self, file_path, name):
        '''
        it returns FileHandler and StreamHandler logger
        if you do not need to use multiprocess logging,
        just call this function and use returned logger.
        '''
        # err file handler
        # file handler
        fh_dbg = logging.handlers.RotatingFileHandler(file_path + '_debug.log', 'a', 300, 10)
        fh_dbg.setLevel(logging.DEBUG)
        # console handler
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        # logging format setting
        ff = logging.Formatter('''[%(asctime)s] %(levelname)s : %(message)s''')
        sf = logging.Formatter('''[%(levelname)s] %(message)s''')
        fh_dbg.setFormatter(ff)
        sh.setFormatter(sf)
        if platform.system() == 'Windows':
            import msvcrt
            import win32api
            import win32con
            win32api.SetHandleInformation(msvcrt.get_osfhandle(fh_dbg.stream.fileno()),
                                          win32con.HANDLE_FLAG_INHERIT, 0)
        # create logger, assign handler
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(fh_dbg)
        logger.addHandler(sh)
        return logger