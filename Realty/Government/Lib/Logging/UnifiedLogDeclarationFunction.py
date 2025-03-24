import os
import sys



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
        filename=strLogRootDirectory+strFilePath+'_'+logFileName,
        maxBytes=log_max_size,
        backupCount=log_file_count
    )

    rotatingFileHandler.setFormatter(formatter)
    # RotatingFileHandler
    timeFileHandler = logging.handlers.TimedRotatingFileHandler(
        filename=strLogRootDirectory+strFilePath+'_'+logFileName,
        when='midnight',
        interval=1,
        encoding='utf-8'
    )
    timeFileHandler.setFormatter(formatter)
    logger.addHandler(streamingHandler)
    logger.addHandler(timeFileHandler)

    return logger




def setLogFileV2(logging,strFilePath):

    from datetime import datetime as DateTime, timedelta as TimeDelta

    dtNow = DateTime.today()

    logFileName = str(dtNow.year).zfill(4) + str(dtNow.month).zfill(2) + str(dtNow.day).zfill(2) + ".log"
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
        filename=strLogRootDirectory+strFilePath+'_'+logFileName,
        maxBytes=log_max_size,
        backupCount=log_file_count
    )

    rotatingFileHandler.setFormatter(formatter)
    # RotatingFileHandler
    timeFileHandler = logging.handlers.TimedRotatingFileHandler(
        filename=strLogRootDirectory+strFilePath+'_'+logFileName,
        when='midnight',
        interval=1,
        encoding='utf-8'
    )
    timeFileHandler.setFormatter(formatter)
    logger.addHandler(streamingHandler)
    logger.addHandler(timeFileHandler)

    return logger


def removeHandler(logger):
    logger.removeHandler(logger);