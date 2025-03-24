import os


def fineLogInit():

    print("fineLogInit")



def setLogFile(dtNow,logging,strFilePath):


    logFileName = str(dtNow.year).zfill(4)  + str(dtNow.month).zfill(2) + str(dtNow.day).zfill(2)
    logFileName += "_"
    logFileName += str(dtNow.hour).zfill(2) + str(dtNow.minute).zfill(2) + str(dtNow.second).zfill(2) + ".log"

    strLogRootDirectory = 'D:/PythonProjects/airstock/Shell/logs/'


    listFilePaths = str(strFilePath).split('/')

    strPopCode = listFilePaths.pop()
    strFilePaths = strLogRootDirectory + '/'.join(listFilePaths)

    if not os.path.exists(strFilePaths):
        os.makedirs(strFilePaths)