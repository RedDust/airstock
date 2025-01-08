import os

from datetime import datetime as DateTime, timedelta as TimeDelta
import sys, inspect as Isp, logging, logging.handlers
import traceback
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Init.Functions.Logs import GetLogDef as SLog

dtNow = DateTime.today()

LogPath = 'CronLog'
setLogger = ULF.setLogFile(dtNow, logging, LogPath)


def main(strLogPath):

    dtNow = DateTime.today()

    nbaseDate = dtNow - TimeDelta(days=31)

    strBaseYYYY = str(nbaseDate.year).zfill(4)
    strBaseMM = str(nbaseDate.month).zfill(2)
    strBaseDD = str(nbaseDate.day).zfill(2)

    intBaseDate = int(strBaseYYYY+strBaseMM+strBaseDD)

    logging.info(SLog.Ins(Isp.getframeinfo,Isp.currentframe()) + "[" + str(intBaseDate) + "]"+ str(intBaseDate))



    listLogFiles = os.listdir(strLogPath)
    intLoop = 0
    for listLogFile in listLogFiles:
        print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "["+str(intLoop)+"]",type(listLogFile) , listLogFile)

        try:

            strObjectFullPath = strLogPath+"/"+listLogFile

            boolisFilePath = os.path.isfile(strObjectFullPath)
            boolisDirPath = os.path.isdir(strObjectFullPath)
            ctime = int(round(os.path.getmtime(strObjectFullPath)))
            datetimeobj = DateTime.fromtimestamp(ctime)


            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "[" + str(intLoop) + "]", "strObjectFullPath", strObjectFullPath)
            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "[" + str(intLoop) + "]", "boolisFilePath", type(boolisFilePath),  boolisFilePath)
            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()), "[" + str(intLoop) + "]", "boolisDirPath", type(boolisDirPath), boolisDirPath)
            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "[" + str(intLoop) + "]", "ctime", type(ctime), ctime)
            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "[" + str(intLoop) + "]", "datetimeobj", type(datetimeobj), datetimeobj)
            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "[" + str(intLoop) + "]", "nbaseDate", type(nbaseDate), nbaseDate)



            if boolisFilePath == True:
                # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "FILE:=================", strObjectFullPath)
                listParseLogName = listLogFile.split(".")
                # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "[" + str(intLoop) + "]", "boolisDirPath",
                #       type(listParseLogName), len(listParseLogName), listParseLogName)

                if len(listParseLogName)>=2 and listParseLogName[1] == 'log':

                    if datetimeobj < nbaseDate:
                        if os.path.exists(strObjectFullPath) == True and os.path.isfile(strObjectFullPath) == True:
                            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "[" + str(intLoop) + "]",
                                  type(strObjectFullPath), strObjectFullPath)
                            os.remove(strObjectFullPath)


            elif boolisDirPath== True:
                logging.info(SLog.Ins(Isp.getframeinfo,Isp.currentframe())+"[" + str(intLoop) + "]" + "DIR:================================" + strObjectFullPath)
                print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()),"[" + str(intLoop) + "]", strObjectFullPath, type(strObjectFullPath) , len(strObjectFullPath))
                main(strObjectFullPath)

            else:
                print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()),"[" + str(intLoop) + "]", "ELSE:================================" , strObjectFullPath)


        except Exception as e:
            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()),"[" + str(intLoop) + "]", "Exception" , strObjectFullPath)
            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
            err_msg = str(traceback.format_exc())
            print("Exception err_msg=>", err_msg)
            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))

            continue
        else:
            print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()),"[" + str(intLoop) + "]", "else" , strObjectFullPath)
        finally:
            intLoop += 1

            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "[" + str(intLoop) + "]", "---------------------------------------------------")




    saa = os.path.exists(strLogPath)
    print(saa)

    saa = os.path.isdir(strLogPath)
    print(saa)

    saa = os.path.isfile(strLogPath)
    print(saa)



if __name__ == '__main__':
    main('D:/PythonProjects/airstock/Shell/logs')