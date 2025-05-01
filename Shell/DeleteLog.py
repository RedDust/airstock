import os

from datetime import datetime as DateTime, timedelta as TimeDelta
import traceback

dtNow = DateTime.today()


def main(strLogPath):

    dtNow = DateTime.today()

    nbaseDate = dtNow - TimeDelta(days=15)

    strBaseYYYY = str(nbaseDate.year).zfill(4)
    strBaseMM = str(nbaseDate.month).zfill(2)
    strBaseDD = str(nbaseDate.day).zfill(2)

    intBaseDate = int(strBaseYYYY+strBaseMM+strBaseDD)


    listLogFiles = os.listdir(strLogPath)
    intLoop = 0
    for listLogFile in listLogFiles:

        try:

            strObjectFullPath = strLogPath+"/"+listLogFile

            boolisFilePath = os.path.isfile(strObjectFullPath)
            boolisDirPath = os.path.isdir(strObjectFullPath)
            ctime = int(round(os.path.getmtime(strObjectFullPath)))
            datetimeobj = DateTime.fromtimestamp(ctime)



            if boolisFilePath == True:
                # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "FILE:=================", strObjectFullPath)
                listParseLogName = listLogFile.split(".")
                # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "[" + str(intLoop) + "]", "boolisDirPath",
                #       type(listParseLogName), len(listParseLogName), listParseLogName)

                if len(listParseLogName)>=2 and listParseLogName[1] == 'log':

                    if datetimeobj < nbaseDate:

                            os.remove(strObjectFullPath)


            elif boolisDirPath== True:
                main(strObjectFullPath)

            else:
                print("[" + str(intLoop) + "]", "ELSE:================================" , strObjectFullPath)


        except Exception as e:
            print( "Exception" , strObjectFullPath)
            print(str(type(e)))
            err_msg = str(traceback.format_exc())
            print("Exception err_msg=>", err_msg)
            print(str(err_msg))

            continue
        else:
            print("[" + str(intLoop) + "]", "else" , strObjectFullPath)
        finally:
            intLoop += 1

            print("[" + str(intLoop) + "]", "---------------------------------------------------")




    saa = os.path.exists(strLogPath)
    print(saa)

    saa = os.path.isdir(strLogPath)
    print(saa)

    saa = os.path.isfile(strLogPath)
    print(saa)



if __name__ == '__main__':
    strLogPath = 'D:/PythonProjects/airstock/Logs'
    main(strLogPath)