# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import sys

import pymysql
import json
from collections import OrderedDict
from datetime import datetime as DateTime, timedelta as TimeDelta


from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

profile = webdriver.FirefoxProfile()
profile.set_preference('browser.download.folderList', 2)
profile.set_preference('browser.download.manager.showWhenStarting', False)
profile.set_preference('browser.helperApps.neverAsk.saveToDisk', ('application/vnd.ms-excel'))
profile.set_preference('general.warnOnAboutConfig', False)
profile.update_preferences()
gecko_path = "D:/PythonProjects/geckodriver_win64/geckodriver.exe"
path = "C:/Users/reddu/AppData/Local/Mozilla Firefox/firefox.exe"
binary = FirefoxBinary(path)
driver = webdriver.Firefox(firefox_profile=profile,executable_path=gecko_path)


driver.get('https://www.google.com')

quit("rhwocns")



strDailyDefine = "22.12.21."

date_1 = DateTime.today()
end_date = date_1 - TimeDelta(days=12)
print(end_date)  # 21:28:20

time = end_date.strftime('%Y%m%d')

print(time, type(time))  # 21:28:20

time = int(time)

print(time, type(time))  # 21:28:20


print("==========================================")


dtBaseYear = "20"+strDailyDefine[0:2]

dtBaseMonth = strDailyDefine[3:5]

dtBaseDay = strDailyDefine[6:8]

dtBaseDate = DateTime(int(dtBaseYear), int(dtBaseMonth), int(dtBaseDay))



print(dtBaseYear)  # 21:28:20
print(dtBaseMonth)  # 21:28:20
print(dtBaseDay)  # 21:28:20
print(dtBaseDate)  # 21:28:20


print("==========================================")

dtBaseYear = "20"+strDailyDefine[0:2]

dtBaseMonth = strDailyDefine[3:5]

dtBaseDay = strDailyDefine[6:8]

dtBaseDate = DateTime(int(date_1.year), int(date_1.month), int(date_1.day))



print(dtBaseDate)  # 21:28:20
print(type(dtBaseDate))
print("==========================================")


time = end_date.strftime('%y.%m.%d.')

print(time)  # 21:28:20
print(type(time))











quit()







quit()












def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.







# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('chun ko')

    conn = pymysql.connect(host='localhost', user='kt_real_estate_auction', port=3366,
                          password='rhwocns!@84', db='kt_real_estate_auction', charset='utf8')
    # 한글처리 (charset = 'utf8')

    # Connection 으로부터 Dictoionary Cursor 생성
    cursor = conn.cursor(pymysql.cursors.DictCursor)


    quit()

    sql = "SELECT * FROM ktrea_test ORDER BY item_code ASC"
    print(sql)
    cursor.execute(sql)
    rows = cursor.fetchall()

    for row in rows:
        print(row)
        # 출력 : {'category': 1, 'id': 1, 'region': '서울', 'name': '김정수'}
        # print(row['item_code'], row['item_name'], row['alarm_type'])
        # 1 김정수 서울
        # print(row['seq'])

    #Insert Test
    # for i in range(10):
    #     insertKtreaTestSql = "Insert ktrea_test SET item_code=%s, item_name=%s,alarm_info=%s "
    #
    #     itemCode = str("A"+str(i)+"0"+"1"+"0" + str(3))
    #
    #     itemName = str("테스트_" + str(i) + "0")
    #
    #     orderdic = OrderedDict();
    #     orderdic[1] = str(i);
    #     orderdic[2] = str(i) + '1';
    #     orderdic[3] = str(i) + '3';
    #
    #     strInsertKtreaTestItemCodeValues = (itemCode)
    #     strInsertKtreaTestItemNameValues = (itemName)
    #     jsonInsertKtreaTestAlramInfo = (json.dumps(orderdic, ensure_ascii=False, indent="\t"))
    #
    #     print(strInsertKtreaTestItemCodeValues + strInsertKtreaTestItemNameValues)
    #
    #     insertKtreaTestValues = (strInsertKtreaTestItemCodeValues, strInsertKtreaTestItemNameValues, jsonInsertKtreaTestAlramInfo);
    #
    #     print(insertKtreaTestValues)
    #
    #     cursor.execute(insertKtreaTestSql, insertKtreaTestValues)
    #     conn.commit()






