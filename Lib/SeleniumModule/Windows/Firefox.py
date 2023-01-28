from selenium import webdriver    # 라이브러리에서 사용하는 모듈만 호출
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

def defFireBoxDrive():

    try:

        profile = webdriver.FirefoxProfile()
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference('browser.download.manager.showWhenStarting', False)
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk', ('application/vnd.ms-excel'))
        profile.set_preference('general.warnOnAboutConfig', False)
        profile.update_preferences()

        gecko_path = "D:/PythonProjects/geckodriver_win64/geckodriver.exe"
        path = "C:/Users/reddu/AppData/Local/Mozilla Firefox/firefox.exe"

        binary = FirefoxBinary(path)
        driver = webdriver.Firefox(firefox_profile=profile, executable_path=gecko_path)



    except Exception as e:
        print("defFireBoxDrive Error Exception")
        print(e)
        print(type(e))
        return False

    else:
        return driver

    finally:
        print("========================================================")
        print("defFireBoxDrive Finally END")
