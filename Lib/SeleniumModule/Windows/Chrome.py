from selenium import webdriver    # 라이브러리에서 사용하는 모듈만 호출

def defChromeDrive():

    try:
        options = webdriver.ChromeOptions()
        options.add_argument("headless")  # 웹 브라우저를 띄우지 않는 headless chrome 옵션 적용
        options.add_argument("disable-gpu")  # GPU 사용 안함
        options.add_argument("lang=ko_KR")  # 언어 설정
        driver = webdriver.Chrome("D:/PythonProjects/chromedriver_win32/chromedriver.exe")

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
