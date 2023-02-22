import inspect
import re
import os

def lineno(strPath = __file__):
    """이 함수를 호출한 곳의 라인번호를 리턴한다."""

    strReturnValue = False

    try:
        strReturnValue = (os.path.abspath(strPath)) +"(" +str(inspect.getlineno(inspect.getouterframes(inspect.currentframe())[-1][0]))+")"

    except Exception as e:
        print(os.path.basename(__path__))
        print(e)
        print(type(e))
        return False

    finally:
        return strReturnValue



def rmEmoji(inputString):
    return inputString.encode('ascii', 'ignore').decode('ascii')


def rmEmojiUTF(inputString):
    return inputString.encode('utf-8', 'ignore').decode('utf-8')

def removeSpechars(inputString):
    return re.sub(r"[^\uAC00-\uD7A30-9a-zA-Z\s]", "", inputString)