import inspect
import re
import os
import json
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



def GerLine(strFileName, nFileLine_):
    """이 함수를 호출한 곳의 라인번호를 리턴한다."""
    strReturnValue = False

    try:
        strReturnValue = strFileName +"(" +str(nFileLine_)+")"

    except Exception as e:
        print(os.path.basename(__path__))
        print(strFileName)
        print(nFileLine_)

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


def stripSpecharsForText(inputString):

    try:
        inputString = inputString.replace("\'", "")
        inputString = inputString.replace("'", "")
        inputString = inputString.replace("\\n", "")
        inputString = inputString.replace("\\t", "")
        inputString = inputString.replace("\\r", "")
        inputString = inputString.replace("\\r", "")
        inputString = inputString.replace("\r", "")
        inputString = inputString.replace("\n", "")
        inputString = inputString.replace("\xa0", "")
        inputString = str(inputString).strip()

    except Exception as e:
        print(os.path.basename(__path__))
        print(e)
        print(type(e))
        return False

    finally:
        return inputString



def is_json(obj):
    try:
        json_object = json.loads(obj)
        # { } 가 포함된 string이 invalid json 인 경우 Exception
        iterator = iter(json_object)
        # { } 가 없는 경우는 string의 경우 Exception
    except Exception as e:
        return False
    return True
