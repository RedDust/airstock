import inspect
import re

def lineno():
    """이 함수를 호출한 곳의 라인번호를 리턴한다."""
    return inspect.getlineno(inspect.getouterframes(inspect.currentframe())[-1][0])


def rmEmoji(inputString):
    return inputString.encode('ascii', 'ignore').decode('ascii')


def rmEmojiUTF(inputString):
    return inputString.encode('utf-8', 'ignore').decode('utf-8')

def removeSpechars(inputString):
    return re.sub(r"[^\uAC00-\uD7A30-9a-zA-Z\s]", "", inputString)