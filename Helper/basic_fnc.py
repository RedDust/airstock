
import re

def OnlyKorean(strInput):
    strOutput = re.sub(r"[^ㄱ-ㅣ가-힣\s]", "", strInput).strip() # 한글 + 공백만 남기기 + 앞뒤 공백제거

    return strOutput