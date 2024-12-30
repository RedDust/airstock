class QuitException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        if len(self.value) == 1:
            return str(self.value[0])
        else:
            return str(self.value)

    def raise_exception(err_msg):
        raise QuitException.QuitException(err_msg)