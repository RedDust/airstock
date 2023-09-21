class QuitException(Exception):
    def __init__(self, value):
        self.value = value

        def __str__(self):
            return self.value

        def raise_exception(err_msg):
            raise QuitException.QuitException(err_msg)