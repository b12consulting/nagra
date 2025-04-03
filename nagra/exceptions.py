class BaseException(Exception):
    pass


class NoActiveTransaction(BaseException):
    def __init__(self, msg="No active transaction"):
        super().__init__(msg)


class UnresolvedFK(BaseException):
    pass


class ValidationError(BaseException):
    pass


class IncorrectSchema(BaseException):
    pass


class EvalTypeError(BaseException):
    pass
