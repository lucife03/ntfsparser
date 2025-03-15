from enum import Enum
from typing import Optional, TypeVar, Generic

T = TypeVar('T')

class NTFSError(Enum):
    SUCCESS = 0
    INVALID_PARAMETER = -1
    IO_ERROR = -2
    CORRUPT_VOLUME = -3
    INVALID_BOOT_SECTOR = -4
    INVALID_MFT = -5
    NOT_FOUND = -6

class Result(Generic[T]):
    def __init__(self, value: Optional[T] = None, 
                 error: Optional[NTFSError] = None, 
                 message: str = ""):
        self.value = value
        self.error = error
        self.message = message

    @staticmethod
    def ok(value: T) -> 'Result[T]':
        return Result(value=value)

    @staticmethod
    def err(error: NTFSError, message: str = "") -> 'Result[T]':
        return Result(error=error, message=message)

    def is_ok(self) -> bool:
        return self.error is None

    def is_err(self) -> bool:
        return self.error is not None
