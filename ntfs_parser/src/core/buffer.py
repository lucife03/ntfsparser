from dataclasses import dataclass
from typing import Optional
from .errors import Result, NTFSError

@dataclass
class Buffer:
    data: bytearray
    offset: int
    size: int

class BufferPool:
    def __init__(self, buffer_size: int = 4096, max_buffers: int = 10):
        self.buffer_size = buffer_size
        self.max_buffers = max_buffers
        self.free_buffers = []
        self.used_buffers = set()

    def acquire(self) -> Result[Buffer]:
        if self.free_buffers:
            buf = self.free_buffers.pop()
            self.used_buffers.add(buf)
            return Result.ok(buf)
        
        if len(self.used_buffers) >= self.max_buffers:
            return Result.err(NTFSError.IO_ERROR, 
                            "No buffers available")

        new_buf = Buffer(
            data=bytearray(self.buffer_size),
            offset=0,
            size=self.buffer_size
        )
        self.used_buffers.add(new_buf)
        return Result.ok(new_buf)

    def release(self, buffer: Buffer) -> None:
        if buffer in self.used_buffers:
            self.used_buffers.remove(buffer)
            self.free_buffers.append(buffer)
