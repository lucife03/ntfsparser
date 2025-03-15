from dataclasses import dataclass
from typing import Optional
import struct

@dataclass
class NTFSAttribute:
    type_id: int
    name: Optional[str]
    flags: int
    data: bytes

def create_standard_information_attr() -> bytes:
    """Create $STANDARD_INFORMATION attribute"""
    attr = bytearray(96)  # Total size including header
    
    # Type (0x10)
    struct.pack_into('<I', attr, 0, 0x10)
    # Length
    struct.pack_into('<I', attr, 4, 96)
    # Non-resident flag (0 = resident)
    attr[8] = 0
    # Name length
    attr[9] = 0
    # Offset to name
    struct.pack_into('<H', attr, 10, 0)
    # Flags
    struct.pack_into('<H', attr, 12, 0)
    # Attribute ID
    struct.pack_into('<H', attr, 14, 0)
    # Content size
    struct.pack_into('<I', attr, 16, 48)
    # Content offset
    struct.pack_into('<H', attr, 20, 24)
    
    return attr

def create_file_name_attr(name: str, parent_ref: int = 5) -> bytes:
    """Create $FILE_NAME attribute"""
    name_bytes = name.encode('utf-16le')
    total_size = 88 + len(name_bytes)  # Header + fixed part + name
    
    attr = bytearray(total_size)
    
    # Type (0x30)
    struct.pack_into('<I', attr, 0, 0x30)
    # Length
    struct.pack_into('<I', attr, 4, total_size)
    # Non-resident flag
    attr[8] = 0
    # Name length
    attr[9] = 0
    # Offset to name
    struct.pack_into('<H', attr, 10, 0)
    # Flags
    struct.pack_into('<H', attr, 12, 0)
    # Attribute ID
    struct.pack_into('<H', attr, 14, 0)
    # Content size
    struct.pack_into('<I', attr, 16, total_size - 24)
    # Content offset
    struct.pack_into('<H', attr, 20, 24)
    
    # Parent directory reference
    struct.pack_into('<Q', attr, 24, parent_ref)
    
    # File name length
    attr[66] = len(name)
    # Namespace
    attr[67] = 1  # WIN32
    
    # File name
    attr[68:68 + len(name_bytes)] = name_bytes
    
    return attr 