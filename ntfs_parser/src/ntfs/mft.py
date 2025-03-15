from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
from ..core.errors import Result, NTFSError
import traceback

@dataclass
class DataRun:
    cluster: int  # Starting cluster number
    length: int   # Number of clusters

@dataclass
class MFTAttribute:
    type_id: int
    name: Optional[str]
    resident: bool
    data: bytes
    data_runs: List[DataRun]
    data_size: int = 0

    @classmethod
    def from_raw_data(cls, attr_type: int, name: Optional[str], resident: bool, 
                      data: bytes, offset: int = 0) -> 'MFTAttribute':
        """Create attribute from raw data"""
        print(f"Creating attribute: type=0x{attr_type:x}, resident={resident}, data_len={len(data)}")
        print(f"First 32 bytes: {data[:32].hex()}")
        
        attr = cls(
            type_id=attr_type,
            name=name,
            resident=resident,
            data=data if resident else b'',
            data_runs=[],
            data_size=len(data) if resident else 0
        )
        
        # Parse data runs for non-resident attributes
        if not resident:
            try:
                # For non-resident attributes, the header is at the start of the data
                # Get data runs offset from the start of the attribute
                runs_offset = int.from_bytes(data[32:34], 'little')  # Offset is at 0x20
                data_size = int.from_bytes(data[48:56], 'little')   # Size is at 0x30
                
                print(f"Non-resident attribute: size={data_size}, runs_offset={runs_offset}")
                print(f"Data at runs offset: {data[runs_offset:runs_offset+16].hex()}")
                
                # Parse data runs
                current_offset = runs_offset
                current_lcn = 0  # Logical cluster number
                
                while current_offset < len(data):
                    header = data[current_offset]
                    if header == 0:
                        break
                        
                    length_size = header & 0x0F
                    offset_size = (header >> 4) & 0x0F
                    current_offset += 1
                    
                    print(f"Data run at {current_offset-1}: header=0x{header:02x}, length_size={length_size}, offset_size={offset_size}")
                    
                    if current_offset + length_size + offset_size > len(data):
                        print(f"Data run would exceed buffer: {current_offset + length_size + offset_size} > {len(data)}")
                        break
                    
                    # Read run length
                    length_bytes = data[current_offset:current_offset+length_size]
                    length = int.from_bytes(length_bytes, 'little')
                    current_offset += length_size
                    
                    # Read cluster offset (can be negative)
                    lcn_bytes = data[current_offset:current_offset+offset_size]
                    if lcn_bytes[-1] & 0x80:  # Negative number
                        lcn_offset = int.from_bytes(lcn_bytes, 'little', signed=True)
                    else:
                        lcn_offset = int.from_bytes(lcn_bytes, 'little')
                    current_offset += offset_size
                    
                    current_lcn += lcn_offset
                    print(f"Found data run: cluster={current_lcn}, length={length}")
                    print(f"Length bytes: {length_bytes.hex()}, LCN bytes: {lcn_bytes.hex()}")
                    
                    attr.data_runs.append(DataRun(cluster=current_lcn, length=length))
                
                attr.data_size = data_size
                print(f"Parsed {len(attr.data_runs)} data runs")
                
            except Exception as e:
                print(f"Error parsing data runs: {str(e)}")
                print(f"Stack trace:")
                traceback.print_exc()
        
        return attr

@dataclass
class MFTEntry:
    reference: int
    sequence: int
    base_reference: int
    flags: int
    used_size: int
    allocated_size: int
    attributes: List[MFTAttribute]

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> Result['MFTEntry']:
        try:
            # Verify "FILE" signature
            if data[offset:offset+4] != b'FILE':
                return Result.err(
                    NTFSError.INVALID_MFT,
                    "Invalid MFT signature"
                )

            # Parse MFT entry header
            sequence = int.from_bytes(data[offset+16:offset+18], 'little')
            flags = int.from_bytes(data[offset+22:offset+24], 'little')
            used_size = int.from_bytes(data[offset+24:offset+28], 'little')
            alloc_size = int.from_bytes(data[offset+28:offset+32], 'little')
            base_ref = int.from_bytes(data[offset+32:offset+40], 'little')

            # Parse attributes
            attrs: List[MFTAttribute] = []
            attr_offset = offset + int.from_bytes(data[offset+20:offset+22], 'little')

            while attr_offset < offset + used_size:
                attr_type = int.from_bytes(data[attr_offset:attr_offset+4], 'little')
                if attr_type == 0xFFFFFFFF:
                    break

                attr_len = int.from_bytes(data[attr_offset+4:attr_offset+8], 'little')
                resident_flag = data[attr_offset+8]
                name_len = data[attr_offset+9]
                name_offset = attr_offset + int.from_bytes(data[attr_offset+10:attr_offset+12], 'little')

                # Get attribute name if present
                name = None
                if name_len > 0:
                    name = data[name_offset:name_offset+name_len*2].decode('utf-16-le')

                # Get attribute data
                if resident_flag == 0:  # Resident
                    content_size = int.from_bytes(data[attr_offset+16:attr_offset+20], 'little')
                    content_offset = attr_offset + int.from_bytes(data[attr_offset+20:attr_offset+22], 'little')
                    attr_data = data[content_offset:content_offset+content_size]
                else:  # Non-resident
                    # For non-resident, include the entire attribute record
                    attr_data = data[attr_offset:attr_offset+attr_len]

                print(f"Attribute 0x{attr_type:02x}: resident={resident_flag==0}, length={attr_len}")

                # Create attribute with data runs parsing
                attr = MFTAttribute.from_raw_data(
                    attr_type=attr_type,
                    name=name,
                    resident=(resident_flag == 0),
                    data=attr_data
                )
                attrs.append(attr)

                attr_offset += attr_len

            return Result.ok(cls(
                reference=0,  # Set by caller
                sequence=sequence,
                base_reference=base_ref,
                flags=flags,
                used_size=used_size,
                allocated_size=alloc_size,
                attributes=attrs
            ))

        except Exception as e:
            return Result.err(
                NTFSError.INVALID_MFT,
                f"Failed to parse MFT entry: {str(e)}"
            )

    def is_in_use(self) -> bool:
        return bool(self.flags & 0x0001)

    def is_directory(self) -> bool:
        return bool(self.flags & 0x0002)

    def has_file_name(self) -> bool:
        """Check if entry has a filename attribute"""
        for attr in self.attributes:
            if attr.type_id == 0x30:  # $FILE_NAME
                return True
        return False

    def is_in_use(self) -> bool:
        """Check if the entry is in use"""
        return bool(self.flags & 0x01)
