from dataclasses import dataclass
from typing import Optional
import struct
from ..core.errors import Result, NTFSError

@dataclass
class NTFSBootSector:
    bytes_per_sector: int
    sectors_per_cluster: int
    mft_lcn: int
    mft_record_size: int
    index_record_size: int
    serial_number: bytes
    total_sectors: int

    @classmethod
    def from_bytes(cls, data: bytes) -> Result['NTFSBootSector']:
        try:
            if len(data) < 512:
                return Result.err(
                    NTFSError.INVALID_BOOT_SECTOR,
                    "Boot sector too small"
                )

            # Verify NTFS signature
            if data[3:7] != b'NTFS':
                return Result.err(
                    NTFSError.INVALID_BOOT_SECTOR,
                    "Invalid NTFS signature"
                )

            # Parse boot sector fields
            bps = struct.unpack_from('<H', data, 0x0B)[0]
            spc = data[0x0D]
            mft_lcn = struct.unpack_from('<Q', data, 0x30)[0]
            mft_rec_size = data[0x40]
            idx_rec_size = data[0x44]
            serial = data[0x48:0x50]
            total_sectors = struct.unpack_from('<Q', data, 0x28)[0]

            return Result.ok(cls(
                bytes_per_sector=bps,
                sectors_per_cluster=spc,
                mft_lcn=mft_lcn,
                mft_record_size=mft_rec_size,
                index_record_size=idx_rec_size,
                serial_number=serial,
                total_sectors=total_sectors
            ))

        except Exception as e:
            return Result.err(
                NTFSError.INVALID_BOOT_SECTOR,
                f"Failed to parse boot sector: {str(e)}"
            )
