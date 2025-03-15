from typing import List, Optional, BinaryIO
import os
import binascii
import traceback

class SplitImageFile:
    """Handles reading from split NTFS image files"""
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.files = []
        self.current_file = None
        self.current_offset = 0
        
    def open(self) -> bool:
        """Open all split image files"""
        try:
            print("Looking for split files in: ")
            print(f"Base name: {self.base_path}")
            
            # Check for first file (.001)
            filename = f"{self.base_path}.001"
            print(f"Checking for file: {filename}")
            if not os.path.exists(filename):
                print(f"Primary image file not found: {filename}")
                return False
            
            self.files.append(open(filename, 'rb'))
            
            # Look for additional split files (optional)
            i = 2
            while True:
                filename = f"{self.base_path}.{i:03d}"
                print(f"Checking for optional file: {filename}")
                if not os.path.exists(filename):
                    break
                self.files.append(open(filename, 'rb'))
                i += 1
            
            print(f"Found {len(self.files)} split image files")
            self.current_file = self.files[0]
            return True
            
        except Exception as e:
            print(f"Error opening split files: {str(e)}")
            return False
            
    def read(self, offset: int, size: int) -> Optional[bytes]:
        """Read data from split files at given offset"""
        try:
            # Find which file contains the offset
            file_size = os.path.getsize(f"{self.base_path}.001")
            file_index = offset // file_size
            if file_index >= len(self.files):
                return None
                
            # Calculate offset within file
            file_offset = offset % file_size
            
            # Read from file
            self.files[file_index].seek(file_offset)
            data = self.files[file_index].read(size)
            
            # Handle reads that span multiple files
            remaining = size - len(data)
            while remaining > 0 and file_index + 1 < len(self.files):
                file_index += 1
                next_data = self.files[file_index].read(remaining)
                if not next_data:
                    break
                data += next_data
                remaining -= len(next_data)
                
            return data
            
        except Exception as e:
            print(f"Error reading from split files: {str(e)}")
            return None
            
    def close(self):
        """Close all open files"""
        for f in self.files:
            f.close()

    def read_ftk_header(self) -> Optional[dict]:
        """Read and parse FTK Imager logical image header"""
        try:
            # Read first chunk of first file
            header_data = self.read(0, 512)
            if not header_data:
                return None
            
            print("\nAnalyzing FTK Image header:")
            print(f"First 32 bytes: {binascii.hexlify(header_data[:32]).decode()}")
            
            # FTK logical images typically start with case information
            # and then the actual NTFS data follows
            
            # Look for NTFS signature in first few blocks
            for offset in range(0, 16384, 512):  # Check first 32 sectors
                sector = self.read(offset, 512)
                if not sector:
                    continue
                
                if sector[3:7] == b'NTFS':
                    print(f"\nFound NTFS signature at offset {offset}")
                    return {
                        'type': 'FTK Logical',
                        'ntfs_offset': offset,
                        'sector_size': 512
                    }
                    
            return None
            
        except Exception as e:
            print(f"Error reading FTK header: {str(e)}")
            return None

    def find_ntfs_partition(self) -> Optional[int]:
        """Find the start offset of the NTFS volume in FTK logical image"""
        try:
            print("\nAnalyzing FTK logical image...")
            
            # For FTK logical images, we need to scan more thoroughly
            # Try larger chunks to find valid data
            chunk_size = 65536  # 64KB chunks
            
            for file_idx, file_size in enumerate(self.file_sizes):
                print(f"\nScanning file {file_idx + 1} ({self.file_sizes[file_idx]} bytes)")
                base_offset = sum(self.file_sizes[:file_idx])
                
                # Scan through the file
                for chunk_offset in range(0, file_size, chunk_size):
                    if chunk_offset % (1024*1024) == 0:  # Progress indicator every 1MB
                        print(f"Scanning offset: {chunk_offset:,} bytes")
                        
                    chunk = self.read(base_offset + chunk_offset, chunk_size)
                    if not chunk:
                        continue
                    
                    # Look for NTFS signature or common NTFS structures
                    for i in range(0, len(chunk) - 512, 512):
                        sector = chunk[i:i+512]
                        
                        # Check for NTFS signature
                        if sector[3:7] == b'NTFS':
                            found_offset = base_offset + chunk_offset + i
                            print(f"\nFound NTFS signature at offset {found_offset:,}")
                            return found_offset
                        
                        # Check for MFT entry signature "FILE"
                        if sector[0:4] == b'FILE':
                            print(f"\nPossible MFT entry found at {base_offset + chunk_offset + i:,}")
                            # Look backwards for NTFS boot sector
                            boot_search_start = max(0, base_offset + chunk_offset + i - 16384)
                            print(f"Searching backwards from {boot_search_start:,} for boot sector")
                            
                            for back_offset in range(boot_search_start, boot_search_start - 65536, -512):
                                if back_offset < 0:
                                    break
                                boot_sector = self.read(back_offset, 512)
                                if boot_sector and boot_sector[3:7] == b'NTFS':
                                    print(f"Found NTFS boot sector at {back_offset:,}")
                                    return back_offset
                
                # If no signature found, look for other NTFS indicators
                print(f"\nChecking file {file_idx + 1} for NTFS structures...")
                sample_offset = base_offset
                sample = self.read(sample_offset, 512)
                if sample:
                    print(f"Sample data at offset {sample_offset:,}:")
                    print(f"First 16 bytes: {binascii.hexlify(sample[:16]).decode()}")
                    print(f"Bytes 3-7: {sample[3:7]}")
                    print(f"Possible signatures: {[sample[i:i+8] for i in range(0, 512, 8) if any(c != 0 for c in sample[i:i+8])]}")
            
            print("\nNo valid NTFS structures found. This might be:")
            print("1. An encrypted or compressed FTK image")
            print("2. A different file system type")
            print("3. A forensic container format")
            print("\nPlease verify the image format and try:")
            print("- Using FTK Imager to check the image type")
            print("- Converting to raw format if possible")
            print("- Checking if encryption or compression is used")
            
            return None
            
        except Exception as e:
            print(f"Error analyzing FTK image: {str(e)}")
            print("Stack trace:", traceback.format_exc())
            return None 

    def check_ftk_format(self) -> bool:
        """Check if this is a valid FTK image format"""
        try:
            # Read start of first file
            header = self.read(0, 512)
            if not header:
                return False
            
            print("\nAnalyzing file format:")
            print(f"First 32 bytes: {binascii.hexlify(header[:32]).decode()}")
            
            # Check for known FTK signatures or patterns
            if all(b == 0 for b in header):
                print("Warning: First sector is all zeros")
            
            # Check file sizes
            print("\nFile size analysis:")
            for i, size in enumerate(self.file_sizes):
                print(f"File {i+1}: {size:,} bytes ({size/1024/1024:.2f} MB)")
            
            # Look for common patterns
            patterns = [b'FTK', b'NTFS', b'FILE', b'MFT']
            for pattern in patterns:
                for i in range(min(4096, len(header)-len(pattern))):
                    if header[i:i+len(pattern)] == pattern:
                        print(f"Found {pattern} signature at offset {i}")
            
            return True
            
        except Exception as e:
            print(f"Error checking FTK format: {str(e)}")
            return False 