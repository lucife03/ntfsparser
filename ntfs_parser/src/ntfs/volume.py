from typing import Optional, Dict, List, Generator
from .boot_sector import NTFSBootSector
from .mft import MFTEntry, MFTAttribute
from ..core.buffer import BufferPool, Buffer
from ..core.errors import Result, NTFSError
import os
import binascii
import logging
from .split_volume import SplitImageFile
import traceback

class NTFSFile:
    def __init__(self, mft_entry: MFTEntry, volume: 'NTFSVolume'):
        self.mft_entry = mft_entry
        self.volume = volume
        self.name = None
        self.size = 0
        self.is_directory = self.mft_entry.is_directory()
        self.creation_time = None
        self.modification_time = None
        self._parse_attributes()
        
        # Fix: Set size to 0 for directories
        if self.is_directory:
            self.size = 0
        
    def _parse_attributes(self):
        """Parse MFT entry attributes"""
        for attr in self.mft_entry.attributes:
            try:
                if attr.type_id == 0x30:  # $FILE_NAME
                    if not self.name:  # Only set if not already set
                        if hasattr(attr, 'name') and attr.name:
                            self.name = attr.name
                        elif attr.resident and len(attr.data) > 66:
                            name_length = attr.data[64]
                            self.name = attr.data[66:66+name_length*2].decode('utf-16le')
                            
                elif attr.type_id == 0x80:  # $DATA
                    # Only use unnamed $DATA attribute for size
                    if not hasattr(attr, 'name') or not attr.name:
                        if not attr.resident:
                            # For non-resident, use data_size
                            if hasattr(attr, 'data_size'):
                                self.size = attr.data_size
                        else:
                            # For resident, use actual data length
                            self.size = len(attr.data)
                        
                elif attr.type_id == 0x10:  # $STANDARD_INFORMATION
                    if attr.resident and len(attr.data) >= 24:
                        # Parse timestamps
                        self.creation_time = int.from_bytes(attr.data[0:8], 'little')
                        self.modification_time = int.from_bytes(attr.data[8:16], 'little')
                        
            except Exception as e:
                self.volume.logger.error(f"Error parsing attribute {attr.type_id}: {str(e)}")
    
    def read_data(self) -> Result[bytes]:
        """Read file contents"""
        try:
            # Find the unnamed $DATA attribute
            data_attr = None
            for attr in self.mft_entry.attributes:
                if attr.type_id == 0x80:  # $DATA
                    if not hasattr(attr, 'name') or not attr.name:
                        data_attr = attr
                        break
            
            if not data_attr:
                return Result.ok(b'')  # No data attribute found
            
            if data_attr.resident:
                # Resident data - return directly
                return Result.ok(data_attr.data)
            else:
                # Non-resident data - read from clusters
                data = bytearray()
                for run in data_attr.data_runs:
                    buffer_result = self.volume.read_clusters(run.cluster, run.length)
                    if buffer_result.is_err():
                        return buffer_result
                    data.extend(buffer_result.value)
                
                return Result.ok(bytes(data[:data_attr.data_size]))
            
        except Exception as e:
            return Result.err(NTFSError.IO_ERROR, f"Failed to read file data: {str(e)}")

    def read_deleted_data(self) -> Result[bytes]:
        """Read data from a deleted file"""
        try:
            # Find the unnamed $DATA attribute
            data_attr = None
            for attr in self.mft_entry.attributes:
                if attr.type_id == 0x80:  # $DATA
                    if not hasattr(attr, 'name') or not attr.name:
                        data_attr = attr
                        break
            
            if not data_attr:
                return Result.ok(b'')  # No data attribute found
            
            if data_attr.resident:
                # Resident data - return directly
                return Result.ok(data_attr.data)
            else:
                # For deleted files, only read what's still available
                # Some clusters might be reallocated
                data = bytearray()
                for run in data_attr.data_runs:
                    try:
                        buffer_result = self.volume.read_clusters(run.cluster, run.length)
                        if buffer_result.is_ok():
                            data.extend(buffer_result.value)
                    except:
                        # Skip errors for deleted files
                        continue
                
                return Result.ok(bytes(data[:data_attr.data_size]))
            
        except Exception as e:
            return Result.err(NTFSError.IO_ERROR, f"Failed to read deleted file data: {str(e)}")

class NTFSVolume:
    def __init__(self, image_path: str):
        self.image_path = image_path
        self.image_file = None
        self.split_image = None
        self.boot_sector: Optional[NTFSBootSector] = None
        self.buffer_pool = BufferPool()
        self.mft_cache: Dict[int, MFTEntry] = {}
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger('NTFSVolume')

    def mount(self) -> Result[None]:
        try:
            print(f"\nMounting split image...")
            print(f"Image path: {self.image_path}")
            
            # Get base path by stripping extension
            base_path = self.image_path.rsplit('.', 1)[0]  # Remove .001 extension
            print(f"Base path: {base_path}")
            
            self.split_image = SplitImageFile(base_path)
            if not self.split_image.open():
                return Result.err(NTFSError.IO_ERROR, "Failed to open split image files")
            
            # First read the MBR
            print("\nReading MBR...")
            mbr_data = self.split_image.read(0, 512)
            if not mbr_data:
                return Result.err(NTFSError.IO_ERROR, "Failed to read MBR")

            # Parse partition table from MBR
            partition_offset = None
            for i in range(4):  # Check all 4 primary partitions
                offset = 0x1BE + (i * 16)  # Partition table starts at 0x1BE
                partition_type = mbr_data[offset + 4]
                if partition_type == 0x07:  # NTFS partition type
                    partition_offset = int.from_bytes(mbr_data[offset + 8:offset + 12], 'little') * 512
                    print(f"Found NTFS partition at offset: {partition_offset}")
                    break

            if partition_offset is None:
                return Result.err(NTFSError.IO_ERROR, "No NTFS partition found")

            # Store partition offset for future reads
            self.partition_offset = partition_offset
            
            # Now read the NTFS boot sector from partition start
            print(f"\nReading NTFS boot sector from offset {partition_offset}...")
            boot_data = self.split_image.read(partition_offset, 512)
            if not boot_data:
                return Result.err(NTFSError.IO_ERROR, "Failed to read NTFS boot sector")
            
            # Verify NTFS signature
            if boot_data[3:7] != b'NTFS':
                print("\nBoot sector contents (hex):")
                print(binascii.hexlify(boot_data).decode())
                return Result.err(NTFSError.IO_ERROR, "Invalid NTFS signature in boot sector")
            
            print("\nBoot sector details:")
            print(f"NTFS signature: {boot_data[3:7]}")
            print(f"Bytes per sector: {int.from_bytes(boot_data[0x0B:0x0D], 'little')}")
            print(f"Sectors per cluster: {boot_data[0x0D]}")
            
            boot_result = NTFSBootSector.from_bytes(boot_data)
            if boot_result.is_err():
                return boot_result

            self.boot_sector = boot_result.value
            print("\nNTFS volume information:")
            print(f"Bytes per sector: {self.boot_sector.bytes_per_sector}")
            print(f"Sectors per cluster: {self.boot_sector.sectors_per_cluster}")
            print(f"Total sectors: {self.boot_sector.total_sectors}")
            print(f"MFT cluster number: {self.boot_sector.mft_lcn}")
            
            return Result.ok(None)

        except Exception as e:
            return Result.err(
                NTFSError.IO_ERROR,
                f"Failed to mount volume: {str(e)}"
            )

    def read_clusters(self, cluster: int, count: int) -> Result[bytes]:
        """Read clusters from volume, handling split files"""
        if not self.boot_sector:
            return Result.err(NTFSError.INVALID_PARAMETER, "Volume not mounted")
            
        offset = (self.partition_offset +  # Add partition offset
                 cluster * self.boot_sector.sectors_per_cluster * 
                 self.boot_sector.bytes_per_sector)
        size = (count * self.boot_sector.sectors_per_cluster * 
               self.boot_sector.bytes_per_sector)
               
        try:
            if self.split_image:
                data = self.split_image.read(offset, size)
                if not data:
                    return Result.err(NTFSError.IO_ERROR, "Failed to read from split image")
                return Result.ok(data)
            else:
                self.image_file.seek(offset)
                return Result.ok(self.image_file.read(size))
        except Exception as e:
            return Result.err(NTFSError.IO_ERROR, f"Failed to read clusters: {str(e)}")

    def read_mft_entry(self, entry_number: int) -> Result[MFTEntry]:
        try:
            # Check cache first
            if entry_number in self.mft_cache:
                return Result.ok(self.mft_cache[entry_number])

            if not self.boot_sector:
                return Result.err(NTFSError.INVALID_PARAMETER, "Volume not mounted")

            # Calculate MFT entry location
            mft_offset = (self.partition_offset +  # Add partition offset
                         self.boot_sector.mft_lcn * 
                         self.boot_sector.sectors_per_cluster * 
                         self.boot_sector.bytes_per_sector)
            entry_size = 1024  # Standard MFT entry size
            offset = mft_offset + (entry_number * entry_size)
            
            self.logger.debug(f"Reading MFT entry {entry_number} from offset {offset}")

            # Read MFT entry using split image
            entry_data = self.split_image.read(offset, entry_size)
            if not entry_data:
                return Result.err(NTFSError.IO_ERROR, f"Failed to read MFT entry {entry_number}")
            
            # Debug: Print first few bytes
            self.logger.debug(f"MFT Entry {entry_number} data starts with: {entry_data[:16].hex()}")

            entry_result = MFTEntry.from_bytes(entry_data)
            if entry_result.is_err():
                return entry_result

            entry = entry_result.value
            entry.reference = entry_number
            
            # Debug: Print attributes
            self.logger.debug(f"MFT Entry {entry_number} has {len(entry.attributes)} attributes")
            for attr in entry.attributes:
                self.logger.debug(f"Attribute type: 0x{attr.type_id:02x}")

            # Cache the entry
            self.mft_cache[entry_number] = entry
            
            return Result.ok(entry)

        except Exception as e:
            return Result.err(NTFSError.IO_ERROR, f"Failed to read MFT entry {entry_number}: {str(e)}")

    def get_file_by_path(self, path: str) -> Result[MFTEntry]:
        """Get MFT entry for a file by its path"""
        if not self.boot_sector:
            return Result.err(
                NTFSError.INVALID_PARAMETER,
                "Volume not mounted"
            )

        # Start from root directory (MFT entry 5)
        current_entry_num = 5
        
        # Handle root directory case
        if path in ['/', '.']:
            return self.read_mft_entry(5)

        # Split path into components, handling spaces correctly
        path_parts = [part for part in path.strip('/').split('/') if part]
        
        for part in path_parts:
            # Read current directory entry
            dir_result = self.read_mft_entry(current_entry_num)
            if dir_result.is_err():
                return dir_result
            
            dir_entry = dir_result.value
            if not dir_entry.is_directory():
                return Result.err(
                    NTFSError.NOT_FOUND,
                    f"Path component not a directory: {part}"
                )

            # List files in current directory
            files = self._list_directory(dir_entry)
            
            # Find matching file/directory
            found = False
            for file in files:
                if file.name.lower() == part.lower():  # Case-insensitive comparison
                    current_entry_num = file.mft_entry.reference
                    found = True
                    break
                
            if not found:
                return Result.err(
                    NTFSError.NOT_FOUND,
                    f"Path component not found: {part}"
                )

        return self.read_mft_entry(current_entry_num)

    def close(self):
        if self.split_image:
            self.split_image.close()
        if self.image_file:
            self.image_file.close()

    def _list_directory(self, dir_entry: MFTEntry) -> List[NTFSFile]:
        """Internal method to list contents of a directory"""
        files = []
        try:
            self.logger.debug(f"Listing directory with {len(dir_entry.attributes)} attributes")
            
            # Look for $INDEX_ROOT and $INDEX_ALLOCATION attributes
            index_root = None
            index_allocation = None
            
            for attr in dir_entry.attributes:
                self.logger.debug(f"Processing attribute type 0x{attr.type_id:02x}")
                
                if attr.type_id == 0x90:  # $INDEX_ROOT
                    self.logger.debug("Found $INDEX_ROOT attribute")
                    self._dump_attribute(attr, "  ")
                    index_root = attr
                elif attr.type_id == 0xA0:  # $INDEX_ALLOCATION
                    index_allocation = attr
            
            # Parse $INDEX_ROOT
            if index_root and index_root.resident:
                self.logger.debug("Parsing $INDEX_ROOT")
                data = index_root.data
                
                # Parse index root header (first 16 bytes)
                if len(data) < 16:
                    self.logger.debug("$INDEX_ROOT too short")
                    return files
                    
                # Parse attribute type (should be 0x30 for $FILE_NAME)
                attr_type = int.from_bytes(data[0:4], 'little')
                collation_rule = int.from_bytes(data[4:8], 'little')
                index_size = int.from_bytes(data[8:12], 'little')
                clusters_per_index = data[12]
                
                self.logger.debug(f"Index root: type=0x{attr_type:x}, collation={collation_rule}, size={index_size}")
                
                # Skip index root header (16 bytes) to get to the node header
                node_header_offset = 16
                
                # Parse node header (16 bytes)
                if len(data) < node_header_offset + 16:
                    self.logger.debug("No space for node header")
                    return files
                    
                entries_offset = node_header_offset + int.from_bytes(data[node_header_offset:node_header_offset+4], 'little')
                total_size = int.from_bytes(data[node_header_offset+4:node_header_offset+8], 'little')
                allocated_size = int.from_bytes(data[node_header_offset+8:node_header_offset+12], 'little')
                flags = data[node_header_offset+12]
                
                self.logger.debug(f"Node header: entries_offset={entries_offset}, size={total_size}, alloc={allocated_size}")
                
                # Start parsing entries from the entries offset
                offset = entries_offset
                self.logger.debug(f"Starting to parse entries at offset {offset}")
                
                while offset + 8 <= len(data):  # Need at least 8 bytes for entry header
                    # Parse entry header
                    entry_length = int.from_bytes(data[offset:offset+4], 'little')
                    self.logger.debug(f"Entry at {offset}: length={entry_length}")
                    
                    if entry_length == 0:
                        break
                        
                    if offset + entry_length > len(data):
                        self.logger.debug(f"Entry would exceed data length")
                        break
                    
                    # Get file reference (8 bytes at offset+8)
                    file_ref = int.from_bytes(data[offset+8:offset+16], 'little')
                    self.logger.debug(f"File reference: {file_ref}")
                    
                    if file_ref != 0:
                        # Parse filename from the filename attribute in the entry
                        fn_offset = offset + 16  # Start of filename attribute
                        
                        if fn_offset + 66 <= len(data):  # Need space for filename header + 2 bytes
                            try:
                                # Get filename length (1 byte)
                                name_length = data[fn_offset+64]
                                name_offset = fn_offset + 66
                                
                                if name_offset + name_length*2 <= len(data):
                                    filename = data[name_offset:name_offset+name_length*2].decode('utf-16le')
                                    self.logger.debug(f"Found filename: {filename}")
                                    
                                    if filename not in [".", ".."]:
                                        if filename not in [".", ".."] and not (filename.startswith("$") and file_ref <= 11):
                                            # Read the actual MFT entry
                                            file_entry_result = self.read_mft_entry(file_ref & 0xFFFFFFFFFFFF)
                                            if file_entry_result.is_ok():
                                                ntfs_file = NTFSFile(file_entry_result.value, self)
                                                if ntfs_file.name:
                                                    self.logger.debug(f"Adding file: {ntfs_file.name}")
                                                    files.append(ntfs_file)
                            except Exception as e:
                                self.logger.error(f"Error parsing filename: {str(e)}")
                                self.logger.error(traceback.format_exc())
                    
                    offset += entry_length
                
            # Parse $INDEX_ALLOCATION for large directories
            if index_allocation and not index_allocation.resident:
                self.logger.debug("Found non-resident $INDEX_ALLOCATION - parsing large directory")
                allocation_files = self._parse_index_allocation(index_allocation)
                files.extend(allocation_files)
                self.logger.debug(f"Found {len(allocation_files)} files in $INDEX_ALLOCATION")

        except Exception as e:
            self.logger.error(f"Error listing directory: {str(e)}")
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
        
        self.logger.debug(f"Found {len(files)} files in directory")
        return files

    def list_files(self, path: str = "/") -> Result[List[NTFSFile]]:
        """List all files in a directory"""
        try:
            if path == "/":
                # Get root directory (MFT entry 5)
                root_result = self.read_mft_entry(5)
                if root_result.is_err():
                    return root_result
                return Result.ok(self._list_directory(root_result.value))
            else:
                # Get directory entry for the path
                dir_result = self.get_file_by_path(path)
                if dir_result.is_err():
                    return dir_result
                
                dir_entry = dir_result.value
                if not dir_entry.is_directory():
                    return Result.err(NTFSError.NOT_FOUND, f"Not a directory: {path}")
                
                return Result.ok(self._list_directory(dir_entry))
            
        except Exception as e:
            return Result.err(NTFSError.IO_ERROR, f"Failed to list files: {str(e)}")

    def extract_file(self, path: str, output_path: str) -> Result[None]:
        """Extract a file to the specified location"""
        try:
            # Get file entry
            file_result = self.get_file_by_path(path)
            if file_result.is_err():
                return file_result
            
            ntfs_file = NTFSFile(file_result.value, self)
            if ntfs_file.is_directory:
                return Result.err(NTFSError.INVALID_PARAMETER, "Cannot extract directory")
            
            # Read file data
            data_result = ntfs_file.read_data()
            if data_result.is_err():
                return data_result
            
            # Write to output file
            with open(output_path, 'wb') as f:
                f.write(data_result.value)
            
            return Result.ok(None)
        except Exception as e:
            return Result.err(NTFSError.IO_ERROR, f"Failed to extract file: {str(e)}")

    def extract_all_files(self, output_dir: str, path: str = "/") -> Result[None]:
        """Extract all files recursively"""
        try:
            files_result = self.list_files(path)
            if files_result.is_err():
                return files_result
            
            os.makedirs(output_dir, exist_ok=True)
            
            for file in files_result.value:
                file_path = os.path.join(output_dir, file.name)
                if file.is_directory:
                    os.makedirs(file_path, exist_ok=True)
                    self.extract_all_files(file_path, os.path.join(path, file.name))
                else:
                    self.extract_file(os.path.join(path, file.name), file_path)
            
            return Result.ok(None)
        except Exception as e:
            return Result.err(NTFSError.IO_ERROR, f"Failed to extract all files: {str(e)}")

    def search_files(self, pattern: str) -> Result[List[str]]:
        """Search for files matching pattern"""
        try:
            results = []
            def search_dir(path: str):
                files_result = self.list_files(path)
                if files_result.is_err():
                    return
                
                for file in files_result.value:
                    full_path = os.path.join(path, file.name)
                    if pattern.lower() in file.name.lower():
                        results.append(full_path)
                    if file.is_directory:
                        search_dir(full_path)
            
            search_dir("/")
            return Result.ok(results)
        except Exception as e:
            return Result.err(NTFSError.IO_ERROR, f"Failed to search files: {str(e)}")

    def list_deleted_files(self) -> Result[List[NTFSFile]]:
        """List all deleted files"""
        try:
            deleted_files = []
            
            # Scan MFT for deleted entries
            for mft_ref in range(0, 100000):  # Scan first 100K entries
                entry_result = self.read_mft_entry(mft_ref)
                if entry_result.is_ok():
                    entry = entry_result.value
                    if not entry.is_in_use() and entry.has_file_name():
                        deleted_files.append(NTFSFile(entry, self))
            
            return Result.ok(deleted_files)
        except Exception as e:
            return Result.err(NTFSError.IO_ERROR, f"Failed to list deleted files: {str(e)}")

    def get_volume_info(self) -> Result[object]:
        """Get basic volume information"""
        if not self.boot_sector:
            return Result.err(NTFSError.INVALID_PARAMETER, "Volume not mounted")
        
        class VolumeInfo:
            def __init__(self, boot):
                self.bytes_per_sector = boot.bytes_per_sector
                self.sectors_per_cluster = boot.sectors_per_cluster
                self.total_sectors = boot.total_sectors
                self.mft_cluster = boot.mft_lcn
            
        return Result.ok(VolumeInfo(self.boot_sector))

    def _dump_attribute(self, attr, prefix=""):
        """Debug helper to dump attribute contents"""
        self.logger.debug(f"{prefix}Attribute type: 0x{attr.type_id:02x}")
        self.logger.debug(f"{prefix}Resident: {attr.resident}")
        if attr.resident:
            self.logger.debug(f"{prefix}Data length: {len(attr.data)}")
            self.logger.debug(f"{prefix}First 32 bytes: {attr.data[:32].hex()}")
        else:
            self.logger.debug(f"{prefix}Non-resident attribute")

    def _parse_index_allocation(self, index_allocation: MFTAttribute) -> List[NTFSFile]:
        """Parse a non-resident $INDEX_ALLOCATION attribute"""
        files = []
        try:
            if not hasattr(index_allocation, 'data_runs'):
                self.logger.debug("No data runs in $INDEX_ALLOCATION")
                return files

            for i, run in enumerate(index_allocation.data_runs):
                self.logger.debug(f"Processing run {i}: cluster={run.cluster}, length={run.length}")
                
                read_result = self.read_clusters(run.cluster, run.length)
                if read_result.is_err():
                    self.logger.error(f"Failed to read clusters: {read_result.message}")
                    continue
                    
                data = read_result.value
                
                # Verify INDX signature
                if data[0:4] != b'INDX':
                    self.logger.debug(f"Invalid index block signature: {data[0:4].hex()}")
                    continue

                # Parse index block header
                usa_offset = int.from_bytes(data[4:6], 'little')
                usa_count = int.from_bytes(data[6:8], 'little')
                
                # Skip to node header
                node_header_offset = 24
                entries_offset = node_header_offset + int.from_bytes(data[node_header_offset:node_header_offset+4], 'little')
                total_size = int.from_bytes(data[node_header_offset+4:node_header_offset+8], 'little')
                allocated_size = int.from_bytes(data[node_header_offset+8:node_header_offset+12], 'little')
                
                self.logger.debug(f"Index block header: entries_offset={entries_offset}, total_size={total_size}, alloc={allocated_size}")
                
                # Parse entries
                offset = entries_offset
                while offset + 16 < len(data):  # Need at least entry header
                    # Parse entry header
                    file_ref = int.from_bytes(data[offset:offset+8], 'little')  # 8 bytes file reference
                    entry_length = int.from_bytes(data[offset+8:offset+10], 'little')  # 2 bytes length
                    key_length = int.from_bytes(data[offset+10:offset+12], 'little')  # 2 bytes key length
                    flags = data[offset+12]  # 1 byte flags
                    
                    self.logger.debug(f"Entry at {offset}: file_ref={file_ref}, length={entry_length}, key_length={key_length}, flags={flags}")
                    
                    if flags & 2:  # Last entry
                        self.logger.debug("Found last entry marker")
                        break
                        
                    if entry_length == 0:  # End marker
                        break

                    if offset + entry_length > len(data):
                        self.logger.debug(f"Entry would exceed data length: {offset + entry_length} > {len(data)}")
                        break

                    # Parse filename attribute (starts after the entry header)
                    fn_offset = offset + 16  # Skip entry header
                    
                    try:
                        if fn_offset + key_length <= len(data):
                            # Parse standard filename attribute
                            name_length = data[fn_offset+64]  # Filename length
                            namespace = data[fn_offset+65]    # Filename namespace
                            name_offset = fn_offset + 66      # Filename starts here
                            
                            if name_offset + name_length*2 <= len(data):
                                filename = data[name_offset:name_offset+name_length*2].decode('utf-16le')
                                self.logger.debug(f"Found file: {filename} (len={name_length}, ns={namespace})")
                                
                                # Skip system files (those starting with $ and having low MFT numbers)
                                if not (filename.startswith("$") and file_ref <= 11):
                                    # Read the actual MFT entry (lower 48 bits of file reference)
                                    mft_ref = file_ref & 0xFFFFFFFFFFFF
                                    file_entry_result = self.read_mft_entry(mft_ref)
                                    if file_entry_result.is_ok():
                                        ntfs_file = NTFSFile(file_entry_result.value, self)
                                        if ntfs_file.name:
                                            self.logger.debug(f"Adding file: {ntfs_file.name}")
                                            files.append(ntfs_file)
                    except Exception as e:
                        self.logger.error(f"Error parsing filename at offset {fn_offset}: {str(e)}")
                        self.logger.error(traceback.format_exc())
                    
                    offset += entry_length

                self.logger.debug(f"Found {len(files)} files in index block")

        except Exception as e:
            self.logger.error(f"Error parsing $INDEX_ALLOCATION: {str(e)}")
            self.logger.error(traceback.format_exc())
        
        return files
