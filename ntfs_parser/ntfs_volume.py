class NTFSVolume:
    # ... existing code ...

    def is_file_deleted(self, mft_entry) -> bool:
        """Check if a file is deleted"""
        return not (mft_entry.flags & 0x1)  # Check if FILE_RECORD_SEGMENT_IN_USE flag is not set
    
    def get_file_created_time(self, mft_entry) -> int:
        """Get file creation timestamp"""
        for attr in mft_entry.attributes:
            if attr.type_code == STANDARD_INFORMATION:
                return attr.created_time
        return 0
    
    def get_file_size(self, mft_entry) -> int:
        """Get file size"""
        for attr in mft_entry.attributes:
            if attr.type_code == DATA:
                return attr.length if attr.is_resident else attr.data_size
        return 0

    def list_directory(self, path: str) -> Result[List[FileEntry]]:
        """Enhanced directory listing with file information"""
        try:
            mft_entry = self._get_mft_entry_by_path(path)
            if mft_entry is None:
                return Result.err("Path not found")

            entries = []
            for idx_entry in self._list_directory(mft_entry):
                entry_result = self.read_mft_entry(idx_entry.mft_reference)
                if entry_result.is_ok():
                    file_entry = entry_result.value
                    entries.append(FileEntry(
                        name=idx_entry.filename,
                        size=self.get_file_size(file_entry),
                        created_time=self.get_file_created_time(file_entry),
                        is_directory=bool(file_entry.flags & 0x2),  # Directory flag
                        is_deleted=self.is_file_deleted(file_entry)
                    ))
            return Result.ok(entries)
        except Exception as e:
            return Result.err(str(e)) 