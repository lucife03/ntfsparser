import os
import sys
from src.ntfs.volume import NTFSVolume
from src.core.errors import NTFSError

def print_file_list(files, indent=""):
    """Print file list with indentation"""
    for file in files:
        print(f"{indent}{file.name} {'<DIR>' if file.is_directory else file.size} bytes")

def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_raw.py <ntfs_image_base> <command> [args]")
        print("Note: For split images like 'ntfs image.002', use 'ntfs image' as base name")
        print("\nCommands:")
        print("  list [path]          - List files in directory (default: root)")
        print("  extract <src> <dst>  - Extract file to destination")
        print("  extractall <dst>     - Extract all files to directory")
        print("  search <pattern>     - Search for files")
        print("  deleted             - List deleted files")
        return

    # Get the base name for the image files
    image_base = sys.argv[1]
    command = sys.argv[2] if len(sys.argv) > 2 else "list"

    # Print current working directory and image path
    print(f"Current directory: {os.getcwd()}")
    print(f"Image base name: {image_base}")

    # Create and mount volume
    try:
        volume = NTFSVolume(image_base)
        print(f"Attempting to mount volume: {image_base}")
        mount_result = volume.mount()
        if mount_result.is_err():
            print(f"Failed to mount volume: {mount_result.message}")
            return

        # Add volume information display
        info = volume.get_volume_info()
        if info.is_ok():
            print("\nVolume Information:")
            print(f"Bytes per sector: {info.value.bytes_per_sector}")
            print(f"Sectors per cluster: {info.value.sectors_per_cluster}")
            print(f"Total sectors: {info.value.total_sectors}")
            print(f"MFT location: {info.value.mft_cluster}")
            print(f"Volume size: {info.value.total_sectors * info.value.bytes_per_sector / (1024*1024*1024):.2f} GB")
            print()

        if command == "list":
            path = sys.argv[3] if len(sys.argv) > 3 else "/"
            result = volume.list_files(path)
            if result.is_ok():
                print(f"\nContents of {path}:")
                print_file_list(result.value)
            else:
                print(f"Error: {result.message}")

        elif command == "extract":
            if len(sys.argv) < 5:
                print("Error: extract command needs source and destination paths")
                return
            src, dst = sys.argv[3], sys.argv[4]
            result = volume.extract_file(src, dst)
            if result.is_ok():
                print(f"Successfully extracted {src} to {dst}")
            else:
                print(f"Error: {result.message}")

        elif command == "extractall":
            if len(sys.argv) < 4:
                print("Error: extractall command needs destination directory")
                return
            dst = sys.argv[3]
            result = volume.extract_all_files(dst)
            if result.is_ok():
                print(f"Successfully extracted all files to {dst}")
            else:
                print(f"Error: {result.message}")

        elif command == "search":
            if len(sys.argv) < 4:
                print("Error: search command needs a pattern")
                return
            pattern = sys.argv[3]
            result = volume.search_files(pattern)
            if result.is_ok():
                print("\nFound files:")
                for path in result.value:
                    print(path)
            else:
                print(f"Error: {result.message}")

        elif command == "deleted":
            result = volume.list_deleted_files()
            if result.is_ok():
                print("\nDeleted files:")
                print_file_list(result.value)
            else:
                print(f"Error: {result.message}")

        else:
            print(f"Unknown command: {command}")

    finally:
        volume.close()

if __name__ == "__main__":
    main() 