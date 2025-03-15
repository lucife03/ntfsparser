import os
import sys
from src.ntfs.volume import NTFSVolume
from src.core.errors import NTFSError

def print_file_list(files, indent=""):
    """Print file list with indentation"""
    for file in files:
        print(f"{indent}{file.name} {'<DIR>' if file.is_directory else file.size} bytes")

def print_usage():
    print("Usage:")
    print("  python analyze.py <image_file> <command> [args]")
    print("\nCommands:")
    print("  list                     - List all files")
    print("  extract <file> <output>  - Extract a specific file")
    print("  extract-all <output_dir> - Extract all files")
    print("  search <pattern>         - Search for files by name")
    print("  deleted                  - List deleted files")
    print("  extract-deleted <dir>    - Extract all deleted files")

def main():
    if len(sys.argv) < 3:
        print_usage()
        return

    image_path = sys.argv[1]
    command = sys.argv[2]

    volume = NTFSVolume(image_path)
    mount_result = volume.mount()
    if mount_result.is_err():
        print(f"Error mounting volume: {mount_result.message}")
        return

    try:
        if command == "list":
            list_result = volume.list_files()
            if list_result.is_err():
                print(f"Error listing files: {list_result.message}")
                return
                
            print("\nContents of /:")
            for file in list_result.value:
                size_str = "<DIR>" if file.is_directory else f"{file.size} bytes"
                print(f"{file.name} {size_str}")

        elif command == "extract" and len(sys.argv) == 5:
            file_path = sys.argv[3]
            output_path = sys.argv[4]
            
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            print(f"Extracting: {file_path}")
            print(f"To: {os.path.abspath(output_path)}")
            
            result = volume.extract_file(file_path, output_path)
            if result.is_err():
                print(f"Error extracting file: {result.message}")
            else:
                print(f"Successfully extracted to: {os.path.abspath(output_path)}")
                print(f"File size: {os.path.getsize(output_path)} bytes")

        elif command == "extract-all" and len(sys.argv) == 4:
            output_dir = sys.argv[3]
            
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            print(f"\nExtracting all files to: {os.path.abspath(output_dir)}")
            
            list_result = volume.list_files()
            if list_result.is_err():
                print(f"Error listing files: {list_result.message}")
                return
                
            for file in list_result.value:
                if not file.is_directory and not file.name.startswith("$"):
                    out_path = os.path.join(output_dir, file.name)
                    print(f"\nExtracting: {file.name}")
                    print(f"To: {os.path.abspath(out_path)}")
                    print(f"Size: {file.size} bytes")
                    
                    result = volume.extract_file(file.name, out_path)
                    if result.is_err():
                        print(f"  Error: {result.message}")
                    else:
                        print(f"  Success -> {os.path.abspath(out_path)}")
                        print(f"  Written: {os.path.getsize(out_path)} bytes")

        elif command == "search" and len(sys.argv) == 4:
            pattern = sys.argv[3].lower()
            list_result = volume.list_files()
            if list_result.is_err():
                print(f"Error listing files: {list_result.message}")
                return
                
            print(f"\nFiles matching '{pattern}':")
            for file in list_result.value:
                if pattern in file.name.lower():
                    size_str = "<DIR>" if file.is_directory else f"{file.size} bytes"
                    print(f"{file.name} {size_str}")

        elif command == "deleted":
            result = volume.list_deleted_files()
            if result.is_err():
                print(f"Error listing deleted files: {result.message}")
                return
                
            print("\nDeleted files:")
            for file in result.value:
                print(f"{file.name} {file.size} bytes")

        elif command == "extract-deleted" and len(sys.argv) == 4:
            output_dir = sys.argv[3]
            
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            print(f"\nExtracting deleted files to: {os.path.abspath(output_dir)}")
            
            result = volume.list_deleted_files()
            if result.is_err():
                print(f"Error listing deleted files: {result.message}")
                return

            for file in result.value:
                if not file.is_directory:
                    # Add _deleted suffix to avoid name conflicts
                    base, ext = os.path.splitext(file.name)
                    out_name = f"{base}_deleted{ext}"
                    out_path = os.path.join(output_dir, out_name)
                    
                    print(f"\nExtracting deleted file: {file.name}")
                    print(f"To: {os.path.abspath(out_path)}")
                    print(f"Original size: {file.size} bytes")
                    
                    try:
                        data_result = file.read_data()
                        if data_result.is_err():
                            print(f"  Error: {data_result.message}")
                            continue
                            
                        data = data_result.value
                        with open(out_path, 'wb') as f:
                            f.write(data)
                            
                        print(f"  Success -> {os.path.abspath(out_path)}")
                        print(f"  Written: {os.path.getsize(out_path)} bytes")
                        
                    except Exception as e:
                        print(f"  Error extracting file: {str(e)}")

        else:
            print_usage()

    finally:
        volume.close()

if __name__ == "__main__":
    main() 