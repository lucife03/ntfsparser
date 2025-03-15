import os
import sys
from create_test_image import create_ntfs_test_image
from analyze_raw import analyze_ntfs_raw

def main():
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Set image path
    image_path = os.path.join(script_dir, "ntfs.raw")
    
    print("\n1. Creating test image...")
    create_ntfs_test_image("ntfs.raw", 100)  # 100MB test image
    
    print("\n2. Analyzing created image...")
    results = analyze_ntfs_raw(image_path)
    
    print("\nAnalysis Results:")
    print("-" * 50)
    
    if 'error' in results:
        print("Error analyzing file:")
        print(f"  {results['error']}")
        if 'first_sector_hex' in results:
            print("\nFirst sector (hex):")
            print(f"  {results['first_sector_hex']}")
    else:
        for key, value in results.items():
            if key != 'first_sector_hex':  # Skip raw hex dump unless there's an error
                print(f"{key:20}: {value}")

if __name__ == "__main__":
    main() 