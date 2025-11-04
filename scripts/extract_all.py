import os
import zipfile
import shutil

# Base folder where state folders (az, ga, etc.) are located
base_dir = r"manual downloads"

# Folder to store all extracted outputs
output_base = os.path.join(base_dir, "extracted")
os.makedirs(output_base, exist_ok=True)

for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith(".zip"):
            zip_path = os.path.join(root, file)
            
            # Get the state abbreviation (the folder name like az, ga, etc.)
            state_folder = os.path.basename(root)
            extract_state_dir = os.path.join(output_base, state_folder)
            os.makedirs(extract_state_dir, exist_ok=True)

            # Create a subfolder named after the ZIP file (without .zip)
            extract_target = os.path.join(extract_state_dir, file.replace(".zip", ""))
            os.makedirs(extract_target, exist_ok=True)

            print(f"Extracting {zip_path} → {extract_target}")
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_target)
            except zipfile.BadZipFile:
                print(f"❌ Skipping invalid ZIP: {zip_path}")

print("\n✅ All ZIP files extracted successfully into the 'extracted' folder.")
