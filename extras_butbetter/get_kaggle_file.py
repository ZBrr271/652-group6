# 685.652 - Group 6
# get_kaggle_file.py
# Retrieve a particular file from kaggle via API
# Unzip and extract to data subdirectory

# The dataset includes the songs, artists, and chart positions on
# The Billboard Hot 100 chart from 1958 to 2024

import os
import zipfile
import subprocess


# Retrieves a specific file from kaggle via API
def get_kag_file(kag_username, kag_key):

    # User needs to set up account on kaggle and generate API key
    os.environ['KAGGLE_USERNAME'] = kag_username  
    os.environ['KAGGLE_KEY'] = kag_key 

    # Retrieving a specific dataset
    dataset_name = 'elizabethearhart/billboard-hot-1001958-2024'
    zip_file_name = f"{dataset_name.split('/')[-1]}.zip"
    
    # Data is to go into data subdirectory
    data_dir = os.path.join(os.getcwd(), 'data')
    os.makedirs(data_dir, exist_ok=True)

    # Download the dataset by running the kaggle command
    subprocess.run([
        'kaggle', 'datasets', 'download', 
        '-d', dataset_name, 
        '--force', '-p', data_dir])

    # Unzip to data subdirectory
    zip_path = os.path.join(data_dir, zip_file_name)
    try:

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir) 
            extracted_files = zip_ref.namelist()

        if extracted_files:
            extracted_file_name = extracted_files[0]
            print(f"Extracted Kaggle file: {extracted_file_name} to data directory")

    except PermissionError:
        print("Error extracting ZIP file: Permission denied")
        print("Close Kaggle data csv if you already have it open in another program")
        return None

    # Extract first file
    extracted_file_name = extracted_files[0] if extracted_files else None
    print(f"Extracted Kaggle file: {extracted_file_name} to data directory")

    # Keep extracted file, delete zip
    os.remove(zip_path)
    print(f"The ZIP file {zip_file_name} has been deleted\n")

    return extracted_file_name


username = 'zmbarrett'  
key = 'eca1fde2463f087bbeea1ee3b57d4400'

print(f"Kaggle file: {get_kag_file(username, key)}")