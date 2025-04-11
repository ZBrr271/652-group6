import os
import zipfile
import subprocess


os.environ['KAGGLE_USERNAME'] = 'zmbarrett'  
os.environ['KAGGLE_KEY'] = 'eca1fde2463f087bbeea1ee3b57d4400'  


dataset_name = 'elizabethearhart/billboard-hot-1001958-2024'
subprocess.run(['kaggle', 'datasets', 'download', '-d', dataset_name, '--force'])


current_directory = os.getcwd()


zip_file_name = f"{dataset_name.split('/')[-1]}.zip"


with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
    zip_ref.extractall(current_directory) 
    extracted_files = zip_ref.namelist() 


extracted_file_name = extracted_files[0] if extracted_files else None

print(f"Extracted file: {extracted_file_name}")


os.remove(zip_file_name)

print(f"ZIP file {zip_file_name} has been deleted.")