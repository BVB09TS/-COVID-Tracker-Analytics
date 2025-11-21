# src/ingestion/copy_from_hdfs.py

import os
from hdfs import InsecureClient

# ------------------------------
# Configuration
# ------------------------------

# NOTE: The raw data for this project is stored in HDFS in the folder /healthcare/raw/
# Make sure this script is run from a container or environment that can access HDFS.

HDFS_URL = 'http://namenode:9870'  # Use the Hadoop Namenode container name, not localhost
HDFS_RAW_DIR = '/healthcare/raw/'   # HDFS directory containing raw data
LOCAL_RAW_DIR = '/home/jovyan/data/raw'  # Local folder inside container to copy files to

# HDFS username (adjust if needed)
HDFS_USER = 'root'

# ------------------------------
# Ensure local folder exists
# ------------------------------
os.makedirs(LOCAL_RAW_DIR, exist_ok=True)

# ------------------------------
# Connect to HDFS
# ------------------------------
client = InsecureClient(HDFS_URL, user=HDFS_USER)

# ------------------------------
# Copy files from HDFS to local folder
# ------------------------------
def copy_hdfs_to_local(hdfs_dir, local_dir):
    print(f"Copying data from HDFS directory {hdfs_dir} to local folder {local_dir} ...")
    
    try:
        files = client.list(hdfs_dir)
        if not files:
            print(f"No files found in HDFS directory: {hdfs_dir}")
            return
        
        for file_name in files:
            hdfs_path = os.path.join(hdfs_dir, file_name)
            local_path = os.path.join(local_dir, file_name)
            
            client.download(hdfs_path, local_path, overwrite=True)
            print(f"Copied {hdfs_path} -> {local_path}")
            
        print("All files copied successfully!")
            
    except Exception as e:
        print(f"Error copying from HDFS: {e}")
        print("Make sure the HDFS URL, container network, and user are correct.")

# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    print("INFO: This script copies raw data from HDFS to the local container folder.")
    copy_hdfs_to_local(HDFS_RAW_DIR, LOCAL_RAW_DIR)
