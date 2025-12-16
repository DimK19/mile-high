import os
from pathlib import Path
from configparser import ConfigParser
from azure.storage.blob import BlobServiceClient, ContentSettings

def upload_video_to_azure(file_path, container_name, connection_string):
    try:
        # 1. Create the BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # 2. Extract filename from path to use as the blob name
        blob_name = os.path.basename(file_path)

        # 3. Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        print(f"Uploading {blob_name} to container '{container_name}'...")

        # 4. Upload the created file
        # 'use_byte_buffer=True' is often better for large video files
        with open(file_path, "rb") as data:
            blob_client.upload_blob(
                data, 
                overwrite=True,
                content_settings=ContentSettings(content_type='video/mp4') # Best practice for videos
            )

        print(f"Success! Video uploaded to: {blob_client.url}")

    except Exception as ex:
        print(f"Exception: {ex}")

if (__name__ == '__main__'):
    config = ConfigParser()
    config.read('config.ini', encoding = 'utf-8')
    # CONFIGURATION
    # Replace these with your actual values
    CONNECTION_STR = config['KEYS']['CONNECTION_STR']
    CONTAINER_NAME = config['KEYS']['CONTAINER_NAME']
    VIDEO_PATH = Path(config['PATHS']['VIDEO_PATH'])

    upload_video_to_azure(VIDEO_PATH, CONTAINER_NAME, CONNECTION_STR)