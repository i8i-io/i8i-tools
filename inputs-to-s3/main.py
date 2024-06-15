import boto3
import os
import time
import zipfile

def create_zip_incremental(directory_path, output_zip_path):
    """
    Creates a zip archive of the given directory incrementally.

    Parameters:
    - directory_path (str): Path to the directory to be archived.
    - output_zip_path (str): Path to save the output zip file.
    """
    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                fullpath = os.path.join(root, file)
                zipf.write(fullpath, arcname=os.path.relpath(fullpath, directory_path))
    
def multipart_upload_to_s3(local_file_path, bucket_name, s3_key, region_name):
    """
    Uploads a large file to an S3 bucket using multipart upload.

    Parameters:
    - local_file_path (str): Path to the local file to be uploaded.
    - bucket_name (str): Name of the S3 bucket.
    - s3_key (str): Key for the file in the S3 bucket.
    - region_name (str, optional): AWS region name. If not provided, the default configured region will be used.

    Returns:
    - str: URL of the uploaded file.
    """
    try:
        s3_client = boto3.client('s3',
                                     region_name=region_name)
        

        # Create a multipart upload
        mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)

        # Set the chunk size to 20MB (or another suitable size)
        chunk_size = 20 * 1024 * 1024
        parts = []
        part_number = 1

        # Read the file and upload it in parts
        with open(local_file_path, 'rb') as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break

                part = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=mpu['UploadId'],
                    Body=data
                )
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['ETag']
                })
                part_number += 1

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=mpu['UploadId'],
            MultipartUpload={'Parts': parts}
        )

        file_url = f"https://{bucket_name}.s3.{region_name}.amazonaws.com/{s3_key}" if region_name else f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
        return file_url


    except FileNotFoundError:
        print("The file was not found")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def zip_and_multipart_upload_directory(directory_path, bucket_name, s3_key,  region_name):
    """
    Creates a zip archive of a directory incrementally and uploads it to an S3 bucket using multipart upload.

    Parameters:
    - directory_path (str): Path to the directory to be archived and uploaded.
    - bucket_name (str): Name of the S3 bucket.
    - s3_key (str): Key for the file in the S3 bucket.
    - region_name (str, optional): AWS region name. If not provided, the default configured region will be used.

    Returns:
    - str: URL of the uploaded file.
    """
    zip_path = "/tmp/zipped_directory.zip"
    create_zip_incremental(directory_path, zip_path)
    
    return multipart_upload_to_s3(zip_path, bucket_name, s3_key,  region_name)

def get_folders_in_directory(directory_path):
    """
    Returns a list of folders in the given directory.
    
    :param directory_path: The path to the directory to search for folders.
    :return: A list of folder names in the specified directory.
    """
    try:
        # List all entries in the directory
        entries = os.listdir(directory_path)
        # Filter out only the directories
        folders = [entry for entry in entries if os.path.isdir(os.path.join(directory_path, entry))]
        return folders
    except FileNotFoundError:
        return f"The directory {directory_path} does not exist."
    except PermissionError:
        return f"Permission denied for accessing the directory {directory_path}."
    except Exception as e:
        return str(e)
    
if __name__ == "__main__":
    bucket = os.environ.get("i8i_OUTPUT_S3_BUCKET")
    s3_prefix = os.environ.get("i8i_OUTPUT_S3_PREFIX")
    region = os.environ.get("REGION")
    
    folders = get_folders_in_directory("/input")
    for folder in folders:
        s3_key = f'{s3_prefix}{folder}-outputs-{int(time.time())}.zip'
        directory_path = f'/input/{folder}'
        uploaded_file_url = zip_and_multipart_upload_directory(directory_path, bucket, s3_key, region)
    
        if uploaded_file_url:
            print(f"Directory successfully zipped and uploaded to: {uploaded_file_url}")
        else:
            print("Directory upload failed.")
     