import os
import time
import platform
from typing import Optional, Dict, List, Any

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config

from ayon_sitesync.utils import time_function
from ayon_sitesync.providers.abstract_provider import AbstractProvider


class S3Handler(AbstractProvider):
    """
        Implementation of AWS S3 API.
        Uses boto3 for S3 operations. S3 has a flat structure but simulates
        folders using key prefixes with '/' delimiters.

        Configuration for provider is in
            'settings/defaults/project_settings/global.json'

        Settings could be overwritten per project.

        Example of config:
          "s3": {   - site name
            "provider": "s3", - type of provider, label must be registered
            "credentials": {
                "aws_access_key_id": "your_key",
                "aws_secret_access_key": "your_secret",
                "aws_session_token": "optional_token"  # for temp credentials
            },
            "bucket": "my-s3-bucket",
            "root": {  - could be "root": "/" for single root
                "root_one": "/project1",
                "root_two": "/project2/another"
            },
            "endpoint_url": "https://custom-endpoint.com",  # optional
            "region_name": "us-east-1"  # optional
          }
    """
    CODE = "s3"
    LABEL = "AWS S3"

    CHUNK_SIZE = 8388608  # 8MB chunks for multipart upload

    def __init__(self, project_name, site_name, tree=None, presets=None):
        self.active = False
        self.project_name = project_name
        self.site_name = site_name
        self.client = None
        self.bucket = None

        self.presets = presets
        if not self.presets:
            self.log.info(
                "Sync Server: There are no presets for {}.".format(site_name)
            )
            return

        if not self.presets.get("enabled"):
            self.log.debug(
                "Sync Server: Site {} not enabled for {}.".format(
                    site_name, project_name
                )
            )
            return

        # Get credentials configuration
        credentials = self.presets.get("credentials", {})
        if not credentials:
            msg = "Sync Server: No credentials configured for S3 provider"
            self.log.info(msg)
            return

        # Get bucket
        self.bucket = self.presets.get("bucket")
        if not self.bucket:
            msg = "Sync Server: No bucket configured for S3 provider"
            self.log.info(msg)
            return

        # Optional parameters
        endpoint_url = self.presets.get("endpoint_url")
        region_name = self.presets.get("region_name", "us-east-1")

        # Configure boto3 with retries
        config = Config(
            retries={
                'max_attempts': 3,
                'mode': 'standard'
            }
        )

        try:
            # Create S3 client
            client_kwargs = {
                'service_name': 's3',
                'region_name': region_name,
                'config': config
            }

            # Add credentials if provided
            if credentials.get("aws_access_key_id"):
                client_kwargs["aws_access_key_id"] = credentials["aws_access_key_id"]
                client_kwargs["aws_secret_access_key"] = credentials.get("aws_secret_access_key", "")
                if credentials.get("aws_session_token"):
                    client_kwargs["aws_session_token"] = credentials["aws_session_token"]

            # Add endpoint URL if provided (for MinIO or other S3-compatible services)
            if endpoint_url:
                client_kwargs["endpoint_url"] = endpoint_url

            self.client = boto3.client(**client_kwargs)

            # Test connection by checking bucket existence
            self.client.head_bucket(Bucket=self.bucket)

        except NoCredentialsError:
            msg = "Sync Server: No valid AWS credentials found for S3 provider"
            self.log.info(msg)
            return
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                msg = f"Sync Server: Bucket '{self.bucket}' does not exist"
            else:
                msg = f"Sync Server: Failed to connect to S3: {str(e)}"
            self.log.info(msg)
            return
        except Exception as e:
            msg = f"Sync Server: Failed to initialize S3 client: {str(e)}"
            self.log.info(msg)
            return

        self._tree = tree
        self.active = True

    def is_active(self):
        """
            Returns True if provider is activated, eg. has working credentials.
        Returns:
            (boolean)
        """
        return self.presets.get("enabled") and self.client is not None

    def get_roots_config(self, anatomy=None):
        """
            Returns root values for path resolving

            Use only Settings as S3 cannot be modified by Local Settings

        Returns:
            (dict) - {"root": {"root": "/"}}
                     OR
                     {"root": {"root_ONE": "value", "root_TWO":"value}}
            Format is importing for usage of python's format ** approach
        """
        roots = self.presets.get("root", {})
        if not roots:
            # Default to bucket root
            roots = {"root": "/"}
        return {"root": roots}

    def get_tree(self):
        """
            S3 is flat structure, tree is optional and used for caching
            folder structure if needed.

        Returns:
             (dictionary) - path to metadata mapping
        """
        if not self._tree:
            self._tree = {}
        return self._tree

    def create_folder(self, path):
        """
            Create all nonexistent folders and subfolders in 'path'.
            In S3, folders are just zero-byte objects with key ending in '/'.

        Args:
            path (string): absolute path, starts with root, without filename
        Returns:
            (string) folder path of lowest subfolder from 'path'
        """
        # Clean path
        path = path.strip('/')
        if not path:
            return "/"

        parts = path.split("/")
        current_path = ""

        for part in parts:
            current_path = f"{current_path}/{part}" if current_path else part
            folder_key = f"{current_path}/"

            if not self.folder_path_exists(current_path):
                try:
                    self.client.put_object(
                        Bucket=self.bucket,
                        Key=folder_key,
                        Body=b''
                    )
                    # Update tree cache
                    if self._tree is not None:
                        self._tree[current_path] = {"key": folder_key}
                except ClientError as e:
                    self.log.error(f"Failed to create folder {current_path}: {str(e)}")
                    raise

        return f"{current_path}/"

    def upload_file(
            self,
            source_path,
            target_path,
            addon,
            project_name,
            file,
            repre_status,
            site_name,
            overwrite=False
    ):
        """
            Uploads single file from 'source_path' to destination 'path'.
            It creates all folders on the path if are not existing.

        Args:
            source_path (string): absolute path on provider
            target_path (string): absolute path with or without name of the file
            addon (SiteSyncAddon): addon instance to call update_db on
            project_name (str):
            file (dict): info about uploaded file (matches structure from db)
            repre_status (dict): complete representation containing
                sync progress
            site_name (str): site name
            overwrite (boolean): replace existing file

        Returns:
            (string) file_id/key of created/modified file ,
                throws FileExistsError, FileNotFoundError exceptions
        """
        if not os.path.isfile(source_path):
            raise FileNotFoundError("Source file {} doesn't exist."
                                    .format(source_path))

        # Parse target path
        target_path = target_path.strip('/')
        root, ext = os.path.splitext(target_path)
        if ext:
            # full path with file name
            target_name = os.path.basename(target_path)
            target_folder = os.path.dirname(target_path)
        else:
            # just folder path, use source filename
            target_name = os.path.basename(source_path)
            target_folder = target_path

        s3_key = f"{target_folder}/{target_name}" if target_folder else target_name
        s3_key = s3_key.strip('/')

        # Check if file exists
        target_file = self.file_path_exists(s3_key)
        if target_file and not overwrite:
            raise FileExistsError("File already exists, "
                                  "use 'overwrite' argument")

        # Ensure folder exists
        if target_folder:
            self.create_folder(target_folder)

        # Get file size for progress tracking
        file_size = os.path.getsize(source_path)

        try:
            # Use multipart upload for large files
            if file_size > self.CHUNK_SIZE:
                response = self._multipart_upload(
                    source_path, s3_key, addon, project_name,
                    file, repre_status, site_name
                )
            else:
                # Simple upload for small files
                with open(source_path, 'rb') as f:
                    self.client.put_object(
                        Bucket=self.bucket,
                        Key=s3_key,
                        Body=f.read()
                    )
                response = {"ETag": ""}  # Simplified response

            # Update tree cache
            if self._tree is not None:
                self._tree[s3_key] = {"key": s3_key}

            return s3_key

        except ClientError as e:
            self.log.error(f"Failed to upload file {source_path}: {str(e)}")
            raise

    def _multipart_upload(self, source_path, s3_key, addon, project_name,
                          file, repre_status, site_name):
        """
            Performs multipart upload for large files with progress tracking.
        """
        mpu = self.client.create_multipart_upload(
            Bucket=self.bucket,
            Key=s3_key
        )
        upload_id = mpu['UploadId']

        parts = []
        uploaded_bytes = 0
        file_size = os.path.getsize(source_path)

        try:
            with open(source_path, 'rb') as f:
                part_number = 1
                last_tick = None

                while True:
                    data = f.read(self.CHUNK_SIZE)
                    if not data:
                        break

                    # Check for pause
                    if addon.is_representation_paused(
                            repre_status["representationId"],
                            check_parents=True,
                            project_name=project_name):
                        # Abort upload if paused
                        self.client.abort_multipart_upload(
                            Bucket=self.bucket,
                            Key=s3_key,
                            UploadId=upload_id
                        )
                        raise ValueError("Paused during process, please redo.")

                    part = self.client.upload_part(
                        Bucket=self.bucket,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=data
                    )
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })

                    uploaded_bytes += len(data)
                    status_val = uploaded_bytes / file_size

                    # Log progress periodically
                    if not last_tick or \
                            time.time() - last_tick >= addon.LOG_PROGRESS_SEC:
                        last_tick = time.time()
                        self.log.debug("Uploaded %d%%." % int(status_val * 100))
                        addon.update_db(
                            project_name=project_name,
                            new_file_id=None,
                            file=file,
                            repre_status=repre_status,
                            site_name=site_name,
                            side="remote",
                            progress=status_val
                        )

                    part_number += 1

            # Complete multipart upload
            result = self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            return result

        except Exception as e:
            # Abort upload on error
            self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key,
                UploadId=upload_id
            )
            raise

    def download_file(
            self,
            source_path,
            local_path,
            addon,
            project_name,
            file,
            repre_status,
            site_name,
            overwrite=False
    ):
        """
            Downloads single file from 'source_path' (remote) to 'local_path'.
            It creates all folders on the local_path if are not existing.
            By default existing file on 'local_path' will trigger an exception

        Args:
            source_path (string): absolute path on provider
            local_path (string): absolute path with or without name of the file
            addon (SiteSyncAddon): addon instance to call update_db on
            project_name (str):
            file (dict): info about uploaded file (matches structure from db)
            repre_status (dict): complete representation containing
                sync progress
            site_name (str): site name
            overwrite (boolean): replace existing file

        Returns:
            (string) file_id/key of created/modified file ,
                throws FileExistsError, FileNotFoundError exceptions
        """
        s3_key = source_path.strip('/')

        # Check if file exists in S3
        if not self.file_path_exists(s3_key):
            raise FileNotFoundError("Source file {} doesn't exist."
                                    .format(s3_key))

        # Parse local path
        root, ext = os.path.splitext(local_path)
        if ext:
            # full path with file name
            target_name = os.path.basename(local_path)
            local_folder = os.path.dirname(local_path)
        else:
            # just folder path, use source filename
            target_name = os.path.basename(source_path)
            local_folder = local_path

        local_file_path = os.path.join(local_folder, target_name)

        if os.path.isfile(local_file_path) and not overwrite:
            raise FileExistsError("File already exists, "
                                  "use 'overwrite' argument")

        # Create local folders if needed
        os.makedirs(local_folder, exist_ok=True)

        try:
            # Get file size for progress tracking
            head_response = self.client.head_object(
                Bucket=self.bucket,
                Key=s3_key
            )
            file_size = head_response['ContentLength']

            # Download with progress tracking
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=s3_key
            )

            with open(local_file_path, 'wb') as f:
                body = response['Body']
                downloaded_bytes = 0
                last_tick = None

                while True:
                    chunk = body.read(self.CHUNK_SIZE)
                    if not chunk:
                        break

                    # Check for pause
                    if addon.is_representation_paused(
                            repre_status["representationId"],
                            check_parents=True,
                            project_name=project_name
                    ):
                        # Clean up partial download
                        if os.path.exists(local_file_path):
                            os.remove(local_file_path)
                        raise ValueError("Paused during process, please redo.")

                    f.write(chunk)
                    downloaded_bytes += len(chunk)
                    status_val = downloaded_bytes / file_size

                    # Log progress periodically
                    if not last_tick or \
                            time.time() - last_tick >= addon.LOG_PROGRESS_SEC:
                        last_tick = time.time()
                        self.log.debug("Downloaded %d%%." % int(status_val * 100))
                        addon.update_db(
                            project_name=project_name,
                            new_file_id=None,
                            file=file,
                            repre_status=repre_status,
                            site_name=site_name,
                            side="local",
                            progress=status_val
                        )

            return target_name

        except ClientError as e:
            self.log.error(f"Failed to download file {s3_key}: {str(e)}")
            # Clean up partial download
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
            raise

    def delete_file(self, path):
        """
            Deletes file from 'path'. Expects path to specific file.

        Args:
            path: absolute path to particular file

        Returns:
            None
        """
        s3_key = path.strip('/')

        if not self.file_path_exists(s3_key):
            raise ValueError("File {} doesn't exist".format(s3_key))

        try:
            self.client.delete_object(
                Bucket=self.bucket,
                Key=s3_key
            )

            # Update tree cache
            if self._tree is not None and s3_key in self._tree:
                del self._tree[s3_key]

        except ClientError as e:
            self.log.error(f"Failed to delete file {s3_key}: {str(e)}")
            raise

    def list_folder(self, folder_path):
        """
            List all files and subfolders of particular path non-recursively.

        Args:
            folder_path (string): absolute path on provider
        Returns:
             (list) of dictionaries with file/folder info
        """
        prefix = folder_path.strip('/')
        if prefix and not prefix.endswith('/'):
            prefix += '/'

        items = []

        try:
            paginator = self.client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.bucket,
                Prefix=prefix,
                Delimiter='/'
            )

            for page in page_iterator:
                # Add folders (common prefixes)
                for common_prefix in page.get('CommonPrefixes', []):
                    folder_path = common_prefix['Prefix'].rstrip('/')
                    items.append({
                        'name': os.path.basename(folder_path),
                        'path': folder_path,
                        'type': 'folder'
                    })

                # Add files
                for obj in page.get('Contents', []):
                    # Skip the folder marker itself
                    if obj['Key'] == prefix:
                        continue

                    file_path = obj['Key']
                    items.append({
                        'name': os.path.basename(file_path),
                        'path': file_path,
                        'type': 'file',
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag']
                    })

            return items

        except ClientError as e:
            self.log.error(f"Failed to list folder {folder_path}: {str(e)}")
            raise

    def folder_path_exists(self, folder_path):
        """
            Checks if folder path exists in S3 by looking for zero-byte
            objects with folder prefix.

        Args:
            folder_path (string): S3 path with / as separator
        Returns:
            (string) folder key or False
        """
        if not folder_path:
            return False

        folder_path = folder_path.strip('/')
        folder_key = f"{folder_path}/" if folder_path else ""

        # Check if we have it in tree cache
        if self._tree is not None and folder_path in self._tree:
            return self._tree[folder_path].get("key", folder_key)

        # Check if folder exists by listing with delimiter
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=folder_key,
                Delimiter='/',
                MaxKeys=1
            )

            # If there are contents or common prefixes, folder exists
            if response.get('Contents') or response.get('CommonPrefixes'):
                return folder_key

        except ClientError:
            pass

        return False

    def file_path_exists(self, file_path):
        """
            Checks if 'file_path' exists in S3

        Args:
            file_path (string): S3 key path, from root, with file name
        Returns:
            (dictionary|boolean) file metadata | False if not found
        """
        s3_key = file_path.strip('/')

        try:
            response = self.client.head_object(
                Bucket=self.bucket,
                Key=s3_key
            )
            return {
                'key': s3_key,
                'size': response['ContentLength'],
                'etag': response['ETag'].strip('"'),
                'last_modified': response['LastModified']
            }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

        return False

    def get_signed_url(self, path, expiration=3600):
        """
            Generate a presigned URL for temporary file access.
            Useful for sharing files without requiring AWS credentials.

        Args:
            path (string): S3 key path
            expiration (int): URL expiration in seconds

        Returns:
            (string) Presigned URL
        """
        s3_key = path.strip('/')

        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket,
                    'Key': s3_key
                },
                ExpiresIn=expiration
            )
            return url
        except ClientError as e:
            self.log.error(f"Failed to generate signed URL: {str(e)}")
            raise
