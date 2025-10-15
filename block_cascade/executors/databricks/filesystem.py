"""
Custom filesystem implementation for Databricks Unity Catalog Volumes.

This module provides fsspec-compatible filesystem class for interacting with
Unity Catalog Volumes (/Volumes/<catalog>/<schema>/<volume>/<path>).

Unity Catalog Volumes provide:
- Proper security and permissions through Unity Catalog governance
- Serverless compute compatibility
- Fine-grained access control
- Cross-workspace accessibility

This filesystem uses the Databricks Files API to perform file operations
from the client side.
"""

import io
import os
import logging
from typing import BinaryIO

logger = logging.getLogger(__name__)

# Buffer size for streaming uploads
BUFFER_SIZE_BYTES = 2**20  # 1MB


class DatabricksFilesystem:
    """
    An fsspec-compatible filesystem for Databricks Unity Catalog Volumes.
    
    This filesystem provides file operations (open, upload, delete) that work with
    Unity Catalog Volumes from the client side using the Databricks Files API.
    
    Unity Catalog Volumes provide proper security and permissions through UC governance,
    making them the recommended storage location for serverless Databricks compute.
    
    Path format: /Volumes/<catalog>/<schema>/<volume>/<path>
    Example: /Volumes/main/my_team/cascade/job_artifacts/
    
    Parameters
    ----------
    api_client : ApiClient
        Databricks API client from databricks-cli
    auto_mkdir : bool
        Automatically create parent directories when opening files for writing
    """
    
    def __init__(self, api_client, auto_mkdir: bool = True):
        self.api_client = api_client
        self.auto_mkdir = auto_mkdir
    
    def _normalize_path(self, path: str) -> str:
        """Normalize the path for Unity Catalog Volumes."""
        # Unity Catalog Volumes paths should start with /Volumes/
        if not path.startswith("/Volumes/"):
            if path.startswith("dbfs:/Volumes/"):
                path = path.replace("dbfs:", "")
        return path
    
    def _ensure_parent_dir(self, path: str) -> None:
        """Ensure parent directory exists if auto_mkdir is enabled."""
        if not self.auto_mkdir:
            return
            
        parent_path = os.path.dirname(path)
        if parent_path and parent_path != "/Volumes":
            try:
                # Create directory using Files API
                self.api_client.perform_query(
                    'PUT',
                    '/fs/directories' + parent_path,
                    data=None,
                    headers={'Content-Type': 'application/json'}
                )
                logger.debug(f"Created parent directory: {parent_path}")
            except Exception as e:
                # Directory may already exist, which is fine
                logger.debug(f"Could not create parent directory {parent_path}: {e}")
    
    def open(self, path: str, mode: str = "rb") -> BinaryIO:
        """
        Open a file for reading or writing in Unity Catalog Volumes.
        
        Parameters
        ----------
        path : str
            Path to the file in Unity Catalog Volumes
            Example: /Volumes/catalog/schema/volume/file.pkl
        mode : str
            File mode: 'rb' for reading, 'wb' for writing
            
        Returns
        -------
        BinaryIO
            File-like object for reading/writing
        """
        path = self._normalize_path(path)
        
        if mode == "rb":
            # Read mode: download file using Files API
            try:
                # Download using Files API - use session directly for binary data
                # perform_query() tries to parse as JSON, but /fs/files returns raw bytes
                url = self.api_client.get_url('/fs/files' + path)
                headers = {
                    'Authorization': self.api_client.default_headers.get('Authorization', '')
                }
                response = self.api_client.session.get(url, headers=headers)
                response.raise_for_status()
                
                return io.BytesIO(response.content)
                
            except Exception as e:
                logger.error(f"Failed to read file {path}: {e}")
                raise FileNotFoundError(f"Could not read file {path}: {e}")
                
        elif mode == "wb":
            # Write mode: return a special file object that uploads on close
            self._ensure_parent_dir(path)
            return _DatabricksUploadFile(self.api_client, path)
            
        else:
            raise ValueError(f"Unsupported mode: {mode}. Only 'rb' and 'wb' are supported.")
    
    def upload(self, local_path: str, remote_path: str, overwrite: bool = True) -> None:
        """
        Upload a local file to Unity Catalog Volumes.
        
        Parameters
        ----------
        local_path : str
            Local file path to upload
        remote_path : str
            Remote path in Unity Catalog Volumes
            Example: /Volumes/catalog/schema/volume/file.py
        overwrite : bool
            Whether to overwrite existing file
        """
        remote_path = self._normalize_path(remote_path)
        self._ensure_parent_dir(remote_path)
        
        try:
            # Read local file
            with open(local_path, "rb") as f:
                content = f.read()
            
            # Upload using Files API - use session directly for binary data
            url = self.api_client.get_url('/fs/files' + remote_path)
            headers = {
                'Content-Type': 'application/octet-stream',
                'Authorization': self.api_client.default_headers.get('Authorization', '')
            }
            response = self.api_client.session.put(
                url,
                data=content,
                headers=headers,
                params={'overwrite': str(overwrite).lower()}
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to upload file {local_path} to {remote_path}: {e}")
            raise RuntimeError(f"Upload failed: {e}")
    
    def rm(self, path: str, recursive: bool = False) -> None:
        """
        Delete a file or directory.
        
        Parameters
        ----------
        path : str
            Path to delete
        recursive : bool
            If True, delete directory and all its contents
        """
        path = self._normalize_path(path)
        
        try:
            if recursive:
                # For recursive deletion, delete files individually first, then the directory
                try:
                    list_url = self.api_client.get_url('/fs/directories' + path)
                    headers = {
                        'Authorization': self.api_client.default_headers.get('Authorization', '')
                    }
                    list_response = self.api_client.session.get(list_url, headers=headers)
                    list_response.raise_for_status()
                    
                    contents = list_response.json()
                    files = contents.get('contents', [])
                    
                    # Delete each file
                    for item in files:
                        item_path = item.get('path')
                        if item_path:
                            try:
                                file_url = self.api_client.get_url('/fs/files' + item_path)
                                file_response = self.api_client.session.delete(file_url, headers=headers)
                                file_response.raise_for_status()
                            except Exception:
                                pass  # File may already be deleted
                    
                    # Delete the empty directory
                    dir_url = self.api_client.get_url('/fs/directories' + path)
                    dir_response = self.api_client.session.delete(dir_url, headers=headers)
                    dir_response.raise_for_status()
                    
                except Exception:
                    # Directory might already be empty or not exist, which is fine
                    pass
            else:
                # Use files API for single file deletion
                url = self.api_client.get_url('/fs/files' + path)
                headers = {
                    'Authorization': self.api_client.default_headers.get('Authorization', '')
                }
                response = self.api_client.session.delete(url, headers=headers)
                response.raise_for_status()
        except Exception:
            # Don't raise - deletion failures shouldn't break the workflow
            pass


class _DatabricksUploadFile(io.BytesIO):
    """
    A BytesIO-like object that uploads to Databricks when closed.
    
    This class allows us to use the standard `with open(path, 'wb') as f: f.write()`
    pattern while uploading to Databricks via the Files API.
    """
    
    def __init__(self, api_client, remote_path: str):
        super().__init__()
        self.api_client = api_client
        self.remote_path = remote_path
        self._closed = False
    
    def close(self) -> None:
        """Upload the buffer contents to Databricks when closing."""
        if self._closed:
            return
            
        try:
            # Get the buffer contents
            content = self.getvalue()
            
            # Upload using Files API - use session directly for binary data
            url = self.api_client.get_url('/fs/files' + self.remote_path)
            headers = {
                'Content-Type': 'application/octet-stream',
                'Authorization': self.api_client.default_headers.get('Authorization', '')
            }
            response = self.api_client.session.put(
                url,
                data=content,
                headers=headers,
                params={'overwrite': 'true'}
            )
            response.raise_for_status()
            
        except Exception as e:
            logger.error(f"Failed to upload to {self.remote_path}: {e}")
            raise RuntimeError(f"Upload failed: {e}")
        finally:
            self._closed = True
            super().close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._closed:
            self.close()
        return False

