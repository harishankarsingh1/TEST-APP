# sftp_connection/scripts/sftp_transfer_types.py
import time
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any
import logging # Added for logging within __post_init__

# It's good practice to get a logger for the module
logger = logging.getLogger(__name__)

class TransferDirection(Enum):
    UPLOAD = "Upload"
    DOWNLOAD = "Download"

class JobStatus(Enum):
    QUEUED = "Queued"
    IN_PROGRESS = "In Progress"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    SCANNING = "Scanning"
    PENDING_RESOURCES = "Pending Resources"
    ZIPPING = "Zipping"
    UNZIPPING = "Unzipping"

@dataclass
class TransferJob:
    local_path: str 
    remote_path: str
    direction: TransferDirection

    id: int = field(default_factory=lambda: int(time.time() * 1000000 + time.perf_counter_ns() % 1000))
    is_directory_transfer: bool = False

    filename: str = "" 
    
    sftp_transfer_path_local: Optional[str] = None
    sftp_transfer_path_remote: Optional[str] = None

    total_size: int = 0
    progress: int = 0
    bytes_transferred: int = 0
    status: JobStatus = JobStatus.QUEUED
    error_message: str = ""
    task_runner_id: Optional[Any] = None

    zip_before_upload: bool = False
    unzip_after_download: bool = False
    
    original_source_path: Optional[str] = None
    final_extraction_path: Optional[str] = None

    def __post_init__(self):
        # --- Pre-computation Logging ---
        # Using print for __post_init__ as logger might not be fully set up when dataclass is made by other modules
        # Or, ensure a basic logger is configured globally if using self.logger here.
        # For simplicity with dataclasses, print can be more reliable for initial debug.
        # logger.debug(f"Job {self.id} __post_init__ START: filename='{self.filename}', remote_path='{self.remote_path}', "
        #             f"sftp_remote='{self.sftp_transfer_path_remote}', zip_flag={self.zip_before_upload}, local_path='{self.local_path}'")


        # --- 1. Finalize `filename` (display name) ---
        if not self.filename: 
            if self.direction == TransferDirection.UPLOAD:
                source_for_basename: Optional[str] = self.original_source_path if self.original_source_path else self.local_path
                if not source_for_basename:
                    raise ValueError(f"Job {self.id}: For UPLOAD, cannot determine filename. 'original_source_path' or 'local_path' must be provided.")
                self.filename = os.path.basename(source_for_basename.rstrip('/\\'))
                if self.zip_before_upload:
                    if not self.filename.lower().endswith(".zip"): # Avoid double .zip if already there
                        self.filename += ".zip"
            elif self.direction == TransferDirection.DOWNLOAD:
                if not self.remote_path:
                    raise ValueError(f"Job {self.id}: For DOWNLOAD, 'remote_path' must be set to determine filename.")
                self.filename = os.path.basename(self.remote_path.rstrip('/\\'))
            else:
                raise ValueError(f"Job {self.id}: Invalid transfer direction '{self.direction}'.")
        elif self.zip_before_upload and self.direction == TransferDirection.UPLOAD and not self.filename.lower().endswith(".zip"):
            # If filename was preset but zip_before_upload is true, ensure .zip suffix
            self.filename += ".zip"
        
        # logger.debug(f"Job {self.id} POST FILENAME: filename='{self.filename}', zip_flag={self.zip_before_upload}")


        # --- 2. Finalize SFTP operational paths ---
        if self.direction == TransferDirection.UPLOAD:
            # ** sftp_transfer_path_remote **
            if self.zip_before_upload:
                # If zipping, sftp_transfer_path_remote MUST use the (now) zipped self.filename.
                # self.remote_path from PTM is the target *directory* on server.
                if not self.remote_path: 
                    raise ValueError(f"Job {self.id}: For UPLOAD (zip), 'remote_path' (target server directory) must be set.")
                if not self.filename.lower().endswith(".zip"): # Sanity check
                    # This should not happen if filename logic above is correct
                    raise ValueError(f"Job {self.id}: For UPLOAD (zip), filename '{self.filename}' does not end with .zip.")
                self.sftp_transfer_path_remote = os.path.join(self.remote_path, self.filename).replace("\\","/")
                # logger.debug(f"Job {self.id} UPLOAD ZIP: Set sftp_remote to '{self.sftp_transfer_path_remote}' from remote_dir '{self.remote_path}' and zipped_filename '{self.filename}'")
            elif not self.sftp_transfer_path_remote: # Not zipping, and path not preset by a hint
                if not self.remote_path: 
                    raise ValueError(f"Job {self.id}: For UPLOAD (no zip), 'remote_path' (target server directory) must be set.")
                self.sftp_transfer_path_remote = os.path.join(self.remote_path, self.filename).replace("\\","/")
            # If not zipping AND self.sftp_transfer_path_remote was already correctly set by a hint, this will use the hint.

            # ** sftp_transfer_path_local **
            if self.zip_before_upload:
                # This is set by SFTPTransferTask to the temp zip path. Initialize as None.
                self.sftp_transfer_path_local = None 
            elif not self.sftp_transfer_path_local: # Not zipping, and not preset by hint
                source_file = self.original_source_path if self.original_source_path else self.local_path
                if not source_file:
                    raise ValueError(f"Job {self.id}: For non-zip UPLOAD, no local source path ('original_source_path' or 'local_path') defined.")
                self.sftp_transfer_path_local = source_file
        
        elif self.direction == TransferDirection.DOWNLOAD:
            if not self.sftp_transfer_path_local: 
                if not self.local_path: 
                     raise ValueError(f"Job {self.id}: For DOWNLOAD, 'local_path' (target local directory) must be set.")
                self.sftp_transfer_path_local = os.path.join(self.local_path, self.filename)
            
            if not self.sftp_transfer_path_remote: 
                if not self.remote_path: 
                    raise ValueError(f"Job {self.id}: For DOWNLOAD, 'remote_path' (source server path) must be set.")
                self.sftp_transfer_path_remote = self.remote_path

        # --- 3. Initial `total_size` for non-zipped uploads ---
        if self.direction == TransferDirection.UPLOAD and \
           not self.zip_before_upload and \
           self.sftp_transfer_path_local and \
           self.total_size == 0:
            try:
                if os.path.isfile(self.sftp_transfer_path_local):
                    self.total_size = os.path.getsize(self.sftp_transfer_path_local)
            except OSError: pass 
        
        # --- 4. Final Sanity Checks (optional but good for debugging) ---
        # logger.debug(f"Job {self.id} POST __post_init__: filename='{self.filename}', "
        #             f"sftp_local='{self.sftp_transfer_path_local}', sftp_remote='{self.sftp_transfer_path_remote}', "
        #             f"zip_flag={self.zip_before_upload}, unzip_flag={self.unzip_after_download}, total_size={self.total_size}")

        if not self.filename: raise ValueError(f"Job {self.id}: Filename is empty after __post_init__.")
        if self.direction == TransferDirection.UPLOAD:
            if not self.sftp_transfer_path_remote: raise ValueError(f"Job {self.id}: sftp_transfer_path_remote is not set for UPLOAD.")
            if not self.zip_before_upload and not self.sftp_transfer_path_local: raise ValueError(f"Job {self.id}: sftp_transfer_path_local is not set for non-zip UPLOAD.")
            if self.zip_before_upload and not (self.original_source_path or self.local_path): raise ValueError(f"Job {self.id}: zip_before_upload is True, but no source path defined.")
        elif self.direction == TransferDirection.DOWNLOAD:
            if not self.sftp_transfer_path_local: raise ValueError(f"Job {self.id}: sftp_transfer_path_local is not set for DOWNLOAD.")
            if not self.sftp_transfer_path_remote: raise ValueError(f"Job {self.id}: sftp_transfer_path_remote is not set for DOWNLOAD.")