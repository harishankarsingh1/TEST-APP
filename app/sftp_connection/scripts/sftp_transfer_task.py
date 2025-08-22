# sftp_connection/scripts/sftp_transfer_task.py
import os
import stat
import time
import traceback
import logging
import zipfile # For zip/unzip functionality
import tempfile # For temporary zip files
# import shutil # Not currently used, can be removed if not needed for other plans
from typing import Optional, Any
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, QThread
import paramiko

from .sftp_transfer_types import TransferJob, TransferDirection, JobStatus

class SFTPTaskCancelledError(Exception):
    """Custom exception for explicitly cancelled SFTP tasks."""
    pass

class SFTPTaskSignals(QObject):
    task_started = pyqtSignal(int) # job_id
    task_status_update = pyqtSignal(int, JobStatus, str) # job_id, new_status, Optional message
    task_progress = pyqtSignal(int, int, int) # job_id, bytes_done, total_bytes
    task_completed = pyqtSignal(int, str) # job_id, message
    task_failed = pyqtSignal(int, str, bool) # job_id, error_message, was_cancelled
    task_authoritative_size_determined = pyqtSignal(int, int) # job_id, size
    task_log_message = pyqtSignal(str, str) # message, level

class SFTPTransferTask(QRunnable):
    def __init__(self, job: TransferJob, main_ssh_client: paramiko.SSHClient, parent_logger_name: str):
        super().__init__()
        self.job = job # This is a reference to the TransferJob object
        self.main_ssh_client = main_ssh_client
        self.signals = SFTPTaskSignals()
        self._is_cancelled = False
        self.logger = logging.getLogger(f"{parent_logger_name}.SFTPTask.J{self.job.id}.T{id(self)}")
        self.setAutoDelete(True)
        self.authoritative_total_size_for_this_task = self.job.total_size

        self.logger.critical(
            f"SFTPTask __init__: Task ID {id(self)}, Job ID {self.job.id} (from job object), "
            f"Job Object ID {id(self.job)}, Filename '{self.job.filename}', "
            f"zip_before_upload: {self.job.zip_before_upload}, "
            f"unzip_after_download: {self.job.unzip_after_download}, "
            f"sftp_local: '{self.job.sftp_transfer_path_local}', sftp_remote: '{self.job.sftp_transfer_path_remote}'"
        )

    def _log(self, message: str, level: str = "debug"):
        self.signals.task_log_message.emit(f"[{self.logger.name}] {message}", level)

    def _emit_critical_debug_log_pre_emit(self, event_name: str, id_to_emit: int):
        """Helper for verbose logging before signal emission for Job ID Mismatch debugging."""
        self.logger.debug(
            f"TASK PRE-EMIT '{event_name}': "
            f"Captured id_to_emit = {id_to_emit}. "
            f"Current self.job.id for check = {self.job.id}. "
            f"Task ID {id(self)}, Job Obj ID {id(self.job)}"
        )

    def cancel(self):
        self._log(f"Cancellation requested for task of job {self.job.id}.", "warning")
        self._is_cancelled = True

    def _emit_progress(self, bytes_done: int, total_bytes_for_progress: int):
        id_to_emit = self.job.id
        self._emit_critical_debug_log_pre_emit("progress_direct_emit", id_to_emit)
        self.signals.task_progress.emit(id_to_emit, bytes_done, total_bytes_for_progress)

    def _progress_callback(self, bytes_done: int, total_bytes_paramiko: int):
        if self._is_cancelled:
            self._log(f"Transfer for job {self.job.id} cancelled during progress callback.", "warning")
            raise SFTPTaskCancelledError(f"SFTPTransferTaskCancelled_{self.job.id}_ProgressCallback")

        current_authoritative_total = self.authoritative_total_size_for_this_task
        
        if total_bytes_paramiko > 0 and \
           (current_authoritative_total == 0 or \
            (self.job.direction == TransferDirection.DOWNLOAD and current_authoritative_total != total_bytes_paramiko)):
            
            if current_authoritative_total != total_bytes_paramiko:
                self._log(f"Progress callback for job {self.job.id} received new total_bytes {total_bytes_paramiko} from paramiko, updating authoritative size from {current_authoritative_total}.", "debug")
                self.authoritative_total_size_for_this_task = total_bytes_paramiko
                
                id_to_emit = self.job.id
                self._emit_critical_debug_log_pre_emit("authoritative_size_determined_from_callback", id_to_emit)
                self.signals.task_authoritative_size_determined.emit(id_to_emit, self.authoritative_total_size_for_this_task)
            current_authoritative_total = self.authoritative_total_size_for_this_task

        if current_authoritative_total > 0:
            self._emit_progress(bytes_done, current_authoritative_total)
        elif bytes_done > 0 : 
            self._emit_progress(bytes_done, bytes_done)
            
    def get_safe_base_name(self,zip_file_path: str) -> str:
        filename = os.path.basename(zip_file_path)
        
        # Strip .zip
        name, ext = os.path.splitext(filename)
        if ext.lower() != '.zip':
            return zip_file_path
        
        # Repeatedly strip any other extensions (like .py, .txt, etc.)
        while True:
            name, ext = os.path.splitext(name)
            if ext == '':
                break
        return name

    def run(self):
        id_to_emit_on_start = self.job.id
        self._emit_critical_debug_log_pre_emit("task_started", id_to_emit_on_start)
        self.signals.task_started.emit(id_to_emit_on_start)
        self._log(f"Starting {self.job.direction.value} for '{self.job.filename}'. TaskID: {id(self)}. Thread: {QThread.currentThreadId()}", "info")
        
        sftp_channel: Optional[paramiko.SFTPClient] = None
        temp_zip_file_path: Optional[str] = None 

        actual_local_path_for_sftp = self.job.sftp_transfer_path_local
        actual_remote_path_for_sftp = self.job.sftp_transfer_path_remote

        try:
            if self._is_cancelled: raise SFTPTaskCancelledError(f"SFTPTaskCancelled_AtStart_{self.job.id}")

            if not self.main_ssh_client or \
               not self.main_ssh_client.get_transport() or \
               not self.main_ssh_client.get_transport().is_active():
                raise Exception("Main SSH client is not connected or transport not active for task.")

            sftp_channel = self.main_ssh_client.open_sftp()
            if not sftp_channel: raise Exception("Failed to open SFTP channel for task.")
            self._log("SFTP channel opened successfully.", "debug")

            if self._is_cancelled: raise SFTPTaskCancelledError(f"SFTPTaskCancelled_AfterChannelOpen_{self.job.id}")

            if self.job.direction == TransferDirection.UPLOAD:
                source_content_path = self.job.original_source_path if self.job.original_source_path else self.job.local_path
                if not source_content_path:
                     raise ValueError(f"Job {self.job.id}: No source content path (original_source_path or local_path) defined for upload.")
                if not os.path.exists(source_content_path):
                    raise FileNotFoundError(f"Upload source content path not found: {source_content_path}")

                if self.job.zip_before_upload:                    
                    id_to_emit_status_zip = self.job.id
                    self._emit_critical_debug_log_pre_emit("status_update_zipping", id_to_emit_status_zip)
                    self.signals.task_status_update.emit(id_to_emit_status_zip, JobStatus.ZIPPING, "")
                    self._log(f"Starting to zip '{source_content_path}' for upload (Job ID {self.job.id})...", "info")
                    
                    temp_fd, temp_zip_file_path_os = tempfile.mkstemp(suffix=".zip", prefix=f"sftp_upload_{self.job.id}_")
                    os.close(temp_fd) 
                    temp_zip_file_path = temp_zip_file_path_os 
                    self._log(f"Temporary zip for job {self.job.id} at: {temp_zip_file_path}", "debug")

                    with zipfile.ZipFile(temp_zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                        if os.path.isdir(source_content_path):
                            for root, _, files in os.walk(source_content_path):
                                if self._is_cancelled: raise SFTPTaskCancelledError(f"ZippingCancelled_{self.job.id}")
                                for file_item in files:
                                    full_file_path = os.path.join(root, file_item)
                                    arcname = os.path.relpath(full_file_path, source_content_path)
                                    zf.write(full_file_path, arcname=arcname)
                        elif os.path.isfile(source_content_path):
                            zf.write(source_content_path, arcname=os.path.basename(source_content_path))
                        else: # Should have been caught by os.path.exists earlier, but defensive.
                            raise FileNotFoundError(f"Source for zipping is not a file or directory: {source_content_path}")
                    
                    actual_local_path_for_sftp = temp_zip_file_path # Update to use the zip file for upload
                    self.authoritative_total_size_for_this_task = os.path.getsize(actual_local_path_for_sftp)
                    
                    id_to_emit_size_zip = self.job.id
                    self._emit_critical_debug_log_pre_emit("authoritative_size_determined_zip", id_to_emit_size_zip)
                    self.signals.task_authoritative_size_determined.emit(id_to_emit_size_zip, self.authoritative_total_size_for_this_task)
                    self._log(f"Zipping complete for job {self.job.id}. Zip size: {self.authoritative_total_size_for_this_task}", "info")
                else: # Not zipping
                    if not actual_local_path_for_sftp:
                         raise ValueError(f"Job {self.job.id}: sftp_transfer_path_local is None for non-zip upload.")
                    if os.path.isfile(actual_local_path_for_sftp):
                        if self.authoritative_total_size_for_this_task == 0:
                            self.authoritative_total_size_for_this_task = os.path.getsize(actual_local_path_for_sftp)
                    
                    id_to_emit_size_nozip = self.job.id
                    self._emit_critical_debug_log_pre_emit("authoritative_size_determined_nozip", id_to_emit_size_nozip)
                    self.signals.task_authoritative_size_determined.emit(id_to_emit_size_nozip, self.authoritative_total_size_for_this_task)

                if not actual_remote_path_for_sftp:
                    raise ValueError(f"Job {self.job.id}: Remote path (sftp_transfer_path_remote) is None for upload.")

                if self._is_cancelled: raise SFTPTaskCancelledError(f"SFTPTransferTaskCancelled_BeforePut_{self.job.id}")
                
                id_to_emit_status_upload = self.job.id
                self._emit_critical_debug_log_pre_emit("status_update_in_progress_upload", id_to_emit_status_upload)
                self.signals.task_status_update.emit(id_to_emit_status_upload, JobStatus.IN_PROGRESS, "")
                self._log(f"Job {self.job.id}: Uploading '{actual_local_path_for_sftp}' to '{actual_remote_path_for_sftp}'", "info")
                sftp_channel.put(actual_local_path_for_sftp, actual_remote_path_for_sftp, callback=self._progress_callback, confirm=True)

            elif self.job.direction == TransferDirection.DOWNLOAD:
                if not actual_local_path_for_sftp:
                    raise ValueError(f"Job {self.job.id}: Local path (sftp_transfer_path_local) is None for download.")
                if not actual_remote_path_for_sftp:
                    raise ValueError(f"Job {self.job.id}: Remote path (sftp_transfer_path_remote) is None for download.")

                local_target_dir = os.path.dirname(actual_local_path_for_sftp)
                if local_target_dir and not os.path.exists(local_target_dir):
                    os.makedirs(local_target_dir, exist_ok=True)

                if self.authoritative_total_size_for_this_task == 0:
                    try:
                        r_stat = sftp_channel.stat(actual_remote_path_for_sftp)
                        if r_stat.st_size is not None: 
                            if r_stat.st_size != self.authoritative_total_size_for_this_task : # Update if different or was 0
                                self.authoritative_total_size_for_this_task = r_stat.st_size
                                id_to_emit_size_stat = self.job.id
                                self._emit_critical_debug_log_pre_emit("authoritative_size_determined_download_stat", id_to_emit_size_stat)
                                self.signals.task_authoritative_size_determined.emit(id_to_emit_size_stat, self.authoritative_total_size_for_this_task)
                                self._log(f"Job {self.job.id}: Authoritative size from stat: {self.authoritative_total_size_for_this_task}", "debug")
                    except Exception as e_stat:
                        self._log(f"Could not stat remote file '{actual_remote_path_for_sftp}' for size (Job {self.job.id}): {e_stat}. Will rely on callback total.", "warning")
                
                if self._is_cancelled: raise SFTPTaskCancelledError(f"SFTPTransferTaskCancelled_BeforeGet_{self.job.id}")
                
                id_to_emit_status_download = self.job.id
                self._emit_critical_debug_log_pre_emit("status_update_in_progress_download", id_to_emit_status_download)
                self.signals.task_status_update.emit(id_to_emit_status_download, JobStatus.IN_PROGRESS, "")
                self._log(f"Job {self.job.id}: Downloading '{actual_remote_path_for_sftp}' to '{actual_local_path_for_sftp}'", "info")
                sftp_channel.get(actual_remote_path_for_sftp, actual_local_path_for_sftp, callback=self._progress_callback)

                if not self._is_cancelled and self.job.unzip_after_download and actual_local_path_for_sftp.lower().endswith(".zip"):
                    # Normalize the path of the downloaded zip file first
                    normalized_zip_file_path = os.path.normpath(actual_local_path_for_sftp)
                    self._log(f"Normalized downloaded zip path: {normalized_zip_file_path}", "debug")
                    
                    if not os.path.isfile(normalized_zip_file_path):
                        raise FileNotFoundError(f"Downloaded zip file not found at expected location: {normalized_zip_file_path}")
                    
                    id_to_emit_status_unzip = self.job.id
                    self._emit_critical_debug_log_pre_emit("status_update_unzipping", id_to_emit_status_unzip)
                    self.signals.task_status_update.emit(id_to_emit_status_unzip, JobStatus.UNZIPPING, "")
                    self._log(f"Download complete for '{self.job.filename}' (Job {self.job.id}). Starting unzipping '{normalized_zip_file_path}'.", "info")
                    
                    extraction_path: str
                    if self.job.final_extraction_path:
                        extraction_path = self.job.final_extraction_path
                    else:
                        zip_dir = os.path.dirname(normalized_zip_file_path)
                        base_name = self.get_safe_base_name(normalized_zip_file_path )
                        folder_name = f"{base_name}_{int(time.time())}"
                        extraction_path = os.path.join(zip_dir, folder_name)
                        self._log(f"job.final_extraction_path not set for job {self.job.id}, defaulting to: {extraction_path}", "debug")

                    if not os.path.exists(extraction_path):
                        os.makedirs(extraction_path, exist_ok=True)
                    
                    with zipfile.ZipFile(normalized_zip_file_path, 'r') as zf:
                        zf.extractall(path=extraction_path) # Python's zipfile.extractall (3.6+ has some path safety)
                    self._log(f"Unzipped '{actual_local_path_for_sftp}' to '{extraction_path}' for job {self.job.id}.", "info")
                    
                    try: 
                        os.remove(actual_local_path_for_sftp)
                        self._log(f"Removed downloaded zip file: {actual_local_path_for_sftp} (Job {self.job.id})", "debug")
                    except Exception as e_del_zip:
                        self._log(f"Could not remove downloaded zip {actual_local_path_for_sftp} (Job {self.job.id}): {e_del_zip}", "warning")

            if self._is_cancelled:
                raise SFTPTaskCancelledError(f"Transfer for '{self.job.filename}' (Job ID {self.job.id}) was cancelled post-operation.")
            else:
                if self.authoritative_total_size_for_this_task > 0:
                    self._emit_progress(self.authoritative_total_size_for_this_task, self.authoritative_total_size_for_this_task)
                elif self.job.bytes_transferred >= 0 and self.authoritative_total_size_for_this_task == 0: 
                     # Ensure 100% for 0-byte files or if size was unknown but transfer happened (bytes_transferred might be >0 if callback was hit)
                    self._emit_progress(self.job.bytes_transferred, self.job.bytes_transferred if self.job.bytes_transferred > 0 else 0 )


                id_to_emit_completed = self.job.id
                self._emit_critical_debug_log_pre_emit("task_completed", id_to_emit_completed)
                self.signals.task_completed.emit(id_to_emit_completed, f"'{self.job.filename}' operation successful.")

        except SFTPTaskCancelledError as ce:
            self._log(f"Task explicitly cancelled for job {self.job.id}: {ce}", "warning")
            id_to_emit_failed_cancel = self.job.id
            self._emit_critical_debug_log_pre_emit("task_failed_cancelled_custom_ex", id_to_emit_failed_cancel)
            self.signals.task_failed.emit(id_to_emit_failed_cancel, f"Operation for '{self.job.filename}' cancelled.", True)
        except Exception as e:
            error_msg = str(e)
            was_cancelled = self._is_cancelled or \
                            f"SFTPTransferTaskCancelled_{self.job.id}" in error_msg or \
                            f"ZippingCancelled_{self.job.id}" in error_msg
            
            id_to_emit_failed_general = self.job.id
            if was_cancelled and not isinstance(e, SFTPTaskCancelledError):
                self._log(f"Task for job {self.job.id} resulted in error but also marked cancelled: {error_msg}", "warning")
                self._emit_critical_debug_log_pre_emit("task_failed_cancelled_general_ex", id_to_emit_failed_general)
                self.signals.task_failed.emit(id_to_emit_failed_general, f"Operation for '{self.job.filename}' cancelled amidst error: {error_msg}", True)
            else:
                self.logger.error(f"Task for job {self.job.id} ('{self.job.filename}') failed: {error_msg}", exc_info=True)
                self._emit_critical_debug_log_pre_emit("task_failed_error", id_to_emit_failed_general)
                self.signals.task_failed.emit(id_to_emit_failed_general, f"Error for '{self.job.filename}': {error_msg}", False)
        finally:
            if sftp_channel:
                try: sftp_channel.close(); self._log("SFTP channel closed.", "debug")
                except Exception as e_close: self._log(f"Error closing SFTP channel for job {self.job.id}: {e_close}", "warning")
            
            if temp_zip_file_path and os.path.exists(temp_zip_file_path):
                try: os.remove(temp_zip_file_path); self._log(f"Removed temporary zip file: {temp_zip_file_path}", "debug")
                except Exception as e_del_temp: self._log(f"Error removing temp zip {temp_zip_file_path} for job {self.job.id}: {e_del_temp}", "warning")
            
            self._log(f"Task run method for job {self.job.id} finished execution.", "info")