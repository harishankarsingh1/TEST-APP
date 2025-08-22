# sftp_connection/scripts/sftp_transfer_manager.py
import os
import stat
import time
from PyQt5.QtCore import QObject, pyqtSignal, QTimer, QThread
from typing import List, Dict, Optional, Callable, Any
import functools # For partial
import logging

from .sftp_transfer_types import TransferJob, JobStatus, TransferDirection

class DirectoryScannerThread(QThread):
    files_ready_for_job_creation = pyqtSignal(list, TransferDirection) # list of job_details_list, direction
    scan_error = pyqtSignal(str) # error_message
    scan_finished = pyqtSignal() # Signals completion of run method

    def __init__(self, sftp_client_getter: Callable, base_path: str, target_base_path_for_job: str,
                 direction: TransferDirection, original_job_id: int, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.sftp_client_getter = sftp_client_getter
        self.base_path = base_path  # Source directory for scanning
        self.target_base_path_for_job = target_base_path_for_job # Target parent directory for generated file jobs
        self.direction = direction
        self.original_job_id = original_job_id
        self._is_running = True
        self._logger = logging.getLogger(f"{__name__}.DirectoryScannerThread.{self.original_job_id}") # Per-instance logger

    def stop(self):
        self._logger.info("Stop requested.")
        self._is_running = False

    def run(self):
        self._logger.info(f"Starting scan. Direction: {self.direction.value}, Source base: '{self.base_path}', Target parent: '{self.target_base_path_for_job}'")
        job_details_list: List[Dict[str, Any]] = []
        try:
            if self.direction == TransferDirection.UPLOAD:
                if not os.path.isdir(self.base_path):
                    self.scan_error.emit(f"Local path is not a directory: {self.base_path}"); return

                dir_name_to_create_remotely = os.path.basename(self.base_path.rstrip('/\\'))
                remote_dir_root_for_contents = os.path.join(self.target_base_path_for_job, dir_name_to_create_remotely).replace("\\", "/")
                self._logger.debug(f"Upload scan: Local source='{self.base_path}', Remote root for contents='{remote_dir_root_for_contents}'")

                for root, _, files in os.walk(self.base_path):
                    if not self._is_running: self._logger.debug("Upload scan: Interrupted by stop request in os.walk loop."); break
                    relative_dir_from_base = os.path.relpath(root, self.base_path)
                    for file_name in files:
                        if not self._is_running: self._logger.debug("Upload scan: Interrupted by stop request in files loop."); break
                        local_file_full_path = os.path.join(root, file_name)
                        if relative_dir_from_base == ".":
                            remote_file_full_path = os.path.join(remote_dir_root_for_contents, file_name).replace("\\", "/")
                        else:
                            remote_file_full_path = os.path.join(remote_dir_root_for_contents, relative_dir_from_base.replace("\\", "/"), file_name).replace("\\", "/")
                        try:
                            size = os.path.getsize(local_file_full_path)
                            job_details_list.append({'local_path': local_file_full_path, 'remote_path': remote_file_full_path,
                                                     'filename': file_name, 'size': size})
                        except OSError as e: self._logger.warning(f"Could not get size for local file '{local_file_full_path}': {e}")
                    if not self._is_running: break
            elif self.direction == TransferDirection.DOWNLOAD:
                sftp = self.sftp_client_getter()
                if not sftp: self.scan_error.emit("SFTP client not available for remote directory scan."); return

                dir_name_to_create_locally = os.path.basename(self.base_path.rstrip('/'))
                local_dir_root_for_contents = os.path.join(self.target_base_path_for_job, dir_name_to_create_locally)
                self._logger.debug(f"Download scan: Remote source='{self.base_path}', Local root for contents='{local_dir_root_for_contents}'")

                items_to_scan: List[tuple[str, str]] = [(self.base_path.replace("\\", "/"), "")] # (remote_dir, relative_local_subdir)
                scanned_remote_paths = set() # To avoid potential issues with symlink loops or re-listing

                while items_to_scan:
                    if not self._is_running: self._logger.debug("Download scan: Interrupted by stop request in items_to_scan loop."); break
                    current_remote_dir, relative_local_subdir = items_to_scan.pop(0)
                    
                    if current_remote_dir in scanned_remote_paths:
                        self._logger.warning(f"Skipping already scanned remote path: {current_remote_dir}")
                        continue
                    scanned_remote_paths.add(current_remote_dir)
                    self._logger.debug(f"Download scan: Listing remote dir '{current_remote_dir}'")

                    try:
                        for attr in sftp.listdir_attr(current_remote_dir):
                            if not self._is_running: self._logger.debug("Download scan: Interrupted by stop request in listdir_attr loop."); break
                            item_remote_full = os.path.join(current_remote_dir, attr.filename).replace("\\","/")
                            item_local_target_full = os.path.join(local_dir_root_for_contents, relative_local_subdir, attr.filename)

                            if stat.S_ISDIR(attr.st_mode):
                                if attr.filename not in [".", ".."]: # Avoid listing . and ..
                                    items_to_scan.append( (item_remote_full, os.path.join(relative_local_subdir, attr.filename) ) )
                            elif stat.S_ISREG(attr.st_mode):
                                job_details_list.append({'remote_path': item_remote_full, 'local_path': item_local_target_full,
                                                         'filename': attr.filename, 'size': attr.st_size})
                        if not self._is_running: break
                    except Exception as e:
                        self.scan_error.emit(f"Error scanning remote dir {current_remote_dir}: {e}"); break
            if self._is_running:
                self.files_ready_for_job_creation.emit(job_details_list, self.direction)
                self._logger.info(f"Scan yielded {len(job_details_list)} items.")
            else:
                self._logger.info("Scan was stopped, not emitting files_ready signal.")
        except Exception as e:
            self._logger.error(f"Critical error during directory scan: {e}", exc_info=True)
            self.scan_error.emit(f"Critical error during directory scan: {str(e)}")
        finally:
            self._logger.info("Scan run method finished.")
            self.scan_finished.emit()


class TransferManager(QObject):
    job_added_to_ui = pyqtSignal(TransferJob)
    job_updated_in_ui = pyqtSignal(TransferJob)
    job_removed_from_ui = pyqtSignal(int)
    processing_state_changed = pyqtSignal(bool)
    log_message = pyqtSignal(str, str) # message, level
    request_sftp_transfer = pyqtSignal(TransferJob)

    def __init__(self, sftp_client_getter_func: Callable, parent: Optional[QObject]=None):
        super().__init__(parent)
        self.sftp_client_getter = sftp_client_getter_func
        self.queue: List[TransferJob] = []
        self.active_job_id: Optional[int] = None
        self.is_processing_queue = False
        self._directory_scan_threads: Dict[int, DirectoryScannerThread] = {}
        self._logger = logging.getLogger(f"{__name__}.TransferManager")

        self.process_timer = QTimer(self)
        self.process_timer.setInterval(500)
        self.process_timer.timeout.connect(self._process_next_job_in_queue)

    def _log(self, message: str, level: str = "info"):
        # Emit signal for external logging, and also log internally for TM's own context
        self.log_message.emit(message, level)
        getattr(self._logger, level.lower(), self._logger.info)(message)

    def add_item_to_queue(self, local_path_arg: str, remote_path_arg: str, direction: TransferDirection, is_source_directory: bool):
        # (Code from previous corrected version, using self._log for messages)
        job_final_local_path = ""
        job_final_remote_path = ""
        display_filename = ""

        if is_source_directory:
            if direction == TransferDirection.UPLOAD:
                display_filename = os.path.basename(local_path_arg.rstrip('/\\'))
                job_final_local_path = local_path_arg
                job_final_remote_path = remote_path_arg
            else: # DOWNLOAD directory
                display_filename = os.path.basename(remote_path_arg.rstrip('/\\'))
                job_final_local_path = local_path_arg
                job_final_remote_path = remote_path_arg
        else: # Single file
            if direction == TransferDirection.UPLOAD:
                display_filename = os.path.basename(local_path_arg)
                job_final_local_path = local_path_arg
                job_final_remote_path = os.path.join(remote_path_arg, display_filename).replace("\\", "/")
            else: # DOWNLOAD single file
                display_filename = os.path.basename(remote_path_arg)
                job_final_local_path = os.path.join(local_path_arg, display_filename)
                job_final_remote_path = remote_path_arg

        job = TransferJob(local_path=job_final_local_path,
                          remote_path=job_final_remote_path,
                          direction=direction,
                          is_directory_transfer=is_source_directory,
                          filename=display_filename) # ID is auto-generated

        if is_source_directory:
            job.status = JobStatus.SCANNING
            self.queue.append(job)
            self.job_added_to_ui.emit(job)
            self._log(f"Directory job '{job.filename}' (ID: {job.id}) added, starting scan.", "info")
            self._start_directory_scan(job)
        else:
            self.queue.append(job)
            self.job_added_to_ui.emit(job)
            self._log(f"File job '{job.filename}' (ID: {job.id}) added to queue.", "info")

        if self.is_processing_queue and not self.active_job_id:
            self._process_next_job_in_queue()

    def _start_directory_scan(self, dir_job: TransferJob):
        # (Code from previous corrected version, ensure functools.partial is used for signal connections)
        if dir_job.id in self._directory_scan_threads and self._directory_scan_threads[dir_job.id].isRunning():
            self._log(f"Scan for directory job '{dir_job.filename}' (ID: {dir_job.id}) is already in progress.", "warning")
            return

        source_dir_for_scanner = dir_job.local_path if dir_job.direction == TransferDirection.UPLOAD else dir_job.remote_path
        target_base_for_scanner_files = dir_job.remote_path if dir_job.direction == TransferDirection.UPLOAD else dir_job.local_path

        scanner = DirectoryScannerThread(self.sftp_client_getter,
                                         source_dir_for_scanner,
                                         target_base_for_scanner_files,
                                         dir_job.direction,
                                         dir_job.id,
                                         parent=self)

        # Using functools.partial to pass the original_dir_job_id with the signals
        scanner.files_ready_for_job_creation.connect(
            functools.partial(self._handle_scanned_files, dir_job.id)
        ) # handler signature: (original_dir_job_id, job_details_list, direction)
        scanner.scan_error.connect(
            functools.partial(self._handle_scan_error, dir_job.id)
        ) # handler signature: (original_dir_job_id, error_message)
        scanner.scan_finished.connect(
            functools.partial(self._handle_scan_finished, dir_job.id)
        ) # handler signature: (original_dir_job_id)

        self._directory_scan_threads[dir_job.id] = scanner
        scanner.start()
        self._log(f"DirectoryScannerThread started for job ID {dir_job.id}, scanning '{source_dir_for_scanner}'.", "debug")


    def _handle_scanned_files(self, original_dir_job_id: int, job_details_list: List[Dict[str, Any]], direction: TransferDirection):
        # (Code from previous corrected version)
        self._log(f"Scan for dir job ID {original_dir_job_id} found {len(job_details_list)} files. Adding to queue.", "info")
        parent_dir_job = next((j for j in self.queue if j.id == original_dir_job_id), None)
        if not parent_dir_job:
            self._log(f"Original directory job ID {original_dir_job_id} not found in queue for scanned files.", "error"); return

        files_added_count = 0
        if job_details_list:
            for detail in job_details_list:
                file_job = TransferJob(local_path=detail['local_path'], remote_path=detail['remote_path'],
                                       direction=direction, is_directory_transfer=False,
                                       filename=detail['filename'], total_size=detail['size'],
                                       status=JobStatus.QUEUED)
                self.queue.append(file_job)
                self.job_added_to_ui.emit(file_job)
                files_added_count +=1
        
        # Update parent dir job status after files are queued or if no files were found
        if parent_dir_job.status == JobStatus.SCANNING:
            parent_dir_job.status = JobStatus.COMPLETED
            parent_dir_job.progress = 100
            if files_added_count == 0:
                parent_dir_job.error_message = "Directory is empty or no scannable files found."
                self._log(f"Directory job ID {original_dir_job_id} ('{parent_dir_job.filename}') scan complete: No files.", "info")
            else:
                parent_dir_job.error_message = f"Scan complete: {files_added_count} files queued."
                self._log(f"Directory job ID {original_dir_job_id} ('{parent_dir_job.filename}') scan complete: {files_added_count} files queued.", "info")
            self.job_updated_in_ui.emit(parent_dir_job)

        if self.is_processing_queue and not self.active_job_id:
            self._process_next_job_in_queue()

    def _handle_scan_error(self, original_dir_job_id: int, error_message: str):
        # (Code from previous corrected version)
        self._log(f"Error scanning directory (Original Job ID {original_dir_job_id}): {error_message}", "error")
        original_job = next((j for j in self.queue if j.id == original_dir_job_id), None)
        if original_job:
            original_job.status = JobStatus.FAILED
            original_job.error_message = error_message
            self.job_updated_in_ui.emit(original_job)
        # scan_finished will still be emitted by the thread for cleanup in _handle_scan_finished.

    def _handle_scan_finished(self, original_dir_job_id: int):
        # (Code from previous corrected version, ensuring thread is cleaned up)
        scanner = self._directory_scan_threads.pop(original_dir_job_id, None)
        if scanner:
            self._log(f"DirectoryScannerThread finished for original job ID {original_dir_job_id}. Cleaning up thread.", "debug")
            if scanner.isRunning(): scanner.quit(); scanner.wait(500)
            scanner.deleteLater()
        
        original_job = next((j for j in self.queue if j.id == original_dir_job_id and j.status == JobStatus.SCANNING), None)
        if original_job: # If it was still scanning and no files/error handled it
            self._log(f"Finalizing dir job ID {original_dir_job_id} ('{original_job.filename}') as completed (scan finished, was still scanning).", "info")
            original_job.status = JobStatus.COMPLETED
            original_job.progress = 100
            if not original_job.error_message: original_job.error_message = "Scan complete: Directory empty or no scannable files."
            self.job_updated_in_ui.emit(original_job)

        if self.is_processing_queue and not self.active_job_id:
            self._process_next_job_in_queue()

    def start_queue(self): # (Code from previous corrected version)
        if not self.is_processing_queue:
            self.is_processing_queue = True; self.process_timer.start()
            self._log("Transfer queue processing started.", "info"); self.processing_state_changed.emit(True)
            self._process_next_job_in_queue()
        else: self._log("Transfer queue already started.", "debug")

    def stop_queue(self): # (Code from previous corrected version)
        if self.is_processing_queue:
            self.is_processing_queue = False; self.process_timer.stop()
            self._log("Transfer queue processing stopped by user.", "info"); self.processing_state_changed.emit(False)

    def _process_next_job_in_queue(self): # (Code from previous corrected version, ensures job state is reset for processing)
        if not self.is_processing_queue or self.active_job_id is not None: return
        if not self.sftp_client_getter(): self._log("SFTP not connected. Queue paused.", "warning"); return
        job_to_process = next((job for job in self.queue if job.status == JobStatus.QUEUED and not job.is_directory_transfer), None)
        if job_to_process:
            self.active_job_id = job_to_process.id
            job_to_process.status = JobStatus.IN_PROGRESS
            job_to_process.progress = 0; job_to_process.bytes_transferred = 0; job_to_process.error_message = ""
            self.job_updated_in_ui.emit(job_to_process)
            self._log(f"Processing job {job_to_process.id}: {job_to_process.direction.value} '{job_to_process.filename}'", "info")
            self.request_sftp_transfer.emit(job_to_process)

    def on_job_progress(self, job_id: int, bytes_val: int, total_val: int, direction_enum_val: int):
        self._log(f"MANAGER_ON_PROGRESS: job_id={job_id}, bytes={bytes_val}, total={total_val}, active_id={self.active_job_id}", "debug")
        if self.active_job_id == job_id:
            job = next((j for j in self.queue if j.id == job_id), None)
            if job:
                job.bytes_transferred = bytes_val
                if total_val > 0: job.total_size = total_val
                job.progress = int((bytes_val / total_val) * 100) if total_val > 0 else (100 if bytes_val > 0 and bytes_val == total_val else 0)
                self._log(f"MANAGER_ON_PROGRESS: Job '{job.filename}' updated to progress {job.progress}%, bytes {bytes_val}/{total_val}", "debug")
                self.job_updated_in_ui.emit(job)
                self._log(f"MANAGER_ON_PROGRESS: Emitted job_updated_in_ui for job {job.id}", "debug")
            # else: (log if job not found)
        # else: (log if job_id is not active_job_id)

    def on_job_remote_size_discovered(self, job_id: int, total_size: int): # (Code from previous corrected version)
        self._log(f"TM_ON_REMOTE_SIZE: job_id={job_id}, size={total_size}", "debug")
        job = next((j for j in self.queue if j.id == job_id), None)
        if job and (job.total_size == 0 or job.total_size != total_size) and total_size > 0:
            job.total_size = total_size; self.job_updated_in_ui.emit(job)

    def on_job_completed(self, job_id: int, message: str):
        self._log(f"MANAGER_ON_COMPLETED: Received for job_id={job_id}, msg='{message}'", "debug") # Existing log, ensure level is visible
        if self.active_job_id == job_id:
            job = next((j for j in self.queue if j.id == job_id), None)
            if job:
                self._log(f"MANAGER_ON_COMPLETED: Found job '{job.filename}', current status {job.status.value}, progress {job.progress}", "debug")
                job.status = JobStatus.COMPLETED
                job.progress = 100
                job.error_message = "" # Clear any previous error
                self._log(f"MANAGER_ON_COMPLETED: Job '{job.filename}' updated to status {job.status.value}, progress {job.progress}", "debug")
                self.job_updated_in_ui.emit(job) # <<< CRUCIAL EMIT
                self._log(f"MANAGER_ON_COMPLETED: Emitted job_updated_in_ui for job {job.id}", "debug")
            else:
                self._log(f"MANAGER_ON_COMPLETED: Job with active_job_id {job_id} not found in queue!", "error")
            self.active_job_id = None
            if self.is_processing_queue: self._process_next_job_in_queue()
        else:
            self._log(f"MANAGER_ON_COMPLETED: Received completion for non-active or unknown job_id={job_id} (active is {self.active_job_id})")

    def on_job_failed(self, job_id: int, error_message: str): # (Code from previous corrected version)
        self._log(f"TM_ON_FAILED: job_id={job_id}, error='{error_message}'", "error")
        if self.active_job_id == job_id:
            job = next((j for j in self.queue if j.id == job_id), None)
            if job: job.status = JobStatus.FAILED; job.error_message = error_message; self.job_updated_in_ui.emit(job)
            self.active_job_id = None; self._process_next_job_in_queue() if self.is_processing_queue else None
        # else: self._log(f"TM_ON_FAILED: Failure for non-active/unknown job_id={job_id}", "warning")

    def on_job_cancelled(self, job_id: int, message: str): # (Code from previous corrected version)
        self._log(f"TM_ON_CANCELLED: job_id={job_id}, msg='{message}'", "warning")
        job_was_active = (self.active_job_id == job_id)
        job = next((j for j in self.queue if j.id == job_id), None)
        if job and job.status != JobStatus.CANCELLED:
            job.status = JobStatus.CANCELLED; job.error_message = message; self.job_updated_in_ui.emit(job)
        if job_was_active:
            self.active_job_id = None; self._process_next_job_in_queue() if self.is_processing_queue else None
        # else: self._log(f"TM_ON_CANCELLED: Cancel for non-active/unknown job_id={job_id}", "warning")


    def clear_successful_jobs_from_queue(self):
        self._log(f"MANAGER_CLEAR_SUCCESSFUL: Current queue length {len(self.queue)}", "debug")
        removed_ids = [job.id for job in self.queue if job.status == JobStatus.COMPLETED] # Includes dir jobs too
    
        if not removed_ids:
            self._log("MANAGER_CLEAR_SUCCESSFUL: No completed jobs to clear.", "info")
            return
    
        self._log(f"MANAGER_CLEAR_SUCCESSFUL: Found {len(removed_ids)} completed job IDs: {removed_ids}", "debug")
        self.queue = [job for job in self.queue if job.id not in removed_ids]
    
        for job_id_val in removed_ids:
            self._log(f"MANAGER_CLEAR_SUCCESSFUL: Emitting job_removed_from_ui for ID {job_id_val}", "debug")
            self.job_removed_from_ui.emit(job_id_val)
        self._log(f"MANAGER_CLEAR_SUCCESSFUL: Cleared {len(removed_ids)} jobs. New queue length {len(self.queue)}", "info")

    def remove_jobs_from_queue(self, job_ids: List[int]): # (Code from previous corrected version)
        self._log(f"MANAGER_REMOVE_JOBS: Requested to remove job IDs: {job_ids}. Current queue length {len(self.queue)}", "debug")
        removed_count = 0
        active_job_was_removed = False
        for job_id in job_ids:
            if self.active_job_id == job_id: 
                active_job_was_removed = True
            self._log(f"Active job {job_id} marked for removal.", "warning")
            if job_id in self._directory_scan_threads:
                scanner = self._directory_scan_threads.pop(job_id)
                if scanner.isRunning(): self._log(f"Stopping scanner for dir job ID {job_id}.", "info"); scanner.stop(); scanner.wait(200)
                scanner.deleteLater()
            original_len = len(self.queue)
            self.queue = [j for j in self.queue if j.id != job_id]
            if len(self.queue) < original_len:
                self.job_removed_from_ui.emit(job_id)
                removed_count += 1
        if removed_count > 0: 
            self._log(f"MANAGER_REMOVE_JOBS: Removed {removed_count} jobs. New queue length {len(self.queue)}", "info")
        if active_job_was_removed:
            # Mark the job as cancelled in UI if it was active
            # The worker will eventually finish or fail its current operation.
            # The manager needs to signal this explicitly to the UI for the removed active job.
            job_obj = next((j for j in self.queue if j.id == self.active_job_id), None) # Re-fetch, it's removed from queue
            # Actually, since it's removed, we can't update it via queue.
            # The SFTPWorker's cancellation due to request_stop will trigger on_job_cancelled.
            self.active_job_id = None
            if self.is_processing_queue: self._process_next_job_in_queue()

    def retry_jobs_in_queue(self, job_ids: List[int]): # (Code from previous corrected version)
        retried_count = 0
        for job_id in job_ids:
            job = next((j for j in self.queue if j.id == job_id), None)
            if job and job.status in [JobStatus.FAILED, JobStatus.CANCELLED]:
                self._log(f"Retrying job ID {job.id} ('{job.filename}'). Prev status: {job.status.value}", "info")
                job.progress = 0; job.bytes_transferred = 0; job.error_message = ""
                if job.is_directory_transfer: job.status = JobStatus.SCANNING; self.job_updated_in_ui.emit(job); self._start_directory_scan(job)
                else: job.status = JobStatus.QUEUED; self.job_updated_in_ui.emit(job)
                retried_count +=1
        if retried_count > 0: self._log(f"Retrying {retried_count} jobs.", "info")
        if self.is_processing_queue and not self.active_job_id: self._process_next_job_in_queue()

    def cancel_all_directory_scans(self):
        if not self._directory_scan_threads: return
        self._log(f"Cancelling all active directory scans ({len(self._directory_scan_threads)} found).", "info")
        for job_id, scanner in list(self._directory_scan_threads.items()):
            if scanner.isRunning(): self._log(f"Stopping scanner for dir job ID {job_id}.", "debug"); scanner.stop(); scanner.wait(500)
            scanner.deleteLater()
            original_job = next((j for j in self.queue if j.id == job_id and j.is_directory_transfer), None)
            if original_job and original_job.status == JobStatus.SCANNING:
                original_job.status = JobStatus.CANCELLED; original_job.error_message = "Scan cancelled during shutdown/cleanup."
                self.job_updated_in_ui.emit(original_job)
        self._directory_scan_threads.clear()