# sftp_connection/scripts/sftp_parallel_manager.py
import time
import os
import stat
import logging
import functools
from PyQt5.QtCore import QObject, pyqtSignal, QThreadPool, QTimer, QThread
from typing import List, Dict, Optional, Callable, Any

from .sftp_transfer_types import TransferJob, JobStatus, TransferDirection
from .sftp_transfer_task import SFTPTransferTask, SFTPTaskSignals # Assuming SFTPTaskCancelledError is in sftp_transfer_task

class DirectoryScannerThread(QThread):
    # original_job_id is the ID of the "meta-job" for the directory transfer
    files_ready_for_job_creation = pyqtSignal(int, list, TransferDirection) # original_dir_job_id, list_of_job_details, direction
    scan_error = pyqtSignal(int, str) # original_dir_job_id, error_message
    scan_finished = pyqtSignal(int) # original_dir_job_id

    def __init__(self, sftp_client_getter: Callable, base_path: str, target_base_path_for_job: str,
                 direction: TransferDirection, original_job_id: int, parent_logger_name: str, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.sftp_client_getter = sftp_client_getter
        self.base_path = base_path
        self.target_base_path_for_job = target_base_path_for_job
        self.direction = direction
        self.original_job_id = original_job_id
        self._is_running = True
        self.logger = logging.getLogger(f"{parent_logger_name}.DirScanner.J{self.original_job_id}")

    def stop(self):
        self.logger.info(f"Stop requested for directory scan of job ID {self.original_job_id}.")
        self._is_running = False

    def run(self):
        self.logger.info(f"Starting scan for dir job ID {self.original_job_id}. Direction: {self.direction.value}, Source base: '{self.base_path}', Target parent for sub-jobs: '{self.target_base_path_for_job}'")
        job_details_list: List[Dict[str, Any]] = []
        sftp_for_scan = None
        try:
            if self.direction == TransferDirection.UPLOAD:
                if not os.path.isdir(self.base_path):
                    self.scan_error.emit(self.original_job_id, f"Local path is not a directory: {self.base_path}"); return

                dir_name_to_create_remotely = os.path.basename(self.base_path.rstrip('/\\'))
                remote_dir_root_for_contents = os.path.join(self.target_base_path_for_job, dir_name_to_create_remotely).replace("\\", "/")
                self.logger.debug(f"Upload scan (Job ID {self.original_job_id}): Local source='{self.base_path}', Remote root for contents='{remote_dir_root_for_contents}'")

                for root, _, files in os.walk(self.base_path):
                    if not self._is_running: self.logger.info(f"Upload scan for job ID {self.original_job_id} interrupted by stop request in os.walk loop."); break
                    relative_dir_from_base = os.path.relpath(root, self.base_path)
                    
                    for file_name in files:
                        if not self._is_running: self.logger.info(f"Upload scan for job ID {self.original_job_id} interrupted by stop request in files loop."); break
                        local_file_full_path = os.path.join(root, file_name)
                        
                        current_rel_dir_cleaned = relative_dir_from_base.replace("\\", "/")
                        if current_rel_dir_cleaned == ".": 
                            current_rel_dir_cleaned = "" 
                        
                        remote_file_full_path_temp = os.path.join(remote_dir_root_for_contents, current_rel_dir_cleaned, file_name)
                        remote_file_full_path = remote_file_full_path_temp.replace("\\", "/")
                        
                        try:
                            size = os.path.getsize(local_file_full_path)
                            job_details_list.append({'local_path': local_file_full_path, 
                                                     'remote_path': remote_file_full_path,
                                                     'filename': file_name, 'size': size})
                        except OSError as e:
                            self.logger.warning(f"Could not get size for local file '{local_file_full_path}' (Job ID {self.original_job_id}): {e}")
                    if not self._is_running: break 
            
            elif self.direction == TransferDirection.DOWNLOAD:
                ssh_client = self.sftp_client_getter()
                if not ssh_client or not ssh_client.get_transport() or not ssh_client.get_transport().is_active():
                    self.scan_error.emit(self.original_job_id, "SSH client unavailable for remote directory scan."); return
                
                sftp_for_scan = ssh_client.open_sftp()
                if not sftp_for_scan:
                    self.scan_error.emit(self.original_job_id, "Failed to open SFTP channel for scan."); return

                dir_name_to_create_locally = os.path.basename(self.base_path.rstrip('/'))
                local_dir_root_for_contents = os.path.join(self.target_base_path_for_job, dir_name_to_create_locally)
                self.logger.debug(f"Download scan (Job ID {self.original_job_id}): Remote source='{self.base_path}', Local root for contents='{local_dir_root_for_contents}'")

                items_to_scan: List[tuple[str, str]] = [(self.base_path.replace("\\", "/"), "")] 
                scanned_remote_paths = set() 

                while items_to_scan:
                    if not self._is_running: self.logger.info(f"Download scan for job ID {self.original_job_id} interrupted by stop request in items_to_scan loop."); break
                    current_remote_dir, relative_local_subdir = items_to_scan.pop(0)
                    
                    normalized_current_remote_dir = current_remote_dir.rstrip('/') if current_remote_dir != '/' else '/'
                    if normalized_current_remote_dir in scanned_remote_paths:
                        self.logger.warning(f"Skipping already scanned or queued remote path: {normalized_current_remote_dir} (Job ID {self.original_job_id})")
                        continue
                    scanned_remote_paths.add(normalized_current_remote_dir)
                    self.logger.debug(f"Download scan (Job ID {self.original_job_id}): Listing remote dir '{normalized_current_remote_dir}'")

                    try:
                        for attr in sftp_for_scan.listdir_attr(normalized_current_remote_dir):
                            if not self._is_running: self.logger.info(f"Download scan for job ID {self.original_job_id} interrupted by stop request in listdir_attr loop."); break
                            
                            item_remote_full_path = os.path.join(normalized_current_remote_dir, attr.filename).replace("\\","/")
                            item_local_target_full_path = os.path.join(local_dir_root_for_contents, relative_local_subdir, attr.filename)

                            if stat.S_ISDIR(attr.st_mode):
                                if attr.filename not in [".", ".."]: 
                                    items_to_scan.append( (item_remote_full_path, os.path.join(relative_local_subdir, attr.filename) ) )
                            elif stat.S_ISREG(attr.st_mode):
                                job_details_list.append({'remote_path': item_remote_full_path, 
                                                         'local_path': item_local_target_full_path,
                                                         'filename': attr.filename, 'size': attr.st_size})
                        if not self._is_running: break 
                    except Exception as e_list:
                        self.logger.error(f"Error scanning remote dir {normalized_current_remote_dir} (Job ID {self.original_job_id}): {e_list}")
                        self.scan_error.emit(self.original_job_id, f"Error scanning {normalized_current_remote_dir}: {e_list}"); break 
            
            if self._is_running:
                self.files_ready_for_job_creation.emit(self.original_job_id, job_details_list, self.direction)
                self.logger.info(f"Scan for dir job ID {self.original_job_id} yielded {len(job_details_list)} items.")
            else:
                self.logger.info(f"Scan for dir job ID {self.original_job_id} was stopped, not emitting files_ready signal.")
        except Exception as e_outer:
            self.logger.error(f"Critical error during directory scan for job ID {self.original_job_id}: {e_outer}", exc_info=True)
            self.scan_error.emit(self.original_job_id, f"Critical scan error: {str(e_outer)}")
        finally:
            if sftp_for_scan:
                try: sftp_for_scan.close()
                except Exception as e_sftp_close: self.logger.warning(f"Error closing scanner SFTP channel for job ID {self.original_job_id}: {e_sftp_close}")
            self.scan_finished.emit(self.original_job_id) 
            self.logger.info(f"Scan run method finished for dir job ID {self.original_job_id} ('{self.base_path}').")


class ParallelTransferManager(QObject):
    job_added_to_ui = pyqtSignal(TransferJob)
    job_updated_in_ui = pyqtSignal(TransferJob)
    job_removed_from_ui = pyqtSignal(int) 
    processing_state_changed = pyqtSignal(bool) 
    log_message = pyqtSignal(str, str) 

    def __init__(self, main_ssh_client_getter: Callable, parent_logger_name: str, max_concurrent_transfers: int = 3, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.main_ssh_client_getter = main_ssh_client_getter
        self.parent_logger_name = parent_logger_name
        self.logger = logging.getLogger(f"{parent_logger_name}.ParallelTransferManager")

        self.queue: List[TransferJob] = []
        self.active_tasks: Dict[int, SFTPTransferTask] = {} 
        self.max_concurrent_transfers = max_concurrent_transfers
        self.is_queue_globally_active = False 

        self.thread_pool = QThreadPool(self)
        self.thread_pool.setMaxThreadCount(max_concurrent_transfers)
        self.logger.info(f"QThreadPool initialized with max {self.thread_pool.maxThreadCount()} threads.")

        self._directory_scan_threads: Dict[int, DirectoryScannerThread] = {}
        self._ssh_client_unavailable_retries = 0
        self._max_ssh_client_retries = 5 

        self.process_queue_timer = QTimer(self)
        self.process_queue_timer.setInterval(750) 
        self.process_queue_timer.timeout.connect(self._dispatch_tasks_from_queue)

    def _log(self, message: str, level: str = "info"):
        self.log_message.emit(message, level)
        # For direct PTM logging: getattr(self.logger, level.lower(), self.logger.info)(message)

    def add_item_to_queue(self, local_path_arg: str, remote_path_arg: str,
                          direction: TransferDirection, is_source_directory: bool,
                          # NEW PARAMETERS to control zipping/unzipping
                          zip_this_upload: bool = True,
                          unzip_this_download: bool = True):

        original_source_for_naming = local_path_arg if direction == TransferDirection.UPLOAD else remote_path_arg
        job_display_filename = os.path.basename(original_source_for_naming.rstrip('/\\'))
        
        original_source_path_for_job = local_path_arg if direction == TransferDirection.UPLOAD else None

        # --- Special handling for zipping an entire directory for upload ---
        if is_source_directory and direction == TransferDirection.UPLOAD and zip_this_upload:
            self.logger.info(f"Directory Upload Job '{local_path_arg}' will be zipped entirely. Bypassing scan.")
            # TransferJob.__post_init__ will set filename to "dirname.zip" and sftp_transfer_path_remote.
            # local_path here is the source directory itself.
            # remote_path is the target PARENT directory on the remote server.
            job = TransferJob(
                local_path=local_path_arg, 
                remote_path=remote_path_arg, 
                direction=direction,
                is_directory_transfer=True, 
                filename="", # Let __post_init__ create "dirname.zip"
                original_source_path=local_path_arg, # This is the directory to be zipped by the task
                zip_before_upload=True, # Explicitly set
                unzip_after_download=False, # Not applicable for upload
                status=JobStatus.QUEUED # Queue directly for zipping and upload by SFTPTransferTask
            )
            self.logger.critical(f"PTM Directory-Zip Job CREATED: ID {job.id}, ObjID {id(job)}, File '{job.filename}', "
                                 f"sftp_local='{job.sftp_transfer_path_local}', sftp_remote='{job.sftp_transfer_path_remote}', "
                                 f"zip_flag={job.zip_before_upload}")
            self.queue.append(job)
            self.job_added_to_ui.emit(job)
            self._log(f"Directory-as-Zip job '{job.filename}' (ID: {job.id}) added to queue.", "info")
        
        else: # --- Standard handling for single files OR directories to be scanned (or downloaded) ---
            job_initial_local_path = local_path_arg
            job_initial_remote_path = remote_path_arg
            sftp_op_local_path_hint = None
            sftp_op_remote_path_hint = None

            # Provide hints for TransferJob.__post_init__
            if not is_source_directory: # Single file
                if direction == TransferDirection.UPLOAD:
                    # local_path_arg is source file. remote_path_arg is target dir.
                    sftp_op_local_path_hint = local_path_arg 
                    # job_display_filename is basename of local_path_arg.
                    # if zip_this_upload, __post_init__ will change filename to .zip and adjust sftp_op_remote_path_hint
                    sftp_op_remote_path_hint = os.path.join(remote_path_arg, job_display_filename).replace("\\","/")
                else: # DOWNLOAD single file
                    # local_path_arg is target dir. remote_path_arg is source file.
                    # job_display_filename is basename of remote_path_arg.
                    sftp_op_local_path_hint = os.path.join(local_path_arg, job_display_filename) 
                    sftp_op_remote_path_hint = remote_path_arg
            # For directory meta-jobs that will be scanned (not zipped as whole) or downloaded directories:
            # local_path and remote_path are primary. __post_init__ will derive sftp_paths.
            # Example: Uploading a directory (to be scanned): local_path is source_dir, remote_path is target_parent_dir.
            # Example: Downloading a directory: local_path is target_parent_dir, remote_path is source_dir.
            # filename for these meta-jobs will be the directory name.

            job = TransferJob(
                local_path=job_initial_local_path,
                remote_path=job_initial_remote_path,
                direction=direction, 
                is_directory_transfer=is_source_directory, 
                filename=job_display_filename, # Will be adjusted by __post_init__ if zipping a single file upload
                total_size=0, 
                original_source_path=original_source_path_for_job,
                sftp_transfer_path_local=sftp_op_local_path_hint, 
                sftp_transfer_path_remote=sftp_op_remote_path_hint,
                zip_before_upload=zip_this_upload if direction == TransferDirection.UPLOAD and not is_source_directory else False, # Only zip single files this way
                unzip_after_download=unzip_this_download if direction == TransferDirection.DOWNLOAD else False
            )
            self.logger.critical(f"PTM File/Scan-Dir Job CREATED: ID {job.id}, ObjID {id(job)}, File '{job.filename}', "
                                 f"sftp_local='{job.sftp_transfer_path_local}', sftp_remote='{job.sftp_transfer_path_remote}', "
                                 f"zip_flag={job.zip_before_upload}, unzip_flag={job.unzip_after_download}")

            if is_source_directory: # Directory to be scanned (or downloaded, which also implies scan for content list)
                job.status = JobStatus.SCANNING
                self.queue.append(job)
                self.job_added_to_ui.emit(job)
                self._log(f"Directory job '{job.filename}' (ID: {job.id}) added, starting scan.", "info")
                self._start_directory_scan(job)
            else: # Single file job (potentially to be zipped/unzipped based on flags)
                job.status = JobStatus.QUEUED
                self.queue.append(job)
                self.job_added_to_ui.emit(job)
                self._log(f"File job '{job.filename}' (ID: {job.id}, Size: {job.total_size}, ZipUpload: {job.zip_before_upload}) added to queue.", "info")
        
        if self.is_queue_globally_active:
            self._dispatch_tasks_from_queue()

    def _start_directory_scan(self, dir_job: TransferJob):
        if dir_job.id in self._directory_scan_threads and self._directory_scan_threads[dir_job.id].isRunning():
            self.logger.warning(f"Scan for dir job '{dir_job.filename}' (ID: {dir_job.id}) already running.")
            return
        
        source_dir_for_scanner = dir_job.local_path if dir_job.direction == TransferDirection.UPLOAD else dir_job.remote_path
        target_base_for_resulting_jobs = dir_job.remote_path if dir_job.direction == TransferDirection.UPLOAD else dir_job.local_path
        
        scanner = DirectoryScannerThread(self.main_ssh_client_getter, 
                                         source_dir_for_scanner, 
                                         target_base_for_resulting_jobs, 
                                         dir_job.direction, 
                                         dir_job.id,
                                         self.parent_logger_name, 
                                         parent=self) 
        
        scanner.files_ready_for_job_creation.connect(self._handle_scanned_files)
        scanner.scan_error.connect(self._handle_scan_error)
        scanner.scan_finished.connect(self._handle_scan_finished)
        self._directory_scan_threads[dir_job.id] = scanner
        scanner.start()
        self.logger.info(f"DirectoryScannerThread started for job ID {dir_job.id}, scanning '{source_dir_for_scanner}'.")


    def _handle_scanned_files(self, original_dir_job_id: int, job_details_list: List[Dict[str, Any]], direction: TransferDirection):
        self.logger.info(f"Scan for dir job ID {original_dir_job_id} found {len(job_details_list)} files. Adding individual file jobs to queue.")
        parent_dir_job = next((j for j in self.queue if j.id == original_dir_job_id and j.is_directory_transfer), None)
        
        if not parent_dir_job:
            self.logger.error(f"Original directory job ID {original_dir_job_id} not found in queue when trying to process scanned files!")
            return

        files_added_count = 0
        if job_details_list:
            for detail in job_details_list:
                # Individual files from a scan are typically not zipped/unzipped individually by default.
                # If this behavior is desired, flags would need to be passed or determined here.
                file_job = TransferJob(local_path=detail['local_path'], 
                                       remote_path=detail['remote_path'], 
                                       direction=direction, 
                                       is_directory_transfer=False, 
                                       filename=detail['filename'], 
                                       total_size=detail['size'], 
                                       status=JobStatus.QUEUED,
                                       sftp_transfer_path_local=detail['local_path'], 
                                       sftp_transfer_path_remote=detail['remote_path'],
                                       original_source_path=detail['local_path'] if direction == TransferDirection.UPLOAD else None,
                                       # zip_before_upload and unzip_after_download default to False
                                      )
                self.logger.critical(f"PTM Scanned File Job Created: ID {file_job.id}, ObjID {id(file_job)}, File '{file_job.filename}'") # DEBUG
                self.queue.append(file_job)
                self.job_added_to_ui.emit(file_job)
                files_added_count += 1
        
        if parent_dir_job.status == JobStatus.SCANNING: 
            parent_dir_job.status = JobStatus.COMPLETED
            parent_dir_job.progress = 100 
            if files_added_count == 0:
                parent_dir_job.error_message = "Directory is empty or no scannable files found."
                self.logger.info(f"Directory job ID {original_dir_job_id} ('{parent_dir_job.filename}') scan complete: No files found/added.")
            else:
                parent_dir_job.error_message = f"Scan complete: {files_added_count} files added to queue."
                self.logger.info(f"Directory job ID {original_dir_job_id} ('{parent_dir_job.filename}') scan complete: {files_added_count} files added to queue.")
            self.job_updated_in_ui.emit(parent_dir_job)
        
        if self.is_queue_globally_active:
            self._dispatch_tasks_from_queue()

    def _handle_scan_error(self, original_dir_job_id: int, error_message: str):
        self.logger.error(f"Error reported by DirectoryScannerThread for original dir job ID {original_dir_job_id}: {error_message}")
        job = next((j for j in self.queue if j.id == original_dir_job_id and j.is_directory_transfer), None)
        if job and job.status == JobStatus.SCANNING: # Check status before marking failed
            job.status = JobStatus.FAILED
            job.error_message = error_message
            self.job_updated_in_ui.emit(job)

    def _handle_scan_finished(self, original_dir_job_id: int):
        self.logger.debug(f"DirectoryScannerThread finished signal received for original job ID {original_dir_job_id}. Cleaning up scanner instance.")
        scanner = self._directory_scan_threads.pop(original_dir_job_id, None)
        if scanner:
            if scanner.isRunning():
                self.logger.warning(f"Scanner for job {original_dir_job_id} finished signal received, but thread still reports running. Attempting quit/wait.")
                scanner.quit()
                if not scanner.wait(750): 
                       self.logger.error(f"Scanner thread for job {original_dir_job_id} did not terminate gracefully after quit().")
            scanner.deleteLater() 
        
        job = next((j for j in self.queue if j.id == original_dir_job_id and j.is_directory_transfer), None)
        if job and job.status == JobStatus.SCANNING:
            self.logger.info(f"Finalizing dir job ID {original_dir_job_id} ('{job.filename}') as 'COMPLETED' after scan finished (was still in SCANNING state).")
            job.status = JobStatus.COMPLETED
            job.progress = 100
            if not job.error_message: 
                 job.error_message = "Scan finished (directory may be empty or no scannable files found)."
            self.job_updated_in_ui.emit(job)
        
        if self.is_queue_globally_active: 
            self._dispatch_tasks_from_queue()

    def start_queue(self):
        if not self.is_queue_globally_active:
            self.is_queue_globally_active = True
            self._ssh_client_unavailable_retries = 0 
            self.process_queue_timer.start()
            self._log("Parallel transfer queue processing started.", "info")
            self.processing_state_changed.emit(True)
            self._dispatch_tasks_from_queue() 
        else:
            self._log("Parallel transfer queue is already active.", "debug")

    def stop_queue(self):
        if self.is_queue_globally_active:
            self.is_queue_globally_active = False
            self.process_queue_timer.stop()
            self._log("Parallel transfer queue processing stopped by user. No new tasks will be dispatched from timer.", "info")
            self.processing_state_changed.emit(False)

    def cancel_active_transfers(self):
        self.logger.warning(f"User requested to cancel all {len(self.active_tasks)} active transfer tasks.")
        if not self.active_tasks:
            self._log("No active transfers to cancel.", "info")
            return
            
        for job_id, task in list(self.active_tasks.items()): 
            if hasattr(task, 'cancel'):
                self._log(f"Requesting cancellation for active task of job ID {job_id} ('{task.job.filename if task.job else 'N/A'}').", "warning")
                task.cancel() 

    def _dispatch_tasks_from_queue(self):
        if not self.is_queue_globally_active:
            return

        ssh_client = self.main_ssh_client_getter()
        if not ssh_client: 
            self._ssh_client_unavailable_retries += 1
            self.logger.warning(f"Cannot dispatch tasks: Main SSH client not available (Attempt {self._ssh_client_unavailable_retries}/{self._max_ssh_client_retries}).")
            if self._ssh_client_unavailable_retries >= self._max_ssh_client_retries:
                self.logger.error("Max retries for SSH client availability reached. Stopping queue processing.")
                self.stop_queue() 
                self._log("Queue stopped due to persistent SSH client unavailability.", "critical")
            return
        self._ssh_client_unavailable_retries = 0 

        has_queued_file_jobs = any(j.status == JobStatus.QUEUED and not j.is_directory_transfer for j in self.queue)
        has_scanning_jobs = any(j.status == JobStatus.SCANNING for j in self.queue)
        current_active_task_count = len(self.active_tasks)

        if current_active_task_count == 0 and not has_queued_file_jobs and not has_scanning_jobs:
             if self.is_queue_globally_active : self.processing_state_changed.emit(False) 
        else:
             if self.is_queue_globally_active : self.processing_state_changed.emit(True) 

        while len(self.active_tasks) < self.max_concurrent_transfers:
            job_to_process = next((j for j in self.queue if j.status == JobStatus.QUEUED and not j.is_directory_transfer), None)
            
            if not job_to_process:
                break 

            try:
                self.logger.critical(
                    f"PTM PRE-TASK-CREATE: Dispatching Job ID {job_to_process.id}, ObjID {id(job_to_process)}, "
                    f"File '{job_to_process.filename}', Status '{job_to_process.status.value}', "
                    f"zip_flag={job_to_process.zip_before_upload}, " # Log zip flag
                    f"sftp_local='{job_to_process.sftp_transfer_path_local}', sftp_remote='{job_to_process.sftp_transfer_path_remote}'"
                )

                sftp_task = SFTPTransferTask(job_to_process, ssh_client, self.parent_logger_name)
                
                self.logger.critical(
                    f"PTM POST-TASK-CREATE: Task ID {id(sftp_task)}, "
                    f"Task's Job ID {sftp_task.job.id}, Task's Job ObjID {id(sftp_task.job)}, "
                    f"Task's Job zip_flag={sftp_task.job.zip_before_upload}" # Log task's job zip flag
                )

                self.active_tasks[job_to_process.id] = sftp_task 
                job_to_process.task_runner_id = id(sftp_task) 

                task_signals = sftp_task.signals 
                
                task_signals.task_started.connect(functools.partial(self._on_task_event_received, task_signals, "started"))
                task_signals.task_progress.connect(functools.partial(self._on_task_event_received, task_signals, "progress"))
                task_signals.task_completed.connect(functools.partial(self._on_task_event_received, task_signals, "completed"))
                task_signals.task_failed.connect(functools.partial(self._on_task_event_received, task_signals, "failed"))
                task_signals.task_authoritative_size_determined.connect(functools.partial(self._on_task_event_received, task_signals, "authoritative_size"))
                task_signals.task_log_message.connect(self.log_message) 
                task_signals.task_status_update.connect(functools.partial(self._on_task_event_received, task_signals, "status_update"))

                job_to_process.status = JobStatus.PENDING_RESOURCES 
                self.job_updated_in_ui.emit(job_to_process)

                self.thread_pool.start(sftp_task)
                self.logger.info(f"Dispatched job {job_to_process.id} ('{job_to_process.filename}') to QThreadPool. Task ID: {id(sftp_task)}. Active tasks: {len(self.active_tasks)}.")
            except Exception as e:
                self.logger.error(f"Error dispatching job {job_to_process.id} ('{job_to_process.filename}'): {e}", exc_info=True)
                job_to_process.status = JobStatus.FAILED
                job_to_process.error_message = f"Dispatch error: {e}"
                self.job_updated_in_ui.emit(job_to_process)
                if job_to_process.id in self.active_tasks: 
                    del self.active_tasks[job_to_process.id]

    def _find_job_by_task_signals(self, signals_object: SFTPTaskSignals) -> Optional[TransferJob]:
        """Finds the TransferJob associated with a given SFTPTaskSignals instance."""
        for job_id_key, task_instance_val in self.active_tasks.items():
            if task_instance_val.signals is signals_object:
                job = next((j for j in self.queue if j.id == job_id_key), None)
                if not job:
                    self.logger.error(f"PTM._find_job_by_task_signals: Task for job_id_key {job_id_key} found in active_tasks, "
                                      f"but the job object itself is not in the main queue. This is a critical state inconsistency. "
                                      f"Task: {id(task_instance_val)}, Signals: {id(signals_object)}")
                elif job: 
                    self.logger.debug(
                        f"PTM _find_job: Matched signals_object {id(signals_object)} to task {id(task_instance_val)} "
                        f"keyed by PTM job_id_key {job_id_key}. "
                        f"Found Job in queue: ID {job.id}, ObjID {id(job)}. "
                        f"Task's current job.id: {task_instance_val.job.id}, Task's current job ObjID: {id(task_instance_val.job)}"
                    )
                return job
        self.logger.warning(f"PTM _find_job: No active task found for signals_object {id(signals_object)}.")
        return None

    def _disconnect_task_signals(self, task_signals_obj: SFTPTaskSignals, job_id_for_log: Optional[int]):
        if not task_signals_obj: return
        log_id_str = str(job_id_for_log) if job_id_for_log is not None else "UnknownOrStaleJob"
        self.logger.debug(f"Attempting to disconnect all signals for task of job ID context '{log_id_str}' (Signals obj ID {id(task_signals_obj)})")
        # Disconnecting all known signals individually.
        # Using try-except for each because a signal might have already been disconnected or never connected to this specific slot.
        signals_to_disconnect = [
            task_signals_obj.task_started,
            task_signals_obj.task_progress,
            task_signals_obj.task_completed,
            task_signals_obj.task_failed,
            task_signals_obj.task_authoritative_size_determined,
            task_signals_obj.task_status_update
        ]
        for signal in signals_to_disconnect:
            try: signal.disconnect() # Disconnect all slots from this signal
            except TypeError: pass # Raised if no connections exist or issues with specific disconnects
        
        try: task_signals_obj.task_log_message.disconnect(self.log_message) # Disconnect specific if one known connection
        except TypeError:
            try: task_signals_obj.task_log_message.disconnect() # Fallback to generic disconnect
            except TypeError: pass

        self.logger.debug(f"Finished attempting to disconnect signals for task of job ID context '{log_id_str}'")


    def _on_task_event_received(self, task_signals_instance: SFTPTaskSignals, event_type: str, *args):
        job = self._find_job_by_task_signals(task_signals_instance)
        emitted_job_id_from_signal_args = args[0] if args and isinstance(args[0], int) else None

        self.logger.debug(
            f"PTM _on_task_event: Event '{event_type}', SignalsObj {id(task_signals_instance)}, EmittedID {emitted_job_id_from_signal_args}, Args {args}. "
            f"Found Job via signals: ID {job.id if job else 'None'}, ObjID {id(job) if job else 'None'}."
        )

        if not job: 
            self.logger.debug(f"PTM._on_task_event_received: Event '{event_type}' for signals obj {id(task_signals_instance)} "
                                f"(emitted job_id: {emitted_job_id_from_signal_args}), but no matching active task/job found. "
                                f"This might be a stale signal. Attempting to disconnect its signals.")
            if task_signals_instance: 
                self._disconnect_task_signals(task_signals_instance, emitted_job_id_from_signal_args)
            return

        manager_expected_job_id = job.id 

        if emitted_job_id_from_signal_args is None  and emitted_job_id_from_signal_args != manager_expected_job_id:
            self.logger.debug(
                f"PTM JOB ID MISMATCH: Event '{event_type}'. Manager expected job ID {manager_expected_job_id} "
                f"(based on active task's signals object {id(task_signals_instance)}). "
                f"Task's signal emitted job ID {emitted_job_id_from_signal_args}. This indicates a serious inconsistency. "
                f"Args from signal: {args}. "
                f"The task (ID: {job.task_runner_id if job.task_runner_id else 'N/A'}) associated with job '{job.filename}' "
                f"may be emitting an incorrect ID. This needs investigation in SFTPTransferTask."
            )
            # Proceeding with manager_expected_job_id, but this is a critical error to investigate.
        elif emitted_job_id_from_signal_args is None and event_type not in ["task_log_message"]: # task_log_message doesn't start with job_id
             self.logger.debug(
                f"PTM JOB ID MISMATCH: Event '{event_type}'. Manager expected {manager_expected_job_id}, "
                f"Task emitted {emitted_job_id_from_signal_args}. Args: {args}."
                )
             self.logger.error(f"PTM._on_task_event_received: Event '{event_type}' for PTM job ID {manager_expected_job_id} "
                               f"did not receive an integer job ID as the first argument from the signal. Args: {args}")


        # Process the event using 'job' (which has job.id == manager_expected_job_id)
        if event_type == "started":
            job.status = JobStatus.IN_PROGRESS; job.progress = 0; job.bytes_transferred = 0; job.error_message = ""
            self.job_updated_in_ui.emit(job)
            self.logger.info(f"Task for job {job.id} ('{job.filename}') confirmed started by task.")

        elif event_type == "progress":
            if len(args) < 3: self.logger.error(f"Progress event for job {job.id} missing arguments: {args}"); return
            bytes_transferred, total_bytes_from_op = args[1], args[2]
            job.bytes_transferred = bytes_transferred
            
            current_total_for_calc = job.total_size 
            if current_total_for_calc == 0 and total_bytes_from_op > 0:
                job.total_size = total_bytes_from_op 
            
            # Recalculate current_total_for_calc if job.total_size was updated
            current_total_for_calc = job.total_size 
            if current_total_for_calc > 0:
                progress_val = int((bytes_transferred / current_total_for_calc) * 100)
                job.progress = min(progress_val, 100) 
            elif bytes_transferred > 0 and total_bytes_from_op == 0: 
                job.progress = 0 
            elif bytes_transferred >= 0 and bytes_transferred == total_bytes_from_op : # Handles 0-byte files too
                job.progress = 100
            else: 
                job.progress = 0
            self.job_updated_in_ui.emit(job)

        elif event_type == "completed":
            if len(args) < 2: self.logger.error(f"Completed event for job {job.id} missing arguments: {args}"); return
            message = args[1]
            self.logger.debug(f"PTM processing 'completed' event for job_id={job.id}. Message: {message}")
            
            task_instance = self.active_tasks.pop(job.id, None) 
            if task_instance:
                self.logger.debug(f"Removed task for job {job.id} from active_tasks. Disconnecting its signals.")
                self._disconnect_task_signals(task_instance.signals, job.id)
            else:
                self.logger.warning(f"PTM 'completed': Task for job {job.id} not found in active_tasks (was it already removed/failed?).")
            
            job.status = JobStatus.COMPLETED; job.progress = 100
            job.error_message = "" # Clear any transient error/status messages
            self.job_updated_in_ui.emit(job)
            self.logger.info(f"Task for job {job.id} ('{job.filename}') reported completed: {message}")
            if self.is_queue_globally_active: self._dispatch_tasks_from_queue()

        elif event_type == "failed":
            if len(args) < 3: self.logger.error(f"Failed event for job {job.id} missing arguments: {args}"); return
            error_message, was_cancelled_by_task = args[1], args[2]
            self.logger.debug(f"PTM processing 'failed' event for job_id={job.id}. Cancelled by task: {was_cancelled_by_task}. Error: {error_message}")

            task_instance = self.active_tasks.pop(job.id, None)
            if task_instance:
                self.logger.debug(f"Removed task for job {job.id} from active_tasks due to failure/cancellation. Disconnecting its signals.")
                self._disconnect_task_signals(task_instance.signals, job.id)
            else:
                self.logger.warning(f"PTM 'failed': Task for job {job.id} not found in active_tasks (was it already removed?).")

            job.status = JobStatus.CANCELLED if was_cancelled_by_task else JobStatus.FAILED
            job.error_message = error_message
            self.job_updated_in_ui.emit(job)
            log_level_str = "warning" if was_cancelled_by_task else "error"
            self.logger.log(logging.getLevelName(log_level_str.upper()), f"Task for job {job.id} ('{job.filename}') failed/cancelled by task: {error_message}")
            
            if self.is_queue_globally_active: self._dispatch_tasks_from_queue()

        elif event_type == "authoritative_size":
            if len(args) < 2: self.logger.error(f"Authoritative_size event for job {job.id} missing arguments: {args}"); return
            authoritative_size = args[1]
            if authoritative_size >= 0 and job.total_size != authoritative_size : # Allow 0 size
                job.total_size = authoritative_size
                self.job_updated_in_ui.emit(job)
                self.logger.debug(f"Authoritative size {authoritative_size}B set for job {job.id} ('{job.filename}') by task.")
        
        elif event_type == "status_update":
            if len(args) < 3: self.logger.error(f"Status_update event for job {job.id} missing arguments: {args}"); return
            new_status_enum, status_message_str = args[1], args[2]
            if isinstance(new_status_enum, JobStatus):
                job.status = new_status_enum
                if status_message_str is not None: # Allow empty string to clear, but None to not change
                    job.error_message = status_message_str 
                self.job_updated_in_ui.emit(job)
                self.logger.debug(f"Status update for job {job.id} from task: {new_status_enum.value} {'- ' + status_message_str if status_message_str else ''}")
            else:
                self.logger.error(f"Received status_update for job {job.id} with invalid status type: {type(new_status_enum)}. Args: {args}")

    def clear_successful_jobs_from_queue(self):
        self.logger.debug(f"PTM.clear_successful_jobs_from_queue: Current queue length {len(self.queue)}")
        ids_to_remove_from_ui: List[int] = []
        new_queue: List[TransferJob] = []
        
        for job_in_q in self.queue:
            if job_in_q.status == JobStatus.COMPLETED and job_in_q.id not in self.active_tasks:
                ids_to_remove_from_ui.append(job_in_q.id)
                self.logger.debug(f"Marking job ID {job_in_q.id} ('{job_in_q.filename}') for removal (status: COMPLETED).")
            else:
                new_queue.append(job_in_q)
        
        if not ids_to_remove_from_ui:
            self.logger.info("PTM.clear_successful_jobs_from_queue: No completed and inactive jobs to clear from queue.")
            return
        
        num_actually_removed = len(self.queue) - len(new_queue)
        self.queue = new_queue
        
        for job_id_val in ids_to_remove_from_ui:
            self.logger.debug(f"PTM.clear_successful_jobs_from_queue: Emitting job_removed_from_ui for ID {job_id_val}")
            self.job_removed_from_ui.emit(job_id_val)
            
        if num_actually_removed > 0 :
            self.logger.info(f"PTM.clear_successful_jobs_from_queue: Cleared {num_actually_removed} jobs. New queue length {len(self.queue)}")

    def remove_jobs_from_queue(self, job_ids_to_remove: List[int]):
        self.logger.info(f"PTM.remove_jobs_from_queue: Requested to remove job IDs: {job_ids_to_remove}. Current queue length: {len(self.queue)}")
        
        jobs_effectively_removed_count = 0

        for job_id in job_ids_to_remove:
            job_found_in_queue_for_direct_removal = False
            # 1. Handle active SFTPTransferTasks: Signal them to cancel
            task_instance = self.active_tasks.get(job_id)
            if task_instance:
                self.logger.warning(f"PTM.remove_jobs_from_queue: Job ID {job_id} is an active transfer. Requesting its task to cancel.")
                if hasattr(task_instance, 'cancel'):
                    task_instance.cancel()
                # The task will emit 'task_failed' with was_cancelled=True.
                # Its handler will update the job status and remove from active_tasks.
                # The job itself (now CANCELLED) will be removed from queue below if still in job_ids_to_remove.
            
            # 2. Handle active DirectoryScannerThreads
            if job_id in self._directory_scan_threads:
                scanner = self._directory_scan_threads.pop(job_id) # Remove from tracking immediately
                self.logger.warning(f"PTM.remove_jobs_from_queue: Job ID {job_id} is an active directory scan. Stopping scanner.")
                if scanner.isRunning():
                    scanner.stop()
                    if not scanner.wait(300):
                        self.logger.error(f"Scanner thread for job {job_id} did not stop gracefully.")
                scanner.deleteLater()
                
                # Update the directory meta-job in the queue to CANCELLED
                dir_job_in_queue = next((j for j in self.queue if j.id == job_id and j.is_directory_transfer), None)
                if dir_job_in_queue:
                    dir_job_in_queue.status = JobStatus.CANCELLED
                    dir_job_in_queue.error_message = "Scan cancelled by user removal."
                    self.job_updated_in_ui.emit(dir_job_in_queue)
                    # This job will be picked up by the list comprehension below for removal from queue
            
            # 3. Remove the job from the actual self.queue list and emit UI removal
            # This handles:
            #   - Jobs that were QUEUED and never started.
            #   - Jobs that were FAILED/COMPLETED.
            #   - Jobs whose active task/scan was just cancelled above and are now (or will shortly be via signal) CANCELLED.
            # We iterate over a copy for safe removal or build a new list.
            
            # Check if job is still active (maybe cancellation is processing)
            if job_id in self.active_tasks:
                self.logger.info(f"Job ID {job_id} task cancellation initiated. Deferring queue removal until task signals failure/completion.")
                continue # Don't remove from queue yet, let the task's final signal handle its active_tasks removal.

            # If not active, proceed with removal from queue
            initial_queue_len = len(self.queue)
            self.queue = [j for j in self.queue if j.id != job_id]
            if len(self.queue) < initial_queue_len:
                self.job_removed_from_ui.emit(job_id)
                jobs_effectively_removed_count += 1
                self.logger.info(f"PTM.remove_jobs_from_queue: Removed job ID {job_id} from queue and signaled UI.")
            else:
                 # This might happen if the job was an active task, cancellation was signaled,
                 # but its failure signal hasn't been processed yet to remove it from active_tasks.
                 # Or if the job_id was invalid.
                 self.logger.warning(f"PTM.remove_jobs_from_queue: Job ID {job_id} not found in queue for direct removal or still marked active.")


        if jobs_effectively_removed_count > 0:
             self.logger.info(f"PTM.remove_jobs_from_queue: Effectively removed {jobs_effectively_removed_count} jobs.")
        
        if self.is_queue_globally_active:
            self._dispatch_tasks_from_queue()


    def retry_jobs_in_queue(self, job_ids: List[int]):
        retried_count = 0
        for job_id_to_retry in job_ids:
            job = next((j for j in self.queue if j.id == job_id_to_retry), None)
            if job and job.status in [JobStatus.FAILED, JobStatus.CANCELLED]:
                if job.id in self.active_tasks:
                    self.logger.error(f"Cannot retry job {job.id} ('{job.filename}'): it is still listed in active_tasks."); continue
                if job.id in self._directory_scan_threads and self._directory_scan_threads[job.id].isRunning():
                    self.logger.error(f"Cannot retry directory job {job.id} ('{job.filename}'): its scanner is still running."); continue

                self.logger.info(f"Retrying job ID {job.id} ('{job.filename}'). Previous status: {job.status.value}")
                job.error_message = ""; job.progress = 0; job.bytes_transferred = 0; job.task_runner_id = None
                
                if job.direction == TransferDirection.DOWNLOAD or job.zip_before_upload : # Reset size if it's dynamic or was for a zip
                    job.total_size = 0 
                
                if job.is_directory_transfer:
                    # If the original job was a directory that was zipped as a whole (not scanned)
                    if job.zip_before_upload and job.direction == TransferDirection.UPLOAD :
                         job.status = JobStatus.QUEUED # Re-queue for zipping and upload
                         # Ensure original_source_path is still valid (it should be from initial creation)
                         if not job.original_source_path:
                             self.logger.error(f"Cannot retry directory-zip job {job.id}: original_source_path is missing.")
                             job.status = JobStatus.FAILED; job.error_message = "Retry failed: missing original source path."
                             self.job_updated_in_ui.emit(job)
                             continue
                    else: # Directory to be scanned (e.g. download, or upload without full zip)
                         job.status = JobStatus.SCANNING
                         self.job_updated_in_ui.emit(job)
                         self._start_directory_scan(job) 
                else: # Single file job
                    job.status = JobStatus.QUEUED
                    # Re-evaluate size for non-zip uploads if it was 0
                    if job.direction == TransferDirection.UPLOAD and not job.zip_before_upload and job.sftp_transfer_path_local and os.path.isfile(job.sftp_transfer_path_local):
                        try: job.total_size = os.path.getsize(job.sftp_transfer_path_local)
                        except OSError: job.total_size = 0 
                
                self.job_updated_in_ui.emit(job) 
                retried_count += 1
            elif job:
                 self.logger.warning(f"Cannot retry job {job.id} ('{job.filename}'). Status is {job.status.value}, not FAILED or CANCELLED.")
            else:
                 self.logger.warning(f"Cannot retry job ID {job_id_to_retry}: Not found in queue.")

        if retried_count > 0:
            self.logger.info(f"PTM: Re-queued/re-scanned {retried_count} jobs for retry.")
        
        if self.is_queue_globally_active:
            self._dispatch_tasks_from_queue()

    def cancel_all_directory_scans(self):
        if not self._directory_scan_threads:
            self.logger.info("cancel_all_directory_scans: No active directory scans to cancel.")
            return
            
        self.logger.warning(f"Cancelling all active directory scans ({len(self._directory_scan_threads)} found).")
        for job_id, scanner in list(self._directory_scan_threads.items()): 
            self.logger.info(f"Stopping scanner for dir job ID {job_id}.")
            if scanner.isRunning():
                scanner.stop()
                if not scanner.wait(500): 
                    self.logger.error(f"Scanner thread for job ID {job_id} did not terminate gracefully after stop().")
            
            # Scanner's finished signal might still fire and call _handle_scan_finished which pops it.
            # If not, remove it here.
            if job_id in self._directory_scan_threads:
                del self._directory_scan_threads[job_id]
            scanner.deleteLater() 

            original_job = next((j for j in self.queue if j.id == job_id and j.is_directory_transfer), None)
            if original_job and original_job.status == JobStatus.SCANNING:
                original_job.status = JobStatus.CANCELLED
                original_job.error_message = "Scan cancelled by global scan cancellation."
                self.job_updated_in_ui.emit(original_job)
        
        self.logger.info("Finished cancelling all directory scans.")