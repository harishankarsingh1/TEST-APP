# sftp_connection/scripts/sftp_worker.py
import os
import stat
import traceback
import socket
from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot # QGenericArgument not needed if not using invokeMethod with it
from .sftp_transfer_types import TransferJob, JobStatus, TransferDirection

try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

class SFTPWorker(QObject):
    connected = pyqtSignal(str)
    disconnected = pyqtSignal(str)
    error_occurred = pyqtSignal(str, str)
    log_message_signal = pyqtSignal(str, str) # message, level
    operation_successful = pyqtSignal(str, str)
    directory_listing_ready = pyqtSignal(str, list)
    directory_listing_error = pyqtSignal(str, str)
    operation_cancelled_signal = pyqtSignal(str, str)

    job_progress = pyqtSignal(int, int, int, int)
    job_completed = pyqtSignal(int, str)
    job_failed = pyqtSignal(int, str)
    job_cancelled = pyqtSignal(int, str)
    job_remote_size_discovered = pyqtSignal(int, int)

    def __init__(self, host, port, username, auth_type, auth_details):
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.auth_type = auth_type
        self.auth_details = auth_details
        self.ssh_client = None
        self.sftp_client = None
        self._is_running = True
        self.current_job_id_processing = None # ID of the job this worker is actively transferring

    def _log(self, message, level="debug"):
        self.log_message_signal.emit(message, level)

    @pyqtSlot()
    def request_stop(self):
        self._log(f"SFTPWorker: Global stop requested. Current job: {self.current_job_id_processing}", level="info")
        self._is_running = False
        # If a job is active, its callback or operation loop should check _is_running
        # and then emit job_cancelled itself when it detects the stop.
        # No need to emit job_cancelled directly from here as it might be premature
        # or conflict with the natural termination of the operation.

    @pyqtSlot()
    def run_connect(self):
        if not PARAMIKO_AVAILABLE:
            self.error_occurred.emit("Connect", "paramiko library is not installed.")
            self._log("paramiko library is not installed.", level="critical")
            return
        self._is_running = True # Reset for a new connection attempt
        self._log(f"Attempting to connect to {self.username}@{self.host}:{self.port}...", level="info")
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            connect_args = {
                "hostname": self.host, "port": int(self.port), "username": self.username,
                "timeout": 15, "look_for_keys": False, "allow_agent": False, "banner_timeout": 20
            }
            if self.auth_type == "Password":
                connect_args["password"] = self.auth_details.get("password")
            elif self.auth_type == "Key File":
                key_file_path = self.auth_details.get("key_file")
                passphrase = self.auth_details.get("passphrase")
                pkey = None
                key_types_to_try = [paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey, paramiko.DSSKey] # Common order
                last_exception = None
                for key_type in key_types_to_try:
                    if not self._is_running: break
                    try:
                        self._log(f"Attempting to load key {key_file_path} as {key_type.__name__}")
                        pkey = key_type.from_private_key_file(key_file_path, password=passphrase if passphrase else None)
                        self._log(f"Successfully loaded key as {key_type.__name__}")
                        break
                    except paramiko.ssh_exception.PasswordRequiredException: # Key needs passphrase, but none/wrong one given
                        self._log(f"Key file '{os.path.basename(key_file_path)}' requires a passphrase (tried with {key_type.__name__}).", level="warning")
                        self.error_occurred.emit("Connect", "Key file requires a passphrase, but it was incorrect or not provided.")
                        self._cleanup_connection_objects(); return
                    except (paramiko.ssh_exception.SSHException, IOError) as e: # IOError for file not found/permissions
                        self._log(f"Error with {key_type.__name__} for key {key_file_path}: {type(e).__name__} - {e}", level="debug")
                        last_exception = e
                    # except Exception as e_gen: # Catchall for unexpected key errors
                    #     self._log(f"Generic exception with {key_type.__name__} for key {key_file_path}: {e_gen}", level="debug")
                    #     last_exception = e_gen; break
                if not self._is_running: self.operation_cancelled_signal.emit("Connect", "Connection cancelled during key loading."); self._cleanup_connection_objects(); return
                if not pkey:
                    err_msg = f"Failed to load private key: {key_file_path}."
                    if last_exception: err_msg += f" Last Error: {type(last_exception).__name__} - {last_exception}"
                    else: err_msg += " Ensure the key format is supported and the file is accessible."
                    self._log(err_msg, level="error"); self.error_occurred.emit("Connect", err_msg); self._cleanup_connection_objects(); return
                connect_args["pkey"] = pkey

            if not self._is_running: self.operation_cancelled_signal.emit("Connect", "Connection attempt cancelled before SSH connect."); self._cleanup_connection_objects(); return
            self.ssh_client.connect(**connect_args)
            if not self._is_running: self.operation_cancelled_signal.emit("Connect", "Connection cancelled during SSH connect phase."); self._cleanup_connection_objects(full_close=True); return

            self.sftp_client = self.ssh_client.open_sftp()
            if not self._is_running: self.operation_cancelled_signal.emit("Connect", "Connection cancelled after opening SFTP session."); self._cleanup_connection_objects(full_close=True); return

            self._log(f"Successfully connected to {self.host}.", level="info")
            self.connected.emit(f"Successfully connected to {self.host}.")
        except paramiko.ssh_exception.AuthenticationException as e:
            self._log(f"Authentication failed: {e}", level="error")
            self.error_occurred.emit("Connect", f"Authentication failed: {e}")
            self._cleanup_connection_objects(full_close=True)
        except (socket.error, paramiko.ssh_exception.SSHException) as e: # More specific network/SSH errors
             self._log(f"Connection error: {type(e).__name__} - {e}", level="error")
             self.error_occurred.emit("Connect", f"Connection error: {e}")
             self._cleanup_connection_objects(full_close=True)
        except Exception as e: # General catch-all
            if not self._is_running or "cancel" in str(e).lower() or "aborted" in str(e).lower():
                self.operation_cancelled_signal.emit("Connect", f"Connection process aborted: {e}")
                self._log(f"Connection process aborted: {e}", level="warning")
            else:
                self._log(f"An unexpected error occurred during connection: {e}\n{traceback.format_exc()}", level="error")
                self.error_occurred.emit("Connect", f"Unexpected error: {e}")
            self._cleanup_connection_objects(full_close=True) # Assume full cleanup on any unexpected error

    def _cleanup_connection_objects(self, full_close=False):
        if self.sftp_client:
            try: self.sftp_client.close()
            except Exception: pass
            self.sftp_client = None
        if full_close and self.ssh_client:
            try: self.ssh_client.close()
            except Exception: pass
            self.ssh_client = None
        elif self.ssh_client and not self.sftp_client: # If SFTP is gone, SSH likely should be too
            try: self.ssh_client.close()
            except Exception: pass
            self.ssh_client = None

    @pyqtSlot()
    def run_disconnect(self):
        self._log("SFTPWorker: run_disconnect called.", level="info")
        self._is_running = False # Signal that we are stopping
        sftp_closed, ssh_closed = True, True
        if self.sftp_client:
            try: self.sftp_client.close()
            except Exception as e: self._log(f"Error closing SFTP client: {e}", level="warning"); sftp_closed = False
            self.sftp_client = None
        if self.ssh_client:
            try: self.ssh_client.close()
            except Exception as e: self._log(f"Error closing SSH client: {e}", level="warning"); ssh_closed = False
            self.ssh_client = None
        self.disconnected.emit("Disconnected successfully." if (sftp_closed and ssh_closed) else "Disconnected with errors.")

    def _job_transfer_progress_callback(self, job_id: int, filename: str, bytes_transferred: int, total_bytes: int, direction: TransferDirection):
        self._log(f"PROGRESS_CALLBACK: job_id={job_id}, current_worker_job_id={self.current_job_id_processing}, file='{filename}', sent/recvd={bytes_transferred}, total={total_bytes}, running={self._is_running}")
        if not self._is_running or self.current_job_id_processing != job_id:
            msg = f"Job {job_id} ({filename}): Transfer callback detected stop/mismatch. " \
                  f"(Global stop: {not self._is_running}, job_id current: {self.current_job_id_processing}, callback for: {job_id})."
            self._log(msg, level="warning")
            raise Exception("TransferAbortedStopOrMismatch") # This will stop paramiko's put/get
        self.job_progress.emit(job_id, bytes_transferred, total_bytes, 0 if direction == TransferDirection.UPLOAD else 1)

    def _ensure_remote_dir_exists(self, remote_dir_path: str, job_id_for_log: int) -> bool:
        """ Helper to create remote directory structure iteratively """
        if not remote_dir_path or remote_dir_path == '/':
            return True # Root directory always exists

        parts = [part for part in remote_dir_path.strip('/').split('/') if part]
        current_path_to_check = ""
        for part in parts:
            if not self._is_running: return False # Check before each SFTP operation
            # Build absolute path component by component
            current_path_to_check = "/" + os.path.join(current_path_to_check.strip('/'), part).replace("\\", "/")
            try:
                self.sftp_client.stat(current_path_to_check)
            except FileNotFoundError:
                if not self._is_running: return False
                self._log(f"Job {job_id_for_log}: Remote directory '{current_path_to_check}' does not exist. Creating.", level="debug")
                try:
                    self.sftp_client.mkdir(current_path_to_check)
                except Exception as e_mkdir:
                    self._log(f"Job {job_id_for_log}: Failed to create remote dir '{current_path_to_check}': {e_mkdir}", level="error")
                    return False # Failed to create directory
            except Exception as e_stat: # Other stat errors
                self._log(f"Job {job_id_for_log}: Error stating remote dir '{current_path_to_check}': {e_stat}", level="error")
                return False
        return True


    @pyqtSlot(TransferJob)
    def run_queued_file_transfer(self, job: TransferJob):
        if not self._is_running:
            self.job_cancelled.emit(job.id, "Transfer cancelled (worker not running).")
            self._log(f"Job {job.id} ('{job.filename}') cancelled before start, worker not running.", level="warning")
            return
        if not self.sftp_client:
            self.job_failed.emit(job.id, "Not connected.")
            self._log(f"Job {job.id} ('{job.filename}') failed before start, not connected.", level="error")
            return

        self.current_job_id_processing = job.id
        self._log(f"Job {job.id}: Starting {job.direction.value} of '{job.filename}' ({job.local_path} <=> {job.remote_path})", level="info")

        try:
            if job.direction == TransferDirection.UPLOAD:
                remote_target_dir = os.path.dirname(job.remote_path.replace("\\", "/"))
                if not self._ensure_remote_dir_exists(remote_target_dir, job.id):
                    self.job_failed.emit(job.id, f"Failed to ensure/create remote directory: {remote_target_dir}")
                    self.current_job_id_processing = None; return

                if not self._is_running: self.job_cancelled.emit(job.id, "Cancelled before sftp_client.put."); self.current_job_id_processing = None; return
                self._log(f"Job {job.id}: Calling sftp_client.put for '{job.local_path}' to '{job.remote_path}'", level="debug")
                self.sftp_client.put(job.local_path, job.remote_path.replace("\\", "/"),
                                     callback=lambda sent, total: self._job_transfer_progress_callback(job.id, job.filename, sent, total, TransferDirection.UPLOAD),
                                     confirm=True)

            elif job.direction == TransferDirection.DOWNLOAD:
                local_target_dir = os.path.dirname(job.local_path)
                if local_target_dir and not os.path.exists(local_target_dir):
                    try: os.makedirs(local_target_dir, exist_ok=True)
                    except Exception as e_mkdir_local:
                        self.job_failed.emit(job.id, f"Failed to create local dir {local_target_dir}: {e_mkdir_local}"); self.current_job_id_processing = None; return

                actual_total_size = job.total_size
                try:
                    self._log(f"Job {job.id}: Stating remote file '{job.remote_path}'. Current job.total_size: {actual_total_size}", level="debug")
                    r_stat = self.sftp_client.stat(job.remote_path.replace("\\", "/"))
                    discovered_size = r_stat.st_size
                    self._log(f"Job {job.id}: Remote stat size: {discovered_size}", level="debug")
                    if discovered_size > 0 and (actual_total_size == 0 or actual_total_size != discovered_size):
                        actual_total_size = discovered_size
                        self.job_remote_size_discovered.emit(job.id, actual_total_size)
                except Exception as e_stat:
                    self._log(f"Job {job.id}: Could not stat remote file '{job.remote_path}': {e_stat}. Using job.total_size or callback total.", level="warning")

                if not self._is_running: self.job_cancelled.emit(job.id, "Cancelled before sftp_client.get."); self.current_job_id_processing = None; return
                self._log(f"Job {job.id}: Calling sftp_client.get for '{job.remote_path}' to '{job.local_path}'", level="debug")
                self.sftp_client.get(job.remote_path.replace("\\", "/"), job.local_path,
                                     callback=lambda recvd, total_from_cb: self._job_transfer_progress_callback(job.id, job.filename, recvd, actual_total_size if actual_total_size > 0 else total_from_cb, TransferDirection.DOWNLOAD))

            if not self._is_running: # Check after operation
                self.job_cancelled.emit(job.id, f"{job.direction.value} for '{job.filename}' cancelled post-operation check.")
            else:
                self.job_completed.emit(job.id, f"File '{job.filename}' {job.direction.value.lower()}ed successfully.")
        except Exception as e:
            if "TransferAborted" in str(e) or (not self._is_running and self.current_job_id_processing == job.id) :
                self.job_cancelled.emit(job.id, f"{job.direction.value} for '{job.filename}' cancelled: {e}")
                self._log(f"Job {job.id} ({job.filename}) explicitly cancelled via exception: {e}", level="warning")
            else:
                self._log(f"Job {job.id}: Error during {job.direction.value} for '{job.filename}': {e}\n{traceback.format_exc()}", level="error")
                self.job_failed.emit(job.id, f"Error during {job.direction.value.lower()} of '{job.filename}': {e}")
        finally:
            if self.current_job_id_processing == job.id:
                self.current_job_id_processing = None # Clear active job ID for this worker

    @pyqtSlot(str)
    def run_list_directory(self, path_to_list: str):
        # (Code from previous corrected version, ensure _log and error emissions are consistent)
        if not self._is_running: self.operation_cancelled_signal.emit("List", f"List '{path_to_list}' cancelled (worker not running)."); return
        if not self.sftp_client: self.directory_listing_error.emit(path_to_list, "Not connected."); return
        self._log(f"Listing directory: {path_to_list}", level="info")
        try:
            items_data = []
            if not self._is_running: self.operation_cancelled_signal.emit("List", "Cancelled before listdir_attr."); return
            listing = self.sftp_client.listdir_attr(path_to_list.replace("\\", "/"))
            if not self._is_running: self.operation_cancelled_signal.emit("List", "Cancelled after listdir_attr."); return

            for attr in listing:
                if not self._is_running: break
                item_type = "Dir" if stat.S_ISDIR(attr.st_mode) else ("File" if stat.S_ISREG(attr.st_mode) else "Other")
                base_path = path_to_list.replace("\\", "/")
                if not base_path.endswith('/') and base_path != '/': base_path += '/'
                
                item_name_cleaned = attr.filename # No need to strip slashes from filename component itself
                
                full_path = f"{base_path}{item_name_cleaned}".replace('//', '/') # Ensure no double slashes
                items_data.append({"name": item_name_cleaned,
                                   "size": attr.st_size if item_type == "File" else 0,
                                   "type": item_type,
                                   "modified": attr.st_mtime,
                                   "full_path": full_path})
            if self._is_running:
                self.directory_listing_ready.emit(path_to_list, items_data)
            else:
                self.operation_cancelled_signal.emit("List", f"Listing of '{path_to_list}' cancelled during processing.")
        except Exception as e:
            if not self._is_running or "cancel" in str(e).lower(): self.operation_cancelled_signal.emit("List", f"Listing of '{path_to_list}' cancelled due to: {e}")
            else: self._log(f"Error listing dir {path_to_list}: {e}", level="error"); self.directory_listing_error.emit(path_to_list, str(e))


    @pyqtSlot(str)
    def run_create_directory(self, remote_path_full: str):
        # (Code from previous corrected version)
        if not self._is_running: self.operation_cancelled_signal.emit("Create Directory", "Cancelled (worker not running)."); return
        if not self.sftp_client: self.error_occurred.emit("Create Directory", "Not connected."); return
        self._log(f"Attempting to create remote directory: {remote_path_full}", level="info")
        try:
            self.sftp_client.mkdir(remote_path_full.replace("\\", "/"))
            if not self._is_running: self.operation_cancelled_signal.emit("Create Directory", "Cancelled after mkdir."); return
            self.operation_successful.emit("Create Directory", f"Directory '{remote_path_full}' created.")
        except Exception as e:
            if not self._is_running or "cancel" in str(e).lower(): self.operation_cancelled_signal.emit("Create Directory", f"Cancelled due to error: {e}")
            else: self.error_occurred.emit("Create Directory", str(e)); self._log(f"Error creating directory {remote_path_full}: {e}", level="error")


    @pyqtSlot(str)
    def run_delete_file(self, remote_file_path: str):
        # (Code from previous corrected version)
        if not self._is_running: self.operation_cancelled_signal.emit("Delete File", "Cancelled (worker not running)."); return
        if not self.sftp_client: self.error_occurred.emit("Delete File", "Not connected."); return
        self._log(f"Attempting to delete remote file: {remote_file_path}", level="info")
        try:
            self.sftp_client.remove(remote_file_path.replace("\\", "/")) # 'remove' is for files (like unlink)
            if not self._is_running: self.operation_cancelled_signal.emit("Delete File", "Cancelled after remove."); return
            self.operation_successful.emit("Delete File", f"File '{remote_file_path}' deleted.")
        except Exception as e:
            if not self._is_running or "cancel" in str(e).lower(): self.operation_cancelled_signal.emit("Delete File", f"Cancelled due to error: {e}")
            else: self.error_occurred.emit("Delete File", str(e)); self._log(f"Error deleting file {remote_file_path}: {e}", level="error")


    def _delete_directory_recursive_internal(self, remote_dir_path: str) -> bool:
        # (Code from previous corrected version, ensure path normalization)
        if not self._is_running: return False
        self._log(f"Recursively deleting content of: {remote_dir_path}", level="debug")
        try:
            # List items in the directory to be deleted
            for item_attr in self.sftp_client.listdir_attr(remote_dir_path.replace("\\", "/")):
                if not self._is_running: return False
                item_full_path = os.path.join(remote_dir_path, item_attr.filename).replace("\\", "/") # Use os.path.join for safety then normalize
                
                if stat.S_ISDIR(item_attr.st_mode):
                    if not self._delete_directory_recursive_internal(item_full_path): # Recurse
                        return False
                else: # It's a file or link
                    if not self._is_running: return False
                    self.sftp_client.remove(item_full_path) # remove for files/links
                    self._log(f"Deleted remote item: {item_full_path}", level="debug")
                if not self._is_running: return False
            
            # After all items in directory are deleted (or if it was empty)
            if not self._is_running: return False
            self.sftp_client.rmdir(remote_dir_path.replace("\\", "/")) # Remove the now-empty directory
            self._log(f"Deleted remote directory: {remote_dir_path}", level="debug")
            return True
        except Exception as e:
            self._log(f"Error during recursive delete of {remote_dir_path}: {e}", level="error")
            return False

    @pyqtSlot(str)
    def run_delete_directory(self, remote_dir_path: str):
        # (Code from previous corrected version, ensure path normalization)
        if not self._is_running: self.operation_cancelled_signal.emit("Delete Directory", "Cancelled (worker not running)."); return
        if not self.sftp_client: self.error_occurred.emit("Delete Directory", "Not connected."); return
        
        clean_remote_dir_path = remote_dir_path.replace("\\", "/")
        self._log(f"Attempting to delete remote directory (recursively): {clean_remote_dir_path}", level="info")
        
        try:
            if not self._is_running: self.operation_cancelled_signal.emit("Delete Directory", "Cancelled before stat."); return
            mode = self.sftp_client.stat(clean_remote_dir_path).st_mode
            if not stat.S_ISDIR(mode):
                self.error_occurred.emit("Delete Directory", f"'{clean_remote_dir_path}' is not a directory.")
                return
        except Exception as e_stat:
            self.error_occurred.emit("Delete Directory", f"Error accessing '{clean_remote_dir_path}': {e_stat}")
            self._log(f"Error accessing directory for delete {clean_remote_dir_path}: {e_stat}", level="error")
            return

        if self._delete_directory_recursive_internal(clean_remote_dir_path):
            if self._is_running:
                self.operation_successful.emit("Delete Directory", f"Directory '{clean_remote_dir_path}' deleted.")
            else:
                self.operation_cancelled_signal.emit("Delete Directory", "Cancelled during recursive deletion.")
        else:
            if self._is_running:
                 self.error_occurred.emit("Delete Directory", f"Failed to delete directory '{clean_remote_dir_path}' or part of its contents.")
            else:
                 self.operation_cancelled_signal.emit("Delete Directory", f"Deletion of '{clean_remote_dir_path}' was cancelled.")