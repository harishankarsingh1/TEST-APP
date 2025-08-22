# sftp_connection/scripts/sftp_adhoc_task.py
import os
import stat
import logging
from typing import Optional, Dict, Any
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, QThread # QThread for currentThreadId
import paramiko

class AdhocSFTPTaskSignals(QObject):
    finished = pyqtSignal(str, object, str)  # operation_name, result, error_message (None for success)
    log_message = pyqtSignal(str, str)       # message, level

class AdhocSFTPTask(QRunnable):
    def __init__(self, operation_name: str, ssh_client: paramiko.SSHClient, params: dict, parent_logger_name: str):
        super().__init__()
        self.operation_name = operation_name
        self.ssh_client = ssh_client
        self.params = params
        self.signals = AdhocSFTPTaskSignals()
        self.logger = logging.getLogger(f"{parent_logger_name}.AdhocTask.{self.operation_name}.T{id(self)}")
        self.setAutoDelete(True)
        self._is_cancelled = False

    def _log(self, message: str, level: str = "debug"):
        self.signals.log_message.emit(f"[{self.operation_name} Task] {message}", level)

    def cancel(self):
        self._is_cancelled = True
        self._log("Cancellation requested for ad-hoc task.", "warning")

    def run(self):
        sftp_channel: Optional[paramiko.SFTPClient] = None
        result: Any = None
        error_string_for_signal: Optional[str] = None # Explicitly None for success

        self._log(f"Starting. Thread: {QThread.currentThreadId()}", "info")

        try:
            if self._is_cancelled:
                raise Exception("Operation cancelled before start.")

            if not self.ssh_client or \
               not self.ssh_client.get_transport() or \
               not self.ssh_client.get_transport().is_active():
                raise Exception("SSH client not connected or transport not active.")

            sftp_channel = self.ssh_client.open_sftp()
            if not sftp_channel:
                raise Exception("Failed to open SFTP channel.")
            self._log("SFTP channel opened for operation.", "debug")

            if self._is_cancelled:
                raise Exception("Operation cancelled after opening SFTP channel.")

            if self.operation_name == "list_directory":
                path = self.params.get("path", "/")
                path = path.replace("\\", "/") # Ensure forward slashes for SFTP path
                self._log(f"Listing directory: {path}", "info")
                
                # It's possible listdir_attr itself returns, but iterating it fails if channel is then closed
                listing = sftp_channel.listdir_attr(path)
                items_data = []
                
                base_path_for_item_fullpath = path.rstrip('/') + '/' if path != '/' else '/'

                for attr in listing: # This iteration might fail if channel closed prematurely
                    if self._is_cancelled: raise Exception("Listing cancelled mid-operation.")
                    item_type = "Dir" if stat.S_ISDIR(attr.st_mode) else ("File" if stat.S_ISREG(attr.st_mode) else "Other")
                    item_name = attr.filename 
                    
                    # Robust path joining for SFTP (always forward slashes)
                    # If base_path is '/', avoid double slash, e.g. //file
                    if base_path_for_item_fullpath == '/':
                        full_item_path_normalized = f"/{item_name.lstrip('/')}"
                    else:
                        full_item_path_normalized = f"{base_path_for_item_fullpath}{item_name}"
                    
                    items_data.append({
                        "name": item_name,
                        "size": attr.st_size if item_type == "File" else 0,
                        "type": item_type,
                        "modified": attr.st_mtime,
                        "full_path": full_item_path_normalized
                    })
                result = {"path": path, "items_data": items_data}
                self._log(f"Listed {len(items_data)} items in {path}.", "info")

            elif self.operation_name == "create_directory":
                path = self.params.get("path")
                if not path: raise ValueError("Path parameter missing for create_directory")
                path = path.replace("\\", "/")
                self._log(f"Creating directory: {path}", "info")
                try:
                    sftp_channel.stat(path)
                    self._log(f"Directory '{path}' already exists.", "warning")
                    error_string_for_signal = f"Directory '{path}' already exists." 
                    # result remains None, or you can set a specific result for "already exists"
                except FileNotFoundError:
                    sftp_channel.mkdir(path) 
                    result = f"Directory '{path}' created successfully."
            
            elif self.operation_name == "delete_file":
                path = self.params.get("path")
                if not path: raise ValueError("Path parameter missing for delete_file")
                path = path.replace("\\", "/")
                self._log(f"Deleting file: {path}", "info")
                sftp_channel.remove(path) 
                result = f"File '{path}' deleted successfully."

            elif self.operation_name == "delete_directory":
                path = self.params.get("path")
                if not path: raise ValueError("Path parameter missing for delete_directory")
                path = path.replace("\\", "/")
                self._log(f"Deleting directory recursively: {path}", "info")
                try:
                    mode = sftp_channel.stat(path).st_mode
                    if not stat.S_ISDIR(mode):
                        raise ValueError(f"Path '{path}' is not a directory.")
                except FileNotFoundError:
                    raise FileNotFoundError(f"Directory '{path}' not found for deletion.")
                
                self._delete_dir_recursive(sftp_channel, path) # Pass original path for context
                result = f"Directory '{path}' and its contents deleted successfully."
            
            elif self.operation_name == "normalize_path":
                path_to_normalize = self.params.get("path", ".")
                if not path_to_normalize: raise ValueError("Path parameter missing for normalize_path")
                result = sftp_channel.normalize(path_to_normalize)
                self._log(f"Normalized '{path_to_normalize}' to '{result}'", "debug")

            else:
                raise ValueError(f"Unknown ad-hoc SFTP operation: {self.operation_name}")

        except Exception as e:
            exception_type_name = type(e).__name__
            current_error_str = str(e)
            
            # Log with full traceback for this task's logger
            self.logger.error(
                f"Exception in AdhocTask '{self.operation_name}'. Type: {exception_type_name}, Str: '{current_error_str}'", 
                exc_info=True # IMPORTANT: Get full traceback in this task's log
            )
            
            if self._is_cancelled: # If cancellation was requested prior to or during the error
                error_string_for_signal = f"Operation '{self.operation_name}' was cancelled. Additional error: {exception_type_name}{f' - {current_error_str}' if current_error_str else ''}"
            elif not current_error_str: # If str(e) is empty, provide a more descriptive message
                error_string_for_signal = f"{self.operation_name} failed: {exception_type_name} occurred (no further details)."
            else:
                error_string_for_signal = current_error_str # Use the original error string
        finally:
            if sftp_channel:
                try:
                    sftp_channel.close()
                    self._log("SFTP channel closed.", "debug")
                except Exception as e_close:
                    self.logger.warning(f"Error closing SFTP channel: {e_close}", exc_info=True) # Log this too
            
            # error_string_for_signal will be None on success, or a string on failure.
            self.signals.finished.emit(self.operation_name, result, error_string_for_signal)
            self._log(f"Finished ad-hoc operation '{self.operation_name}'. Success: {error_string_for_signal is None}", "debug")

    def _delete_dir_recursive(self, sftp: paramiko.SFTPClient, path: str):
        if self._is_cancelled:
            raise Exception(f"Recursive delete of '{path}' cancelled (check at start).")
        
        self._log(f"Recursively processing path for deletion: {path}", "debug")
        for item_attr in sftp.listdir_attr(path): # This could fail if channel is dead
            if self._is_cancelled:
                raise Exception(f"Recursive delete of '{path}' cancelled (during item processing).")
            
            item_name = item_attr.filename
            # Construct full path robustly
            if path == '/': item_full_path = f"/{item_name.lstrip('/')}"
            else: item_full_path = f"{path.rstrip('/')}/{item_name.lstrip('/')}"
            item_full_path = item_full_path.replace("\\", "/") # Ensure forward slashes

            if stat.S_ISDIR(item_attr.st_mode):
                self._log(f"Recursing into directory for deletion: {item_full_path}", "debug")
                self._delete_dir_recursive(sftp, item_full_path)
            else:
                self._log(f"Deleting file/item: {item_full_path}", "debug")
                sftp.remove(item_full_path)
        
        if self._is_cancelled:
            raise Exception(f"Recursive delete of '{path}' cancelled (before rmdir).")
        self._log(f"Removing now-empty directory: {path}", "debug")
        sftp.rmdir(path)