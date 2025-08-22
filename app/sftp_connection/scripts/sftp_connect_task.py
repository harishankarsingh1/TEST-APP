# sftp_connection/scripts/sftp_connect_task.py
import logging
from typing import Dict, Any, Optional
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, QThread
import paramiko

class ConnectTaskSignals(QObject):
    connected = pyqtSignal(paramiko.SSHClient, str) # Emits the connected SSHClient instance and a success message
    error = pyqtSignal(str)                         # Emits an error message string if connection fails
    log_message = pyqtSignal(str, str)              # message, level (for internal logging of the task)

class ConnectTask(QRunnable):
    def __init__(self, connect_params: Dict[str, Any], parent_logger_name: str):
        super().__init__()
        self.connect_params = connect_params
        self.signals = ConnectTaskSignals()
        self.logger = logging.getLogger(f"{parent_logger_name}.ConnectTask.{id(self)}")
        self.setAutoDelete(True)
        self._is_cancelled = False

    def _log(self, message: str, level: str = "info"): # Default level to info for connect task
        self.signals.log_message.emit(f"[ConnectTask:{id(self)}] {message}", level)
        # Actual logging to console/file is handled by the receiver of log_message signal or root logger config

    def cancel(self):
        self._is_cancelled = True
        self._log("Cancellation requested for connection task.", "warning")

    def run(self):
        hostname_for_log = self.connect_params.get('hostname', 'UnknownHost')
        self._log(f"Attempting connection to {hostname_for_log}. Thread: {QThread.currentThreadId()}")
        
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        final_ssh_client_to_emit: Optional[paramiko.SSHClient] = None

        try:
            if self._is_cancelled:
                raise Exception("Connection cancelled before start.")

            args_for_connect = self.connect_params.copy()
            pkey_path = args_for_connect.pop("pkey_path", None)
            # Use a distinct variable name for passphrase from params to avoid conflict if 'password' is also a direct connect arg.
            passphrase_for_key = args_for_connect.pop("passphrase", None) 
            args_for_connect.pop("auth_type", None) # Not a paramiko.connect argument

            if pkey_path:
                self._log(f"Using key file: {pkey_path}", level="debug")
                pkey_obj = None
                key_types = [paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey, paramiko.DSSKey]
                last_key_exception = None
                
                for key_type_class in key_types:
                    if self._is_cancelled: raise Exception("Connection cancelled during key loading.")
                    try:
                        pkey_obj = key_type_class.from_private_key_file(
                            pkey_path,
                            password=passphrase_for_key if passphrase_for_key else None
                        )
                        self._log(f"Successfully loaded private key as {key_type_class.__name__}.", level="debug")
                        break 
                    except paramiko.ssh_exception.PasswordRequiredException:
                        self._log(f"Private key file '{pkey_path}' (tried as {key_type_class.__name__}) requires a passphrase, but none or incorrect one provided.", "error")
                        raise # Re-raise: this is a definitive failure for this attempt unless user is prompted.
                    except (IOError, paramiko.ssh_exception.SSHException) as e_key_load: # File not found, permissions, or bad key format for this type
                        last_key_exception = e_key_load
                        self._log(f"Failed to load key as {key_type_class.__name__} for {pkey_path}: {type(e_key_load).__name__} - {e_key_load}", "debug")
                        continue # Try next key type
                    except Exception as e_key_unexpected: # Catch any other unexpected key loading errors
                        last_key_exception = e_key_unexpected
                        self._log(f"Unexpected error loading key as {key_type_class.__name__} for {pkey_path}: {type(e_key_unexpected).__name__} - {e_key_unexpected}", "warning")
                        # Depending on policy, might want to 'break' or 'continue'
                        continue
                
                if not pkey_obj:
                    err_msg = f"Failed to load any supported private key type from '{pkey_path}'."
                    if last_key_exception:
                        err_msg += f" Last error: {type(last_key_exception).__name__} - {last_key_exception}"
                    raise paramiko.ssh_exception.SSHException(err_msg)
                args_for_connect["pkey"] = pkey_obj

            if self._is_cancelled:
                raise Exception("Connection cancelled before client.connect().")

            self._log(f"Connecting to {args_for_connect.get('hostname')} with params: { {k:v for k,v in args_for_connect.items() if k != 'password'} }", level="debug") # Avoid logging password
            
            # Standard paramiko connect arguments: hostname, port, username, password, pkey, key_filename (can use instead of pkey obj),
            # timeout, allow_agent, look_for_keys, compress, sock, gss_auth, gss_kex, gss_deleg_creds, gss_host, gss_trust_dns,
            # banner_timeout, auth_timeout, channel_timeout, disabled_algorithms, transport_factory
            # Remove any custom args not in this list if necessary, though connect(**kwargs) is flexible.
            
            ssh_client.connect(**args_for_connect)
            
            if self._is_cancelled: # Check immediately after connect returns
                # If cancelled, we must close the successfully opened client before raising
                if ssh_client.get_transport() and ssh_client.get_transport().is_active():
                     ssh_client.close()
                raise Exception("Connection cancelled immediately after successful connect call.")

            self._log(f"Successfully connected to {args_for_connect.get('hostname')}")
            final_ssh_client_to_emit = ssh_client # Mark for emission
            self.signals.connected.emit(ssh_client, f"Successfully connected to {args_for_connect.get('hostname')}.")

        except Exception as e:
            error_str = str(e)
            # Check for cancellation string in error or flag
            was_cancelled_by_exception_text = "cancel" in error_str.lower() or "aborted" in error_str.lower()
            
            if self._is_cancelled or was_cancelled_by_exception_text:
                 self._log(f"Connection process was cancelled or aborted: {error_str}", "warning")
                 # Don't emit error signal if it was a clean cancellation, or handle as specific cancellation signal if one existed
                 self.signals.error.emit(f"Connection cancelled: {error_str}") # Or a more generic "Connection cancelled."
            else:
                self._log(f"Connection failed for {hostname_for_log}: {type(e).__name__} - {error_str}", "error")
                # Log full traceback for unexpected errors to the task's internal logger
                self.logger.error(f"Connection task for {hostname_for_log} encountered an error.", exc_info=True) 
                self.signals.error.emit(error_str)
            
            # Ensure client is closed if it was instantiated and not passed out via 'connected' signal
            if ssh_client and final_ssh_client_to_emit is None: 
                try:
                    if ssh_client.get_transport() and ssh_client.get_transport().is_active():
                        ssh_client.close()
                except Exception as e_close:
                     self._log(f"Exception during cleanup close of SSH client: {e_close}", "debug")