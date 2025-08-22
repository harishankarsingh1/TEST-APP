# sftp_connection/sftp_tab.py
import os
import stat
# import datetime # Not strictly needed if format_timestamp imports it locally
import logging
# import traceback # Not directly used in this file, but good for debugging elsewhere
from typing import Optional, List, Dict, Any, Callable, Union
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton,
    QComboBox, QFileDialog, QGroupBox, QTextEdit, QMessageBox, # QTextEdit not used
    QSizePolicy, QCheckBox, QTreeWidget, QTreeWidgetItem,
    QHeaderView, QSplitter, QTreeView, QFileSystemModel, QStyle,
    QAbstractItemView, QGridLayout, QTreeWidgetItemIterator, QInputDialog,
    QFrame # QFrame not explicitly used, but often a base or for styling
)
from PyQt5.QtCore import (
    Qt, QObject, pyqtSignal, QThread, QDateTime, QDir, QModelIndex, QFileInfo,
    QMetaObject, Q_ARG, QRunnable, QThreadPool, QTimer, pyqtSlot
)

import sys

try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False
    print("WARNING: paramiko library not found. SFTP functionality will be disabled. Please install it: pip install paramiko")

from .scripts.sftp_transfer_types import TransferJob, JobStatus, TransferDirection
from .scripts.sftp_queue_widget import TransferQueueWidget
from .scripts.sftp_parallel_manager import ParallelTransferManager
from .scripts.sftp_adhoc_task import AdhocSFTPTask
from .scripts.sftp_connect_task import ConnectTask


def format_size(size_bytes):
    if size_bytes is None:
        return ""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes/1024**2:.1f} MB"
    else:
        return f"{size_bytes/1024**3:.1f} GB"


def format_timestamp(ts):
    try:
        import datetime as dt # Import locally
        return dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return "N/A"


class CollapsibleGroupBox(QGroupBox):
    def __init__(self, title="", parent=None):
        super().__init__(title, parent)
        self.setCheckable(True)
        self.setChecked(True)

        self._group_box_layout = QVBoxLayout(self)
        self._group_box_layout.setContentsMargins(
            4, self.fontMetrics().height() + 4, 4, 4)
        self._group_box_layout.setSpacing(3)

        self.content_widget = QWidget()
        self._group_box_layout.addWidget(self.content_widget)

        self.toggled.connect(self._toggle_content)

    def setContentLayout(self, layout: Union[QHBoxLayout, QVBoxLayout, QGridLayout]):
        if self.content_widget.layout() is not None:
            old_layout = self.content_widget.layout()
            while old_layout.count():
                child = old_layout.takeAt(0)
                if child.widget():
                    child.widget().deleteLater()
            old_layout.deleteLater()
        self.content_widget.setLayout(layout)
        if isinstance(layout, (QHBoxLayout, QVBoxLayout, QGridLayout)):
            layout.setContentsMargins(0, 0, 0, 0)
            layout.setSpacing(3)
        self._toggle_content(self.isChecked())

    def _toggle_content(self, checked):
        self.content_widget.setVisible(checked)
        if checked:
            self.content_widget.setSizePolicy(
                QSizePolicy.Preferred, QSizePolicy.Expanding)
            self.content_widget.setMaximumHeight(16777215) # Max height allowed
        else:
            self.content_widget.setSizePolicy(
                QSizePolicy.Preferred, QSizePolicy.Fixed)
            self.content_widget.setFixedHeight(0)


class SFTPConnectionTab(QWidget):
    PATH_ROLE = Qt.UserRole + 1
    TYPE_ROLE = Qt.UserRole + 2
    DUMMY_NODE_TEXT = "..."

    def __init__(self, parent_logger: Optional[logging.Logger] = None, parent: Optional[QWidget] = None):
        super().__init__(parent)
        if isinstance(parent_logger, logging.Logger):
            self.logger = parent_logger
        else:
            if parent_logger is not None:
                print(
                    f"CRITICAL WARNING: [SFTPConnectionTab] Incorrect parent_logger type. Using default.")
            self.logger = logging.getLogger(f"SFTP_Tab_Default_{id(self)}")
            if not self.logger.handlers and (not self.logger.parent or not self.logger.parent.handlers):
                stream_handler = logging.StreamHandler(sys.stdout)
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                stream_handler.setFormatter(formatter)
                self.logger.addHandler(stream_handler)
                self.logger.setLevel(logging.DEBUG)
                self.logger.propagate = False
                self.logger.warning("SFTPConnectionTab created its own console logger as no parent_logger was provided or parent had no handlers.")
        self.logger.info(
            f"SFTPConnectionTab instance {id(self)} using logger: '{self.logger.name}'")

        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.is_connecting_flag = False
        self.is_connected_flag = False
        self.current_remote_listing_path = "/"
        self.active_adhoc_operation: Optional[str] = None
        self.current_adhoc_task_runner: Optional[QRunnable] = None
        self._active_adhoc_operation_cancelled = False # New attribute
        self.adhoc_thread_pool = QThreadPool(self)
        self.adhoc_thread_pool.setMaxThreadCount(2) # For connect, list, mkdir etc.
        
        # Assuming ParallelTransferManager.add_item_to_queue now accepts zip/unzip flags
        self.parallel_transfer_manager = ParallelTransferManager(
            main_ssh_client_getter=self._get_main_ssh_client, 
            parent_logger_name=self.logger.name, 
            max_concurrent_transfers=3, 
            parent=self
        )

        try:
            self.initUI()
        except Exception as e:
            self.logger.critical(f"CRITICAL ERROR during SFTPConnectionTab initUI: {e}", exc_info=True)
            # Fallback UI if init fails
            error_layout = QVBoxLayout(self)
            error_label = QLabel(f"Failed to initialize SFTP Tab UI: {e}\n\nPlease ensure all dependencies are correct and check application logs.")
            error_label.setStyleSheet("color: red; font-weight: bold;")
            error_label.setWordWrap(True)
            error_layout.addWidget(error_label)
            self.setLayout(error_layout)
            global PARAMIKO_AVAILABLE # Make sure to affect the global flag if init fails badly
            PARAMIKO_AVAILABLE = False # Treat as critical failure
        
        self._connect_parallel_manager_signals_to_ui()
        
        if not PARAMIKO_AVAILABLE:
            self.logger.critical(
                "paramiko library not found or critical UI initialization failed. SFTP functionality will be disabled.")
            # Attempt to gracefully disable UI components if they exist
            if hasattr(self, 'status_label'):
                self.status_label.setText("SFTP disabled (paramiko missing or UI error)")
            else: # If status_label itself didn't init, create a basic one
                if not self.layout(): QVBoxLayout(self) # Ensure a layout exists
                self.status_label = QLabel("SFTP disabled (paramiko missing or UI error)")
                if self.layout(): self.layout().addWidget(self.status_label)

            if hasattr(self, 'connect_button'): self.connect_button.setEnabled(False)
            if hasattr(self, 'file_transfer_group'): self.file_transfer_group.setEnabled(False)
            if hasattr(self, 'queue_group_collapsible'): self.queue_group_collapsible.setEnabled(False)
            # Disable new checkboxes too
            if hasattr(self, 'zip_upload_checkbox'): self.zip_upload_checkbox.setEnabled(False)
            if hasattr(self, 'unzip_download_checkbox'): self.unzip_download_checkbox.setEnabled(False)

        self._update_ui_state()
        self.logger.info(
            f"SFTPConnectionTab {id(self)} initialization complete. PARAMIKO_AVAILABLE: {PARAMIKO_AVAILABLE}")


    def _get_main_ssh_client(self) -> Optional[paramiko.SSHClient]:
        if self.ssh_client:
            transport = self.ssh_client.get_transport()
            if transport and transport.is_active():
                return self.ssh_client
            else:
                # If we think we are connected or connecting, but transport is dead, it's an error.
                if self.is_connected_flag or self.is_connecting_flag: 
                    self.logger.warning(
                        "_get_main_ssh_client: SSH client/transport found inactive unexpectedly.")
                    # Use QMetaObject.invokeMethod to ensure _handle_unexpected_disconnect runs in the main GUI thread
                    QMetaObject.invokeMethod(self, "_handle_unexpected_disconnect", Qt.QueuedConnection, 
                                             Q_ARG(str, "SSH client/transport became inactive"))
                elif not self.is_connecting_flag: # Not connecting and not connected, client should be None
                     self.logger.debug("_get_main_ssh_client: SSH client is None or transport not active (and not connecting).")
        return None

    @pyqtSlot(str)
    def _handle_unexpected_disconnect(self, reason: str):
        if not self.is_connected_flag and not self.is_connecting_flag: # Already handled or was never connected
            return
        self.logger.error(
            f"Unexpected disconnection detected: {reason}. Forcing full disconnect sequence.")
        
        # Stop queue and cancel ongoing transfers first
        if self.parallel_transfer_manager:
            self.parallel_transfer_manager.stop_queue() # Stop dispatching new tasks
            self.parallel_transfer_manager.cancel_active_transfers() # Request cancellation of running tasks
            self.parallel_transfer_manager.cancel_all_directory_scans() # Cancel scans

        # Handle ad-hoc operation if one was active
        if self.active_adhoc_operation and self.current_adhoc_task_runner:
            if hasattr(self.current_adhoc_task_runner, 'cancel'):
                self.current_adhoc_task_runner.cancel()
            self._finish_adhoc_operation(self.active_adhoc_operation, success=False,
                                         cancelled=True, message=f"Connection lost during {self.active_adhoc_operation}")
        
        if self.ssh_client:
            try:
                self.ssh_client.close()
            except Exception as e:
                self.logger.error(
                    f"Error closing SSH client during unexpected disconnect: {e}", exc_info=True)
        self.ssh_client = None
        self.is_connected_flag = False
        self.is_connecting_flag = False
        
        if hasattr(self, 'status_label'): # Check if UI elements exist
            self.status_label.setText(f"Status: Connection Lost ({reason.splitlines()[0]})")
        if hasattr(self, 'remote_fs_tree'): self.remote_fs_tree.clear()
        if hasattr(self, 'current_remote_path_display'): self.current_remote_path_display.setText("/")
        
        self._update_ui_state()
        self.logger.info("Unexpected disconnect handling complete.")


    def initUI(self):
        main_tab_layout = QVBoxLayout(self)
        main_tab_layout.setContentsMargins(5,15,2, 2)
        main_tab_layout.setSpacing(3)

        self.connection_group = QGroupBox("SFTP Connection")
        connection_grid_layout = QGridLayout(self.connection_group)
        connection_grid_layout.setSpacing(3)

        # Row 0: Host, Port, User
        connection_grid_layout.addWidget(QLabel("Host:"), 0, 0, Qt.AlignRight)
        self.host_input = QLineEdit(); self.host_input.setPlaceholderText("sftp.example.com")
        connection_grid_layout.addWidget(self.host_input, 0, 1)
        connection_grid_layout.addWidget(QLabel("Port:"), 0, 2, Qt.AlignRight)
        self.port_input = QLineEdit("22"); self.port_input.setFixedWidth(50)
        connection_grid_layout.addWidget(self.port_input, 0, 3)
        connection_grid_layout.addWidget(QLabel("User:"), 0, 4, Qt.AlignRight)
        self.username_input = QLineEdit(); self.username_input.setPlaceholderText("username")
        connection_grid_layout.addWidget(self.username_input, 0, 5)
        
        # Row 1: Auth Type, dynamic auth fields
        connection_grid_layout.addWidget(QLabel("Auth:"), 1, 0, Qt.AlignRight)
        self.auth_type_combo = QComboBox(); self.auth_type_combo.addItems(["Password", "Key File"])
        self.auth_type_combo.currentTextChanged.connect(self.on_auth_type_changed)
        connection_grid_layout.addWidget(self.auth_type_combo, 1, 1)

        self.password_label = QLabel("Password:"); connection_grid_layout.addWidget(self.password_label, 1, 2, Qt.AlignRight)
        self.password_input = QLineEdit(); self.password_input.setEchoMode(QLineEdit.Password)
        connection_grid_layout.addWidget(self.password_input, 1, 3)
        
        self.key_file_label = QLabel("Key File:"); connection_grid_layout.addWidget(self.key_file_label, 1, 2, Qt.AlignRight) # Changed col for key
        key_file_inner_layout = QHBoxLayout(); key_file_inner_layout.setContentsMargins(0,0,0,0); key_file_inner_layout.setSpacing(2)
        self.key_file_input = QLineEdit(); self.key_file_input.setPlaceholderText("path/to/key")
        key_file_inner_layout.addWidget(self.key_file_input, 1)
        self.key_file_browse_button = QPushButton("..."); self.key_file_browse_button.setFixedWidth(30)
        self.key_file_browse_button.setToolTip("Browse for key file"); self.key_file_browse_button.clicked.connect(self.browse_key_file)
        key_file_inner_layout.addWidget(self.key_file_browse_button)
        connection_grid_layout.addLayout(key_file_inner_layout, 1, 3) # Changed col for key input layout

        self.passphrase_label = QLabel("Passphrase:"); connection_grid_layout.addWidget(self.passphrase_label, 1, 4, Qt.AlignRight) # Changed col for passphrase
        self.passphrase_input = QLineEdit(); self.passphrase_input.setEchoMode(QLineEdit.Password); self.passphrase_input.setPlaceholderText("(Optional)")
        connection_grid_layout.addWidget(self.passphrase_input, 1, 5) # Changed col for passphrase input

        # Connect/Disconnect buttons on a new row or to the side
        connect_buttons_layout = QHBoxLayout()
        self.connect_button = QPushButton("Connect"); self.connect_button.setIcon(self.style().standardIcon(QStyle.SP_DialogOkButton))
        self.connect_button.clicked.connect(self.connect_sftp)
        self.disconnect_button = QPushButton("Disconnect"); self.disconnect_button.setIcon(self.style().standardIcon(QStyle.SP_DialogCancelButton))
        self.disconnect_button.clicked.connect(self.disconnect_sftp)
        connect_buttons_layout.addStretch(); connect_buttons_layout.addWidget(self.connect_button); connect_buttons_layout.addWidget(self.disconnect_button); connect_buttons_layout.addStretch()
        connection_grid_layout.addLayout(connect_buttons_layout, 2, 0, 1, 6) # Span across columns

        main_tab_layout.addWidget(self.connection_group)
        self.status_label = QLabel("Status: Disconnected")
        self.status_label.setFixedWidth(500)
        main_tab_layout.addWidget(self.status_label, 0, Qt.AlignCenter)
        self.on_auth_type_changed(self.auth_type_combo.currentText()) # Initialize visibility

        # --- Middle Section: File Browse & Management ---
        self.file_transfer_group = QGroupBox("File Browse & Management")
        file_transfer_outer_layout = QVBoxLayout(self.file_transfer_group)
        file_transfer_outer_layout.setContentsMargins(2, 5, 2, 2)
        file_transfer_outer_layout.setSpacing(3)

        self.transfer_splitter = QSplitter(Qt.Horizontal)
        # (Local FS Pane - code unchanged, assuming it's correct from user)
        local_fs_widget = QWidget(); local_fs_layout = QVBoxLayout(local_fs_widget); local_fs_layout.setContentsMargins(0,0,0,0); local_fs_layout.setSpacing(1)
        local_nav_bar_layout = QHBoxLayout(); local_nav_bar_layout.setSpacing(2); local_nav_bar_layout.addWidget(QLabel("Drive:"),0); self.local_drive_combo=QComboBox()
        drives=QDir.drives(); home_path=QDir.homePath(); current_drive_index=0
        for i,drive_info in enumerate(drives):
            drive_path=drive_info.filePath(); self.local_drive_combo.addItem(QDir.toNativeSeparators(drive_path),drive_path)
            # Try to find the drive containing home_path
            if home_path.lower().startswith(drive_path.lower().rstrip('/\\')): current_drive_index=i # Case-insensitive check for Windows
        self.local_drive_combo.setCurrentIndex(current_drive_index)
        self.local_drive_combo.currentIndexChanged.connect(self.handle_local_drive_selected)
        local_nav_bar_layout.addWidget(self.local_drive_combo,1); local_nav_bar_layout.addWidget(QLabel("Path:"),0)
        initial_local_display_path = self.local_drive_combo.itemData(current_drive_index) if self.local_drive_combo.count()>0 else QDir.homePath()
        self.current_local_path_display=QLineEdit(QDir.toNativeSeparators(initial_local_display_path)); self.current_local_path_display.setReadOnly(True); local_nav_bar_layout.addWidget(self.current_local_path_display,3)
        self.up_local_dir_button=QPushButton(); self.up_local_dir_button.setIcon(self.style().standardIcon(QStyle.SP_FileDialogToParent)); self.up_local_dir_button.setToolTip("Go to parent directory"); self.up_local_dir_button.clicked.connect(self.go_to_parent_local_directory); local_nav_bar_layout.addWidget(self.up_local_dir_button,0); local_fs_layout.addLayout(local_nav_bar_layout)
        self.local_fs_model=QFileSystemModel(self); self.local_fs_model.setRootPath(QDir.rootPath()); self.local_fs_model.setFilter(QDir.AllEntries|QDir.NoDotAndDotDot|QDir.Hidden)
        self.local_fs_tree=QTreeView(); self.local_fs_tree.setModel(self.local_fs_model); self.local_fs_tree.setRootIndex(self.local_fs_model.index(initial_local_display_path)); self.local_fs_tree.setAlternatingRowColors(True); self.local_fs_tree.setSelectionBehavior(QAbstractItemView.SelectRows); self.local_fs_tree.setAnimated(False); self.local_fs_tree.setSortingEnabled(True); self.local_fs_tree.header().setSortIndicator(0,Qt.AscendingOrder)
        self.local_fs_tree.header().setSectionResizeMode(0,QHeaderView.Stretch); self.local_fs_tree.header().setSectionResizeMode(1,QHeaderView.Interactive); self.local_fs_tree.setColumnWidth(1,70); self.local_fs_tree.header().setSectionResizeMode(2,QHeaderView.Interactive); self.local_fs_tree.setColumnWidth(2,80); self.local_fs_tree.header().setSectionResizeMode(3,QHeaderView.Interactive); self.local_fs_tree.setColumnWidth(3,130)
        self.local_fs_tree.selectionModel().selectionChanged.connect(self.handle_local_selection_changed); self.local_fs_tree.doubleClicked.connect(self.handle_local_item_double_clicked); local_fs_layout.addWidget(self.local_fs_tree); self.transfer_splitter.addWidget(local_fs_widget)

        # (Remote FS Pane - code unchanged, assuming it's correct from user)
        remote_fs_widget=QWidget(); remote_fs_layout=QVBoxLayout(remote_fs_widget); remote_fs_layout.setContentsMargins(0,0,0,0); remote_fs_layout.setSpacing(1)
        remote_nav_bar_layout=QHBoxLayout(); remote_nav_bar_layout.setSpacing(2); remote_nav_bar_layout.addWidget(QLabel("Path:"),0); self.current_remote_path_display=QLineEdit("/"); self.current_remote_path_display.setReadOnly(True); remote_nav_bar_layout.addWidget(self.current_remote_path_display,3)
        self.up_remote_dir_button=QPushButton(); self.up_remote_dir_button.setIcon(self.style().standardIcon(QStyle.SP_FileDialogToParent)); self.up_remote_dir_button.setToolTip("Go to parent directory"); self.up_remote_dir_button.clicked.connect(self.go_to_parent_remote_directory); remote_nav_bar_layout.addWidget(self.up_remote_dir_button,0)
        self.refresh_remote_dir_button=QPushButton(); self.refresh_remote_dir_button.setIcon(self.style().standardIcon(QStyle.SP_BrowserReload)); self.refresh_remote_dir_button.setToolTip("Refresh"); self.refresh_remote_dir_button.clicked.connect(self.refresh_current_remote_directory); remote_nav_bar_layout.addWidget(self.refresh_remote_dir_button,0)
        self.create_remote_dir_button=QPushButton(); self.create_remote_dir_button.setIcon(self.style().standardIcon(QStyle.SP_FileDialogNewFolder)); self.create_remote_dir_button.setToolTip("New Folder")
        if hasattr(self,'create_remote_directory_action') and callable(self.create_remote_directory_action): self.create_remote_dir_button.clicked.connect(self.create_remote_directory_action)
        else: self.logger.critical("CRITICAL: create_remote_directory_action method not found!")
        remote_nav_bar_layout.addWidget(self.create_remote_dir_button,0)
        self.delete_remote_item_button=QPushButton(); self.delete_remote_item_button.setIcon(self.style().standardIcon(QStyle.SP_TrashIcon)); self.delete_remote_item_button.setToolTip("Delete")
        if hasattr(self,'delete_remote_item_action') and callable(self.delete_remote_item_action): self.delete_remote_item_button.clicked.connect(self.delete_remote_item_action)
        else: self.logger.critical("CRITICAL: delete_remote_item_action method not found!")
        remote_nav_bar_layout.addWidget(self.delete_remote_item_button,0); remote_fs_layout.addLayout(remote_nav_bar_layout)
        self.remote_fs_tree=QTreeWidget(); self.remote_fs_tree.setHeaderLabels(["Name","Size","Type","Date Modified"]); self.remote_fs_tree.setAlternatingRowColors(True); self.remote_fs_tree.setSortingEnabled(True); self.remote_fs_tree.header().setSortIndicator(0,Qt.AscendingOrder)
        self.remote_fs_tree.header().setSectionResizeMode(0,QHeaderView.Stretch); self.remote_fs_tree.header().setSectionResizeMode(1,QHeaderView.Interactive); self.remote_fs_tree.setColumnWidth(1,70); self.remote_fs_tree.header().setSectionResizeMode(2,QHeaderView.Interactive); self.remote_fs_tree.setColumnWidth(2,60); self.remote_fs_tree.header().setSectionResizeMode(3,QHeaderView.Interactive); self.remote_fs_tree.setColumnWidth(3,130)
        self.remote_fs_tree.itemExpanded.connect(self.handle_remote_item_expanded); self.remote_fs_tree.itemDoubleClicked.connect(self.handle_remote_item_double_clicked); self.remote_fs_tree.itemSelectionChanged.connect(self.handle_remote_selection_changed); remote_fs_layout.addWidget(self.remote_fs_tree); self.transfer_splitter.addWidget(remote_fs_widget)

        self.transfer_splitter.setStretchFactor(0, 1); self.transfer_splitter.setStretchFactor(1, 1)
        QTimer.singleShot(0, lambda: self.transfer_splitter.setSizes([self.transfer_splitter.width()//2, self.transfer_splitter.width()//2]))
        file_transfer_outer_layout.addWidget(self.transfer_splitter, 1)

        # --- Transfer Options (Zip/Unzip Checkboxes) ---
        transfer_options_layout = QHBoxLayout()
        transfer_options_layout.setContentsMargins(0, 5, 0, 5) # Add some vertical margin
        
        self.zip_upload_checkbox = True #QCheckBox("Zip before upload")
        # self.zip_upload_checkbox.setToolTip("If selected, files/directories will be zipped before uploading.\nIf a directory is uploaded with this option, it becomes a single .zip file.")
        
        transfer_options_layout.addStretch()

        self.unzip_download_checkbox = True #QCheckBox("Unzip after download")
        # self.unzip_download_checkbox.setToolTip("If selected, downloaded .zip files will be automatically unzipped.\nThe original downloaded .zip file will then be deleted.")
               # --- Add to Queue Buttons ---
        add_to_queue_layout = QHBoxLayout()
        self.add_upload_to_queue_button = QPushButton("Upload")
        self.add_upload_to_queue_button.setIcon(self.style().standardIcon(QStyle.SP_ArrowUp))
        self.add_upload_to_queue_button.clicked.connect(self.add_upload_to_queue_action)
        self.add_download_to_queue_button = QPushButton("Download")
        self.add_download_to_queue_button.setIcon(self.style().standardIcon(QStyle.SP_ArrowDown))
        self.add_download_to_queue_button.clicked.connect(self.add_download_to_queue_action)
        

        # transfer_options_layout.addWidget(self.zip_upload_checkbox)
        transfer_options_layout.addWidget(self.add_upload_to_queue_button)
        # transfer_options_layout.addWidget(self.unzip_download_checkbox)
        transfer_options_layout.addWidget(self.add_download_to_queue_button)
        
        file_transfer_outer_layout.addLayout(transfer_options_layout)
        
        self.global_op_cancel_button = QPushButton("Cancel Current Operation") # More descriptive text
        self.global_op_cancel_button.clicked.connect(self.cancel_current_adhoc_operation_action)
        self.global_op_cancel_button.setVisible(False) # Initially hidden
        file_transfer_outer_layout.addWidget(self.global_op_cancel_button, 0, Qt.AlignCenter)

        main_tab_layout.addWidget(self.file_transfer_group, 3) # File browser takes more space

        # --- Bottom Section: Transfer Queue (Collapsible) ---
        self.queue_group_collapsible = CollapsibleGroupBox("Transfer Queue")
        queue_content_layout = QVBoxLayout()
        self.transfer_queue_widget = TransferQueueWidget(self)
        queue_content_layout.addWidget(self.transfer_queue_widget)
        self.queue_group_collapsible.setContentLayout(queue_content_layout)
        main_tab_layout.addWidget(self.queue_group_collapsible, 2) # Queue with more stretch

        self.setLayout(main_tab_layout)
        if hasattr(self, 'auth_type_combo'): # Ensure combo exists before accessing currentText
            self.on_auth_type_changed(self.auth_type_combo.currentText())


    def _connect_parallel_manager_signals_to_ui(self):
        if not hasattr(self, 'transfer_queue_widget') or not self.transfer_queue_widget: # Check if initialized
            self.logger.error("CRITICAL: _connect_parallel_manager_signals_to_ui - transfer_queue_widget is not initialized!")
            return
        self.logger.debug("Connecting ParallelTransferManager signals to TransferQueueWidget UI slots...")
        ptm = self.parallel_transfer_manager
        tqw = self.transfer_queue_widget
        
        ptm.job_added_to_ui.connect(tqw.add_job_to_display)
        ptm.job_updated_in_ui.connect(tqw.update_job_in_display)
        ptm.job_removed_from_ui.connect(tqw.remove_job_from_display)
        ptm.processing_state_changed.connect(tqw.set_processing_state)
        # Connect PTM's log_message to the tab's logger for unified logging
        ptm.log_message.connect(lambda msg, level: getattr(self.logger, level.lower(), self.logger.info)(f"[PTM] {msg}"))
        
        tqw.start_queue_processing_requested.connect(ptm.start_queue)
        tqw.stop_queue_processing_requested.connect(ptm.stop_queue)
        tqw.clear_successful_requested.connect(ptm.clear_successful_jobs_from_queue)
        tqw.remove_jobs_requested.connect(ptm.remove_jobs_from_queue)
        tqw.retry_jobs_requested.connect(ptm.retry_jobs_in_queue)
        self.logger.debug("ParallelTransferManager signals connected.")


    def on_auth_type_changed(self, auth_type: str):
        is_password_auth = (auth_type == "Password")
        # Ensure all relevant widgets exist before trying to set visibility
        if hasattr(self, 'password_label'): self.password_label.setVisible(is_password_auth)
        if hasattr(self, 'password_input'): self.password_input.setVisible(is_password_auth)
        
        if hasattr(self, 'key_file_label'): self.key_file_label.setVisible(not is_password_auth)
        if hasattr(self, 'key_file_input'): self.key_file_input.setVisible(not is_password_auth)
        if hasattr(self, 'key_file_browse_button'): self.key_file_browse_button.setVisible(not is_password_auth)
        if hasattr(self, 'passphrase_label'): self.passphrase_label.setVisible(not is_password_auth)
        if hasattr(self, 'passphrase_input'): self.passphrase_input.setVisible(not is_password_auth)
        self.logger.debug(f"Auth type changed to: {auth_type}. UI elements updated.")
        # Adjust grid layout if elements overlap. For now, assuming visibility handles it.
        # A better way for complex visibility changes in QGridLayout is to remove and re-add widgets,
        # or use QStackedWidget, but simple setVisible should work if positions don't clash badly.
        # Corrected layout for key/passphrase in initUI should mitigate this.


    def browse_key_file(self):
        default_dir = os.path.expanduser("~/.ssh")
        if not os.path.isdir(default_dir):
            default_dir = os.path.expanduser("~")
        
        current_key_path = self.key_file_input.text() if hasattr(self, 'key_file_input') else ""
        start_dir = os.path.dirname(current_key_path) if current_key_path and os.path.isdir(os.path.dirname(current_key_path)) else default_dir
        
        file_name, _ = QFileDialog.getOpenFileName(self, "Select Private Key File", start_dir, 
                                                   "Key Files (*.pem *.ppk *.key id_rsa id_dsa id_ecdsa id_ed25519);;All Files (*)")
        if file_name and hasattr(self, 'key_file_input'):
            self.key_file_input.setText(file_name)
            self.logger.info(f"Key file selected: {file_name}")

    def _set_connection_inputs_enabled_state(self, enabled: bool):
        # Ensure connection input widgets exist before enabling/disabling
        if not hasattr(self, 'host_input'): return # Bail if UI not fully initialized

        self.host_input.setEnabled(enabled)
        self.port_input.setEnabled(enabled)
        self.username_input.setEnabled(enabled)
        self.auth_type_combo.setEnabled(enabled)
        
        is_password_auth = (self.auth_type_combo.currentText() == "Password") if hasattr(self, 'auth_type_combo') else False
        
        if hasattr(self, 'password_input'): self.password_input.setEnabled(enabled and is_password_auth)
        if hasattr(self, 'key_file_input'): self.key_file_input.setEnabled(enabled and not is_password_auth)
        if hasattr(self, 'key_file_browse_button'): self.key_file_browse_button.setEnabled(enabled and not is_password_auth)
        if hasattr(self, 'passphrase_input'): self.passphrase_input.setEnabled(enabled and not is_password_auth)

    def _update_ui_state(self):
        # Check if essential UI elements are initialized
        if not all(hasattr(self, widget_name) for widget_name in 
                   ['connect_button', 'disconnect_button', 'file_transfer_group', 'zip_upload_checkbox', 'unzip_download_checkbox']):
            self.logger.warning("_update_ui_state: Essential UI elements not yet initialized. Skipping update.")
            return

        op_active = bool(self.active_adhoc_operation) or self.is_connecting_flag
        
        self.connect_button.setEnabled(PARAMIKO_AVAILABLE and not self.is_connected_flag and not op_active)
        self.disconnect_button.setEnabled(self.is_connected_flag or self.is_connecting_flag) # Can always attempt disconnect if connecting
        self._set_connection_inputs_enabled_state(not self.is_connected_flag and not op_active)
        
        can_do_file_ops = self.is_connected_flag and not op_active
        self.file_transfer_group.setEnabled(can_do_file_ops) # Enable entire group
        if hasattr(self, 'queue_group_collapsible'): self.queue_group_collapsible.setEnabled(True) # Queue always enabled to view/manage

        local_selected = hasattr(self, 'local_fs_tree') and self.local_fs_tree.selectionModel().hasSelection()
        remote_selected = hasattr(self, 'remote_fs_tree') and bool(self.remote_fs_tree.selectedItems())

        self.add_upload_to_queue_button.setEnabled(can_do_file_ops and local_selected)
        self.add_download_to_queue_button.setEnabled(can_do_file_ops and remote_selected)
        self.create_remote_dir_button.setEnabled(can_do_file_ops)
        self.delete_remote_item_button.setEnabled(can_do_file_ops and remote_selected)
        self.refresh_remote_dir_button.setEnabled(can_do_file_ops)
        self.up_remote_dir_button.setEnabled(can_do_file_ops and hasattr(self, 'current_remote_path_display') and self.current_remote_path_display.text() != "/")
        
        # New checkboxes enabled state
        # self.zip_upload_checkbox.setEnabled(can_do_file_ops)
        # self.unzip_download_checkbox.setEnabled(can_do_file_ops)

        if hasattr(self, 'global_op_cancel_button'):
            self.global_op_cancel_button.setVisible(op_active and self.active_adhoc_operation is not None)
            if self.global_op_cancel_button.isVisible():
                self.global_op_cancel_button.setEnabled(True) # Always enabled if visible

    def connect_sftp(self):
        self.logger.info("SFTP Connection process started by user.")
        if self.is_connecting_flag or self.active_adhoc_operation:
            QMessageBox.information(self, "Busy", "An operation is already in progress. Please wait or cancel it.")
            return
        if self.is_connected_flag:
            QMessageBox.information(self, "Already Connected", "Already connected. Please disconnect first if you wish to connect to a different server."); return
        
        host = self.host_input.text().strip()
        port_str = self.port_input.text().strip()
        username = self.username_input.text().strip()

        if not all([host, port_str, username]):
            QMessageBox.warning(self, "Input Error", "Host, Port, and Username are required.")
            return
        try:
            port = int(port_str)
            if not (0 < port < 65536): raise ValueError("Port out of range")
        except ValueError:
            QMessageBox.warning(self, "Input Error", "Invalid Port number. Must be between 1 and 65535.")
            return
        
        auth_type = self.auth_type_combo.currentText()
        auth_details = {"auth_type": auth_type} # For ConnectTask
        connect_params = {"hostname": host, "port": port, "username": username,
                          "timeout": 20, "look_for_keys": False, "allow_agent": False, "auth_type": auth_type} # For ConnectTask

        if auth_type == "Password":
            password = self.password_input.text() # No strip, password can have spaces
            # if not password: # Allow empty password if server configured for it
            #     QMessageBox.warning(self, "Input Error", "Password is required for Password authentication.")
            #     return
            auth_details["password"] = password # For older direct use, not used by ConnectTask
            connect_params["password"] = password
        elif auth_type == "Key File":
            key_file = self.key_file_input.text().strip()
            passphrase = self.passphrase_input.text() # No strip, passphrase can have spaces
            if not key_file:
                QMessageBox.warning(self, "Input Error", "Key File path is required for Key File authentication.")
                return
            if not os.path.exists(key_file): # Check if file exists
                QMessageBox.warning(self, "Input Error", f"Key File not found at the specified path: {key_file}")
                return
            auth_details["key_file"] = key_file # For older direct use
            auth_details["passphrase"] = passphrase # For older direct use
            connect_params["pkey_path"] = key_file # For ConnectTask
            connect_params["passphrase"] = passphrase # For ConnectTask

        # Check if disconnecting from a previous session is needed
        if self.ssh_client and (self.is_connected_flag or self.parallel_transfer_manager.is_queue_globally_active):
            reply = QMessageBox.question(self, "Confirm New Connection", 
                                         "This will disconnect any existing session and clear active/queued transfers. Continue?",
                                         QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply == QMessageBox.No:
                self.logger.info("Connection to new server cancelled by user.")
                return
            self.logger.info("Proceeding with new connection: disconnecting existing session and stopping queue.")
            self.disconnect_sftp() # This will stop queue, cancel tasks, close client

        self.is_connecting_flag = True
        self._start_adhoc_operation("Connect") # Updates UI for "Connect in progress..."

        self.current_adhoc_task_runner = ConnectTask(connect_params, self.logger.name)
        self.current_adhoc_task_runner.signals.connected.connect(self._handle_connect_task_success)
        self.current_adhoc_task_runner.signals.error.connect(self._handle_connect_task_error)
        self.current_adhoc_task_runner.signals.log_message.connect(lambda msg, lvl: getattr(self.logger, lvl.lower(), self.logger.info)(f"[ConnectTask] {msg}"))
        self.adhoc_thread_pool.start(self.current_adhoc_task_runner)
        self._update_ui_state() # Update UI to show "Connecting..." and disable inputs

    # ... (rest of the methods like _handle_connect_task_success, _handle_connect_task_error, disconnect_sftp, etc. remain largely the same)
    # The key changes are in add_upload_to_queue_action and add_download_to_queue_action

    def _handle_connect_task_success(self, ssh_client_instance: paramiko.SSHClient, message: str):
        if self.active_adhoc_operation != "Connect":
            self.logger.warning(f"Stale 'connected' signal. Current op: {self.active_adhoc_operation}. Closing new client.")
            if ssh_client_instance: ssh_client_instance.close()
            return
        
        self.ssh_client = ssh_client_instance
        self.is_connecting_flag = False
        self.is_connected_flag = True
        self._finish_adhoc_operation("Connect", success=True, message=message) # Pass full message
        if hasattr(self,'status_label'): self.status_label.setText(f"Status: Connected to {self.host_input.text()}")
        self.logger.info(message)

        try:
            if self.ssh_client:
                sftp_temp = self.ssh_client.open_sftp()
                self.current_remote_listing_path = sftp_temp.normalize('.')
                if not self.current_remote_listing_path.endswith('/'): self.current_remote_listing_path += '/'
                sftp_temp.close()
            else: self.current_remote_listing_path = "/" # Should not happen if connected
        except Exception as e:
            self.current_remote_listing_path = "/"
            self.logger.error(f"Error getting initial remote path after connect: {e}", exc_info=True)
        
        if hasattr(self,'current_remote_path_display'): self.current_remote_path_display.setText(self.current_remote_listing_path)
        self._trigger_adhoc_sftp_operation("list_directory", {"path": self.current_remote_listing_path})
        self._update_ui_state()


    def _handle_connect_task_error(self, error_message: str):
        if self.active_adhoc_operation != "Connect":
            self.logger.warning(f"Stale 'error' signal for connect. Current op: {self.active_adhoc_operation}")
            return
        
        # ssh_client in ConnectTask is local to it; self.ssh_client here would be from a previous session or None.
        # ConnectTask handles closing its own client on failure.
        self.is_connecting_flag = False
        self.is_connected_flag = False # Ensure this is false
        self.ssh_client = None # Ensure our reference is cleared
        
        # Use first line of error for concise status, full error in log.
        concise_error = error_message.splitlines()[0] if error_message else "Unknown connection error"
        self._finish_adhoc_operation("Connect", success=False, message=f"Connection Failed: {concise_error}")
        if hasattr(self,'status_label'): self.status_label.setText(f"Status: Connection Failed - {concise_error}")
        self.logger.error(f"Connection task failed: {error_message}") # Log full error
        self._update_ui_state()

    def disconnect_sftp(self):
        self.logger.info("SFTP Disconnect initiated by user or internal call.")
        
        if self.is_connecting_flag and self.current_adhoc_task_runner: # If a ConnectTask is running
            self.logger.info("Cancelling ongoing connection attempt due to disconnect request.")
            if hasattr(self.current_adhoc_task_runner, 'cancel'):
                self.current_adhoc_task_runner.cancel()
            # _handle_connect_task_error or success (if cancel is post-connect) will handle cleanup of that task
            # Forcing flags here:
            self.is_connecting_flag = False
            # _finish_adhoc_operation is usually called by the task's signal handler.
            # If we call it here, ensure it's idempotent or handles being called multiple times.
            # It's safer to let the task signal its end.

        # Stop queue and cancel ongoing transfers
        if self.parallel_transfer_manager:
            self.parallel_transfer_manager.stop_queue()
            self.parallel_transfer_manager.cancel_active_transfers() # Signal tasks to cancel
            self.parallel_transfer_manager.cancel_all_directory_scans() # Signal scanners to stop

        # Close the main SSH client if it exists and is active
        if self.ssh_client:
            self._start_adhoc_operation("Disconnect") # Mainly for UI feedback
            try:
                transport = self.ssh_client.get_transport()
                if transport and transport.is_active():
                    self.ssh_client.close()
                self.logger.info("SSH client closed successfully.")
            except Exception as e:
                self.logger.error(f"Error closing SSH client during disconnect: {e}", exc_info=True)
            finally:
                self.ssh_client = None # Clear our reference
                self.is_connected_flag = False # Ensure disconnected state
                self._finish_adhoc_operation("Disconnect", success=True, message="Disconnected.")
                if hasattr(self,'status_label'): self.status_label.setText("Status: Disconnected")
                if hasattr(self,'remote_fs_tree'): self.remote_fs_tree.clear()
                if hasattr(self,'current_remote_path_display'): self.current_remote_path_display.setText("/")
        else: # If no ssh_client instance, just ensure flags are correct
            self.is_connected_flag = False
            self.is_connecting_flag = False # Ensure this is also reset
            if hasattr(self,'status_label'): self.status_label.setText("Status: Disconnected")

        self._update_ui_state()
        self.logger.info("Disconnect process finished.")


    def cleanup_sftp_resources(self): # Called on tab close
        self.logger.info("SFTPConnectionTab.cleanup_sftp_resources invoked.")
        self.disconnect_sftp() # This should handle stopping PTM, cancelling tasks, closing client

        # Wait for adhoc_thread_pool (ConnectTask, AdhocSFTPTask)
        if self.adhoc_thread_pool:
            self.logger.debug("Clearing and waiting for adhoc_thread_pool to finish...")
            self.adhoc_thread_pool.clear() # Requests cancellation of QRunnables not yet started
            # For runnables already started, their cancel() method should be effective.
            if not self.adhoc_thread_pool.waitForDone(2000): # Increased timeout
                self.logger.warning("AdhocSFTPTask thread pool did not finish cleanly within the timeout.")
        
        # ParallelTransferManager's thread_pool is parented to it, should clean up with PTM.
        # PTM itself is parented to SFTPConnectionTab.
        self.logger.info("SFTPConnectionTab resources cleanup process completed.")


    def _start_adhoc_operation(self, op_name: str):
        # self.logger.debug(f"Starting adhoc operation: {op_name}") # Already logged by caller
        self.active_adhoc_operation = op_name
        if hasattr(self, 'status_label'): self.status_label.setText(f"Status: {op_name} in progress...")
        if hasattr(self, 'global_op_cancel_button'):
            self.global_op_cancel_button.setText(f"Cancel {op_name}")
            self.global_op_cancel_button.setVisible(True)
            self.global_op_cancel_button.setEnabled(True)
        self._update_ui_state()

    def _finish_adhoc_operation(self, op_name: str, success: bool = True, message: Optional[str] = None, cancelled: bool = False):
        self.logger.debug(f"Finishing adhoc operation: {op_name}, success={success}, cancelled={cancelled}, message='{message}'")
        if self.active_adhoc_operation == op_name: # Only clear if this was the current one
            self.active_adhoc_operation = None
            self.current_adhoc_task_runner = None # Clear the runner
            if hasattr(self, 'global_op_cancel_button'): self.global_op_cancel_button.setVisible(False)
            
            # Update status label based on outcome
            if hasattr(self, 'status_label'):
                status_text = f"Status: {op_name} "
                if cancelled: status_text += "cancelled."
                elif success: status_text += "successful."
                else: status_text += "failed."
                if message and (not success or cancelled): # Append error/specific message for fail/cancel
                    status_text += f" ({message.splitlines()[0]})" if message else ""
                elif message and success and op_name not in message: # Append success message if distinct
                     status_text = f"Status: {message}"


                # If it was a connect op, the connect handlers set a more specific status
                if op_name == "Connect" and not cancelled: # Let connect handlers set final "Connected to..." or "Failed"
                    pass
                elif op_name == "Disconnect" and success:
                     self.status_label.setText("Status: Disconnected")
                else:
                     self.status_label.setText(status_text)

        else:
            self.logger.warning(f"_finish_adhoc_operation called for '{op_name}', but current op is '{self.active_adhoc_operation}'. State might be inconsistent.")

        self._update_ui_state()


    def cancel_current_adhoc_operation_action(self):
        op_to_cancel = self.active_adhoc_operation
        is_currently_connecting = self.is_connecting_flag # Capture current state
        task_to_cancel = self.current_adhoc_task_runner
        self._active_adhoc_operation_cancelled = True # Mark that cancellation was user-initiated

        if op_to_cancel: 
            self.logger.info(f"User requested to cancel current adhoc operation: {op_to_cancel}")
            if task_to_cancel and hasattr(task_to_cancel, 'cancel'):
                task_to_cancel.cancel() 
                if hasattr(self, 'global_op_cancel_button'):
                    self.global_op_cancel_button.setText(f"Cancelling {op_to_cancel}...")
                    self.global_op_cancel_button.setEnabled(False) 
            else: 
                # If no task runner or it's not cancellable, finish the operation as cancelled immediately
                self._finish_adhoc_operation(op_to_cancel, success=False, cancelled=True, message=f"{op_to_cancel} cancelled by user (no active task to signal or task already ended).")
        elif is_currently_connecting: 
             self.logger.info("User requested to cancel ongoing connection attempt.")
             if task_to_cancel and hasattr(task_to_cancel, 'cancel') and isinstance(task_to_cancel, ConnectTask):
                 task_to_cancel.cancel()
             else: # No task to signal, or wrong type, so force state change
                self.is_connecting_flag = False 
                self._finish_adhoc_operation("Connect", success=False, cancelled=True, message="Connection attempt cancelled by user.")
        else:
            self.logger.info("Cancel button clicked, but no active adhoc operation or connection attempt detected.")
            if hasattr(self, 'global_op_cancel_button'): self.global_op_cancel_button.setVisible(False)
            self._active_adhoc_operation_cancelled = False # Reset if nothing to cancel



    def _trigger_adhoc_sftp_operation(self, op_name: str, params: dict):
        self._active_adhoc_operation_cancelled = False
        if self.active_adhoc_operation or self.is_connecting_flag:
            self.logger.warning(f"Cannot start adhoc operation '{op_name}': Another operation ('{self.active_adhoc_operation}' or connecting) is already in progress.")
            QMessageBox.information(self, "Busy", f"An operation ('{self.active_adhoc_operation}' or connecting) is already in progress.")
            return
        
        current_ssh_client = self._get_main_ssh_client() # Use getter to ensure active client
        if not current_ssh_client: # Handles not connected or unexpectedly disconnected
            self.logger.error(f"Cannot start adhoc operation '{op_name}': Not connected or SSH client is unavailable.")
            QMessageBox.warning(self, "Connection Error", "Not connected or SSH client is unavailable. Please connect first.")
            return
            
        self.logger.info(f"Triggering adhoc SFTP operation: '{op_name}' with params: {params}")
        self.params_for_current_adhoc_task = params # Store for potential use in handler

        self._start_adhoc_operation(op_name.replace("_", " ").title())
        self.current_adhoc_task_runner = AdhocSFTPTask(op_name, current_ssh_client, params, self.logger.name)
        self.current_adhoc_task_runner.signals.finished.connect(self._handle_adhoc_task_finished)
        self.current_adhoc_task_runner.signals.log_message.connect(lambda msg, lvl: getattr(self.logger, lvl.lower(), self.logger.info)(f"[AdhocTask:{op_name}] {msg}"))
        self.adhoc_thread_pool.start(self.current_adhoc_task_runner)
        self._update_ui_state()


    def _handle_adhoc_task_finished(self, operation_name: str, result: Any, error_message_from_signal: Optional[str]):
        op_title = operation_name.replace("_", " ").title()

        # If error_message_from_signal is an empty string "", treat it as success (like None).
        # Actual errors from AdhocSFTPTask should now be non-empty strings.
        is_truly_success = not error_message_from_signal # True if None or ""

        final_status_message_for_ui: str
        if is_truly_success:
            final_status_message_for_ui = f"{op_title} successful."
            # For list_directory, result is a dict; for others, it might be a string or None.
            if operation_name == "list_directory" and result and isinstance(result, dict) and "items_data" in result:
                pass # Success, will be handled below
            elif operation_name == "normalize_path" and isinstance(result, str):
                 final_status_message_for_ui = f"Path normalized to: {result}" # More specific
            elif result and isinstance(result, str): # For create_dir, delete_file etc.
                 final_status_message_for_ui = result


        elif self._active_adhoc_operation_cancelled: # Check if a cancellation was specifically requested for this op
            final_status_message_for_ui = error_message_from_signal if error_message_from_signal else f"{op_title} cancelled."
            self.logger.warning(f"Adhoc operation '{operation_name}' was cancelled, message: '{final_status_message_for_ui}'")
        else: # Actual failure
            final_status_message_for_ui = error_message_from_signal if error_message_from_signal else f"{op_title} failed with an unknown error."
            self.logger.error(f"Adhoc operation '{operation_name}' failed, message: '{final_status_message_for_ui}'")
        
        # _finish_adhoc_operation updates the main status label and UI state
        self._finish_adhoc_operation(op_title, 
                                     success=is_truly_success, 
                                     message=final_status_message_for_ui.splitlines()[0], # Use first line for brief status
                                     cancelled=self._active_adhoc_operation_cancelled) 
                                     # Pass explicit cancelled state
        self._active_adhoc_operation_cancelled = False # Reset flag

        # Now, specific handling for the operation's outcome
        if not is_truly_success: # An actual error occurred (error_message_from_signal was non-empty)
            # The logger.error was already done above.
            if operation_name == "list_directory":
                path_param = self.params_for_current_adhoc_task.get("path", self.current_remote_listing_path) if hasattr(
                    self, 'params_for_current_adhoc_task') else self.current_remote_listing_path
                self.handle_directory_listing_error(path_param, error_message_from_signal)
            # For other ops like mkdir/delete, the status_label update from _finish_adhoc_operation is the main feedback.
        
        else: # Truly successful (error_message_from_signal was None or empty string)
            self.logger.info(f"Adhoc operation '{operation_name}' processed as successful.")
            if operation_name == "list_directory":
                if result and isinstance(result, dict) and "items_data" in result:
                    self.handle_directory_listing_ready(result.get("path"), result.get("items_data", []))
                else: 
                    self.logger.error(f"List directory '{operation_name}' reported success but result was invalid or empty: {result}")
                    path_param = self.params_for_current_adhoc_task.get("path", self.current_remote_listing_path) if hasattr(
                        self, 'params_for_current_adhoc_task') else self.current_remote_listing_path
                    self.handle_directory_listing_error(path_param, "Operation reported success but returned no valid data.")
            
            elif operation_name == "normalize_path" and isinstance(result, str):
                self.logger.info(f"Normalized path result: {result}")
                normalized_path = result
                if not normalized_path.endswith('/') and normalized_path != "/": 
                    normalized_path += '/'
                
                # Avoid redundant listing if path hasn't effectively changed
                if self.current_remote_listing_path != normalized_path:
                    self.current_remote_listing_path = normalized_path
                    if hasattr(self,'current_remote_path_display'): self.current_remote_path_display.setText(self.current_remote_listing_path)
                    self._trigger_adhoc_sftp_operation("list_directory", {"path": self.current_remote_listing_path})
                else:
                    self.logger.debug(f"Normalized path '{normalized_path}' is same as current path. No new listing triggered.")

            elif operation_name in ["create_directory", "delete_file", "delete_directory"]:
                # Result might be a success message string from AdhocSFTPTask
                self.logger.info(str(result) if result else f"{op_title} action processed.")
                self.refresh_current_remote_directory() # Refresh view after Create, Update, Delete ops
        
        if self.active_adhoc_operation == op_title: # If _finish_adhoc_operation didn't clear it due to being called for a different op
            self.active_adhoc_operation = None # Ensure it's cleared
        self.current_adhoc_task_runner = None 
        self._update_ui_state() 

    def add_upload_to_queue_action(self):
        if not self.is_connected_flag:
            QMessageBox.warning(self, "Error", "Not connected. Please connect to an SFTP server first."); return
        
        selected_local_indexes = self.local_fs_tree.selectionModel().selectedRows()
        if not selected_local_indexes:
            QMessageBox.warning(self, "Selection Missing", "Please select a local file or directory to upload."); return
        
        local_item_path = self.local_fs_model.filePath(selected_local_indexes[0])
        remote_target_parent_dir = self.current_remote_path_display.text().strip()

        if not remote_target_parent_dir: # Should default to "/" if empty, but good check
            QMessageBox.warning(self, "Error", "Remote target directory is not specified or invalid."); return
        if not os.path.exists(local_item_path) :
             QMessageBox.warning(self, "Input Error", f"Local path does not exist:\n{local_item_path}"); return

        is_dir = os.path.isdir(local_item_path)
        # Get zipping preference from checkbox
        should_zip_this_upload = True #self.zip_upload_checkbox.isChecked() if hasattr(self, 'zip_upload_checkbox') else False

        self.logger.info(f"Queueing UPLOAD: {'DIR' if is_dir else 'FILE'} '{local_item_path}' to remote dir '{remote_target_parent_dir}'. Zip: {should_zip_this_upload}")
        
        self.parallel_transfer_manager.add_item_to_queue(
            local_path_arg=local_item_path, 
            remote_path_arg=remote_target_parent_dir, 
            direction=TransferDirection.UPLOAD, 
            is_source_directory=is_dir,
            zip_this_upload=should_zip_this_upload, # Pass the flag
            unzip_this_download=False # Not applicable for upload
        )

    def add_download_to_queue_action(self):
        if not self.is_connected_flag:
            QMessageBox.warning(self, "Error", "Not connected. Please connect to an SFTP server first."); return
            
        selected_remote_items = self.remote_fs_tree.selectedItems()
        if not selected_remote_items:
            QMessageBox.warning(self, "Selection Missing", "Please select a remote file or directory to download."); return
        
        selected_item_widget = selected_remote_items[0]
        remote_item_path = selected_item_widget.data(0, self.PATH_ROLE)
        item_type_role = selected_item_widget.data(0, self.TYPE_ROLE)
        
        # Determine local target directory from QFileSystemModel's current root
        current_local_fs_root_path = self.local_fs_model.filePath(self.local_fs_tree.rootIndex())
        local_target_parent_dir = current_local_fs_root_path if current_local_fs_root_path else QDir.homePath()


        if not remote_item_path or remote_item_path == self.DUMMY_NODE_TEXT or remote_item_path == "/":
            QMessageBox.warning(self, "Invalid Selection", "Cannot queue download for root directory or placeholder items.")
            return
        if not local_target_parent_dir or not os.path.isdir(local_target_parent_dir):
            QMessageBox.warning(self, "Local Path Error", f"Invalid local target directory: {local_target_parent_dir}")
            return

        is_remote_dir = (item_type_role == "Dir")
        # Get unzipping preference from checkbox
        should_unzip_this_download = True #self.unzip_download_checkbox.isChecked() if hasattr(self, 'unzip_download_checkbox') else False

        self.logger.info(f"Queueing DOWNLOAD: {'DIR' if is_remote_dir else 'FILE'} '{remote_item_path}' to local dir '{local_target_parent_dir}'. Unzip: {should_unzip_this_download}")
        
        self.parallel_transfer_manager.add_item_to_queue(
            local_path_arg=local_target_parent_dir, 
            remote_path_arg=remote_item_path, 
            direction=TransferDirection.DOWNLOAD, 
            is_source_directory=is_remote_dir,
            zip_this_upload=False, # Not applicable for download
            unzip_this_download=should_unzip_this_download # Pass the flag
        )

    # ... (Local and Remote Tree Handlers: handle_local_selection_changed, handle_local_item_double_clicked, etc.)
    # These methods (handle_local_*, handle_remote_*, refresh_current_remote_directory, go_to_parent_remote_directory,
    # create_remote_directory_action, delete_remote_item_action, handle_directory_listing_ready, handle_directory_listing_error)
    # seem largely okay from previous review and are mostly related to Browse and adhoc ops, not directly queueing with zip.
    # I will keep them as they were in the user's provided code. The key changes were in initUI and add_..._to_queue actions.

    def handle_local_selection_changed(self, selected, deselected): # Keep as is
        self._update_ui_state()

    def handle_local_item_double_clicked(self, index: QModelIndex): # Keep as is
        if self.local_fs_model.isDir(index):
            path = self.local_fs_model.filePath(index)
            self.local_fs_tree.setRootIndex(index)
            if hasattr(self,'current_local_path_display'): self.current_local_path_display.setText(QDir.toNativeSeparators(path))
        self._update_ui_state()

    def go_to_parent_local_directory(self): # Keep as is
        current_root_idx = self.local_fs_tree.rootIndex()
        current_root_path = self.local_fs_model.filePath(current_root_idx)
        parent_dir = QDir(current_root_path)
        if parent_dir.cdUp():
            new_parent_path_str = parent_dir.path()
            self.local_fs_tree.setRootIndex(self.local_fs_model.index(new_parent_path_str))
            if hasattr(self,'current_local_path_display'): self.current_local_path_display.setText(QDir.toNativeSeparators(new_parent_path_str))
        else:
            self.logger.debug(f"Cannot go up from local directory: {current_root_path}")
        self._update_ui_state()

    def handle_local_drive_selected(self, index: int): # Keep as is
        drive_path = self.local_drive_combo.itemData(index)
        if drive_path:
            self.local_fs_tree.setRootIndex(self.local_fs_model.index(drive_path))
            if hasattr(self,'current_local_path_display'): self.current_local_path_display.setText(QDir.toNativeSeparators(drive_path))
        self._update_ui_state()

    def handle_remote_item_expanded(self, item: QTreeWidgetItem): # Keep as is
        item_path = item.data(0, self.PATH_ROLE)
        item_type = item.data(0, self.TYPE_ROLE)
        if item_type == "Dir" and item_path and item.childCount() == 1 and \
           (item.child(0).text(0) == self.DUMMY_NODE_TEXT or self.DUMMY_NODE_TEXT in item.child(0).text(0)):
            self._trigger_adhoc_sftp_operation("list_directory", {"path": item_path})

    def handle_remote_item_double_clicked(self, item: QTreeWidgetItem, column: int): # Keep as is
        item_path = item.data(0, self.PATH_ROLE)
        item_type = item.data(0, self.TYPE_ROLE)
        if not item_path or item_path == self.DUMMY_NODE_TEXT: return
        if item_type == "Dir":
            self.current_remote_listing_path = item_path
            if not self.current_remote_listing_path.endswith('/') and self.current_remote_listing_path != "/":
                self.current_remote_listing_path += '/'
            if hasattr(self,'current_remote_path_display'): self.current_remote_path_display.setText(self.current_remote_listing_path)
            self._trigger_adhoc_sftp_operation("list_directory", {"path": self.current_remote_listing_path})
        self._update_ui_state()

    def handle_remote_selection_changed(self): # Keep as is
        self._update_ui_state()

    def refresh_current_remote_directory(self): # Keep as is
        if hasattr(self,'current_remote_path_display'):
            self._trigger_adhoc_sftp_operation("list_directory", {"path": self.current_remote_path_display.text().strip()})

    def go_to_parent_remote_directory(self): # Keep as is
        if hasattr(self,'current_remote_path_display'):
            current_path = self.current_remote_path_display.text().strip()
            if not current_path or current_path == "/": return
            parent_path_request = os.path.join(current_path, "..").replace("\\", "/")
            self._trigger_adhoc_sftp_operation("normalize_path", {"path": parent_path_request})

    def create_remote_directory_action(self): # Keep as is
        dir_name, ok = QInputDialog.getText(self, "Create Remote Directory", "Enter directory name:")
        if ok and dir_name:
            dir_name = dir_name.strip()
            if not dir_name or "/" in dir_name or "\\" in dir_name:
                QMessageBox.warning(self, "Invalid Name", "Invalid directory name. It cannot be empty or contain slashes."); return
            current_remote_base = self.current_remote_path_display.text().strip()
            if not current_remote_base.endswith('/') and current_remote_base != "/": current_remote_base += '/'
            elif current_remote_base == "//": current_remote_base = "/" # Sanitize double slash at root
            full_path = current_remote_base + dir_name
            self._trigger_adhoc_sftp_operation("create_directory", {"path": full_path})

    def delete_remote_item_action(self): # Keep as is
        selected_remote_items = self.remote_fs_tree.selectedItems()
        if not selected_remote_items: QMessageBox.warning(self, "Selection Missing", "Please select a remote file or directory to delete."); return
        selected_item_widget = selected_remote_items[0]
        remote_item_path = selected_item_widget.data(0, self.PATH_ROLE)
        if not remote_item_path or remote_item_path == "/" or remote_item_path == self.DUMMY_NODE_TEXT:
            QMessageBox.warning(self, "Invalid Selection", "Cannot delete root directory or placeholder items."); return
        item_type = selected_item_widget.data(0, self.TYPE_ROLE)
        op_name = "delete_directory" if item_type == "Dir" else "delete_file"
        confirm_msg = f"Are you sure you want to delete '{selected_item_widget.text(0)}'?"
        if item_type == "Dir": confirm_msg += "\n\nWARNING: This will delete the directory AND ALL ITS CONTENTS recursively."
        if QMessageBox.question(self, "Confirm Delete", confirm_msg, QMessageBox.Yes | QMessageBox.No, QMessageBox.No) == QMessageBox.Yes:
            self._trigger_adhoc_sftp_operation(op_name, {"path": remote_item_path})


    def handle_directory_listing_ready(self, path: str, items_data: list): # Keep as is
        self.logger.info(f"UI Update: Directory listing ready for '{path}' with {len(items_data)} items.")
        target_item_for_population = None
        # Normalize paths for comparison (remove trailing slash unless it's just "/")
        norm_path_arg = path.rstrip('/') if path != '/' else '/'
        norm_current_listing_path = self.current_remote_listing_path.rstrip('/') if self.current_remote_listing_path != '/' else '/'

        if norm_path_arg == norm_current_listing_path: # Listing for the current root view
            self.remote_fs_tree.clear()
            target_item_for_population = self.remote_fs_tree.invisibleRootItem()
        else: # Listing for an expanded subdirectory
            iterator = QTreeWidgetItemIterator(self.remote_fs_tree, QTreeWidgetItemIterator.All)
            while iterator.value():
                current_tree_item = iterator.value()
                item_path_data = current_tree_item.data(0, self.PATH_ROLE)
                if item_path_data:
                    norm_item_path_data = item_path_data.rstrip('/') if item_path_data != '/' else '/'
                    if norm_item_path_data == norm_path_arg and current_tree_item.data(0, self.TYPE_ROLE) == "Dir":
                        target_item_for_population = current_tree_item
                        break
                iterator += 1
            
            if target_item_for_population: # Clear existing children (e.g., dummy node)
                while target_item_for_population.childCount() > 0:
                    target_item_for_population.removeChild(target_item_for_population.child(0))
            else: # Should not happen if expansion triggered it, but as a fallback, refresh root
                self.logger.warning(f"Could not find parent item for sub-listing '{path}'. Refreshing root view with this listing.")
                self.remote_fs_tree.clear()
                target_item_for_population = self.remote_fs_tree.invisibleRootItem()
                self.current_remote_listing_path = path # Update current path to what we are actually showing
                if hasattr(self,'current_remote_path_display'): self.current_remote_path_display.setText(path)

        if target_item_for_population: # Ensure we have a valid parent to add to
            items_data.sort(key=lambda x: (x["type"] != "Dir", x["name"].lower())) # Dirs first, then by name
            for item_data in items_data:
                tree_item = QTreeWidgetItem(target_item_for_population)
                tree_item.setText(0, item_data["name"])
                tree_item.setText(1, format_size(item_data["size"]) if item_data["type"] == "File" else "")
                tree_item.setText(2, item_data["type"])
                tree_item.setText(3, format_timestamp(item_data["modified"]))
                tree_item.setData(0, self.PATH_ROLE, item_data["full_path"])
                tree_item.setData(0, self.TYPE_ROLE, item_data["type"])
                
                icon_to_set = QStyle.SP_FileLinkIcon # Default for "Other"
                if item_data["type"] == "Dir":
                    icon_to_set = QStyle.SP_DirIcon
                    QTreeWidgetItem(tree_item).setText(0, self.DUMMY_NODE_TEXT) # Add dummy for expansion
                elif item_data["type"] == "File":
                    icon_to_set = QStyle.SP_FileIcon
                tree_item.setIcon(0, self.style().standardIcon(icon_to_set))
        
        # Update current path display only if the listing was for the main view
        if norm_path_arg == norm_current_listing_path or (target_item_for_population == self.remote_fs_tree.invisibleRootItem()):
             if hasattr(self,'current_remote_path_display'): self.current_remote_path_display.setText(path)
             self.current_remote_listing_path = path # Update master current path

        if hasattr(self,'remote_fs_tree'): self.remote_fs_tree.header().setSortIndicator(0, Qt.AscendingOrder) # Re-apply sort

    def handle_directory_listing_error(self, path: str, error_message: str): # Keep as is
        self.logger.error(f"UI Update: Error listing directory '{path}': {error_message}")
        iterator = QTreeWidgetItemIterator(self.remote_fs_tree, QTreeWidgetItemIterator.All)
        while iterator.value():
            item = iterator.value()
            item_path_data = item.data(0, self.PATH_ROLE)
            if item_path_data:
                norm_item_path = item_path_data.rstrip('/') if item_path_data != '/' else '/'
                norm_path_arg = path.rstrip('/') if path != '/' else '/'
                if norm_item_path == norm_path_arg and item.data(0, self.TYPE_ROLE) == "Dir":
                    item.setExpanded(False) # Collapse it
                    # Remove existing children (like old dummy node)
                    while item.childCount() > 0:
                        item.removeChild(item.child(0))
                    # Add an error placeholder
                    error_child = QTreeWidgetItem(item)
                    error_child.setText(0, f"{self.DUMMY_NODE_TEXT} (Error: Click to retry)")
                    error_child.setForeground(0, Qt.red) # Make error visible
                    break # Found and updated the item
            iterator += 1
        # Update status bar as well
        if hasattr(self,'status_label'): self.status_label.setText(f"Status: Error listing {path}")


    def closeEvent(self, event): # Keep as is
        self.logger.info("SFTPConnectionTab.closeEvent received. Initiating resource cleanup.")
        self.cleanup_sftp_resources()
        super().closeEvent(event)