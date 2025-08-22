# sftp_connection/scripts/sftp_queue_widget.py
import os
# import stat # Not used directly here
# import datetime # Not used directly here
# import logging # Not used directly here
# import traceback # Not used directly here
from typing import Optional, List, Dict, Any, Callable, Union # Some not used directly
from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTableWidget,
                             QTableWidgetItem, QAbstractItemView, QHeaderView, QProgressBar,
                             QMenu, QAction, QLabel)
from PyQt5.QtCore import Qt, pyqtSignal

from .sftp_transfer_types import TransferJob, JobStatus, TransferDirection 

def format_size_for_queue(size_bytes: Optional[Union[int, float]]) -> str: 
    if size_bytes is None or not isinstance(size_bytes, (int, float)) or size_bytes < 0: return "N/A"
    if size_bytes == 0: return "0 B" 
    if size_bytes < 1024: return f"{int(size_bytes)} B" # Ensure integer for bytes
    elif size_bytes < 1024**2: return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024**3: return f"{size_bytes/1024**2:.1f} MB"
    else: return f"{size_bytes/1024**3:.1f} GB"

class TransferQueueWidget(QWidget):
    start_queue_processing_requested = pyqtSignal()
    stop_queue_processing_requested = pyqtSignal()
    clear_successful_requested = pyqtSignal()
    remove_jobs_requested = pyqtSignal(list) # list of job_ids
    retry_jobs_requested = pyqtSignal(list)  # list of job_ids
    
    COLUMN_ID = 0; COLUMN_FILENAME = 1; COLUMN_DIRECTION = 2; COLUMN_SIZE = 3
    COLUMN_STATUS = 4; COLUMN_PROGRESS = 5; COLUMN_REMOTE = 6; COLUMN_LOCAL = 7
    COLUMN_COUNT = 8

    def __init__(self, parent=None):
        super().__init__(parent)
        self._job_widgets: Dict[int, Dict[str, Any]] = {} # {job_id: {'row': row_idx, 'progress_bar': QProgressBar}}
        self.initUI()

    def initUI(self):
        main_layout = QVBoxLayout(self); main_layout.setContentsMargins(0, 0, 0, 0)
        self.queue_table = QTableWidget(); self.queue_table.setColumnCount(self.COLUMN_COUNT)
        self.queue_table.setHorizontalHeaderLabels(["ID", "File/Folder", "Direction", "Size", "Status", "Progress", "Remote Path", "Local Path"])
        
        self.queue_table.setSelectionMode(QAbstractItemView.ExtendedSelection) 
        self.queue_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.queue_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.queue_table.verticalHeader().setVisible(False)
        self.queue_table.setAlternatingRowColors(True)
        self.queue_table.setSortingEnabled(False) # Sorting handled manually or by manager if needed

        header = self.queue_table.horizontalHeader()
        header.setSectionResizeMode(self.COLUMN_FILENAME, QHeaderView.Stretch)
        header.setSectionResizeMode(self.COLUMN_PROGRESS, QHeaderView.Interactive)
        self.queue_table.setColumnWidth(self.COLUMN_PROGRESS, 160)
        header.setSectionResizeMode(self.COLUMN_REMOTE, QHeaderView.Stretch)
        header.setSectionResizeMode(self.COLUMN_LOCAL, QHeaderView.Stretch)
        self.queue_table.setColumnHidden(self.COLUMN_ID, True) # ID is internal

        self.queue_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.queue_table.customContextMenuRequested.connect(self.show_context_menu)
        main_layout.addWidget(self.queue_table)

        controls_layout = QHBoxLayout()
        self.start_button = QPushButton("Start Queue"); self.start_button.clicked.connect(self.start_queue_processing_requested)
        self.stop_button = QPushButton("Stop Queue"); self.stop_button.clicked.connect(self.stop_queue_processing_requested); self.stop_button.setEnabled(False)
        self.clear_successful_button = QPushButton("Clear Successful"); self.clear_successful_button.clicked.connect(self.clear_successful_requested)
        self.remove_selected_button = QPushButton("Remove Selected"); self.remove_selected_button.clicked.connect(self._remove_selected_action_handler)
        controls_layout.addWidget(self.start_button); controls_layout.addWidget(self.stop_button); controls_layout.addStretch()
        controls_layout.addWidget(self.clear_successful_button); controls_layout.addWidget(self.remove_selected_button)
        main_layout.addLayout(controls_layout)
        # No need for self.setLayout(main_layout) if main_layout is passed to QWidget constructor

    def show_context_menu(self, position):
        menu = QMenu(self) # Parent menu to self
        selected_indexes = self.queue_table.selectedIndexes()
        if not selected_indexes: return

        selected_rows = sorted(list(set(index.row() for index in selected_indexes)))
        selected_job_ids_for_action: List[int] = []
        can_retry_any = False
        
        for row_idx in selected_rows:
            job_id_item = self.queue_table.item(row_idx, self.COLUMN_ID)
            if job_id_item and job_id_item.text().isdigit():
                job_id = int(job_id_item.text())
                selected_job_ids_for_action.append(job_id)
                status_item = self.queue_table.item(row_idx, self.COLUMN_STATUS)
                if status_item and status_item.text() in [JobStatus.FAILED.value, JobStatus.CANCELLED.value]:
                    can_retry_any = True
        
        if selected_job_ids_for_action:
            remove_action = menu.addAction("Remove Selected")
            remove_action.triggered.connect(lambda: self.remove_jobs_requested.emit(list(selected_job_ids_for_action))) # Ensure list is passed
        
        if can_retry_any:
            retry_action = menu.addAction("Retry Selected")
            # Filter again for retryable IDs to be absolutely sure
            retryable_ids: List[int] = []
            for row_idx in selected_rows:
                job_id_item = self.queue_table.item(row_idx, self.COLUMN_ID)
                status_item = self.queue_table.item(row_idx, self.COLUMN_STATUS)
                if job_id_item and job_id_item.text().isdigit() and \
                   status_item and status_item.text() in [JobStatus.FAILED.value, JobStatus.CANCELLED.value]:
                    retryable_ids.append(int(job_id_item.text()))
            if retryable_ids:
                retry_action.triggered.connect(lambda: self.retry_jobs_requested.emit(list(retryable_ids))) # Ensure list
            else: # Should not happen if can_retry_any is true, but defensive
                retry_action.setEnabled(False)
        
        if menu.actions(): # Only show menu if there are actions
            menu.exec_(self.queue_table.viewport().mapToGlobal(position))

    def _remove_selected_action_handler(self):
        selected_indexes = self.queue_table.selectedIndexes()
        if not selected_indexes: return
        selected_rows = sorted(list(set(index.row() for index in selected_indexes)))
        
        job_ids_to_remove: List[int] = []
        for row_idx in selected_rows:
            job_id_item = self.queue_table.item(row_idx, self.COLUMN_ID)
            if job_id_item and job_id_item.text().isdigit():
                job_ids_to_remove.append(int(job_id_item.text()))
        
        if job_ids_to_remove:
            self.remove_jobs_requested.emit(job_ids_to_remove)

    def add_job_to_display(self, job: TransferJob):
        row_position = self.queue_table.rowCount()
        self.queue_table.insertRow(row_position)
        
        self.queue_table.setItem(row_position, self.COLUMN_ID, QTableWidgetItem(str(job.id)))
        self.queue_table.setItem(row_position, self.COLUMN_FILENAME, QTableWidgetItem(job.filename))
        self.queue_table.setItem(row_position, self.COLUMN_DIRECTION, QTableWidgetItem(job.direction.value))
        self.queue_table.setItem(row_position, self.COLUMN_SIZE, QTableWidgetItem(format_size_for_queue(job.total_size)))
        self.queue_table.setItem(row_position, self.COLUMN_STATUS, QTableWidgetItem(job.status.value))
        
        progress_bar = QProgressBar()
        progress_bar.setValue(job.progress) 
        progress_bar.setTextVisible(True)
        # Initial format will be updated by update_job_in_display
        progress_bar.setFormat(f"{job.progress}%" if job.status not in [JobStatus.SCANNING, JobStatus.PENDING_RESOURCES] else job.status.value + "...")
        
        if job.status == JobStatus.SCANNING or job.status == JobStatus.PENDING_RESOURCES:
            progress_bar.setRange(0, 0) # Indeterminate
        else:
            progress_bar.setRange(0, 100)

        self.queue_table.setCellWidget(row_position, self.COLUMN_PROGRESS, progress_bar)
        self.queue_table.setItem(row_position, self.COLUMN_REMOTE, QTableWidgetItem(job.remote_path))
        self.queue_table.setItem(row_position, self.COLUMN_LOCAL, QTableWidgetItem(job.local_path))
        
        self._job_widgets[job.id] = {'row': row_position, 'progress_bar': progress_bar}
        self.queue_table.scrollToBottom()
        # Call update to ensure progress bar format is correct based on initial status
        self.update_job_in_display(job)


    def update_job_in_display(self, job: TransferJob):
        widget_info = self._job_widgets.get(job.id)
        if not widget_info: 
            # This can happen if a job update signal arrives after the job was removed from UI
            # Or if it was never added (e.g. UI error during add_job_to_display before _job_widgets was populated)
            # print(f"DEBUG: TransferQueueWidget.update_job_in_display: No widget_info for job ID {job.id}. Job status: {job.status.value}")
            return 
            
        row = widget_info['row']
        progress_bar: QProgressBar = widget_info['progress_bar'] # Type hint for clarity

        # Check if row still exists (it might have been removed)
        if row >= self.queue_table.rowCount() or self.queue_table.item(row, self.COLUMN_ID) is None or \
           int(self.queue_table.item(row, self.COLUMN_ID).text()) != job.id:
            # print(f"DEBUG: TransferQueueWidget.update_job_in_display: Row {row} for job ID {job.id} seems to be invalid or recycled. Re-finding or skipping.")
            # Attempt to re-find row by job ID if _job_widgets is out of sync (should not happen often with correct remove logic)
            new_row_found = False
            for r_idx in range(self.queue_table.rowCount()):
                id_item = self.queue_table.item(r_idx, self.COLUMN_ID)
                if id_item and id_item.text().isdigit() and int(id_item.text()) == job.id:
                    widget_info['row'] = r_idx
                    row = r_idx
                    new_row_found = True
                    break
            if not new_row_found:
                # print(f"DEBUG: TransferQueueWidget.update_job_in_display: Could not re-find row for job ID {job.id}. Skipping update.")
                return

        status_item = self.queue_table.item(row, self.COLUMN_STATUS)
        if status_item: status_item.setText(job.status.value)
        else: self.queue_table.setItem(row, self.COLUMN_STATUS, QTableWidgetItem(job.status.value))
        
        # Update tooltip for status item if there's an error message
        current_status_item = self.queue_table.item(row, self.COLUMN_STATUS) # Re-fetch in case it was just set
        if current_status_item:
            if job.status in [JobStatus.FAILED, JobStatus.CANCELLED] and job.error_message:
                current_status_item.setToolTip(job.error_message)
            else:
                current_status_item.setToolTip("") # Clear tooltip if no error or not in error state

        # Update progress bar
        clamped_progress = max(0, min(job.progress, 100))
        if job.status in [JobStatus.SCANNING, JobStatus.PENDING_RESOURCES, JobStatus.ZIPPING, JobStatus.UNZIPPING]:
            progress_bar.setRange(0, 0) # Indeterminate
            progress_bar.setFormat(job.status.value + "...")
            progress_bar.setValue(0) # For indeterminate, value is often ignored but good to set.
        elif job.status == JobStatus.IN_PROGRESS :
            progress_bar.setRange(0, 100)
            progress_bar.setValue(clamped_progress)
            size_str = format_size_for_queue(job.total_size)
            bytes_str = format_size_for_queue(job.bytes_transferred)
            if job.total_size > 0:
                progress_bar.setFormat(f"{clamped_progress}% ({bytes_str}/{size_str})")
            else: # Total size unknown or 0
                progress_bar.setFormat(f"{clamped_progress}% ({bytes_str}/N/A)")
        elif job.status == JobStatus.COMPLETED:
            progress_bar.setRange(0, 100)
            progress_bar.setValue(100)
            progress_bar.setFormat("Completed")
        elif job.status in [JobStatus.FAILED, JobStatus.CANCELLED]:
            progress_bar.setRange(0, 100) 
            progress_bar.setValue(clamped_progress) # Show last known progress
            progress_bar.setFormat(job.status.value)
        else: # QUEUED or other initial states
            progress_bar.setRange(0, 100)
            progress_bar.setValue(0)
            progress_bar.setFormat("Queued")
        
        # Update total size display
        size_item = self.queue_table.item(row, self.COLUMN_SIZE)
        new_size_str = format_size_for_queue(job.total_size)
        if size_item:
            if size_item.text() != new_size_str : size_item.setText(new_size_str)
        else: self.queue_table.setItem(row, self.COLUMN_SIZE, QTableWidgetItem(new_size_str))

        # Update remote/local paths if they could change (though not typical post-addition)
        # remote_item = self.queue_table.item(row, self.COLUMN_REMOTE)
        # if remote_item and remote_item.text() != job.remote_path: remote_item.setText(job.remote_path)
        # local_item = self.queue_table.item(row, self.COLUMN_LOCAL)
        # if local_item and local_item.text() != job.local_path: local_item.setText(job.local_path)


    def remove_job_from_display(self, job_id: int):
        widget_info = self._job_widgets.pop(job_id, None)
        if widget_info:
            row_to_remove = widget_info['row']
            # Check if the row index is still valid before attempting removal
            if 0 <= row_to_remove < self.queue_table.rowCount():
                # Verify that the job ID at that row matches, in case rows shifted unexpectedly
                id_item_at_row = self.queue_table.item(row_to_remove, self.COLUMN_ID)
                if id_item_at_row and id_item_at_row.text().isdigit() and int(id_item_at_row.text()) == job_id:
                    self.queue_table.removeRow(row_to_remove)
                    # Adjust row indices for subsequent jobs in _job_widgets
                    for j_id_key, info_val in self._job_widgets.items(): # No need to iterate on copy if only modifying values
                        if info_val['row'] > row_to_remove:
                            info_val['row'] -= 1
                else:
                    # Row mismatch - _job_widgets might be stale, need to find actual row by ID and remove
                    # This indicates a potential earlier issue with row index management.
                    # For now, we've popped from _job_widgets. If this happens, a full refresh might be safer.
                    # print(f"DEBUG: TransferQueueWidget.remove_job_from_display: Row mismatch for job ID {job_id} at cached_row {row_to_remove}. Searching...")
                    found_and_removed = False
                    for r_idx in range(self.queue_table.rowCount()):
                        id_item = self.queue_table.item(r_idx, self.COLUMN_ID)
                        if id_item and id_item.text().isdigit() and int(id_item.text()) == job_id:
                            self.queue_table.removeRow(r_idx)
                            # Re-adjust all row indices after this kind of removal
                            self._reindex_job_widgets()
                            found_and_removed = True
                            break
                    # if not found_and_removed: print(f"DEBUG: Could not find and remove job ID {job_id} after row mismatch.")
            # else: print(f"DEBUG: TransferQueueWidget.remove_job_from_display: Invalid row index {row_to_remove} for job ID {job_id}.")
        # else: print(f"DEBUG: TransferQueueWidget.remove_job_from_display: Job ID {job_id} not found in _job_widgets.")

    def _reindex_job_widgets(self):
        """Re-synchronizes the 'row' value in _job_widgets with the QTableWidget."""
        # print("DEBUG: Re-indexing job widgets...")
        current_job_ids_in_table = {}
        for r_idx in range(self.queue_table.rowCount()):
            id_item = self.queue_table.item(r_idx, self.COLUMN_ID)
            if id_item and id_item.text().isdigit():
                current_job_ids_in_table[int(id_item.text())] = r_idx
        
        # Update existing entries in _job_widgets, remove ones not in table
        for job_id_key in list(self._job_widgets.keys()): # Iterate on copy of keys
            if job_id_key in current_job_ids_in_table:
                self._job_widgets[job_id_key]['row'] = current_job_ids_in_table[job_id_key]
            else: # Job no longer in table, remove from our tracking
                # print(f"DEBUG: Re-indexing: Job ID {job_id_key} not in table, removing from _job_widgets.")
                del self._job_widgets[job_id_key]


    def clear_all_jobs_display(self):
        self.queue_table.setRowCount(0)
        self._job_widgets.clear()

    def set_processing_state(self, processing: bool):
        self.start_button.setEnabled(not processing)
        self.stop_button.setEnabled(processing)