import os
import traceback
import logging
# import ast # For ast.literal_eval
# import sqlite3 
import pandas as pd

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox,
    QLabel, QLineEdit, QFileDialog,
    QSizePolicy, QProgressBar, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QComboBox,
    QAbstractItemView, QMessageBox, QHeaderView,
    QApplication
)
from PyQt5.QtCore import pyqtSignal, Qt, QThread, QUrl, pyqtSlot
from PyQt5.QtGui import QDesktopServices


from .scripts.db_comparator_worker import DbComparatorWorker, ComparisonParameters #, DbComparatorWorkerSignals
from .scripts.diff_viewer_worker import DiffViewerWorker, DiffViewerTaskType #, DiffViewerWorkerSignals
from utils.create_button import createButton 

logger = logging.getLogger(__name__)

class ComparisonSetupWidget(QWidget):
    compare_button_clicked = pyqtSignal(object) 
    cancel_button_clicked = pyqtSignal()
    open_diff_db_button_clicked = pyqtSignal()
    output_path_status_changed = pyqtSignal(str, bool) 

    def __init__(self, parent_logger=None, parent_widget=None):
        super().__init__(parent_widget)
        self.parent_logger = parent_logger 
        self.logger = logging.getLogger(f"{__name__}.ComparisonSetupWidget") 
        self._init_ui()
        self.output_db_path_edit.textChanged.connect(self._check_output_path_and_emit_status)
        self._check_output_path_and_emit_status() 

    def _log(self, message, level="info"): 
        current_logger = self.logger if self.logger else self.parent_logger
        log_message = f"ComparisonSetupWidget: {message}"
        if current_logger:
            log_func = getattr(current_logger, level, current_logger.info)
            log_func(log_message)
        else:
            print(f"LOG FALLBACK [{level.upper()}]: {log_message}")


    def _init_ui(self):
        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(0,0,0,0)
        outer_layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)

        group_box = QGroupBox("Database Comparison Setup")
        main_layout = QVBoxLayout(group_box)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)

        # label_width = 140  
        # button_width = 100 
        # path_edit_min_width = 200 

        def create_form_row(label_text, line_edit_widget, browse_button_widget):
            label = QLabel(label_text)            
            row_layout = QHBoxLayout()
            row_layout.setSpacing(10)
            row_layout.addWidget(label)
            row_layout.addWidget(line_edit_widget) 
            row_layout.addWidget(browse_button_widget)
            row_layout.addStretch(1) # Add stretch at the end of the row
            return row_layout

        self.db1_path_edit = QLineEdit()
        self.db1_browse_button = createButton("Browse...", lambda: self._browse_file(self.db1_path_edit, "Select Database 1"))
        main_layout.addLayout(create_form_row("Database 1 Path:", self.db1_path_edit, self.db1_browse_button))

        self.db2_path_edit = QLineEdit()
        self.db2_browse_button = createButton("Browse...", lambda: self._browse_file(self.db2_path_edit, "Select Database 2"))
        main_layout.addLayout(create_form_row("Database 2 Path:", self.db2_path_edit, self.db2_browse_button))

        self.output_db_path_edit = QLineEdit()
        self.output_db_browse_button = createButton("Save As...", self._browse_save_file)
        main_layout.addLayout(create_form_row("Output Path:", self.output_db_path_edit, self.output_db_browse_button))
        
        outer_layout.addWidget(group_box) 

        self.compare_button = createButton("Compare", self._on_compare_clicked)
        self.cancel_button = createButton("Cancel", self.cancel_button_clicked.emit)
        self.cancel_button.setEnabled(False)
        self.open_diff_db_button = createButton("Open Diff DB", self.open_diff_db_button_clicked.emit)
        self.open_diff_db_button.setEnabled(False) 

        action_button_layout = QHBoxLayout()
        action_button_layout.setAlignment(Qt.AlignLeft)
        action_button_layout.setSpacing(10)
        action_button_layout.addWidget(self.compare_button)
        action_button_layout.addWidget(self.cancel_button)
        action_button_layout.addWidget(self.open_diff_db_button)
        action_button_layout.addStretch(1) 
        main_layout.addLayout(action_button_layout) 

        self.progress_bar = QProgressBar(self)
        self.progress_bar.setRange(0, 0) 
        self.progress_bar.setVisible(False)
        # self.progress_bar.setFixedHeight(15) 
        main_layout.addWidget(self.progress_bar)
        
        self._update_default_output_filename()
        self.db1_path_edit.textChanged.connect(self._update_default_output_filename_if_default_dir)
        self.db2_path_edit.textChanged.connect(self._update_default_output_filename_if_default_dir)


    def _on_compare_clicked(self):
        params = self.get_comparison_parameters()
        if params:
            self.compare_button_clicked.emit(params)
        else:
            self._log("Comparison parameters are not valid. 'Compare' click ignored.", "warning")
            QMessageBox.warning(self, "Input Error", "Please ensure all database paths are set and valid.")


    def _check_output_path_and_emit_status(self):
        path = self.get_output_db_path()
        is_valid = bool(path and os.path.exists(path) and os.path.isfile(path)) 
        self.open_diff_db_button.setEnabled(is_valid)
        self.output_path_status_changed.emit(path, is_valid)

    def _update_default_output_filename_if_default_dir(self):
        current_output_path = self.output_db_path_edit.text()
        current_output_dir = os.path.dirname(current_output_path) if current_output_path else ""
        default_dir_base = os.path.join(os.path.expanduser("~"), "BatchFileTester_Diffs") 
        
        if not current_output_dir or current_output_dir == default_dir_base or not os.path.basename(current_output_path):
            self._update_default_output_filename()


    def _update_default_output_filename(self):
        db1_text = self.db1_path_edit.text().strip()
        db2_text = self.db2_path_edit.text().strip()
        
        db1_name = os.path.splitext(os.path.basename(db1_text))[0] if db1_text else "db1"
        db2_name = os.path.splitext(os.path.basename(db2_text))[0] if db2_text else "db2"
        
        new_filename = f"{db1_name}_vs_{db2_name}_diff.db"
        if db1_name == "db1" and db2_name == "db2": 
            new_filename = "comparison_diff_output.db"

        current_output_text = self.output_db_path_edit.text()
        output_dir_candidate = os.path.dirname(current_output_text) if current_output_text else ""
        
        default_output_dir_base = os.path.join(os.path.expanduser("~"), "BatchFileTester_Diffs")
        
        target_dir = default_output_dir_base
        if output_dir_candidate and os.path.isdir(output_dir_candidate) and output_dir_candidate != default_output_dir_base:
            target_dir = output_dir_candidate
        
        if not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir, exist_ok=True)
            except OSError as e:
                self._log(f"Error creating output directory {target_dir}: {e}", "error")
                target_dir = os.path.expanduser("~") 

        self.output_db_path_edit.setText(os.path.join(target_dir, new_filename))


    def _browse_file(self, line_edit_widget, title="Select SQLite Database File"):
        initial_dir = os.path.dirname(line_edit_widget.text())
        if not initial_dir or not os.path.isdir(initial_dir):
            other_paths = [self.db1_path_edit.text(), self.db2_path_edit.text(), self.output_db_path_edit.text()]
            for p_text in other_paths:
                p = p_text.strip()
                if p and os.path.isdir(os.path.dirname(p)):
                    initial_dir = os.path.dirname(p)
                    break
            else: 
                initial_dir = os.path.expanduser("~")
        
        file_path, _ = QFileDialog.getOpenFileName(self, title, initial_dir, "SQLite Database Files (*.db *.sqlite *.sqlite3);;All Files (*)")
        if file_path:
            line_edit_widget.setText(file_path)


    def _browse_save_file(self):
        current_output_path_text = self.output_db_path_edit.text().strip()
        initial_dir_for_dialog = os.path.dirname(current_output_path_text)
        suggested_filename = os.path.basename(current_output_path_text)

        if not initial_dir_for_dialog or not os.path.isdir(initial_dir_for_dialog):
            initial_dir_for_dialog = os.path.join(os.path.expanduser("~"), "BatchFileTester_Diffs")
        if not suggested_filename: 
             db1_text = self.db1_path_edit.text().strip(); db2_text = self.db2_path_edit.text().strip()
             db1_name = os.path.splitext(os.path.basename(db1_text))[0] if db1_text else "db1"
             db2_name = os.path.splitext(os.path.basename(db2_text))[0] if db2_text else "db2"
             suggested_filename = f"{db1_name}_vs_{db2_name}_diff.db"
             if db1_name == "db1" and db2_name == "db2": suggested_filename = "comparison_diff_output.db"


        if not os.path.exists(initial_dir_for_dialog):
            try:
                os.makedirs(initial_dir_for_dialog, exist_ok=True)
            except OSError as e:
                self._log(f"Error creating directory {initial_dir_for_dialog}: {e}", "error")
                initial_dir_for_dialog = os.path.expanduser("~") 

        initial_path_for_dialog = os.path.join(initial_dir_for_dialog, suggested_filename)
        
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Output Diff Database As", initial_path_for_dialog, "SQLite Database Files (*.db *.sqlite *.sqlite3);;All Files (*)")
        if file_path:
            self.output_db_path_edit.setText(file_path)


    def get_paths(self):
        return {
            "db1_path": self.db1_path_edit.text().strip(), 
            "db2_path": self.db2_path_edit.text().strip(), 
            "output_db_path": self.output_db_path_edit.text().strip()
        }

    def get_output_db_path(self) -> str:
        return self.output_db_path_edit.text().strip()

    def get_comparison_parameters(self):
        paths = self.get_paths()
        db1, db2, output_db = paths["db1_path"], paths["db2_path"], paths["output_db_path"]
        if not (db1 and db2 and output_db):
            self._log("All database paths must be set.", "error")
            return None
        if not os.path.exists(db1):
            self._log(f"Database 1 path does not exist: {db1}", "error")
            return None
        if not os.path.exists(db2):
            self._log(f"Database 2 path does not exist: {db2}", "error")
            return None
        return ComparisonParameters(db1_path=db1, db2_path=db2, output_db_path=output_db)

    def set_ui_for_comparison_start(self):
        self.db1_path_edit.setEnabled(False)
        self.db2_path_edit.setEnabled(False)
        self.output_db_path_edit.setEnabled(False)
        self.db1_browse_button.setEnabled(False)
        self.db2_browse_button.setEnabled(False)
        self.output_db_browse_button.setEnabled(False)
        self.compare_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self.open_diff_db_button.setEnabled(False) 
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0,0) 

    def set_ui_for_comparison_end(self, diffs_saved_successfully: bool):
        self.db1_path_edit.setEnabled(True)
        self.db2_path_edit.setEnabled(True)
        self.output_db_path_edit.setEnabled(True)
        self.db1_browse_button.setEnabled(True)
        self.db2_browse_button.setEnabled(True)
        self.output_db_browse_button.setEnabled(True)
        self.compare_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0,100) 
        self.progress_bar.setValue(0)
        self._check_output_path_and_emit_status() 


class ComparisonTabMainWidget(QWidget):
    ROWS_PER_PAGE = 50
    MAX_COLUMN_WIDTH_DEFAULT = 350
    MAX_ROW_HEIGHT_DEFAULT = 100

    def __init__(self, parent_logger=None, parent=None):
        super().__init__(parent)
        self.parent_logger = parent_logger 
        self.logger = logging.getLogger(f"{__name__}.ComparisonTabMainWidget")
        self._init_attributes()
        self._init_ui()
        self.log_message("ComparisonTabMainWidget initialized.", "debug")

    def _init_attributes(self):
        self.db_thread = None; self.db_worker = None
        self.diff_view_thread = None; self.diff_view_worker = None
        self.is_individual_diff_task_running = False
        self.last_successful_output_path = None
        
        self.current_selected_diff_table_name = None
        self.current_diff_table_page = 0
        self.total_rows_in_current_diff_table = 0
        self.current_diff_table_column_names = []
        
        self.chain_active_for_table = None
        self.chain_next_task_type = None
        self.chain_page_number_for_get_page_data = 0

    def log_message(self, message, level="info"):
        current_logger = self.logger if self.logger else self.parent_logger
        log_msg_prefix = "DBComparisonTab: "
        if current_logger:
            log_func = getattr(current_logger, level, current_logger.info)
            log_func(f"{log_msg_prefix}{message}")
        else:
            print(f"LOG FALLBACK [{level.upper()}]: {log_msg_prefix}{message}")

    def _reset_full_diff_view_components(self, context=""):
        self.log_message(f"Resetting FULL diff view components (context='{context}')", "debug")
        self.diff_table_selector_combo.blockSignals(True)
        self.diff_table_selector_combo.clear()
        self.diff_table_selector_combo.blockSignals(False)
        
        self._clear_diff_data_display_components(context=f"full_reset_from_{context}")
        
        self.current_selected_diff_table_name = None
        self.chain_active_for_table = None
        self.chain_next_task_type = None

    def _clear_diff_data_display_components(self, context=""):
        self.log_message(f"Clearing diff DATA display components. Current table context: '{self.current_selected_diff_table_name}' (Caller context='{context}')", "debug")
        
        self.diff_data_tablewidget.setRowCount(0)
        self.diff_data_tablewidget.setColumnCount(0) 
        self.current_diff_table_column_names = []
        self.total_rows_in_current_diff_table = 0
        self.current_diff_table_page = 0
        
        self._update_pagination_controls()

    def _init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        main_layout.setContentsMargins(10,10,10,10)

        self.comparison_setup_panel = ComparisonSetupWidget(parent_logger=self.parent_logger, parent_widget=self)
        main_layout.addWidget(self.comparison_setup_panel)

        self.comparison_setup_panel.compare_button_clicked.connect(self.start_db_comparison)
        self.comparison_setup_panel.cancel_button_clicked.connect(self.request_db_comparison_cancel)
        self.comparison_setup_panel.open_diff_db_button_clicked.connect(self.open_diff_db_file_from_setup_panel)
        self.comparison_setup_panel.output_path_status_changed.connect(self._handle_output_path_update_from_setup_panel)

        self.view_diff_group_box = QGroupBox("View Differences")
        view_diff_layout = QVBoxLayout(self.view_diff_group_box)

        # Top controls: Table selector and Load/Refresh button
        selector_and_export_layout = QHBoxLayout() 
        selector_and_export_layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        
        selector_label = QLabel("Select Diff Table:"); 
        self.diff_table_selector_combo = QComboBox()
        self.diff_table_selector_combo.setFixedWidth(400)
        self.load_diff_table_button = createButton("Refresh Table", self.trigger_load_current_diff_table_data)
        self.export_table_button = createButton("Export", self._trigger_export_current_table_to_excel) 
        self.export_table_button.setEnabled(False) 

        selector_and_export_layout.addWidget(selector_label)
        selector_and_export_layout.addWidget(self.diff_table_selector_combo) 
        selector_and_export_layout.addWidget(self.load_diff_table_button)
        selector_and_export_layout.addWidget(self.export_table_button) 
        selector_and_export_layout.addStretch(1) 
        view_diff_layout.addLayout(selector_and_export_layout)


        self.diff_data_tablewidget = QTableWidget()
        self.diff_data_tablewidget.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.diff_data_tablewidget.setAlternatingRowColors(True)
        self.diff_data_tablewidget.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.diff_data_tablewidget.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        self.diff_data_tablewidget.horizontalHeader().setMaximumSectionSize(self.MAX_COLUMN_WIDTH_DEFAULT)
        self.diff_data_tablewidget.verticalHeader().setDefaultSectionSize(30) 
        self.diff_data_tablewidget.verticalHeader().setMaximumSectionSize(self.MAX_ROW_HEIGHT_DEFAULT)
        self.diff_data_tablewidget.setWordWrap(True) 
        self.diff_data_tablewidget.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents) 
        self.diff_data_tablewidget.verticalHeader().setMinimumSectionSize(45) 


        view_diff_layout.addWidget(self.diff_data_tablewidget, 1)

        pagination_layout = QHBoxLayout()
        self.prev_page_button = createButton("Previous Page", self.load_prev_diff_page)
        self.page_info_label = QLabel("Page 0 of 0 (Rows 0-0 of 0)")
        self.page_info_label.setStyleSheet("font-weight: bold; color: lightgray; width:250")
        # self.page_info_label.setWordWrap(True)  # Allows text to wrap
        self.page_info_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.next_page_button = createButton("Next Page", self.load_next_diff_page)
        pagination_layout.addWidget(self.prev_page_button); pagination_layout.addStretch(1)
        pagination_layout.addWidget(self.page_info_label); pagination_layout.addStretch(1)
        pagination_layout.addWidget(self.next_page_button)
        view_diff_layout.addLayout(pagination_layout)

        self.view_diff_group_box.setEnabled(False)
        main_layout.addWidget(self.view_diff_group_box, 1)
        self.setLayout(main_layout)
        self._update_pagination_controls() 

    def _handle_output_path_update_from_setup_panel(self, path_from_setup: str, is_valid: bool):
        self.log_message(f"Path update from setup panel: '{path_from_setup}', Valid: {is_valid}", "debug")
        if is_valid and path_from_setup == self.last_successful_output_path:
            if not self.view_diff_group_box.isEnabled():
                self.log_message(f"Output DB '{path_from_setup}' confirmed valid and matches last successful run. Enabling view diffs and listing tables.", "info")
                self.view_diff_group_box.setEnabled(True)
                if not self.is_individual_diff_task_running and (not self.chain_active_for_table or self.chain_next_task_type != DiffViewerTaskType.LIST_TABLES) :
                    self._start_diff_view_task(DiffViewerTaskType.LIST_TABLES)
        elif self.last_successful_output_path and path_from_setup == self.last_successful_output_path and not is_valid:
            self.log_message(f"Last successful output DB '{path_from_setup}' is no longer valid (e.g. deleted). Disabling view diffs.", "warning")
            self.view_diff_group_box.setEnabled(False)
            self._reset_full_diff_view_components(context="output_path_invalidated")


    def start_db_comparison(self, params: ComparisonParameters):
        self.log_message(f"Starting db comparison for: {params.output_db_path}", "info")

        if self.db_thread is not None: 
            self.log_message(f"Cleaning up existing DB comparison thread: {self.db_thread}", "debug")
            if self.db_thread.isRunning():
                if self.db_worker:
                    self.log_message(f"Requesting stop for existing DB worker: {self.db_worker}", "debug")
                    self.db_worker.stop_requested = True
                self.db_thread.quit()
                if not self.db_thread.wait(2000):
                    self.log_message(f"Warning: DB comparison thread {self.db_thread} did not finish in 2s.", "warning")
                else:
                    self.log_message(f"DB comparison thread {self.db_thread} finished.", "debug")
            self.db_thread.deleteLater()
            if self.db_worker:
                self.db_worker.deleteLater()
        self.db_thread = None
        self.db_worker = None

        self.comparison_setup_panel.set_ui_for_comparison_start()
        self.view_diff_group_box.setEnabled(False)
        self._reset_full_diff_view_components(context="start_db_comparison") 
        self.last_successful_output_path = None 

        self.db_thread = QThread(self)
        self.db_worker = DbComparatorWorker(params)
        self.db_worker.moveToThread(self.db_thread)
        self.db_worker.signals.finished_comparison.connect(self.on_db_comparison_finished)
        self.db_worker.signals.progress_update.connect(lambda msg, level="info": self.log_message(f"DB Comp Progress: {msg}", level))
        self.db_worker.signals.error_occurred.connect(lambda msg: self.log_message(f"DB Comparator Error: {msg}", "error"))
        self.db_thread.started.connect(self.db_worker.run_comparison)
        self.db_thread.finished.connect(self.on_db_thread_cleanup)
        self.db_thread.start()

    def request_db_comparison_cancel(self):
        self.log_message("Cancel comparison requested.", "info")
        if self.db_thread and self.db_thread.isRunning() and self.db_worker:
            self.db_worker.stop_requested = True
            self.comparison_setup_panel.cancel_button.setEnabled(False)
        else:
            self.log_message("No active DB comparison to cancel.", "info")
            self.comparison_setup_panel.cancel_button.setEnabled(False)

    def on_db_comparison_finished(self, message: str, diffs_saved_flag: bool):
        self.log_message(f"DB comparison finished. Msg: '{message}', Diffs Saved: {diffs_saved_flag}", "debug")
        self.comparison_setup_panel.set_ui_for_comparison_end(diffs_saved_flag)
        
        if diffs_saved_flag:
            current_output_path = self.comparison_setup_panel.get_output_db_path()
            if current_output_path and os.path.exists(current_output_path):
                self.last_successful_output_path = current_output_path
                self.log_message(f"Diffs saved to: {self.last_successful_output_path}", "info")
                self.view_diff_group_box.setEnabled(True)
                self._start_diff_view_task(DiffViewerTaskType.LIST_TABLES) 
            else:
                self.log_message(f"Diffs reported saved, but output path '{current_output_path}' invalid/missing.", "error")
                self.last_successful_output_path = None
                self.view_diff_group_box.setEnabled(False)
                self._reset_full_diff_view_components(context="on_db_comparison_finished_path_invalid") 
        else:
            self.last_successful_output_path = None
            self.view_diff_group_box.setEnabled(False)
            self._clear_diff_data_display_components(context="on_db_comparison_finished_no_diffs") 
            self.current_selected_diff_table_name = None 
            self.log_message(f"No diffs saved or error. Msg: {message}", "info")
            self._update_pagination_controls() 

        if self.db_thread and self.db_thread.isRunning():
            self.db_thread.quit()

    def on_db_thread_cleanup(self):
        finished_db_thread = self.sender()
        if not isinstance(finished_db_thread, QThread):
            self.log_message(f"Warning: on_db_thread_cleanup called without QThread sender! Sender: {finished_db_thread}", "warning")
            if self.db_thread is not None:
                self.log_message(f"Fallback cleanup for self.db_thread: {self.db_thread}", "debug")
                if self.db_thread.isRunning(): self.db_thread.quit(); self.db_thread.wait(100)
                self.db_thread.deleteLater()
                if self.db_worker and self.db_worker.thread() == self.db_thread: self.db_worker.deleteLater(); self.db_worker = None
                self.db_thread = None
            return

        self.log_message(f"DB comparison QThread '{finished_db_thread}' actual cleanup.", "debug")
        finished_db_thread.deleteLater()

        if finished_db_thread == self.db_thread:
            self.db_thread = None
            if self.db_worker and self.db_worker.thread() == finished_db_thread: self.db_worker = None
        else:
            self.log_message(f"An old/orphaned DB thread ({finished_db_thread}) finished.", "debug")
        self.log_message("DB Thread cleanup complete.", "info")


    def open_diff_db_file_from_setup_panel(self):
        output_path_to_open = self.comparison_setup_panel.get_output_db_path()
        if output_path_to_open and os.path.exists(output_path_to_open):
            self.log_message(f"Opening diff DB via setup panel: {output_path_to_open}", "info")
            if not QDesktopServices.openUrl(QUrl.fromLocalFile(output_path_to_open)):
                self.log_message(f"Error opening file '{output_path_to_open}'.", "error")
        else:
            self.log_message(f"Diff DB path '{output_path_to_open}' from setup is invalid or file does not exist.", "error")

    def _start_diff_view_task(self, task_type: int, table_name: str = None, page_number: int = 0, export_limit: int = 0): # Added export_limit
        task_name_for_log = "UNKNOWN_TASK"
        try:
            task_name_for_log = DiffViewerTaskType.get_task_name(task_type)
        except AttributeError: 
            self.log_message(f"CRITICAL: DiffViewerTaskType.get_task_name not found! Using raw task ID {task_type} for logging.", "error")
            task_name_for_log = f"TASK_ID_{task_type}"
        except Exception as e_get_name:
            self.log_message(f"Error calling DiffViewerTaskType.get_task_name for task ID {task_type}: {e_get_name}", "error")
            task_name_for_log = f"TASK_ID_{task_type}_(err)"

        self.log_message(f"Attempting diff view task: {task_name_for_log}, Tbl: {table_name}, Pg: {page_number}. Running: {self.is_individual_diff_task_running}", "debug")

        if not self.last_successful_output_path or not os.path.exists(self.last_successful_output_path):
            self.log_message(f"Cannot start task {task_name_for_log}: Diff DB path '{self.last_successful_output_path}' invalid.", "error")
            self.view_diff_group_box.setEnabled(False)
            self._reset_full_diff_view_components(context="start_diff_view_task_invalid_path") 
            return

        if self.diff_view_thread is not None:
            current_thread_to_clean = self.diff_view_thread
            current_worker_to_clean = self.diff_view_worker
            self.log_message(f"Cleaning up existing diff_view_thread: {current_thread_to_clean}", "debug")
            if current_thread_to_clean.isRunning():
                if current_worker_to_clean:
                    self.log_message(f"Requesting stop for existing diff_view_worker: {current_worker_to_clean}", "debug")
                    current_worker_to_clean.request_stop()
                self.log_message(f"Quitting existing diff_view_thread: {current_thread_to_clean}", "debug")
                current_thread_to_clean.quit()
                if not current_thread_to_clean.wait(2000):
                    self.log_message(f"Warning: Existing diff_view_thread {current_thread_to_clean} did not finish in 2s.", "warning")
                else:
                    self.log_message(f"Existing diff_view_thread {current_thread_to_clean} finished.", "debug")
            current_thread_to_clean.deleteLater()
            if current_worker_to_clean:
                current_worker_to_clean.deleteLater()
        self.diff_view_thread = None
        self.diff_view_worker = None

        self.is_individual_diff_task_running = True
        self.load_diff_table_button.setEnabled(False); self.prev_page_button.setEnabled(False)
        self.next_page_button.setEnabled(False); self.diff_table_selector_combo.setEnabled(False)
        self.export_table_button.setEnabled(False) # Disable export button during task
        self._update_pagination_controls() 

        self.diff_view_thread = QThread(self)
        self.diff_view_worker = DiffViewerWorker(self.last_successful_output_path)
        self.diff_view_worker.moveToThread(self.diff_view_thread)

        # Connect signals directly from self.diff_view_worker
        self.diff_view_worker.tables_list_ready.connect(self.handle_tables_list_ready)
        self.diff_view_worker.column_names_ready.connect(self.handle_column_names_ready)
        self.diff_view_worker.page_data_ready.connect(self.handle_page_data_ready)
        self.diff_view_worker.total_rows_ready.connect(self.handle_total_rows_ready)
        self.diff_view_worker.error_occurred.connect(self.handle_diff_view_error)
        self.diff_view_worker.task_finished.connect(self.on_diff_view_worker_task_completed)
        self.diff_view_worker.excel_export_data_ready.connect(self.handle_excel_export_data_ready) 

        self.diff_view_thread.finished.connect(self.on_diff_view_thread_cleanup)
        self.diff_view_thread.started.connect(lambda: self.diff_view_worker.run_task(task_type, table_name, page_number, self.ROWS_PER_PAGE, export_limit))
        self.diff_view_thread.start()

    def _process_chain_step(self):
        task_name_for_log = "NONE" 
        if self.chain_next_task_type is not None:
            try:
                task_name_for_log = DiffViewerTaskType.get_task_name(self.chain_next_task_type)
            except AttributeError: task_name_for_log = f"TASK_ID_{self.chain_next_task_type}_(ERR_NO_GET_NAME)"
            except Exception: task_name_for_log = f"TASK_ID_{self.chain_next_task_type}_(ERR_GET_NAME)"
        
        self.log_message(f"CHAIN_PROCESS: ActiveTbl='{self.chain_active_for_table}', NextTask='{task_name_for_log}'", "debug")
        if self.is_individual_diff_task_running:
            self.log_message("CHAIN_PROCESS: Task running, deferring chain step.", "debug"); return

        if self.chain_next_task_type is None or self.chain_active_for_table is None:
            self.log_message("CHAIN_PROCESS: No pending chain task. Updating UI.", "debug")
            self.chain_active_for_table = None; self.chain_next_task_type = None 
            self._update_ui_for_task_end()
            return

        if self.chain_active_for_table != self.current_selected_diff_table_name:
            self.log_message(f"CHAIN_PROCESS: Chain for '{self.chain_active_for_table}' ABORTED. UI on '{self.current_selected_diff_table_name}'. Updating UI.", "warning")
            self.chain_active_for_table = None; self.chain_next_task_type = None
            self._update_ui_for_task_end()
            return

        task_to_run, table_for_task = self.chain_next_task_type, self.chain_active_for_table
        page_for_task = self.chain_page_number_for_get_page_data if task_to_run == DiffViewerTaskType.GET_PAGE_DATA else 0
        self._start_diff_view_task(task_to_run, table_name=table_for_task, page_number=page_for_task)


    def on_diff_view_worker_task_completed(self, task_type_completed: int):
        task_name_for_log = "UNKNOWN_TASK"
        try:
            task_name_for_log = DiffViewerTaskType.get_task_name(task_type_completed)
        except AttributeError: task_name_for_log = f"TASK_ID_{task_type_completed}_(ERR_NO_GET_NAME)"
        except Exception: task_name_for_log = f"TASK_ID_{task_type_completed}_(ERR_GET_NAME)"

        self.log_message(f"DiffWorker task_finished for {task_name_for_log}. Requesting QThread quit.", "debug")
        if self.diff_view_thread and self.diff_view_thread.isRunning():
            self.diff_view_thread.quit()
        else:
            self.log_message(f"Warning: DiffWorker's QThread ({self.diff_view_thread}) not running when task {task_name_for_log} 'task_finished'. is_running_flag={self.is_individual_diff_task_running}", "warning")
            if self.is_individual_diff_task_running and (self.diff_view_thread is None or not self.diff_view_thread.isRunning()):
                self.log_message(f"State Alert: Task running flag true, but diff_view_thread is '{self.diff_view_thread}'. Awaiting proper cleanup or next task start.", "error")


    def _update_ui_for_task_end(self):
        self.log_message(f"UI_UPDATE_END: Enabling. CurrentTbl: '{self.current_selected_diff_table_name}' TaskRunning: {self.is_individual_diff_task_running}", "debug")
        is_chain_pending_for_current_table = (self.chain_active_for_table == self.current_selected_diff_table_name and self.chain_next_task_type is not None)

        if not self.is_individual_diff_task_running and not is_chain_pending_for_current_table:
            self.load_diff_table_button.setEnabled(True)
            self.diff_table_selector_combo.setEnabled(True)
            self.export_table_button.setEnabled(bool(self.current_selected_diff_table_name and self.total_rows_in_current_diff_table > 0))
            self.log_message("Diff viewer UI controls (selector, load, export) re-enabled/updated.", "debug")
        else:
            self.log_message("Diff viewer UI controls not fully re-enabled (task/chain still active or pending for current table).", "debug")
            self.export_table_button.setEnabled(False)
        self._update_pagination_controls()


    def on_diff_view_thread_cleanup(self):
        finished_thread = self.sender()
        task_name_for_log = "NONE" 
        if self.chain_next_task_type is not None:
            try:
                task_name_for_log = DiffViewerTaskType.get_task_name(self.chain_next_task_type)
            except AttributeError: task_name_for_log = f"TASK_ID_{self.chain_next_task_type}_(ERR_NO_GET_NAME)"
            except Exception: task_name_for_log = f"TASK_ID_{self.chain_next_task_type}_(ERR_GET_NAME)"
        
        if not isinstance(finished_thread, QThread):
            self.log_message(f"Warning: on_diff_view_thread_cleanup called without QThread sender! Sender: {finished_thread}. NextChainTask: {task_name_for_log}", "warning")
            if self.diff_view_thread is not None and not self.diff_view_thread.isRunning():
                self.log_message(f"Fallback: Cleaning self.diff_view_thread ({self.diff_view_thread}) as sender was not QThread.", "debug")
                self.diff_view_thread.deleteLater()
                if self.diff_view_worker and self.diff_view_worker.thread() == self.diff_view_thread: self.diff_view_worker.deleteLater(); self.diff_view_worker = None
                self.diff_view_thread = None
                if self.is_individual_diff_task_running: self.is_individual_diff_task_running = False
            self._process_chain_step()
            return

        self.log_message(f"Diff view QThread '{finished_thread}' actual cleanup. Current self.diff_view_thread: {self.diff_view_thread}. NextChainTask: {task_name_for_log}", "debug")
        finished_thread.deleteLater()

        if finished_thread == self.diff_view_thread:
            self.log_message(f"Current tracked diff_view_thread ({self.diff_view_thread}) has finished.", "debug")
            self.diff_view_thread = None
            if self.diff_view_worker and self.diff_view_worker.thread() == finished_thread: self.diff_view_worker = None
            if self.is_individual_diff_task_running:
                self.is_individual_diff_task_running = False
                self.log_message("is_individual_diff_task_running set to False.", "debug")
            else:
                self.log_message("Warning: is_individual_diff_task_running was already False when current thread finished.", "warning")
        else:
            self.log_message(f"An old/orphaned diff view thread ({finished_thread}) finished. self.diff_view_thread is ({self.diff_view_thread}).", "debug")
        self._process_chain_step()


    def handle_tables_list_ready(self, tables: list):
        try:
            self.log_message(f"SLOT handle_tables_list_ready: Received tables: {tables}", "debug")
            self.diff_table_selector_combo.blockSignals(True)
            
            previously_selected_logical_table = self.current_selected_diff_table_name
            
            self.diff_table_selector_combo.clear() 

            if tables:
                self.diff_table_selector_combo.addItems(tables)
                self.view_diff_group_box.setEnabled(True)
                
                if previously_selected_logical_table and previously_selected_logical_table in tables:
                    self.diff_table_selector_combo.setCurrentText(previously_selected_logical_table)
                elif self.diff_table_selector_combo.count() > 0:
                    self.diff_table_selector_combo.setCurrentIndex(0)
                
                self.diff_table_selector_combo.blockSignals(False)
                
                current_combo_text = self.diff_table_selector_combo.currentText()
                if current_combo_text: 
                    if current_combo_text != self.current_selected_diff_table_name or \
                       self.current_selected_diff_table_name is None or \
                       (current_combo_text == self.current_selected_diff_table_name and not self.total_rows_in_current_diff_table):
                        self.on_diff_table_selected_ui_trigger(current_combo_text) 
                    else:
                        self._update_pagination_controls()
                else: 
                    self._clear_diff_data_display_components(context="handle_tables_list_ready_no_selection_possible")
                    self.current_selected_diff_table_name = None
            else:
                self.log_message("No diff tables found.", "info")
                self.view_diff_group_box.setEnabled(False)
                self._clear_diff_data_display_components(context="handle_tables_list_ready_no_tables_found")
                self.current_selected_diff_table_name = None
                self.diff_table_selector_combo.blockSignals(False) 
        except BaseException as e_tables_list:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR in handle_tables_list_ready: {type(e_tables_list).__name__}: {e_tables_list}\n{tb_str}", "critical")
            self.diff_table_selector_combo.blockSignals(False) 


    def handle_total_rows_ready(self, table_name_from_worker: str, count: int):
        try:
            self.log_message(f"SLOT handle_total_rows: Tbl='{table_name_from_worker}', Count={count}. ChainFor='{self.chain_active_for_table}'.", "debug")
            if self.chain_active_for_table != table_name_from_worker :
                self.log_message(f"STALE total_rows for '{table_name_from_worker}' (expected '{self.chain_active_for_table}'). Ignoring.", "warning"); return

            self.total_rows_in_current_diff_table = count
            if count > 0:
                self.chain_next_task_type = DiffViewerTaskType.GET_COLUMNS
                self.log_message(f"CHAIN_SETUP: Next task GET_COLUMNS for '{self.chain_active_for_table}'.", "debug")
            else:
                self.log_message(f"CHAIN_INFO: Table '{self.chain_active_for_table}' has 0 rows. No columns or data to fetch.", "info")
                self.current_diff_table_column_names = []
                self.diff_data_tablewidget.setColumnCount(0)
                self.diff_data_tablewidget.setRowCount(0)
                self.chain_next_task_type = None 
                self._update_pagination_controls() 
        except BaseException as e_total_rows:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR in handle_total_rows_ready: {type(e_total_rows).__name__}: {e_total_rows}\n{tb_str}", "critical")

    def handle_column_names_ready(self, table_name_from_worker: str, column_names: list):
        try:
            self.log_message(f"SLOT handle_column_names: Tbl='{table_name_from_worker}', Cols={column_names}. ChainFor='{self.chain_active_for_table}'.", "debug")
            if self.chain_active_for_table != table_name_from_worker:
                self.log_message(f"STALE column_names for '{table_name_from_worker}' (expected '{self.chain_active_for_table}'). Ignoring.", "warning"); return

            if not column_names:
                self.log_message(f"EMPTY COLUMNS for '{table_name_from_worker}'. Aborting chain.", "error")
                self._clear_diff_data_display_components(context="handle_column_names_ready_empty_cols") 
                if self.chain_active_for_table == table_name_from_worker: 
                    self.chain_active_for_table = None
                    self.chain_next_task_type = None
                return

            self.current_diff_table_column_names = column_names
            self.diff_data_tablewidget.setColumnCount(len(column_names)); self.diff_data_tablewidget.setHorizontalHeaderLabels(column_names)
            self.current_diff_table_page = 0; self.chain_page_number_for_get_page_data = 0

            if self.total_rows_in_current_diff_table > 0:
                self.chain_next_task_type = DiffViewerTaskType.GET_PAGE_DATA
                self.log_message(f"CHAIN_SETUP: Next task GET_PAGE_DATA for '{self.chain_active_for_table}'.", "debug")
            else:
                self.log_message(f"CHAIN_INFO: No rows in '{self.chain_active_for_table}', so no page data to fetch (already handled by total_rows).", "info")
                self.diff_data_tablewidget.setRowCount(0)
                self.chain_next_task_type = None 
                self._update_pagination_controls() 
        except BaseException as e_col_names:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR in handle_column_names_ready: {type(e_col_names).__name__}: {e_col_names}\n{tb_str}", "critical")

    def handle_page_data_ready(self, table_name_from_worker: str, page_number_from_worker: int, data: list):
        self.log_message(f"SLOT handle_page_data: Tbl='{table_name_from_worker}', Pg={page_number_from_worker}, Rows={len(data)}.", "debug")
        
        is_valid_data = False
        if self.chain_active_for_table == table_name_from_worker and \
           self.chain_next_task_type == DiffViewerTaskType.GET_PAGE_DATA and \
           page_number_from_worker == self.chain_page_number_for_get_page_data:
            is_valid_data = True
            self.log_message(f"Page data accepted for chain: '{table_name_from_worker}', page {page_number_from_worker}", "debug")
        elif not self.chain_active_for_table and self.current_selected_diff_table_name == table_name_from_worker:
            is_valid_data = True
            self.log_message(f"Page data accepted for direct load: '{table_name_from_worker}', page {page_number_from_worker}", "debug")

        if not is_valid_data:
            current_context_table = self.chain_active_for_table if self.chain_active_for_table else self.current_selected_diff_table_name
            current_context_page = self.chain_page_number_for_get_page_data if self.chain_active_for_table else self.current_diff_table_page
            self.log_message(
                f"STALE page_data for '{table_name_from_worker}' pg {page_number_from_worker}. "
                f"Expected context: Tbl='{current_context_table}', Pg='{current_context_page}'. Ignoring.", "warning"
            )
            return

        try:
            if not self.current_diff_table_column_names or self.diff_data_tablewidget.columnCount() == 0:
                self.log_message(f"CRITICAL: Columns not set for '{table_name_from_worker}' when page data arrived. Aborting display.", "error")
                if self.diff_data_tablewidget.rowCount() > 0: self.diff_data_tablewidget.setRowCount(0)
                if self.chain_active_for_table == table_name_from_worker : 
                    self.chain_next_task_type = None; self.chain_active_for_table = None
                return

            expected_cols = len(self.current_diff_table_column_names)
            self.diff_data_tablewidget.setRowCount(len(data))

            for r, row_data in enumerate(data):
                if len(row_data) != expected_cols:
                    self.log_message(f"Warning: Row {r} in '{table_name_from_worker}' has {len(row_data)} cells, expected {expected_cols}. Truncating/Padding may occur.", "warning")
                for c, cell in enumerate(row_data):
                    if c < expected_cols:
                        try:
                            cell_str = str(cell)
                            item = QTableWidgetItem(cell_str)
                            self.diff_data_tablewidget.setItem(r, c, item)
                        except BaseException as e_item_set: 
                            tb_item_str = traceback.format_exc()
                            self.log_message(f"CRITICAL ERROR (Cell): Failed to create/set QTableWidgetItem for cell data '{str(cell)[:100]}' at [{r},{c}] in table '{table_name_from_worker}'. Error: {type(e_item_set).__name__}: {e_item_set}\n{tb_item_str}", "critical")
                            try:
                                err_item = QTableWidgetItem(f"ERROR CELL")
                                self.diff_data_tablewidget.setItem(r, c, err_item)
                            except BaseException as e_placeholder:
                                tb_placeholder_str = traceback.format_exc()
                                self.log_message(f"CRITICAL ERROR (Placeholder): Failed to set placeholder at [{r},{c}]. Error: {type(e_placeholder).__name__}: {e_placeholder}\n{tb_placeholder_str}", "critical")
            
            self.diff_data_tablewidget.resizeColumnsToContents() 
            self.diff_data_tablewidget.resizeRowsToContents()    

            self.current_diff_table_page = page_number_from_worker 
            
            if self.chain_active_for_table == table_name_from_worker and \
               self.chain_next_task_type == DiffViewerTaskType.GET_PAGE_DATA: 
                self.log_message(f"CHAIN_FINISH: Initial data load for '{table_name_from_worker}' complete.", "info")
                self.chain_next_task_type = None 
        except BaseException as e_populate_gui:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR (GUI Population): Unhandled exception in handle_page_data_ready for table '{table_name_from_worker}'. Error: {type(e_populate_gui).__name__}: {e_populate_gui}\nTraceback:\n{tb_str}", "critical")
            try:
                self.diff_data_tablewidget.setRowCount(0) 
            except BaseException as e_clear:
                self.log_message(f"Error while trying to clear table after GUI population error: {e_clear}", "error")
            
            if self.chain_active_for_table == table_name_from_worker :
                self.chain_next_task_type = None
                self.chain_active_for_table = None 
            self.page_info_label.setText(f"Error displaying data for {table_name_from_worker}")


    def handle_diff_view_error(self, task_description: str, error_message: str):
        try:
            self.log_message(f"Error reported from DiffViewerWorker task ({task_description}): {error_message}", "error")
            table_in_context = self.chain_active_for_table or self.current_selected_diff_table_name or "N/A"
            self.log_message(f"Diff view task error for table '{table_in_context}'. Chain (if any) aborted.", "warning")

            if self.chain_active_for_table : 
                self.chain_active_for_table = None
                self.chain_next_task_type = None

            self.page_info_label.setText(f"Error loading '{table_in_context}': {error_message[:100]}")
        except BaseException as e_diff_view_err:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR in handle_diff_view_error: {type(e_diff_view_err).__name__}: {e_diff_view_err}\n{tb_str}", "critical")


    def on_diff_table_selected_ui_trigger(self, table_name: str):
        try:
            self.log_message(f"UI TRIGGER: Combobox selected '{table_name}'. TaskRunning: {self.is_individual_diff_task_running}", "debug")

            if self.is_individual_diff_task_running:
                self.log_message(f"UI TRIGGER: Selection '{table_name}' IGNORED, a task is currently running.", "debug")
                if self.current_selected_diff_table_name and self.diff_table_selector_combo.currentText() != self.current_selected_diff_table_name:
                    self.diff_table_selector_combo.blockSignals(True)
                    self.diff_table_selector_combo.setCurrentText(self.current_selected_diff_table_name) # Revert
                    self.diff_table_selector_combo.blockSignals(False)
                return

            if self.chain_active_for_table and self.chain_active_for_table != table_name :
                self.log_message(f"UI TRIGGER: New selection '{table_name}', previous chain for '{self.chain_active_for_table}' will be aborted.", "info")
                self.chain_active_for_table = None
                self.chain_next_task_type = None 

            if table_name and (table_name != self.current_selected_diff_table_name or not self.total_rows_in_current_diff_table):
                self.log_message(f"UI TRIGGER: Processing NEW selection ('{table_name}') or reload for table without data.", "info")
                
                self._clear_diff_data_display_components(context="on_diff_table_selected_ui_trigger") 
                self.current_selected_diff_table_name = table_name 

                self.chain_active_for_table = table_name 
                self.chain_next_task_type = DiffViewerTaskType.GET_ROW_COUNT
                self.chain_page_number_for_get_page_data = 0
                
                self._update_pagination_controls() 
                self._process_chain_step()
            elif not table_name: 
                self.log_message("UI TRIGGER: No table name selected. Clearing view.", "info")
                self._clear_diff_data_display_components(context="on_diff_table_selected_ui_trigger_no_table_name")
                self.current_selected_diff_table_name = None
                self.chain_active_for_table = None; self.chain_next_task_type = None
            else: 
                self.log_message(f"UI TRIGGER: Table '{table_name}' re-selected, data likely present. Refreshing pagination.", "debug")
                self._update_pagination_controls() 
        except BaseException as e_sel_trigger:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR in on_diff_table_selected_ui_trigger: {type(e_sel_trigger).__name__}: {e_sel_trigger}\n{tb_str}", "critical")


    def trigger_load_current_diff_table_data(self):
        try:
            table_name_to_refresh = self.diff_table_selector_combo.currentText()
            self.log_message(f"REFRESH TRIGGER: For '{table_name_to_refresh}'. TaskRunning: {self.is_individual_diff_task_running}", "debug")

            if self.is_individual_diff_task_running:
                self.log_message("REFRESH IGNORED: A task is currently running.", "warning"); return

            if self.chain_active_for_table: 
                self.log_message(f"REFRESH: Aborting any existing chain for '{self.chain_active_for_table}'.", "warning")
                self.chain_active_for_table = None; self.chain_next_task_type = None

            if table_name_to_refresh:
                self.log_message(f"REFRESH: Processing refresh for '{table_name_to_refresh}'.", "info")
                
                if self.current_selected_diff_table_name != table_name_to_refresh:
                    self.current_selected_diff_table_name = table_name_to_refresh

                self._clear_diff_data_display_components(context="trigger_load_current_diff_table_data")

                self.chain_active_for_table = table_name_to_refresh 
                self.chain_next_task_type = DiffViewerTaskType.GET_ROW_COUNT
                self.chain_page_number_for_get_page_data = 0 
                
                self._update_pagination_controls() 
                self._process_chain_step()
            else:
                self.log_message("REFRESH: No table selected to refresh.", "warning")
        except BaseException as e_trigger_load:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR in trigger_load_current_diff_table_data: {type(e_trigger_load).__name__}: {e_trigger_load}\n{tb_str}", "critical")


    def _update_pagination_controls(self):
        try:
            is_loading = self.is_individual_diff_task_running or \
                         (self.chain_active_for_table == self.current_selected_diff_table_name and \
                          self.chain_next_task_type is not None)

            if is_loading:
                loading_tbl = self.current_selected_diff_table_name or self.chain_active_for_table or "table"
                self.page_info_label.setText(f"Loading '{loading_tbl}'...")
                self.prev_page_button.setEnabled(False); self.next_page_button.setEnabled(False); return

            if not self.current_selected_diff_table_name:
                self.page_info_label.setText("No table selected")
                self.prev_page_button.setEnabled(False); self.next_page_button.setEnabled(False); return

            if not self.current_diff_table_column_names and self.total_rows_in_current_diff_table > 0 :
                self.page_info_label.setText(f"Loading columns for '{self.current_selected_diff_table_name}'...")
                self.prev_page_button.setEnabled(False); self.next_page_button.setEnabled(False); return

            if self.total_rows_in_current_diff_table == 0:
                if self.current_selected_diff_table_name: 
                    self.page_info_label.setText(f"Table '{self.current_selected_diff_table_name}' is empty or no data loaded")
                else: 
                    self.page_info_label.setText("No data")
                self.prev_page_button.setEnabled(False); self.next_page_button.setEnabled(False); return

            total_pgs = max(1, (self.total_rows_in_current_diff_table + self.ROWS_PER_PAGE - 1) // self.ROWS_PER_PAGE)
            curr_pg_disp = self.current_diff_table_page + 1
            start_row = (self.current_diff_table_page * self.ROWS_PER_PAGE) + 1
            end_row = min((self.current_diff_table_page + 1) * self.ROWS_PER_PAGE, self.total_rows_in_current_diff_table)

            self.page_info_label.setText(f"Page {curr_pg_disp} of {total_pgs} (Rows {start_row}-{end_row} of {self.total_rows_in_current_diff_table})")
            self.prev_page_button.setEnabled(self.current_diff_table_page > 0)
            self.next_page_button.setEnabled(curr_pg_disp < total_pgs)
        except BaseException as e_update_pag:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR in _update_pagination_controls: {type(e_update_pag).__name__}: {e_update_pag}\n{tb_str}", "critical")


    def load_prev_diff_page(self):
        try:
            if self.is_individual_diff_task_running or self.chain_active_for_table:
                self.log_message("PREV_PAGE: Task or chain active. Ignoring.", "warning"); return
            if not self.current_selected_diff_table_name or not self.current_diff_table_column_names:
                self.log_message("PREV_PAGE: No table/columns loaded. Ignoring.", "warning"); return

            if self.current_diff_table_page > 0:
                target_page = self.current_diff_table_page - 1
                self.log_message(f"PREV_PAGE: Loading page {target_page} for '{self.current_selected_diff_table_name}'.", "info")
                # self.current_diff_table_page = target_page # Worker will confirm page on data return
                self._start_diff_view_task(DiffViewerTaskType.GET_PAGE_DATA, table_name=self.current_selected_diff_table_name, page_number=target_page)
            else:
                self.log_message("PREV_PAGE: Already at the first page.", "debug")
        except BaseException as e_prev_page:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR in load_prev_diff_page: {type(e_prev_page).__name__}: {e_prev_page}\n{tb_str}", "critical")

    def load_next_diff_page(self):
        try:
            if self.is_individual_diff_task_running or self.chain_active_for_table:
                self.log_message("NEXT_PAGE: Task or chain active. Ignoring.", "warning"); return
            if not self.current_selected_diff_table_name or not self.current_diff_table_column_names:
                self.log_message("NEXT_PAGE: No table/columns loaded. Ignoring.", "warning"); return

            total_pages_zero_based = max(0, (self.total_rows_in_current_diff_table + self.ROWS_PER_PAGE - 1) // self.ROWS_PER_PAGE - 1)
            if self.current_diff_table_page < total_pages_zero_based :
                target_page = self.current_diff_table_page + 1
                self.log_message(f"NEXT_PAGE: Loading page {target_page} for '{self.current_selected_diff_table_name}'.", "info")
                # self.current_diff_table_page = target_page # Worker will confirm
                self._start_diff_view_task(DiffViewerTaskType.GET_PAGE_DATA, table_name=self.current_selected_diff_table_name, page_number=target_page)
            else:
                self.log_message("NEXT_PAGE: Already at the last page.", "debug")
        except BaseException as e_next_page:
            tb_str = traceback.format_exc()
            self.log_message(f"CRITICAL ERROR in load_next_diff_page: {type(e_next_page).__name__}: {e_next_page}\n{tb_str}", "critical")

    def _trigger_export_current_table_to_excel(self):
        if self.is_individual_diff_task_running or self.chain_active_for_table:
            self.log_message("EXPORT_TABLE: Task or chain active. Ignoring.", "warning")
            QMessageBox.information(self, "Busy", "Another operation is in progress. Please wait.")
            return
        
        current_table = self.current_selected_diff_table_name
        if not current_table:
            self.log_message("EXPORT_TABLE: No table selected to export.", "warning")
            QMessageBox.warning(self, "No Table Selected", "Please select a table from the dropdown to export.")
            return
        
        if self.total_rows_in_current_diff_table == 0:
            self.log_message(f"EXPORT_TABLE: Table '{current_table}' is empty. Nothing to export.", "info")
            QMessageBox.information(self, "Empty Table", f"Table '{current_table}' has no data to export.")
            return

        self.log_message(f"EXPORT_TABLE: Initiating export for table '{current_table}'.", "info")
        # The export_limit=0 means export all rows.
        self._start_diff_view_task(DiffViewerTaskType.EXPORT_TABLE_TO_EXCEL_DATA, table_name=current_table, export_limit=5000)


    @pyqtSlot(str, list, list)
    def handle_excel_export_data_ready(self, table_name: str, column_names: list, all_data_rows: list):
        self.log_message(f"Excel data ready for table '{table_name}'. Columns: {len(column_names)}, Rows: {len(all_data_rows)}", "info")
        
        if self.current_selected_diff_table_name != table_name:
            self.log_message(f"Excel data received for '{table_name}', but current selection is '{self.current_selected_diff_table_name}'. Ignoring.", "warning")
            return

        options = QFileDialog.Options()
        default_filename = f"{table_name}_export.xlsx"
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Table to Excel", default_filename,
            "Excel Files (*.xlsx);;All Files (*)", options=options
        )

        if file_path:
            try:
                self.log_message(f"Saving table '{table_name}' to Excel: {file_path}", "info")
                QApplication.setOverrideCursor(Qt.WaitCursor)
                df_to_export = pd.DataFrame(all_data_rows, columns=column_names)
                df_to_export.to_excel(file_path, index=False, engine='openpyxl')
                QApplication.restoreOverrideCursor()
                QMessageBox.information(self, "Export Successful", f"Table '{table_name}' successfully exported to:\n{file_path}")
                self.log_message(f"Successfully exported '{table_name}' to {file_path}", "info")
            except Exception as e:
                QApplication.restoreOverrideCursor()
                self.log_message(f"Error exporting table '{table_name}' to Excel: {e}", "error", exc_info=True)
                QMessageBox.critical(self, "Export Error", f"Could not export table '{table_name}':\n{e}")
        else:
            self.log_message(f"Excel export for table '{table_name}' cancelled by user.", "info")
        
        # Re-enable UI elements after export attempt (handled by _update_ui_for_task_end via task_finished)


    def cleanup_on_close(self):
        self.log_message("Cleanup requested for ComparisonTabMainWidget.", "info")

        if self.db_thread :
            if self.db_thread.isRunning():
                self.log_message("cleanup_on_close: Stopping DB worker and thread.", "debug")
                if self.db_worker: self.db_worker.stop_requested = True
                self.db_thread.quit()
                if not self.db_thread.wait(2000):
                    self.log_message("Warning: DB thread did not finish cleanly during close.", "warning")
            self.db_thread.deleteLater()
        if self.db_worker: self.db_worker.deleteLater()
        self.db_thread = None
        self.db_worker = None

        if self.diff_view_thread:
            if self.diff_view_thread.isRunning():
                self.log_message("cleanup_on_close: Stopping Diff View worker and thread.", "debug")
                if self.diff_view_worker: self.diff_view_worker.request_stop()
                self.diff_view_thread.quit()
                if not self.diff_view_thread.wait(2000):
                    self.log_message("Warning: Diff View thread did not finish cleanly during close.", "warning")
            self.diff_view_thread.deleteLater()
        if self.diff_view_worker: self.diff_view_worker.deleteLater()
        self.diff_view_thread = None
        self.diff_view_worker = None
        self.log_message("ComparisonTabMainWidget cleanup finished.", "info")