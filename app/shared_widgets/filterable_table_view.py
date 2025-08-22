import sys
import traceback
import pandas as pd
import numpy as np
from typing import Dict, Optional, Any # Added Any
import logging

from PyQt5.QtWidgets import (
    QTableView, QAbstractItemView, QVBoxLayout, QLineEdit, QWidget,
    QHBoxLayout, QComboBox, QStatusBar, QHeaderView, QMenu, QApplication, QShortcut,
    QDialog, QTextEdit, QDialogButtonBox, QLabel, QPushButton, QFileDialog, QMessageBox
)
from PyQt5.QtCore import (
    QAbstractTableModel, Qt, QModelIndex, QItemSelectionModel, QItemSelection,
    QObject, QThread, pyqtSignal, pyqtSlot, QTimer, QMetaObject, Q_ARG, QPoint # Added QPoint
)
from PyQt5.QtGui import QKeySequence

logger = logging.getLogger(__name__)
# For more detailed logging during development, you can set the level:
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s')
# logger = logging.getLogger(__name__) # Re-get logger if basicConfig was called

# --- Worker Object for Background Processing ---
class TableModelWorker(QObject):
    filterComplete = pyqtSignal(bool, str)
    sortComplete = pyqtSignal(bool, str)
    exportComplete = pyqtSignal(bool, str)
    new_data_processed_by_worker = pyqtSignal()

    def __init__(self):
        super().__init__()
        self._original_df = pd.DataFrame()
        self._last_result_df = pd.DataFrame()
        self._last_error = ""

    @pyqtSlot(object)
    def process_new_data(self, new_df_to_process: Optional[pd.DataFrame]):
        logger.debug(f"Worker: Received request to process new data. Input shape: {new_df_to_process.shape if new_df_to_process is not None else 'None'}")
        try:
            if new_df_to_process is None or new_df_to_process.empty:
                self._original_df = pd.DataFrame()
            else:
                self._original_df = new_df_to_process.copy()
            
            self._last_result_df = self._original_df.copy()
            self._last_error = ""
            logger.info(f"Worker: Successfully processed and copied new data. Original DF shape: {self._original_df.shape}")
            self.new_data_processed_by_worker.emit()
        except Exception as e:
            error_msg = f"Worker: Error processing new data: {e}"
            self._last_error = error_msg
            logger.error(error_msg, exc_info=True)
            self._original_df = pd.DataFrame()
            self._last_result_df = pd.DataFrame()
            self.new_data_processed_by_worker.emit()

    @pyqtSlot(result=object)
    def get_last_result_df(self) -> pd.DataFrame:
        return self._last_result_df.copy()

    @pyqtSlot(result=object)
    def get_original_df(self) -> pd.DataFrame:
        return self._original_df.copy()
    
    @pyqtSlot(result=list) # Decorate as a slot returning a list
    def get_original_column_names(self) -> list:
        """
        Returns a list of column names from the worker's original DataFrame.
        This method is called synchronously from the main thread but is lightweight.
        """
        if self._original_df is not None and not self._original_df.empty:
            return self._original_df.columns.tolist()
        return []

    @pyqtSlot(str, str)
    def perform_filter(self, column_name: str, filter_text: str):
        logger.debug(f"Worker: Performing filter. Column: '{column_name}', Text: '{filter_text}'. Original DF shape: {self._original_df.shape}")
        try:
            if self._original_df.empty:
                self._last_result_df = self._original_df.copy()
                self.filterComplete.emit(True, "")
                return

            temp_df: pd.DataFrame
            if filter_text:
                source_df_for_filter = self._original_df
                if column_name and column_name in source_df_for_filter.columns:
                    mask = source_df_for_filter[column_name].astype(str).str.contains(filter_text, case=False, na=False)
                    temp_df = source_df_for_filter.loc[mask].copy(deep=True)
                else: 
                    string_cols = source_df_for_filter.select_dtypes(include=['object', 'string']).columns
                    if not string_cols.empty:
                        mask = source_df_for_filter[string_cols].apply(
                            lambda x: x.astype(str).str.contains(filter_text, case=False, na=False)
                        ).any(axis=1)
                        temp_df = source_df_for_filter.loc[mask].copy(deep=True)
                    else: 
                        temp_df = pd.DataFrame(columns=source_df_for_filter.columns)
            else: 
                temp_df = self._original_df.copy(deep=True)

            self._last_result_df = temp_df
            self._last_error = ""
            logger.debug(f"Worker: Filter complete. Result DF shape: {self._last_result_df.shape}")
            self.filterComplete.emit(True, "")
        except Exception as e:
            error_msg = f"Worker: Error during filtering: {e}"
            self._last_error = error_msg
            logger.error(error_msg, exc_info=True)
            self.filterComplete.emit(False, error_msg)

    @pyqtSlot(int, int)
    def perform_sort(self, column_index: int, order_int: int):
        logger.debug(f"Worker: Performing sort. Column index: {column_index}, Order: {order_int}. Last result DF shape: {self._last_result_df.shape}")
        order = Qt.AscendingOrder if order_int == 0 else Qt.DescendingOrder
        try:
            df_to_sort = self._last_result_df
            col_name = "N/A" # Initialize for logging in case of early exit
            if df_to_sort.empty or column_index < 0 or column_index >= len(df_to_sort.columns):
                logger.debug("Worker: Sort skipped, DataFrame empty or invalid column index.")
                self.sortComplete.emit(True, "")
                return

            col_name = df_to_sort.columns[column_index]
            ascending_order = (order == Qt.AscendingOrder)
            
            sorted_df = df_to_sort.sort_values(by=col_name, ascending=ascending_order, kind='mergesort', na_position='last')
            
            self._last_result_df = sorted_df
            self._last_error = ""
            logger.debug(f"Worker: Sort complete. Result DF shape: {self._last_result_df.shape}")
            self.sortComplete.emit(True, "")
        except Exception as e:
            error_msg = f"Worker: Error during sorting column {column_index} ('{col_name}'): {e}"
            self._last_error = error_msg
            logger.error(error_msg, exc_info=True)
            self.sortComplete.emit(False, error_msg)

    @pyqtSlot(str)
    def perform_export_to_excel(self, file_name: str):
        logger.debug(f"Worker: Exporting to Excel: {file_name}. Original DF shape: {self._original_df.shape}")
        try:
            if self._original_df.empty:
                logger.info("Worker: Export to Excel skipped, no data to export.")
                self.exportComplete.emit(False, "No data to export.")
                return
            self._original_df.to_excel(file_name, index=False, engine='openpyxl')
            logger.info(f"Worker: Export to Excel successful: {file_name}")
            self.exportComplete.emit(True, f"Successfully exported to {file_name}")
        except Exception as e:
            error_msg = f"Worker: Error exporting to Excel: {e}"
            logger.error(error_msg, exc_info=True)
            self.exportComplete.emit(False, error_msg)


# --- LazyLoadingDataFrameTableModel Class ---
class LazyLoadingDataFrameTableModel(QAbstractTableModel):
    filterRequested = pyqtSignal(str, str)
    sortRequested = pyqtSignal(int, int)
    modelUpdateComplete = pyqtSignal()
    modelUpdateError = pyqtSignal(str)
    request_worker_process_data = pyqtSignal(object)

    def __init__(self, worker: TableModelWorker, parent: Optional[QObject] = None):
        super().__init__(parent)
        self._worker = worker
        self._dataframe = pd.DataFrame()
        self._is_updating_model = False
        
        self._initial_load_count = 150 # Adjusted
        self._load_increment = 150   # Adjusted
        self._loaded_rows = 0

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid(): return 0
        return self._loaded_rows

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid(): return 0
        return len(self._dataframe.columns) if not self._dataframe.empty else 0

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid() or self._dataframe.empty or \
           index.row() >= self._loaded_rows or index.column() >= self.columnCount():
            return None
        try:
            value = self._dataframe.iloc[index.row(), index.column()]
            value_type = type(value)

            if role == Qt.DisplayRole:
                try:
                    if pd.isna(value): return ""
                except (ValueError, TypeError): pass
                if isinstance(value, str): return value
                if isinstance(value, (int, np.integer)): return str(value)
                if isinstance(value, (float, np.floating)): return f"{value:.6f}"
                if isinstance(value, pd.Timestamp): return value.strftime('%Y-%m-%d %H:%M:%S')
                if isinstance(value, bool): return str(value)
                
                is_array_like = isinstance(value, (np.ndarray, pd.Series))
                is_list_or_dict_like = isinstance(value, (list, dict))

                if is_array_like or is_list_or_dict_like:
                    type_name = value_type.__name__
                    if hasattr(value, 'shape'):
                        return f"[{type_name} shape={getattr(value, 'shape', '(unknown)')}]"
                    elif hasattr(value, '__len__'):
                        try: return f"[{type_name} len={len(value)}]"
                        except TypeError: return f"[{type_name} iterable]"
                    else: return f"[{type_name}]"
                return str(value)
            elif role == Qt.EditRole:
                return value
            elif role == Qt.TextAlignmentRole:
                if isinstance(value, (int, float, np.number)):
                    if not pd.isna(value): return Qt.AlignRight | Qt.AlignVCenter
                return Qt.AlignLeft | Qt.AlignVCenter
            elif role == Qt.ToolTipRole:
                try:
                    raw_value_str = str(value) if value is not None else ""
                    if len(raw_value_str) > 250: raw_value_str = raw_value_str[:250] + "..."
                    return f"Type: {value_type.__name__}\nValue: {raw_value_str}"
                except Exception: return f"Type: {value_type.__name__}\nValue: [Error converting value for tooltip]"
        except IndexError: # Less verbose logging for common issues if data changes underneath
            # logger.warning(f"Model: Data access IndexError at R{index.row()},C{index.column()}", exc_info=False)
            return None
        except Exception as e: # Log more critical errors
            logger.error(f"Model: Data access error: {e}", exc_info=True)
            # Emitting error for every cell access failure can be overwhelming.
            # Consider if this should only be logged or if view really needs to react to each one.
            # self.modelUpdateError.emit(f"Data access error: {e}") 
            return None
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:
        try:
            current_view_columns = self._dataframe.columns
            if section < 0 or section >= len(current_view_columns): return None

            if role == Qt.DisplayRole:
                if orientation == Qt.Horizontal: return str(current_view_columns[section])
                else: return str(section + 1)
            elif role == Qt.ToolTipRole and orientation == Qt.Horizontal:
                col_name = current_view_columns[section]
                try:
                    original_df_from_worker = self._worker.get_original_df()
                    if col_name in original_df_from_worker.columns:
                        dtype_str = str(original_df_from_worker[col_name].dtype)
                        if dtype_str == 'object':
                            non_null_sample = original_df_from_worker[col_name].dropna()
                            if not non_null_sample.empty:
                                first_type = type(non_null_sample.iloc[0]).__name__
                                dtype_str = f"object (e.g., {first_type})"
                            else: dtype_str = "object (all nulls?)"
                        return f"Column: {col_name}\nOriginal Type: {dtype_str}"
                    else: return f"Column: {col_name}\nType: {self._dataframe[col_name].dtype} (current view)"
                except Exception as e_tooltip:
                    logger.warning(f"Model: Error getting header tooltip for {col_name}: {e_tooltip}")
                    return f"Column: {col_name}\nType: (Unavailable)"
        except Exception as e:
            logger.error(f"Model: Header data error for section {section}, orientation {orientation}: {e}", exc_info=True)
            return None
        return None
        
    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid(): return Qt.NoItemFlags
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def load_more_rows(self):
        if self._dataframe.empty or self._loaded_rows >= len(self._dataframe): return
        
        rows_to_add = min(self._load_increment, len(self._dataframe) - self._loaded_rows)
        if rows_to_add <= 0: return
            
        self.beginInsertRows(QModelIndex(), self._loaded_rows, self._loaded_rows + rows_to_add - 1)
        self._loaded_rows += rows_to_add
        self.endInsertRows()
        logger.debug(f"Model: Loaded more rows. Total loaded: {self._loaded_rows} of {len(self._dataframe)}")

    def apply_filter(self, column_name: str, filter_text: str):
        logger.debug(f"Model: Filter requested. Column: '{column_name}', Text: '{filter_text}'")
        self.filterRequested.emit(column_name, filter_text)

    @pyqtSlot(object) # Made this a slot in previous iterations
    def update_dataframe(self, new_df: Optional[pd.DataFrame]):
        actual_df_to_process: Optional[pd.DataFrame]
        if new_df is None: actual_df_to_process = pd.DataFrame()
        elif isinstance(new_df, pd.DataFrame): actual_df_to_process = new_df
        else:
            logger.warning(f"Model: update_dataframe received unexpected type: {type(new_df)}. Using empty DataFrame.")
            actual_df_to_process = pd.DataFrame()
        
        logger.debug(f"Model: Requesting worker to process new DataFrame. Input shape: {actual_df_to_process.shape if not actual_df_to_process.empty else 'Empty'}")
        self.request_worker_process_data.emit(actual_df_to_process)

    def sort(self, column_index: int, order: Qt.SortOrder = Qt.AscendingOrder):
        if self._is_updating_model:
            logger.debug("Model: Sort request ignored, model is currently being updated.")
            return
        order_int = 0 if order == Qt.AscendingOrder else 1
        logger.debug(f"Model: Sort requested. Column index: {column_index}, Order (int): {order_int}")
        self.sortRequested.emit(column_index, order_int)

    @pyqtSlot(bool, str)
    def _handle_filter_complete(self, success: bool, error_message: str):
        logger.debug(f"Model: Filter complete signal received. Success: {success}, Msg: '{error_message}'")
        self._is_updating_model = True
        try:
            if success:
                processed_df = self._worker.get_last_result_df()
                logger.debug(f"Model: Received filtered DF from worker. Shape: {processed_df.shape}")
                
                self.beginResetModel()
                self._dataframe = processed_df
                self._loaded_rows = min(self._initial_load_count, len(self._dataframe))
                self.endResetModel()
                self.modelUpdateComplete.emit()
            else:
                self.modelUpdateError.emit(f"Filter Error from worker: {error_message}")
        except Exception as e:
            logger.error(f"Model: Error applying filter results: {e}", exc_info=True)
            self.modelUpdateError.emit(f"Error applying filter results: {e}")
        finally: self._is_updating_model = False

    @pyqtSlot(bool, str)
    def _handle_sort_complete(self, success: bool, error_message: str):
        logger.debug(f"Model: Sort complete signal received. Success: {success}, Msg: '{error_message}'")
        self._is_updating_model = True
        try:
            if success:
                processed_df = self._worker.get_last_result_df()
                logger.debug(f"Model: Received sorted DF from worker. Shape: {processed_df.shape}")

                self.beginResetModel()
                self._dataframe = processed_df
                self._loaded_rows = min(self._initial_load_count, len(self._dataframe))
                self.endResetModel()
                self.modelUpdateComplete.emit()
            else:
                self.modelUpdateError.emit(f"Sort Error from worker: {error_message}")
        except Exception as e:
            logger.error(f"Model: Error applying sort results: {e}", exc_info=True)
            self.modelUpdateError.emit(f"Error applying sort results: {e}")
        finally: self._is_updating_model = False


# --- Selection Dialog ---
class SelectionDialog(QDialog): # Included from original
    def __init__(self, cell_data_str: str, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.setWindowTitle("Cell Content")
        self.setMinimumWidth(500)
        self.setMinimumHeight(300)
        layout = QVBoxLayout(self)
        self.text_edit = QTextEdit(self)
        self.text_edit.setReadOnly(True)
        self.text_edit.setText(cell_data_str)
        layout.addWidget(self.text_edit)
        
        button_box = QDialogButtonBox(self)
        copy_button = QPushButton("Copy to Clipboard", self)
        button_box.addButton(copy_button, QDialogButtonBox.ActionRole)
        button_box.addButton(QDialogButtonBox.Close) 
        
        copy_button.clicked.connect(self.copy_to_clipboard)
        button_box.rejected.connect(self.reject) # Close button triggers rejected

        layout.addWidget(button_box)
        self.setLayout(layout)

    def copy_to_clipboard(self):
        QApplication.clipboard().setText(self.text_edit.toPlainText())


# --- Main Table View Widget ---
class FilterableTableView(QWidget):
    DEFAULT_MAX_COLUMN_WIDTH: Optional[int] = 400
    MAX_ROW_HEIGHT: Optional[int] = 150
    ENABLE_TEXT_WRAPPING: bool = True
    processingStarted = pyqtSignal()
    processingFinished = pyqtSignal(bool, str)

    def __init__(self, dataframe: Optional[pd.DataFrame] = None, tab_name: str = "Data", parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.tab_name = tab_name
        self._initial_resize_done = False
        self._is_currently_processing = False

        self._worker_thread = QThread(self)
        self._worker = TableModelWorker()
        self._worker.moveToThread(self._worker_thread)

        self.model = LazyLoadingDataFrameTableModel(self._worker, parent=self)

        self.table_view = QTableView()
        self.filter_input = QLineEdit()
        self.column_selector = QComboBox()
        self.status_bar = QStatusBar()
        self._filter_timer = QTimer(self)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.setInterval(350)
        
        self.export_button = QPushButton("Export Full Data")

        self._setup_ui_elements()
        self._connect_all_signals()    
        
        self._worker_thread.start()
        logger.info(f"View: FilterableTableView '{tab_name}' initialized. Worker thread started.")

        initial_df_to_load: Optional[pd.DataFrame] = None
        if dataframe is not None and not dataframe.empty:
            initial_df_to_load = dataframe

        if initial_df_to_load is not None:
            logger.info(f"View: Initial DataFrame provided for '{tab_name}'. Scheduling async processing via QTimer.")
            self._set_ui_processing_state(True, "Loading initial data...")
            QTimer.singleShot(0, lambda df=initial_df_to_load: self.model.update_dataframe(df))
        else:
            self._set_ui_processing_state(False)
            self.update_status_bar("Ready. No data loaded.")
            self.refresh_column_selector_list()

    def _set_ui_processing_state(self, is_processing: bool, status_message: Optional[str] = None):
        if self._is_currently_processing == is_processing and status_message is None: return

        self._is_currently_processing = is_processing
        self.filter_input.setEnabled(not is_processing)
        self.column_selector.setEnabled(not is_processing)
        self.table_view.setSortingEnabled(not is_processing)
        self.export_button.setEnabled(not is_processing)

        if status_message: self.update_status_bar(status_message)
        if is_processing: self.processingStarted.emit()

    def _setup_ui_elements(self):
        self.table_view.setModel(self.model)
        self.table_view.setSortingEnabled(False)
        self.table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.table_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)

        if self.ENABLE_TEXT_WRAPPING: self.table_view.setWordWrap(True)
        else: self.table_view.setWordWrap(False); self.table_view.setTextElideMode(Qt.ElideRight)

        h_header = self.table_view.horizontalHeader()
        v_header = self.table_view.verticalHeader()
        h_header.setSectionResizeMode(QHeaderView.Interactive)
        h_header.setSectionsClickable(True)
        
        v_header.setSectionResizeMode(QHeaderView.Fixed) # Changed from ResizeToContents
        font_metrics_height = v_header.fontMetrics().height()
        v_header.setDefaultSectionSize(font_metrics_height + 12 if self.ENABLE_TEXT_WRAPPING else font_metrics_height + 8) # Sensible default, slightly larger if wrapping
        v_header.setMinimumSectionSize(font_metrics_height + 6)

        self.filter_input.setPlaceholderText("Filter text (case-insensitive)...")
        self.filter_input.setClearButtonEnabled(True)
        self.column_selector.addItem("Filter in: All String Columns")

        filter_layout = QHBoxLayout()
        filter_label = QLabel("Filter:")
        filter_layout.addWidget(filter_label)
        filter_layout.addWidget(self.column_selector, 1)
        filter_layout.addWidget(self.filter_input, 2)
        filter_layout.addStretch(0)
        filter_layout.addWidget(self.export_button)

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(2, 2, 2, 2)
        main_layout.addLayout(filter_layout)
        main_layout.addWidget(self.table_view, 1)
        main_layout.addWidget(self.status_bar)
        self.setLayout(main_layout)

    def refresh_column_selector_list(self):
        self.column_selector.blockSignals(True)
        current_text_selection = self.column_selector.currentText()
        logger.debug(f"View: Refreshing columns. Current QComboBox selection: '{current_text_selection}', Index: {self.column_selector.currentIndex()}")

        self.column_selector.clear()
        self.column_selector.addItem("Filter in: All String Columns") # Default item at index 0
        
        actual_original_columns = []
        try:
            # Fetch column names directly from the worker's original DataFrame
            actual_original_columns = self._worker.get_original_column_names()
            logger.debug(f"View: Fetched original columns from worker: {actual_original_columns}")
            if actual_original_columns: # If the worker returned any column names
                self.column_selector.addItems(actual_original_columns)
        except Exception as e:
            logger.error(f"View: Error fetching original column names from worker: {e}", exc_info=True)
            # If worker call fails, actual_original_columns will be empty; dropdown will only have default.

        # Attempt to find and restore the previously selected text
        # Qt.MatchFixedString ensures exact match, Qt.MatchCaseSensitive for case sensitivity
        idx = self.column_selector.findText(current_text_selection, Qt.MatchFixedString | Qt.MatchCaseSensitive) 
        
        if idx != -1:
            self.column_selector.setCurrentIndex(idx)
            logger.debug(f"View: Restored column selection to '{current_text_selection}' at new index {idx}.")
        else:
            # If not found, default to index 0 ("Filter in: All String Columns")
            self.column_selector.setCurrentIndex(0)
            # Log only if it was a meaningful reset from a user's specific column choice
            if current_text_selection != "Filter in: All String Columns" and current_text_selection.strip() != "":
                 logger.warning(f"View: Could not find previous column selection '{current_text_selection}' in original columns {actual_original_columns}. Dropdown reset to default.")
            elif not actual_original_columns and (current_text_selection != "Filter in: All String Columns" and current_text_selection.strip() != ""):
                 logger.info(f"View: Worker reported no original columns. Previous selection '{current_text_selection}' could not be restored. Dropdown reset to default.")
            else:
                 logger.debug(f"View: Dropdown set/kept to default ('Filter in: All String Columns'). Previous: '{current_text_selection}'.")

        self.column_selector.blockSignals(False)

    def _connect_all_signals(self):
        self.filter_input.textChanged.connect(self._filter_timer.start)
        self._filter_timer.timeout.connect(self._initiate_filter_operation)
        self.column_selector.currentIndexChanged.connect(self._initiate_filter_operation)
        self.export_button.clicked.connect(self._initiate_export_to_excel)

        selection_model = self.table_view.selectionModel()
        if selection_model: selection_model.selectionChanged.connect(self._on_table_selection_changed)
        
        scrollbar = self.table_view.verticalScrollBar()
        if scrollbar: scrollbar.valueChanged.connect(self._check_scroll_to_load_more)
        
        self.table_view.customContextMenuRequested.connect(self._show_table_context_menu)
        
        h_header = self.table_view.horizontalHeader()
        if h_header:
            h_header.sectionResized.connect(self._handle_column_resize)
            h_header.sortIndicatorChanged.connect(self._handle_sort_indicator_changed)

        self.model.modelUpdateComplete.connect(self._on_model_update_complete)
        self.model.modelUpdateError.connect(self._on_model_update_error)
        
        self.model.request_worker_process_data.connect(self._worker.process_new_data)
        self.model.filterRequested.connect(self._worker.perform_filter)
        self.model.sortRequested.connect(self._worker.perform_sort)

        self._worker.filterComplete.connect(self.model._handle_filter_complete)
        self._worker.sortComplete.connect(self.model._handle_sort_complete)
        self._worker.new_data_processed_by_worker.connect(self._on_new_data_processed_by_worker)
        self._worker.exportComplete.connect(self._handle_export_operation_complete)

        self._worker_thread.finished.connect(self._worker.deleteLater)
        QShortcut(QKeySequence.Copy, self.table_view, self._copy_selected_cells_to_clipboard)

    @pyqtSlot()
    def _on_new_data_processed_by_worker(self):
        logger.info(f"View: Worker confirmed new data processed for '{self.tab_name}'. Triggering view refresh.")
        self._initiate_filter_operation(is_triggered_by_new_data=True)

    @pyqtSlot()
    def _initiate_filter_operation(self, is_triggered_by_new_data: bool = False):
        if not self._is_currently_processing:
            self._set_ui_processing_state(True, "Filtering data...")
        elif is_triggered_by_new_data:
            self.update_status_bar("Refreshing view with new data...")

        filter_text = self.filter_input.text()
        column_name_to_filter = ""
        
        current_selector_index = self.column_selector.currentIndex()
        if current_selector_index > 0:
            column_name_to_filter = self.column_selector.currentText()
            if self.model.columnCount() > 0 and column_name_to_filter not in self.model._dataframe.columns:
                logger.warning(f"View: Selected filter column '{column_name_to_filter}' not in current view. Defaulting to all.")
                column_name_to_filter = "" 
                self.column_selector.setCurrentIndex(0)

        self.model.apply_filter(column_name_to_filter, filter_text)

    @pyqtSlot(int, Qt.SortOrder)
    def _handle_sort_indicator_changed(self, logical_index: int, order: Qt.SortOrder):
        if self._is_currently_processing:
            logger.debug("View: Sort request ignored, currently processing.")
            return
        self._set_ui_processing_state(True, "Sorting data...")
        self.model.sort(logical_index, order)

    @pyqtSlot()
    def _on_model_update_complete(self):
        logger.info(f"View: Model update complete for '{self.tab_name}'. Finalizing UI.")
        self._set_ui_processing_state(False) # Re-enable UI elements
        
        self.refresh_column_selector_list() 
        self.table_view.clearSelection()    
        self.update_status_bar(self.get_current_status_message()) 
        
        if self.model.columnCount(QModelIndex()) > 0:
            if not self.isVisible() or not self._initial_resize_done:
                 QTimer.singleShot(0, self._perform_layout_adjustments_after_data)
            else: 
                 self._perform_layout_adjustments_after_data()
            self._initial_resize_done = True
        else: 
            self._initial_resize_done = False
        
        self.processingFinished.emit(True, "Data display updated.")

        # ---- Add this line to set focus back to the filter input ----
        if self.filter_input.isEnabled() and self.filter_input.isVisibleTo(self): # Check if it's sensible to set focus
            self.filter_input.setFocus()
        # ---- End of added line ----

    @pyqtSlot(str)
    def _on_model_update_error(self, error_message: str):
        logger.error(f"View: Model update error for '{self.tab_name}': {error_message}")
        self._set_ui_processing_state(False) # Re-enable UI
        self.update_status_bar(f"Error: {error_message}")
        # self.processingFinished.emit(False, error_message)
        # This is implicitly handled by _set_ui_processing_state if you wire it up
        # Or emit it explicitly if needed by external connections.
        # My previous full code did not emit processingFinished from _set_ui_processing_state.
        # So, let's ensure it's emitted.
        self.processingFinished.emit(False, f"Error: {error_message}")


        QMessageBox.warning(self, "Processing Error", f"An error occurred: {error_message}")

        # ---- Add this line to set focus back to the filter input ----
        if self.filter_input.isEnabled() and self.filter_input.isVisibleTo(self): # Check if it's sensible to set focus
            self.filter_input.setFocus()
        # ---- End of added line ----

    def showEvent(self, event: Any): # QShowEvent, but Any for less specific import
        super().showEvent(event)
        if self.model.columnCount(QModelIndex()) > 0 and self.isVisible():
            if not self._initial_resize_done:
                logger.debug(f"View: showEvent triggered for '{self.tab_name}', performing initial layout.")
                QTimer.singleShot(10, self._perform_layout_adjustments_after_data)
                # self._initial_resize_done = True # This is set in _on_model_update_complete now
    
    def closeEvent(self, event: Any): # QCloseEvent
        logger.info(f"View: closeEvent for '{self.tab_name}'. Quitting worker thread.")
        if self._worker_thread.isRunning():
            self._worker_thread.quit()
            if not self._worker_thread.wait(3000):
                logger.warning(f"View: Worker thread for '{self.tab_name}' did not quit gracefully. Terminating.")
                self._worker_thread.terminate()
                self._worker_thread.wait()
        super().closeEvent(event)

    def _perform_layout_adjustments_after_data(self):
        logger.debug(f"View: Performing layout adjustments for '{self.tab_name}'.")
        if self.model.columnCount(QModelIndex()) == 0:
            logger.debug("View: Layout adjustment skipped, no columns in model.")
            return
        try:
            h_header = self.table_view.horizontalHeader()
            self.table_view.resizeColumnsToContents()

            max_col_width = self.DEFAULT_MAX_COLUMN_WIDTH if self.DEFAULT_MAX_COLUMN_WIDTH is not None else sys.maxsize
            min_header_text_width = 50
            fm = h_header.fontMetrics()
            padding = 30

            for col_idx in range(h_header.count()):
                current_width = h_header.sectionSize(col_idx)
                header_text = str(self.model.headerData(col_idx, Qt.Horizontal, Qt.DisplayRole) or "")
                required_header_width = fm.horizontalAdvance(header_text) + padding
                required_header_width = max(min_header_text_width, required_header_width)
                final_width = max(current_width, required_header_width)
                final_width = min(final_width, max_col_width)
                if h_header.sectionSize(col_idx) != final_width:
                     h_header.resizeSection(col_idx, final_width)
            
            if self.ENABLE_TEXT_WRAPPING:
                QTimer.singleShot(0, self._resize_loaded_rows_and_apply_constraints)
        except Exception as e: 
            logger.error(f"View: Error during layout adjustments for '{self.tab_name}': {e}", exc_info=True)

    def _resize_loaded_rows_and_apply_constraints(self):
        """ Resizes rows from 0 to model._loaded_rows - 1 and applies constraints. """
        logger.debug(f"View: Resizing loaded rows (0 to {self.model._loaded_rows -1}) and applying constraints for '{self.tab_name}'.")
        if self.model.rowCount() == 0 or not self.ENABLE_TEXT_WRAPPING: return

        start_row_to_resize = 0 
        end_row_to_resize = self.model._loaded_rows -1

        if start_row_to_resize > end_row_to_resize: return

        for i in range(start_row_to_resize, end_row_to_resize + 1):
            if not self.table_view.verticalHeader().isSectionHidden(i):
                self.table_view.resizeRowToContents(i)
        self._apply_max_row_height_constraints_to_range(start_row_to_resize, end_row_to_resize)

    @pyqtSlot(int, int, int)
    def _handle_column_resize(self, logicalIndex: int, oldSize: int, newSize: int):
        max_width = self.DEFAULT_MAX_COLUMN_WIDTH
        if max_width is not None and newSize > max_width:
            QTimer.singleShot(0, lambda lidx=logicalIndex, mwidth=max_width: 
                              self.table_view.horizontalHeader().resizeSection(lidx, mwidth))

    def _apply_max_row_height_constraints_to_range(self, start_row: int, end_row: int):
        if self.MAX_ROW_HEIGHT is None or self.MAX_ROW_HEIGHT <= 0: return
        for i in range(start_row, end_row + 1):
             if i >= self.model.rowCount(): break 
             if self.table_view.verticalHeader().isSectionHidden(i): continue
             current_height = self.table_view.rowHeight(i)
             if current_height > self.MAX_ROW_HEIGHT:
                 self.table_view.setRowHeight(i, self.MAX_ROW_HEIGHT)
        logger.debug(f"View: Max row height constraints applied for range {start_row}-{end_row} on '{self.tab_name}'.")

    def get_current_status_message(self) -> str:
        if not hasattr(self, 'model') or self.model is None: return "Model not available."
        view_df = self.model._dataframe 
        loaded_rows_in_view = self.model._loaded_rows
        total_rows_in_view_df = len(view_df) if view_df is not None else 0
        filter_text_active = self.filter_input.text() if hasattr(self, 'filter_input') else ""

        if self._is_currently_processing: return self.status_bar.currentMessage() or "Processing data..."
        if total_rows_in_view_df == 0:
            return f"No data matches filter: '{filter_text_active}'." if filter_text_active else "No data to display."
        else:
            status = f"{loaded_rows_in_view} of {total_rows_in_view_df} rows shown."
            if filter_text_active: status += f" Filter: '{filter_text_active}'."
            return status
        return "Ready."

    @pyqtSlot(QItemSelection, QItemSelection)
    def _on_table_selection_changed(self, selected: QItemSelection, deselected: QItemSelection):
        if self._is_currently_processing: return
        selection_model = self.table_view.selectionModel()
        if not selection_model: return
        selected_indexes = selection_model.selectedIndexes()
        num_selected_cells = len(selected_indexes)

        if num_selected_cells == 1:
            idx = selected_indexes[0]
            if idx.isValid():
                value_raw = self.model.data(idx, Qt.EditRole) 
                value_display = self.model.data(idx, Qt.DisplayRole)
                col_name = str(self.model.headerData(idx.column(), Qt.Horizontal, Qt.DisplayRole) or f"Col {idx.column()}")
                row_num_display = idx.row() + 1 
                type_name = type(value_raw).__name__
                display_val_str = str(value_display); limit = 50
                if len(display_val_str) > limit: display_val_str = display_val_str[:limit-3] + "..."
                self.update_status_bar(f"Cell ({row_num_display}, {col_name}) | Type: {type_name} | Val: '{display_val_str}'")
            else: self.update_status_bar(self.get_current_status_message())
        elif num_selected_cells > 1:
            try:
                unique_rows = len(set(idx.row() for idx in selected_indexes))
                unique_cols = len(set(idx.column() for idx in selected_indexes))
                self.update_status_bar(f"{num_selected_cells} cells selected in {unique_rows}R x {unique_cols}C area.")
            except Exception: self.update_status_bar(f"{num_selected_cells} cells selected.")
        else: self.update_status_bar(self.get_current_status_message())

    def update_status_bar(self, message: str):
        try:
            if self.status_bar: self.status_bar.showMessage(str(message), 0)
        except RuntimeError: pass 
        except Exception as e: logger.warning(f"View: Failed to update status bar for '{self.tab_name}': {e}")

    @pyqtSlot(int)
    def _check_scroll_to_load_more(self, value: int):
        scrollbar = self.table_view.verticalScrollBar()
        if not scrollbar or self.model._dataframe.empty: return

        trigger_threshold = scrollbar.maximum() - (scrollbar.pageStep() * 1.5) 
        trigger_threshold = max(0, trigger_threshold) 

        if value >= trigger_threshold and self.model._loaded_rows < len(self.model._dataframe):
            logger.debug(f"View: Scroll near bottom for '{self.tab_name}', loading more rows.")
            current_loaded_rows_before_load = self.model._loaded_rows
            self.model.load_more_rows() # This increases self.model._loaded_rows
            newly_loaded_end_row = self.model._loaded_rows -1
            
            if self.ENABLE_TEXT_WRAPPING and newly_loaded_end_row >= current_loaded_rows_before_load:
                QTimer.singleShot(50, lambda start=current_loaded_rows_before_load, end=newly_loaded_end_row: 
                                  self._resize_specific_row_range_and_apply_constraints(start, end))

    def _resize_specific_row_range_and_apply_constraints(self, start_row: int, end_row: int):
        logger.debug(f"View: Resizing specific row range: {start_row} to {end_row} for '{self.tab_name}'.")
        for i in range(start_row, end_row + 1):
            if i >= self.model.rowCount(): break # Safety check
            if not self.table_view.verticalHeader().isSectionHidden(i):
                self.table_view.resizeRowToContents(i)
        self._apply_max_row_height_constraints_to_range(start_row, end_row)

    @pyqtSlot()
    def _copy_selected_cells_to_clipboard(self):
        if self._is_currently_processing: return
        # ... (rest of copy logic same as before) ...
        selection_model = self.table_view.selectionModel()
        if not selection_model or not selection_model.hasSelection():
            self.update_status_bar("No cells selected to copy.")
            return
        try:
            selected_indexes = selection_model.selectedIndexes()
            if not selected_indexes: return
            min_row = min(idx.row() for idx in selected_indexes)
            max_row = max(idx.row() for idx in selected_indexes)
            min_col = min(idx.column() for idx in selected_indexes)
            max_col = max(idx.column() for idx in selected_indexes)
            row_count = max_row - min_row + 1
            col_count = max_col - min_col + 1
            data_grid = [["" for _ in range(col_count)] for _ in range(row_count)]
            for index in selected_indexes:
                row_relative = index.row() - min_row
                col_relative = index.column() - min_col
                if 0 <= row_relative < row_count and 0 <= col_relative < col_count:
                    value = self.model.data(index, Qt.DisplayRole)
                    data_grid[row_relative][col_relative] = str(value if value is not None else "")
            df_to_copy = pd.DataFrame(data_grid)
            clipboard_text = df_to_copy.to_csv(sep='\t', index=False, header=False)
            QApplication.clipboard().setText(clipboard_text)
            self.update_status_bar(f"Copied {len(selected_indexes)} cells to clipboard.")
        except Exception as e:
            logger.error(f"View: Error during copy to clipboard for '{self.tab_name}': {e}", exc_info=True)
            self.update_status_bar(f"Error during copy: {e}")


    @pyqtSlot(QPoint)
    def _show_table_context_menu(self, pos: QPoint):
        if self._is_currently_processing: return
        # ... (rest of context menu logic same as before) ...
        global_pos = self.table_view.mapToGlobal(pos)
        index_at_pos = self.table_view.indexAt(pos)
        context_menu = QMenu(self)
        selection_model = self.table_view.selectionModel()
        has_any_selection = selection_model is not None and selection_model.hasSelection()
        copy_action = context_menu.addAction("Copy Selection (Ctrl+C)")
        copy_action.triggered.connect(self._copy_selected_cells_to_clipboard)
        copy_action.setEnabled(has_any_selection)
        context_menu.addSeparator()
        view_cell_action = context_menu.addAction("View/Copy Cell Content...")
        view_cell_action.setEnabled(index_at_pos.isValid())
        if index_at_pos.isValid():
            view_cell_action.triggered.connect(
                lambda checked=False, idx=index_at_pos: self._show_cell_content_dialog(idx)
            )
        context_menu.exec_(global_pos)

    def _show_cell_content_dialog(self, index: QModelIndex):
        if not index.isValid():
            self.update_status_bar("Cannot open dialog for invalid cell.")
            return
        # ... (rest of cell content dialog logic same as before) ...
        try:
            cell_data_raw = self.model.data(index, Qt.EditRole)
            cell_data_str = str(cell_data_raw if cell_data_raw is not None else "")
            dialog = SelectionDialog(cell_data_str, self)
            dialog.exec_()
        except Exception as e:
            logger.error(f"View: Error opening cell content dialog for '{self.tab_name}': {e}", exc_info=True)
            self.update_status_bar(f"Error opening cell content dialog: {e}")


    @pyqtSlot()
    def _initiate_export_to_excel(self):
        if self._is_currently_processing:
            QMessageBox.information(self, "Operation in Progress", "Please wait for processing to complete.")
            return
        # ... (rest of export logic same as before) ...
        options = QFileDialog.Options()
        # options |= QFileDialog.DontUseNativeDialog # Optional
        default_filename = f"{self.tab_name.replace(' ', '_')}_export.xlsx"
        file_name, _ = QFileDialog.getSaveFileName(
            self, "Save Full Data to Excel", default_filename,
            "Excel Files (*.xlsx);;All Files (*)", options=options
        )
        if file_name:
            self._set_ui_processing_state(True, "Exporting data to Excel...")
            QMetaObject.invokeMethod(self._worker, "perform_export_to_excel", Qt.QueuedConnection,
                                     Q_ARG(str, file_name))

    @pyqtSlot(bool, str)
    def _handle_export_operation_complete(self, success: bool, message: str):
        self._set_ui_processing_state(False)
        # ... (rest of export complete handling same as before) ...
        if success:
            self.update_status_bar(message)
            QMessageBox.information(self, "Export Successful", message)
        else:
            self.update_status_bar(f"Export Failed: {message}")
            QMessageBox.critical(self, "Export Failed", message)
            
    def set_dataframe(self, new_df: Optional[pd.DataFrame]):
        df_to_set = new_df if new_df is not None else pd.DataFrame()
        logger.info(f"View: Setting new DataFrame for tab '{self.tab_name}'. Requested DF shape: {df_to_set.shape}")

        if self._is_currently_processing:
            logger.warning(f"View: Attempted to set DataFrame for '{self.tab_name}' while another operation is in progress. Request ignored.")
            QMessageBox.information(self, "Busy", "The table is currently processing data. Please try again.")
            return

        self._set_ui_processing_state(True, "Loading new data...")
        QMetaObject.invokeMethod(self.model, "update_dataframe", Qt.QueuedConnection,
                                 Q_ARG(object, df_to_set))

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, # DEBUG for more verbose output from example
                        format='%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s')
    main_logger = logging.getLogger(__name__) # Ensure logger uses the new config for the example

    app = QApplication(sys.argv)

    data_size = 50000 # Reduced for quicker example testing, increase to see performance
    main_logger.info(f"Generating sample DataFrame with {data_size} rows...")
    sample_data = {
        'Row ID': range(data_size),
        'Numeric Value': np.random.rand(data_size) * 1000,
        'Category': np.random.choice(['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon'], size=data_size),
        'Description': [f'This is a detailed description for item {i}. It contains several words and might wrap. ' * (np.random.randint(1,4)) for i in range(data_size)],
        'Timestamp': pd.to_datetime(pd.Timestamp('2024-01-01') + pd.to_timedelta(np.random.randint(0, 365*24*60, size=data_size), unit='m')),
        'Flag': np.random.choice([True, False], size=data_size)
    }
    main_df = pd.DataFrame(sample_data)
    main_logger.info("Sample DataFrame generated.")

    # --- Example with QTabWidget ---
    tab_widget_main = QWidget()
    tab_layout = QVBoxLayout(tab_widget_main)
    tabs = QTabWidget()
    tab_layout.addWidget(tabs)

    # Create a few tables
    table1 = FilterableTableView(dataframe=main_df.sample(frac=0.7).reset_index(drop=True), tab_name="Dataset A (70k)")
    table2 = FilterableTableView(dataframe=main_df.sample(frac=0.5).reset_index(drop=True), tab_name="Dataset B (50k)")
    table3 = FilterableTableView(tab_name="Empty Dataset C") # Starts empty
    
    # Load data into table3 later
    QTimer.singleShot(2000, lambda: table3.set_dataframe(main_df.sample(frac=0.3).reset_index(drop=True)))


    tabs.addTab(table1, table1.tab_name)
    tabs.addTab(table2, table2.tab_name)
    tabs.addTab(table3, table3.tab_name)
    
    tab_widget_main.setMinimumSize(1000, 700)
    tab_widget_main.setWindowTitle("Filterable Table View Test with Tabs")
    tab_widget_main.show()
    
    sys.exit(app.exec_())