import logging
from PyQt5.QtCore import QObject, pyqtSignal
import sqlite3 
import time # For simulating work or if needed
import traceback # For detailed error logging


logger = logging.getLogger(__name__)

class DiffViewerTaskType:
    LIST_TABLES = 1
    GET_ROW_COUNT = 2
    GET_COLUMNS = 3
    GET_PAGE_DATA = 4
    EXPORT_TABLE_TO_EXCEL_DATA = 5 # New task type

    @staticmethod
    def get_task_name(task_id):
        names = {
            DiffViewerTaskType.LIST_TABLES: "LIST_TABLES",
            DiffViewerTaskType.GET_ROW_COUNT: "GET_ROW_COUNT",
            DiffViewerTaskType.GET_COLUMNS: "GET_COLUMNS",
            DiffViewerTaskType.GET_PAGE_DATA: "GET_PAGE_DATA",
            DiffViewerTaskType.EXPORT_TABLE_TO_EXCEL_DATA: "EXPORT_TABLE_TO_EXCEL_DATA", # New
        }
        return names.get(task_id, f"UNKNOWN_TASK_ID_{task_id}")

class DiffViewerWorker(QObject):
    # Standard signals
    tables_list_ready = pyqtSignal(list)
    column_names_ready = pyqtSignal(str, list)    # table_name, column_names
    page_data_ready = pyqtSignal(str, int, list)  # table_name, page_number, data_rows
    total_rows_ready = pyqtSignal(str, int)       # table_name, count
    error_occurred = pyqtSignal(str, str)         # task_description, error_message
    task_finished = pyqtSignal(int)               # task_type that finished

    # New signal for Excel export data
    excel_export_data_ready = pyqtSignal(str, list, list) # table_name, column_names, all_data_rows

    def __init__(self, db_path, parent=None): # Added parent=None for QObject constructor
        super().__init__(parent) # Call QObject constructor
        self.db_path = db_path
        self.stop_requested_flag = False
        logger.debug(f"DiffViewerWorker initialized for DB: {self.db_path}")


    def request_stop(self):
        logger.debug("DiffViewerWorker stop requested.")
        self.stop_requested_flag = True

    def get_task_name_str(self, task_type_id):
        return DiffViewerTaskType.get_task_name(task_type_id)

    def run_task(self, task_type, table_name=None, page_number=0, rows_per_page=0, export_limit=50000):
        if self.stop_requested_flag:
            logger.info(f"Task {self.get_task_name_str(task_type)} for table '{table_name}' cancelled before start.")
            self.task_finished.emit(task_type)
            return

        conn = None 
        task_description = f"Task: {self.get_task_name_str(task_type)}"
        if table_name:
            task_description += f" on table '{table_name}'"
        
        logger.info(f"DiffViewerWorker: Starting {task_description}, Page: {page_number}, RowsPerPage: {rows_per_page}, ExportLimit: {export_limit}")


        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            if self.stop_requested_flag: raise InterruptedError("Task stopped by request before execution.")

            if task_type == DiffViewerTaskType.LIST_TABLES:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
                tables = [row[0] for row in cursor.fetchall()]
                if self.stop_requested_flag: raise InterruptedError("Task stopped during LIST_TABLES.")
                self.tables_list_ready.emit(tables)

            elif task_type == DiffViewerTaskType.GET_ROW_COUNT:
                if not table_name: raise ValueError("Table name required for GET_ROW_COUNT")
                query = f"SELECT COUNT(*) FROM [{table_name.replace(']', ']]')}]" 
                cursor.execute(query)
                count = cursor.fetchone()[0]
                if self.stop_requested_flag: raise InterruptedError("Task stopped during GET_ROW_COUNT.")
                self.total_rows_ready.emit(table_name, count)

            elif task_type == DiffViewerTaskType.GET_COLUMNS:
                if not table_name: raise ValueError("Table name required for GET_COLUMNS")
                cursor.execute(f"PRAGMA table_info([{table_name.replace(']', ']]')}])")
                columns_info = cursor.fetchall()
                column_names = [info[1] for info in columns_info]
                if self.stop_requested_flag: raise InterruptedError("Task stopped during GET_COLUMNS.")
                self.column_names_ready.emit(table_name, column_names)

            elif task_type == DiffViewerTaskType.GET_PAGE_DATA:
                if not table_name: raise ValueError("Table name required for GET_PAGE_DATA")
                if rows_per_page <= 0 : raise ValueError("rows_per_page must be positive for GET_PAGE_DATA")
                offset = page_number * rows_per_page

                query = f"SELECT * FROM [{table_name.replace(']', ']]')}] LIMIT {int(rows_per_page)} OFFSET {int(offset)}"

                cursor.execute(query)
                results = cursor.fetchall()
                if self.stop_requested_flag: raise InterruptedError("Task stopped during GET_PAGE_DATA.")
                self.page_data_ready.emit(table_name, page_number, results)
            
            elif task_type == DiffViewerTaskType.EXPORT_TABLE_TO_EXCEL_DATA:
                if not table_name: raise ValueError("Table name required for EXPORT_TABLE_TO_EXCEL_DATA")
                
                cursor.execute(f"PRAGMA table_info([{table_name.replace(']', ']]')}])")
                columns_info = cursor.fetchall()
                column_names = [info[1] for info in columns_info]

                if not column_names:
                    raise ValueError(f"Could not retrieve column names for table '{table_name}'.")
                
                if self.stop_requested_flag: raise InterruptedError("Task stopped during export (getting columns).")

                query = f"SELECT * FROM [{table_name.replace(']', ']]')}]"
                if export_limit > 0:
                    query += f" LIMIT {int(export_limit)}"
                
                cursor.execute(query)
                all_rows = cursor.fetchall()
                
                if self.stop_requested_flag: raise InterruptedError("Task stopped during export (fetching data).")
                
                self.excel_export_data_ready.emit(table_name, column_names, all_rows)

            else:
                raise ValueError(f"Unknown task type: {task_type}")

        except sqlite3.Error as e_sql:
            logger.error(f"SQLite error in DiffViewerWorker task '{task_description}': {e_sql}", exc_info=True)
            self.error_occurred.emit(task_description, f"SQLite error: {e_sql}")
        except InterruptedError as e_interrupt: 
            logger.info(f"DiffViewerWorker task '{task_description}' interrupted: {e_interrupt}")
            self.error_occurred.emit(task_description, f"Operation interrupted: {e_interrupt}")
        except ValueError as e_val:
            logger.error(f"ValueError in DiffViewerWorker task '{task_description}': {e_val}", exc_info=True)
            self.error_occurred.emit(task_description, f"Value error: {e_val}")
        except Exception as e:
            logger.error(f"Unexpected error in DiffViewerWorker task '{task_description}': {e}", exc_info=True)
            self.error_occurred.emit(task_description, f"Unexpected error: {e}")
        finally:
            if conn:
                conn.close()
            logger.debug(f"DiffViewerWorker: Emitting task_finished for task type {task_type}") 
            self.task_finished.emit(task_type)