import sqlite3
import os
import time # For msleep
import traceback # For detailed error logging
import logging # Use standard logging
from PyQt5.QtCore import QObject, pyqtSignal, QThread

logger = logging.getLogger(__name__) # Logger for this module

class ComparisonParameters:
    """Simple class to hold comparison parameters."""
    def __init__(self, db1_path, db2_path, output_db_path):
        self.db1_path = db1_path
        self.db2_path = db2_path
        self.output_db_path = output_db_path

class DbComparatorWorkerSignals(QObject): # Separate signals class
    progress_update = pyqtSignal(str, str)
    finished_comparison = pyqtSignal(str, bool) # Message, DiffsSavedFlag
    error_occurred = pyqtSignal(str) # General error message

class DbComparatorWorker(QObject):
    """
    Worker QObject to perform database comparison in a separate thread.
    """
    def __init__(self, params: ComparisonParameters, parent=None):
        super().__init__(parent)
        self.params = params
        self.signals = DbComparatorWorkerSignals()
        self.stop_requested = False
        self.diffs_found_overall = False

    def _log(self, message, level="info"): 
        self.signals.progress_update.emit(f"[DB Comp Worker] {message}", level) 

    def get_column_list(self, conn, table_ref_with_schema): 
        self._log(f"Getting column list for ATTACHED table reference: {table_ref_with_schema}", "debug")
        cursor = conn.cursor()
        try:
            schema_name, table_name_only = table_ref_with_schema.split('.', 1)
            query = f'PRAGMA {schema_name}.table_info("{table_name_only}");' 
            self._log(f"Executing PRAGMA query: {query}", "debug")
            cursor.execute(query)
            columns = [row[1] for row in cursor.fetchall()] 
            self._log(f"Columns for {table_ref_with_schema}: {columns}", "debug")
            if not columns:
                self._log(f"Warning: No columns found for {table_ref_with_schema}. PRAGMA returned empty.", "warning")
            return columns
        except sqlite3.Error as e:
            self._log(f"SQLite error getting columns for {table_ref_with_schema}: {e}", "error")
            self.signals.error_occurred.emit(f"SQLite error getting columns for {table_ref_with_schema}: {e}")
            return []
        except ValueError: 
            msg = f"Error: Invalid table reference format for PRAGMA: {table_ref_with_schema}. Expected 'schema.table'."
            self._log(msg, "error")
            self.signals.error_occurred.emit(msg)
            return []

    def save_diff_to_db(self, diff_data, db_conn, table_name_original_source, suffix, column_names_from_source, 
                        is_content_mismatch=False, key_column_name=None):
        self._log(f"Attempting to save diff data for {table_name_original_source}_{suffix}. Column names: {column_names_from_source}", "debug")
        if not diff_data: 
            self._log("No diff data to save.", "debug")
            return False 

        cursor = db_conn.cursor()
                
        final_column_names = []
        columns_def_list = []
        column_names_from_source = [col for col in column_names_from_source if col not in (
            "execution_time", "execution_start", "execution_end", "timestamp", "model_id")
            ]
        if is_content_mismatch:
            if not key_column_name:
                self._log("Key column name missing for content mismatch diff table.", "error")
                return False
            final_column_names.append(key_column_name) # Key column first
            columns_def_list.append(f'"{key_column_name}" TEXT')
            for col_name in column_names_from_source: # Original columns (from db1)
                final_column_names.append(f"db1_{col_name}")
                columns_def_list.append(f'"db1_{col_name}" TEXT')
            for col_name in column_names_from_source: # Original columns (from db2) - assuming same schema for content diff
                final_column_names.append(f"db2_{col_name}")
                columns_def_list.append(f'"db2_{col_name}" TEXT')
        else: # For rows only in one DB or schema diffs
            if not column_names_from_source: # Should not happen if schema diff has its own cols
                self._log(f"No column names provided for '{table_name_original_source}_{suffix}'. Cannot create table.", "error")
                return False
            final_column_names = column_names_from_source
            columns_def_list = [f'"{col_name}" TEXT' for col_name in column_names_from_source]
        
        columns_def_str = ', '.join(columns_def_list)
        # columns_def_str = ', '.join(col for col in columns_def_list if col != "execution_time")

        if not columns_def_str.strip(): 
            self._log(f"Empty column definition string for '{table_name_original_source}_{suffix}'. Cannot create table.", "error")
            return False

        safe_table_name_original = "".join(c if c.isalnum() else "_" for c in table_name_original_source)
        safe_suffix = "".join(c if c.isalnum() else "_" for c in suffix)
        diff_table_name_generated = f"{safe_table_name_original}_{safe_suffix}"
        diff_table_name_generated_quoted = f'"{diff_table_name_generated}"'

        try:
            create_query = f"CREATE TABLE IF NOT EXISTS {diff_table_name_generated_quoted} ({columns_def_str});"
            self._log(f"Executing CREATE TABLE: {create_query}", "debug")
            cursor.execute(create_query)
            
            num_columns_expected = len(final_column_names)
            placeholders = ', '.join(['?'] * num_columns_expected)
            
            insert_query = f"INSERT INTO {diff_table_name_generated_quoted} VALUES ({placeholders});"
            self._log(f"Preparing to insert {len(diff_data)} rows using INSERT query: {insert_query}", "debug")
            
            validated_diff_data = []
            for row_idx, row in enumerate(diff_data):
                if len(row) == num_columns_expected:
                    validated_diff_data.append(row)
                else:
                    self._log(f"""Row {row_idx} in diff data for '{diff_table_name_generated}' has {len(row)} columns, 
                              expected {num_columns_expected}. Skipping row. Data: {row}""", "warning")

            if validated_diff_data:
                cursor.executemany(insert_query, validated_diff_data)
                db_conn.commit()
                self._log(f"Saved {len(validated_diff_data)} differing rows to table '{diff_table_name_generated}'", "info")
                self.diffs_found_overall = True 
                return True
            else:
                self._log(f"No valid data rows to insert into '{diff_table_name_generated}'.", "warning")
                return False 
        except sqlite3.Error as e:
            self._log(
                f"SQLite error while saving diff to {diff_table_name_generated}: {e}. Query was: {create_query if 'create_query' in locals() else 'N/A (insert)'}", "error")
            self.signals.error_occurred.emit(f"SQLite error saving diff to {diff_table_name_generated}: {e}")
            return False
    
    def _compare_raw_data_table_bask(self, main_conn,):
        table_name = "raw_bask_port"
        self._log(f"Performing row-by-row comparison for table: \"{table_name}\"", "info")
        cursor = main_conn.cursor()
        
        cols_db1 = self.get_column_list(main_conn, f"db1.{table_name}")
        cols_db2 = self.get_column_list(main_conn, f"db2.{table_name}")

        if not cols_db1 or not cols_db2:
            self._log(f"Could not get columns for '{table_name}' from one or both databases. Skipping row-by-row.", "error")
            return True 

        if set(cols_db1) != set(cols_db2):
            self._log(f"Schema mismatch for '{table_name}'. DB1 cols: {cols_db1}, DB2 cols: {cols_db2}. Saving schema diff.", "warning")
            schema_diff_data = [("DB1 Columns", ", ".join(cols_db1)), ("DB2 Columns", ", ".join(cols_db2))]
            self.save_diff_to_db(schema_diff_data, main_conn, table_name, "schema_diff", ["Property", "Value"])
            return True 
        
        quoted_table_name = f"{table_name}"
        
        try:
            cursor.execute(f"""
                           select distinct proc_id 
                               from (
                                   SELECT DISTINCT proc_id FROM db1.{quoted_table_name}
                                       UNION
                                   SELECT DISTINCT proc_id FROM db2.{quoted_table_name}
                                   )
                           """)
            keys_db1 = [row[0] for row in cursor.fetchall()]

            if self.stop_requested: return False
            self._log(f"Found {len(keys_db1)} unique expressions in tables", "info")

        except sqlite3.Error as e:
            self._log(f"Error fetching unique keys for '{table_name}': {e}", "error", )
            return False

        processed_keys_count = 0
        total_keys_to_process_db1 = len(keys_db1)
        
        for key_val in keys_db1:
            if self.stop_requested: return False
            processed_keys_count += 1
            if processed_keys_count % 500 == 0: 
                 self.signals.progress_update.emit(
                     f"Comparing '{table_name}': {processed_keys_count}/{total_keys_to_process_db1} keys from DB1", "info")
                 QThread.msleep(10) # Allow GUI to update
            self._log(f"Comparing Data for DB1 using {key_val} from {quoted_table_name}")

            query = query = f"""
                Select * from (
                    -- Part 1: Rows in A not in B or mismatches
                    SELECT {cols_db1}, "DB1" as source from db1.{table_name} where proc_id = ?
                    EXCEPT
                    SELECT {cols_db1}, "DB1" as source from db2.{table_name} where proc_id = ?
            
                    UNION
            
                    SELECT {cols_db1}, "DB2" as source from db2.{table_name}  where proc_id = ?
                    EXCEPT
                    SELECT {cols_db1}, "DB2" as source from db1.{table_name} where proc_id = ?
                )
                order  by {cols_db1}
        
            """
            cursor.execute(query, (key_val, key_val, key_val, key_val))
            output = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            # Step 5: Save actual mismatches
            if len(output)>0:
                print("writing mismatches")
                self.save_diff_to_db(output, main_conn, "raw_bask_port", "mismatches", column_names)

        self.signals.progress_update.emit(
            f"Comparing '{table_name}': Processed all {total_keys_to_process_db1} keys from DB1. Checking keys only in DB2...", "info")
        QThread.msleep(10)
        return True

    def _compare_raw_data_table_exp(self, main_conn,):
        table_name = "raw_data_exp"
        self._log(f"Performing row-by-row comparison for table: \"{table_name}\"", "info")
        cursor = main_conn.cursor()
        
        cols_db1 = self.get_column_list(main_conn, f"db1.{table_name}")
        cols_db2 = self.get_column_list(main_conn, f"db2.{table_name}")

        if not cols_db1 or not cols_db2:
            self._log(f"Could not get columns for '{table_name}' from one or both databases. Skipping row-by-row.", "error")
            return True 

        if set(cols_db1) != set(cols_db2):
            self._log(f"Schema mismatch for '{table_name}'. DB1 cols: {cols_db1}, DB2 cols: {cols_db2}. Saving schema diff.", "warning")
            schema_diff_data = [("DB1 Columns", ", ".join(cols_db1)), ("DB2 Columns", ", ".join(cols_db2))]
            self.save_diff_to_db(schema_diff_data, main_conn, table_name, "schema_diff", ["Property", "Value"])
            return True 
        
        quoted_table_name = f'"raw_data_exp"'
        has_period = [col for col in cols_db1 if 'period' in col]
        
        
        join_keys = ["date", "issueid", "expression", "path"]
        if has_period:
            join_keys.append("period")
        
        # Build JOIN ON clause
        join_conditions = " AND ".join([
            f"a.{col} = b.{col}" for col in join_keys
        ])
    
        # Build COALESCEed column list
        coalesced_columns = ",\n    ".join([
            f"COALESCE(a.{col}, b.{col}) AS {col}" for col in join_keys
        ])
        
        # Build mismatch flags
        mismatch_flags = ",\n    ".join([
            f"""CASE
                  WHEN a.{col} IS NULL and b.{col} IS NULL THEN 'N' 
                  WHEN  a.{col} = ''  AND b.{col}= '' THEN 'N'
                  WHEN a.{col} = b.{col} THEN 'N' 
                  ELSE 'Y'
                END AS {col}_mismatch"""
            for col in join_keys
        ])

        try:
            cursor.execute(f"""
                           select distinct expression 
                               from (
                                   SELECT DISTINCT expression FROM db1.{quoted_table_name}
                                       UNION
                                   SELECT DISTINCT expression FROM db2.{quoted_table_name}
                                   )
                           """)
            keys_db1 = [row[0] for row in cursor.fetchall()]

            if self.stop_requested: return False
            self._log(f"Found {len(keys_db1)} unique expressions in tables", "info")

        except sqlite3.Error as e:
            self._log(f"Error fetching unique keys for '{table_name}': {e}", "error")
            return False

        processed_keys_count = 0
        total_keys_to_process_db1 = len(keys_db1)
        
        for key_val in keys_db1:
            if self.stop_requested: return False
            processed_keys_count += 1
            if processed_keys_count % 500 == 0: 
                 self.signals.progress_update.emit(
                     f"Comparing '{table_name}': {processed_keys_count}/{total_keys_to_process_db1} keys from DB1", "info")
                 QThread.msleep(10) # Allow GUI to update
            self._log(f"Comparing Data for DB1 using {key_val} from {quoted_table_name}")

            query = query = f"""
                -- Part 1: Rows in A not in B or mismatches
                SELECT 
                    {coalesced_columns},
                    a.value AS value_a,
                    b.value AS value_b,
                    {mismatch_flags},
                    CASE WHEN COALESCE(a.value, 0) != COALESCE(b.value, 0) THEN 'Y' ELSE 'N' END AS value_mismatch
                FROM db1.{table_name} a
                LEFT JOIN db2.{table_name} b
                  ON {join_conditions}
                WHERE 
                    ({' OR '.join([f"b.{col} IS NULL" for col in join_keys])}
                    OR COALESCE(a.value, 0) != COALESCE(b.value, 0))
                    AND a.expression = ?
        
                UNION
        
                -- Part 2: Rows in B not in A or mismatches
                SELECT 
                    {coalesced_columns},
                    a.value AS value_a,
                    b.value AS value_b,
                    {mismatch_flags},
                    CASE WHEN COALESCE(a.value, 0) != COALESCE(b.value, 0) THEN 'Y' ELSE 'N' END AS value_mismatch
                FROM db2.{table_name} b
                LEFT JOIN db1.{table_name} a
                  ON {join_conditions}
                WHERE 
                    ({' OR '.join([f"a.{col} IS NULL" for col in join_keys])}
                    OR COALESCE(a.value, 0) != COALESCE(b.value, 0))
                    AND b.expression = ?;
            """

            cursor.execute(query, (key_val, key_val))
            output = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            # Step 5: Save actual mismatches
            if len(output)>0:
                print("writing mismatches")
                self.save_diff_to_db(output, main_conn, "raw_exp", "mismatches", column_names)

        self.signals.progress_update.emit(
            f"Comparing '{table_name}': Processed all {total_keys_to_process_db1} keys from DB1. Checking keys only in DB2...", "info")
        QThread.msleep(10)
        return True

    def _compare_table_data_with_except(self, main_conn, table_name_original_source):
        self._log(f"Performing EXCEPT comparison for table: \"{table_name_original_source}\"", "info")
        if self.stop_requested:
            self._log(f"Stop requested, skipping table: {table_name_original_source}", "warning")
            return False 

        diff1_count = 0
        diff2_count = 0
        table_processed_successfully = True
        
        quoted_table_name = f'"{table_name_original_source}"'

        cols_db1_source = self.get_column_list(main_conn, f"db1.{table_name_original_source}") 
        if not cols_db1_source:
            self._log(f"Could not get columns for source table db1.{table_name_original_source}. Skipping EXCEPT comparison.", "error")
            return True 

        cols_db2_source = self.get_column_list(main_conn, f"db2.{table_name_original_source}")
        if not cols_db2_source:
            self._log(f"Could not get columns for source table db2.{table_name_original_source}. Skipping EXCEPT comparison.", "error")
            return True

        if set(cols_db1_source) != set(cols_db2_source):
            self._log(f"Column mismatch for table '{table_name_original_source}'. DB1: {cols_db1_source}, DB2: {cols_db2_source}. Saving schema diff.", "warning")
            schema_diff_data = [("DB1 Columns", ", ".join(cols_db1_source)), ("DB2 Columns", ", ".join(cols_db2_source))]
            self.save_diff_to_db(schema_diff_data, main_conn, table_name_original_source, "schema_diff", ["Property", "Value"])
            return True 

       
        try:
            cursor = main_conn.cursor()            
            query = '''
                    -- Rows from A that don't match B or have mismatched values
                SELECT 
                    a.path,
                    a.expression,
                    a.universeName,
                    a.timeframe,
                    a.len_df AS len_df_a,
                    b.len_df AS len_df_b,
                    a.dupe_count AS dupe_count_a,
                    b.dupe_count AS dupe_count_b,
                
                    CASE WHEN b.path IS NULL THEN 'Y' ELSE 'N' END AS path_mismatch,
                    CASE WHEN b.expression IS NULL THEN 'Y' ELSE 'N' END AS expression_mismatch,
                    CASE WHEN b.universeName IS NULL THEN 'Y' ELSE 'N' END AS universe_mismatch,
                    CASE WHEN b.timeframe IS NULL THEN 'Y' ELSE 'N' END AS timeframe_mismatch,
                    CASE WHEN COALESCE(a.len_df,0) != COALESCE(b.len_df,0) THEN 'Y' ELSE 'N' END AS len_df_mismatch,
                    CASE WHEN COALESCE(a.dupe_count,0) != COALESCE(b.dupe_count,0) THEN 'Y' ELSE 'N' END AS dupe_count_mismatch
                
                FROM "API Summary" a
                LEFT JOIN db2."API Summary" b
                  ON a.path = b.path
                  AND a.expression = b.expression
                  AND a.universeName = b.universeName
                  AND a.timeframe = b.timeframe
                WHERE 
                    b.path IS NULL OR b.expression IS NULL OR b.universeName IS NULL OR b.timeframe IS NULL
                    OR COALESCE(a.len_df,0) != COALESCE(b.len_df,0)
                    OR COALESCE(a.dupe_count,0) != COALESCE(b.dupe_count,0)
                
                UNION
                
                -- Rows from B that don't match A (to simulate FULL OUTER JOIN)
                SELECT 
                    b.path,
                    b.expression,
                    b.universeName,
                    b.timeframe,
                    a.len_df AS len_df_a,
                    b.len_df AS len_df_b,
                    a.dupe_count AS dupe_count_a,
                    b.dupe_count AS dupe_count_b,
                
                    CASE WHEN a.path IS NULL THEN 'Y' ELSE 'N' END AS path_mismatch,
                    CASE WHEN a.expression IS NULL THEN 'Y' ELSE 'N' END AS expression_mismatch,
                    CASE WHEN a.universeName IS NULL THEN 'Y' ELSE 'N' END AS universe_mismatch,
                    CASE WHEN a.timeframe IS NULL THEN 'Y' ELSE 'N' END AS timeframe_mismatch,
                    CASE WHEN COALESCE(a.len_df,0) != COALESCE(b.len_df,0) THEN 'Y' ELSE 'N' END AS len_df_mismatch,
                    CASE WHEN COALESCE(a.dupe_count,0) != COALESCE(b.dupe_count,0) THEN 'Y' ELSE 'N' END AS dupe_count_mismatch
                
                FROM db2."API Summary" b
                LEFT JOIN "API Summary" a
                  ON a.path = b.path
                  AND a.expression = b.expression
                  AND a.universeName = b.universeName
                  AND a.timeframe = b.timeframe
                WHERE 
                    a.path IS NULL OR a.expression IS NULL OR a.universeName IS NULL OR a.timeframe IS NULL
                    OR COALESCE(a.len_df,0) != COALESCE(b.len_df,0)
                    OR COALESCE(a.dupe_count,0) != COALESCE(b.dupe_count,0)
                    
            '''

            cursor.execute(query)
            output = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            if output:
                self.save_diff_to_db(output, main_conn, table_name_original_source.replace(" ","_"), "mismatch", column_names)

        except sqlite3.Error as e:
            self._log(f"SQLite error during EXCEPT comparison of table \"{table_name_original_source}\": {e}", "error" )
            self.signals.error_occurred.emit(f"SQLite error EXCEPT comparing table \"{table_name_original_source}\": {e}")
            table_processed_successfully = False 
        
        self._log(f"Finished EXCEPT comparison for table: \"{table_name_original_source}\". Success: {table_processed_successfully}", "debug")
        return table_processed_successfully


    def run_comparison(self):
        self._log("run_comparison started.", "info")
        self.stop_requested = False
        self.diffs_found_overall = False 
        final_message = "Comparison process initiated."
        
        db1_p = self.params.db1_path
        db2_p = self.params.db2_path
        output_p = self.params.output_db_path
        
        if output_p == db1_p or output_p == db2_p:
            errmsg = "The output database path cannot be the same as one of the input database paths."
            self._log(errmsg, "error") 
            self.signals.error_occurred.emit(errmsg)
            final_message = f"Comparison aborted: {errmsg}"
            self.signals.finished_comparison.emit(final_message, False) 
            return

        diff_db_conn_local = None 
        
        try:
            if not os.path.exists(db1_p):
                msg = f"Database file not found: {db1_p}"
                self._log(msg, "error"); self.signals.error_occurred.emit(msg)
                self.signals.finished_comparison.emit("Comparison failed: DB1 not found.", False)
                return 
            if not os.path.exists(db2_p):
                msg = f"Database file not found: {db2_p}"
                self._log(msg, "error"); self.signals.error_occurred.emit(msg)
                self.signals.finished_comparison.emit("Comparison failed: DB2 not found.", False)
                return

            output_dir = os.path.dirname(output_p)
            if output_dir and not os.path.exists(output_dir):
                try:
                    os.makedirs(output_dir, exist_ok=True)
                    self._log(f"Created output directory: {output_dir}", "info")
                except OSError as e:
                    msg = f"Could not create output directory {output_dir}: {e}"
                    self._log(msg, "error"); self.signals.error_occurred.emit(msg)
                    self.signals.finished_comparison.emit("Comparison failed: Could not create output directory.", False)
                    return
            
            diff_db_conn_local = sqlite3.connect(output_p) 
            self._log(f"Differences will be saved in: {output_p} (if any found)", "info")

            sanitized_db1_path = db1_p.replace("'", "''") 
            sanitized_db2_path = db2_p.replace("'", "''") 
            self._log(f"Attaching DB1: '{db1_p}' as db1", "debug") 
            diff_db_conn_local.execute(f"ATTACH DATABASE '{sanitized_db1_path}' AS db1")
            self._log(f"Attaching DB2: '{db2_p}' as db2", "debug") 
            diff_db_conn_local.execute(f"ATTACH DATABASE '{sanitized_db2_path}' AS db2")
            
            cursor_main = diff_db_conn_local.cursor()
            tables1, tables2 = set(), set()
            cursor_main.execute('SELECT name FROM db1.sqlite_master WHERE type="table" AND name in ("API Summary", "raw_data_exp","raw_bask_port")')
            tables1 = set(row[0] for row in cursor_main.fetchall())
            cursor_main.execute('SELECT name FROM db1.sqlite_master WHERE type="table" AND name in ("API Summary", "raw_data_exp","raw_bask_port")')
            tables2 = set(row[0] for row in cursor_main.fetchall())
            common_tables = sorted(list(tables1.intersection(tables2)))

            if not common_tables:
                self._log("No common tables found to compare.", "warning")
                final_message = "Comparison complete: No common tables found."
            else:
                self._log(f"Found {len(common_tables)} common tables: {', '.join(common_tables)}", "info")
                all_tables_processed_without_error_or_interruption = True
                for table_idx, table in enumerate(common_tables):
                    self.signals.progress_update.emit(f"Comparing table {table_idx+1}/{len(common_tables)}: {table}", "info")
                    if self.stop_requested:
                        self._log("Comparison interrupted by user request.", "warning")
                        final_message = "Comparison process was interrupted."
                        all_tables_processed_without_error_or_interruption = False
                        break 
                    
                    # Call the appropriate comparison method based on table name
                    if table in ("raw_data_exp"):
                        comparison_successful = self._compare_raw_data_table_exp(diff_db_conn_local, )
                    elif table in ("raw_bask_port"):
                        comparison_successful = self._compare_raw_data_table_bask(diff_db_conn_local, )
                    elif table in ("API Summary"):
                        comparison_successful = self._compare_table_data_with_except(diff_db_conn_local, table)

                    if not comparison_successful: 
                        if self.stop_requested:
                            final_message = "Comparison process was interrupted during table processing."
                        else:
                            final_message = f"Comparison stopped due to error in table '{table}'."
                        all_tables_processed_without_error_or_interruption = False
                        break 
                
                if all_tables_processed_without_error_or_interruption:
                    if self.diffs_found_overall:
                        final_message = "Database comparison complete. Differences found and saved."
                    else:
                        final_message = "Database comparison complete. No differences found in common tables."
            
            try:
                diff_db_conn_local.execute("DETACH DATABASE db1")
                diff_db_conn_local.execute("DETACH DATABASE db2")
            except sqlite3.Error as e_detach:
                self._log(f"Error detaching databases: {e_detach}", "warning")


            if not self.diffs_found_overall: 
                if diff_db_conn_local: 
                    diff_db_conn_local.close()
                    diff_db_conn_local = None 
                if os.path.exists(output_p): 
                    try:
                        os.remove(output_p)
                        self._log(f"Removed empty output file (no differences found): {output_p}", "info")
                    except Exception as e_remove: 
                        self._log(f"Could not remove empty diff file {output_p}: {e_remove}", "warning")
        
        except sqlite3.Error as e:
            msg = f"A top-level SQLite error occurred: {e}"
            self._log(msg, "error", )
            self.signals.error_occurred.emit(msg)
            final_message = f"Comparison failed with SQLite error: {e}"
        except Exception as e_main:
            tb_str = traceback.format_exc()
            msg = f"An unexpected error occurred in run_comparison: {e_main}\n{tb_str}"
            self._log(msg, "critical"); self.signals.error_occurred.emit(msg) 
            final_message = f"Comparison failed with an unexpected error: {e_main}"
        finally:
            if diff_db_conn_local: diff_db_conn_local.close()
            
            self._log(f"run_comparison finished. Emitting finished_comparison with: '{final_message}', DiffsFoundOverall: {self.diffs_found_overall}", "info")
            self.signals.finished_comparison.emit(final_message, self.diffs_found_overall)
