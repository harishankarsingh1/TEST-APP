#sql_insert.py
import datetime  # Added for type checking
import decimal
import json  # For serializing complex dictionary values like 'expression'
import sqlite3
import threading

import numpy as np  # Added for type checking
import pandas as pd

# --- Global variables for buffering ---
# These should be initialized in your main script or context.
# DB_PATH: Path to the SQLite database file.
# DATAFRAME_BUFFER: List to hold DataFrames before flushing.
# METADATA_BUFFER: List to hold result dictionaries before flushing.
# CURRENT_BUFFER_SIZE_BYTES: Tracks the approximate size of data in buffers.
# BUFFER_LIMIT_BYTES: The threshold (e.g., 100MB) to trigger a flush.
# BUFFER_LOCK: A lock to ensure thread-safe access to buffers.

DATAFRAME_BUFFER = []
METADATA_BUFFER = []
CURRENT_BUFFER_SIZE_BYTES = 0
BUFFER_LIMIT_BYTES = 300 * 1024 * 1024  # 50 MB (corrected from 100MB in comment to match value)
BUFFER_LOCK = threading.Lock()

# --- Utility Functions ---

def get_sqlite_type(dtype):
    """Maps pandas dtype to SQLite type."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "INTEGER"  # SQLite uses 0 and 1 for booleans
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TEXT" # Store datetimes as ISO format strings
    # Add other specific type mappings if needed (e.g., pd.Timedelta)
    else:
        return "TEXT"

def table_exists(conn, table_name):
    """Checks if a table exists in the SQLite database."""
    cursor = conn.cursor()
    print(table_name)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cursor.fetchone() is not None

def ensure_table_columns(conn, table_name, data_source, is_df=True, log_emitter=None):
    """
    Ensures all columns from a DataFrame or all keys from a list of dictionaries
    exist in the SQLite table. Adds missing columns and a primary key if applicable.

    Args:
        conn: Active SQLite connection.
        table_name (str): The name of the table to check/alter.
        data_source: A pandas DataFrame (if is_df=True) or a list of dictionaries (if is_df=False).
        is_df (bool): True if data_source is a DataFrame, False if it's a list of dicts.
        log_emitter: Optional logging function.
    """
    logger = log_emitter or print
    cursor = conn.cursor()
    
    # Check if table exists
    if not table_exists(conn, table_name):
        # Create table from DataFrame
        if is_df and not data_source.empty:
            cols_sql = ", ".join([f'"{col}" {get_sqlite_type(data_source[col].dtype)}' for col in data_source.columns])
            if cols_sql:
                # Determine primary key columns
                pk_columns = ["date", "path","date_", "issueId", "captialiq_tradingitem", "tradingitemid","issueId_",
                              "issue_id", "expression", "id", "proc", "proc_id", "proc_sql", 
                              "unknown","period","issueid"]
                defined_columns = {col.split()[0].strip('"') for col in cols_sql.split(",")}
                available_columns = [col for col in pk_columns if col in defined_columns]
                
                if available_columns:
                    pk_clause = 'PRIMARY KEY (' + ', '.join([f'"{col}"' for col in available_columns]) + ')'
                    cols_sql += ", " + pk_clause
                
                cursor.execute(f'CREATE TABLE "{table_name}" ({cols_sql})')
                conn.commit()
                logger(f"Created table '{table_name}' with initial columns from DataFrame.")
            else:
                logger(f"Warning: DataFrame for new table '{table_name}' has no columns. Table not created.")
                return

        # Create table from list of dictionaries
        elif not is_df and data_source:
            # Infer schema from all keys present in all dictionaries
            all_keys_in_source = set()
            for item in data_source:
                all_keys_in_source.update(item.keys())

            if not all_keys_in_source:
                logger(f"Warning: List of dictionaries for new table '{table_name}' is empty or items have no keys. Table not created.")
                return

            cols_sql_parts = []
            for key in all_keys_in_source:
                col_type = "TEXT"  # Default type
                for item in data_source:
                    sample_val = item.get(key)
                    if sample_val is not None:
                        if isinstance(sample_val, bool): col_type = "INTEGER"; break
                        elif isinstance(sample_val, (int, np.integer)): col_type = "INTEGER"; break
                        elif isinstance(sample_val, (float, np.floating)): col_type = "REAL"; break
                        elif isinstance(sample_val, (list, dict)): col_type = "TEXT"; break  # JSON
                        elif isinstance(sample_val, (datetime.datetime, datetime.date)): col_type = "TEXT"; break
                cols_sql_parts.append(f'"{key}" {col_type}')

            # Add primary key if applicable
            pk_columns = ["date", "path","date_", "issueId", "captialiq_tradingitem", "tradingitemid",
                              "issue_id", "expression", "id", "proc", "proc_id", "proc_sql", 
                              "unknown","period", "issueid"]
            defined_columns = {col.split()[0].strip('"') for col in cols_sql_parts}
            available_columns = [col for col in pk_columns if col in defined_columns]
            if available_columns:
                pk_clause = 'PRIMARY KEY (' + ', '.join([f'"{col}"' for col in available_columns]) + ')'
                cols_sql_parts.append(pk_clause)

            # Create the table
            cursor.execute(f'CREATE TABLE "{table_name}" ({", ".join(cols_sql_parts)})')
            conn.commit()
            logger(f"Created table '{table_name}' with initial columns from dictionaries.")
        else:
            logger(f"Warning: Table '{table_name}' does not exist and no data provided to infer schema.")
            return  # No data to create table

    # Fetch existing columns in the table
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    existing_columns = {row[1] for row in cursor.fetchall()}

    new_columns_added = False
    if is_df:
        df_columns = data_source.columns
        for col in df_columns:
            if col not in existing_columns:
                col_type = get_sqlite_type(data_source[col].dtype)
                try:
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {col_type}')
                    existing_columns.add(col)
                    new_columns_added = True
                    logger(f"Added column '{col}' (type {col_type}) to table '{table_name}'.")
                except sqlite3.OperationalError as e:
                    logger(f"Could not add column '{col}' to '{table_name}': {e}")
    else:
        all_keys = set()
        for item in data_source:
            all_keys.update(item.keys())

        for key in all_keys:
            if key not in existing_columns:
                col_type = "TEXT"  # Default to TEXT
                for item in data_source:
                    value = item.get(key)
                    if value is not None:
                        if isinstance(value, bool): col_type = "INTEGER"; break
                        elif isinstance(value, (int, np.integer)): col_type = "INTEGER"; break
                        elif isinstance(value, (float, np.floating)): col_type = "REAL"; break
                        elif isinstance(value, (list, dict)): col_type = "TEXT"; break
                        elif isinstance(value, (datetime.datetime, datetime.date)): col_type = "TEXT"; break
                        break  # Found a value, exit loop
                try:
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{key}" {col_type}')
                    existing_columns.add(key)
                    new_columns_added = True
                    logger(f"Added column '{key}' (type {col_type}) to table '{table_name}'.")
                except sqlite3.OperationalError as e:
                    logger(f"Could not add column '{key}' to '{table_name}': {e}")
    if table_name =="raw_data_exp":
        # cursor.execute('''
        #     CREATE INDEX IF NOT EXISTS idx_raw_data_all
        #         ON raw_data_exp("date", "issueId", "expression", "value", "path");
        # ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_raw_data_exp
                ON raw_data_exp("expression");
''')
        conn.commit()
    elif table_name =="raw_bask_port":
                cursor.execute('''
                       CREATE INDEX IF NOT EXISTS idx_raw_data_all ON raw_bask_port("id");
                       ''')

    if new_columns_added:
        conn.commit()

def flush_buffers_to_sqlite(db_conn_path, df_table_name="raw_data", metadata_table_name="metadata", log_emitter=None):
    """
    Writes the content of DATAFRAME_BUFFER and METADATA_BUFFER to SQLite.
    This function assumes it's called with BUFFER_LOCK acquired.
    """
    global DATAFRAME_BUFFER, METADATA_BUFFER, CURRENT_BUFFER_SIZE_BYTES

    logger = log_emitter or print

    if not DATAFRAME_BUFFER and not METADATA_BUFFER:
        logger("Buffers are empty. Nothing to flush.")
        return

    conn = None
    try:
        conn = sqlite3.connect(db_conn_path, timeout=20.0) # Increased timeout
        # conn.execute("PRAGMA journal_mode=WAL;") # Optional: For potentially better concurrency, but can have complexities
        cursor = conn.cursor()

        # --- Process DataFrames ---
        if DATAFRAME_BUFFER:
            for i, df_to_write in enumerate(DATAFRAME_BUFFER):
                if not df_to_write.empty:
                    ensure_table_columns(conn, df_table_name, df_to_write, is_df=True, log_emitter=logger)
                    try:
                        # Ensure datetime64 columns are converted to strings if not already
                        for col in df_to_write.select_dtypes(include=['datetime64[ns]']).columns:
                            df_to_write[col] = df_to_write[col].astype(str) # Or .dt.isoformat()
                            
                        # Convert Decimal values to float
                        for col in df_to_write.columns:
                            if df_to_write[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
                                df_to_write[col] = df_to_write[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x) 
                                
                        # Ensure boolean columns are 0/1 for SQLite
                        for col in df_to_write.select_dtypes(include=['bool']).columns:
                            df_to_write[col] = df_to_write[col].astype(int)
                            
                        for col in df_to_write.select_dtypes(include=['object']).columns:
                            df_to_write[col] = df_to_write[col].apply(
                                lambda x: json.dumps(x) if isinstance(x, (list, dict))
                                else str(x) if not pd.isnull(x) 
                                else None
                            )
                        df_to_write = df_to_write.replace([np.inf, -np.inf], np.nan)
                        df_to_write = df_to_write.where(pd.notnull(df_to_write), None)
                        df_to_write.to_sql(df_table_name, conn, if_exists='append', index=False)
                        logger(f"Flushed DataFrame {i+1}/{len(DATAFRAME_BUFFER)} ({len(df_to_write)} rows) to table '{df_table_name}'.")
                    except Exception as e:
                        logger(f"Error writing DataFrame to SQL table '{df_table_name}': {e}")
                        logger(str(df_to_write.columns))
                        
                else:
                    logger(f"Skipped empty DataFrame {i+1}/{len(DATAFRAME_BUFFER)}.")
            DATAFRAME_BUFFER.clear()

        # --- Process Metadata ---
        if METADATA_BUFFER:
            # Ensure all columns exist based on all keys in the current metadata buffer
            ensure_table_columns(conn, metadata_table_name, METADATA_BUFFER, is_df=False, log_emitter=logger)

            # Fetch valid columns once before the loop if table structure is stable after ensure_table_columns
            cursor.execute(f'PRAGMA table_info("{metadata_table_name}")')
            valid_columns = {row[1] for row in cursor.fetchall()}

            for i, meta_dict_original in enumerate(METADATA_BUFFER):
                meta_dict_processed = {} # Store processed key-value pairs

                for key, value in meta_dict_original.items():
                    if key not in valid_columns: # Skip keys that don't have a corresponding column
                        # logger(f"Skipping key '{key}' from metadata item {i+1} as no column exists in '{metadata_table_name}'.")
                        continue                    

                    if isinstance(value, (datetime.datetime, datetime.date)):
                        meta_dict_processed[key] = value.isoformat()
                    elif isinstance(value, str):
                        meta_dict_processed[key] = value
                    elif isinstance(value, (list, dict)):
                        meta_dict_processed[key] = json.dumps(value)
                    elif isinstance(value, (int,np.integer)):
                        meta_dict_processed[key] = int(value)
                    elif isinstance(value, np.floating):
                        meta_dict_processed[key] = float(value)
                    elif isinstance(value, np.bool_):
                        meta_dict_processed[key] = int(value)
                    elif isinstance(value, bool):
                        meta_dict_processed[key] = int(value)
                    # Add other specific type conversions if needed
                    # elif isinstance(value, decimal.Decimal):
                    #    meta_dict_processed[key] = str(value)
                    else:
                        meta_dict_processed[key] = value # Assume str, int, float, None, or bytes

                if not meta_dict_processed:
                    logger(f"Skipping metadata item {i+1}/{len(METADATA_BUFFER)} as it has no keys matching table columns after processing. Original keys: {list(meta_dict_original.keys())}")
                    continue

                cols = ', '.join(f'"{k}"' for k in meta_dict_processed.keys())
                placeholders = ', '.join(['?'] * len(meta_dict_processed))
                sql = f'INSERT INTO "{metadata_table_name}" ({cols}) VALUES ({placeholders})'

                try:
                    cursor.execute(sql, list(meta_dict_processed.values()))
                except sqlite3.Error as e:
                    logger(f"Error inserting metadata into '{metadata_table_name}': {e}. SQL: {sql}. Data: {meta_dict_processed}")
                    

            conn.commit()
            # logger(f"Flushed {len(METADATA_BUFFER)} metadata records to table '{metadata_table_name}'.")
            METADATA_BUFFER.clear()

        CURRENT_BUFFER_SIZE_BYTES = 0 # Reset buffer size
        logger("Buffers flushed and cleared.")

    except sqlite3.Error as e:
        import traceback
        logger(f"SQLite error during flush to '{db_conn_path}': {e} \nTraceback: {traceback.format_exc()}")
        # Consider re-queueing or saving to temp file if critical. For now, logs error.
    finally:
        if conn:
            conn.close()

def save_to_sqlite_buffered(df, result_dict,
                            db_conn_path, # Path to the SQLite DB file
                            df_table_name="raw_data",
                            metadata_table_name="metadata",
                            log_emitter=None, cancel_event=None):
    """
    Adds DataFrame and result dictionary to buffers.
    Flushes buffers to SQLite if their combined size exceeds BUFFER_LIMIT_BYTES.
    This function is thread-safe for buffer modifications.
    """
    global DATAFRAME_BUFFER, METADATA_BUFFER, CURRENT_BUFFER_SIZE_BYTES, BUFFER_LIMIT_BYTES

    logger = log_emitter or print

    if cancel_event and cancel_event.is_set():
        logger("Cancellation detected before buffering data.")
        # Optionally, still buffer a "cancelled" metadata entry if desired
        # if result_dict:
        #     result_dict_copy = result_dict.copy()
        #     result_dict_copy['status_note'] = 'Cancelled before buffering'
        #     # ... (add to METADATA_BUFFER and update size)
        return

    with BUFFER_LOCK: # Acquire lock for modifying shared buffers
        df_size_bytes = 0
        if df is not None and not df.empty:
            try:
                df_size_bytes = df.memory_usage(deep=True).sum()
            except Exception as e: # Handle rare cases where memory_usage might fail
                logger(f"Could not estimate DataFrame memory: {e}. Using 0.")
                df_size_bytes = 0 # Fallback

        dict_size_bytes = 0
        if result_dict:
            try:
                # Estimate dict size by JSONifying it (more accurate for varied content)
                # This is just an estimate; actual SQLite storage might differ.
                temp_serializable_dict = {}
                for k, v in result_dict.items():
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        temp_serializable_dict[k] = v.isoformat()
                    elif isinstance(v, (np.integer, np.floating, np.bool_)):
                        temp_serializable_dict[k] = str(v) # Convert to string for size estimation
                    else:
                        temp_serializable_dict[k] = v
                dict_size_bytes = len(json.dumps(temp_serializable_dict, default=str).encode('utf-8'))
            except Exception as e:
                logger(f"Could not estimate dictionary size: {e}. Using 0.")
                dict_size_bytes = 0


        if df is not None and not df.empty:
            DATAFRAME_BUFFER.append(df.copy()) # Append a copy
            CURRENT_BUFFER_SIZE_BYTES += df_size_bytes
            # logger(f"DataFrame (approx {df_size_bytes / (1024*1024):.2f} MB) added to buffer. DF Buffer items: {len(DATAFRAME_BUFFER)}.")

        if result_dict:
            METADATA_BUFFER.append(result_dict.copy()) # Append a copy
            CURRENT_BUFFER_SIZE_BYTES += dict_size_bytes
            # logger(f"Metadata (approx {dict_size_bytes / 1024:.2f} KB) added to buffer. Meta Buffer items: {len(METADATA_BUFFER)}.")

        # logger(f"Current estimated buffer size: {CURRENT_BUFFER_SIZE_BYTES / (1024*1024):.2f} MB / {BUFFER_LIMIT_BYTES / (1024*1024):.2f} MB limit.")

        if CURRENT_BUFFER_SIZE_BYTES >= BUFFER_LIMIT_BYTES:
            logger(f"Buffer limit reached ({CURRENT_BUFFER_SIZE_BYTES / (1024*1024):.2f} MB). Flushing to SQLite at '{db_conn_path}'...")
            # Release lock before calling flush to prevent holding it too long if flush is slow
            # The flush_buffers_to_sqlite itself is not designed to be called concurrently by multiple
            # save_to_sqlite_buffered calls if it operates on global buffers without its own internal lock
            # for those specific buffer accesses. However, the global BUFFER_LOCK here ensures only one
            # thread at a time adds to buffers OR triggers a flush.
            # The flush_buffers_to_sqlite accesses globals, so it expects to be called under this lock
            # or have the lock passed and used internally if it were refactored.
            # For now, assuming flush is called with the lock held is fine as per original design.
            flush_buffers_to_sqlite(db_conn_path, df_table_name, metadata_table_name, logger)
            # CURRENT_BUFFER_SIZE_BYTES is reset inside flush_buffers_to_sqlite
        # else:
            # logger(f"Data buffered. Total buffer size: {CURRENT_BUFFER_SIZE_BYTES / (1024*1024):.2f}MB. Not flushing yet.")

def final_flush_all_buffers(db_conn_path, df_table_name="raw_data", metadata_table_name="metadata", log_emitter=None):
    """
    Call this at the very end of your script to ensure any data remaining in
    buffers is written to the database.
    """
    logger = log_emitter or print
    logger(f"Performing final flush of all buffered data to '{db_conn_path}'...")
    with BUFFER_LOCK: # Ensure no other thread is modifying buffers during final flush
        flush_buffers_to_sqlite(db_conn_path, df_table_name, metadata_table_name, logger)
    logger("Final flush complete.")
