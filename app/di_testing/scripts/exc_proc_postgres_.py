# exc_proc_postgres_adapted.py

import time
import os
import itertools
import pandas as pd
import numpy as np
import re
import traceback # For detailed error logging
import threading # For default cancel_event
# from datetime import datetime
from sqlalchemy import create_engine, text, exc as sqlalchemy_exc
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from contextlib import contextmanager
import apiClarifi as cf # Assuming this exists and works
from .sqlite_insert import save_to_sqlite_buffered, final_flush_all_buffers


# --- Dummy Emitters/Event for Standalone Execution ---
def dummy_log_emitter(msg):
    """Default logger if none provided."""
    print(f"\r\nLOG: {msg} ")

def dummy_progress_emitter(current, total):
    """Default progress reporter if none provided."""
    # Limit printing progress updates frequency for console readability
    if total > 0 and (current % max(1, total // 20) == 0 or current == total):
        percent = int((current * 100) / total)
        print(f"\r\nPROGRESS: {current}/{total} ({percent}%)")

# ----------------------------------------------------
# Database and Session Management
# ----------------------------------------------------

def create_db_engine(database_url, pool_size=10, max_overflow=5, log_emitter=None):
    """Create and return a SQLAlchemy engine with connection pooling."""
    log_emitter = log_emitter or dummy_log_emitter
    try:
        log_emitter(f"Creating database engine for: {database_url.split('@')[-1]} with pool_size={pool_size}, max_overflow={max_overflow}")
        engine = create_engine(
            database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=30,        # Time to wait for connection from pool
            pool_recycle=600,       # Recycle connections older than 10 mins
            connect_args={
                "options": "-c statement_timeout=1200000" # 20 min timeout per statement
                # Consider adding application_name for easier DB monitoring:
                # "application_name": "BatchProcessorApp"
            }
        )
        # Optional: Test connection
        #with engine.connect() as connection:
        #     log_emitter("Database engine created and connection tested successfully.")
        return engine
    except Exception as e:
        log_emitter(f"ERROR: Failed to create database engine: {e}")
        raise # Re-raise the exception to be caught by the main process

@contextmanager
def get_session(engine, log_emitter=None):
    """Context manager to handle SQLAlchemy session lifecycle."""
    log_emitter = log_emitter or dummy_log_emitter
    Session = sessionmaker(bind=engine)
    session = Session()
    # log_emitter("DB session opened.") # Can be noisy, enable if needed
    try:
        yield session
    except sqlalchemy_exc.SQLAlchemyError as e:
        log_emitter(f"ERROR: Database error occurred within session: {e}")
        session.rollback() # Rollback on error
        raise # Re-raise the exception
    except Exception as e:
        log_emitter(f"ERROR: Non-DB error occurred within session scope: {e}")
        session.rollback() # Rollback on general errors too
        raise # Re-raise
    finally:
        session.close()
        # log_emitter("DB session closed.") # Can be noisy

# ----------------------------------------------------
# SQL Execution and Concurrency (Performance Optimized)
# ----------------------------------------------------

def execute_proc_batch(engine, procedures_batch, batch_id, db_file, artifact_type, savetosqlite, log_emitter, cancel_event):
    """
    Executes a batch of procedures within a single database session in one thread.
    Designed to be called by ThreadPoolExecutor.
    """
    results = []
    processed_count = 0
    batch_start_time = time.time()
    log_emitter(f"Thread {threading.get_ident()} starting batch {batch_id} ({len(procedures_batch)} procedures)...")
    log_emitter(f"artifact type is {artifact_type}")

    try:
        with get_session(engine, log_emitter) as session:
            for proc_data in procedures_batch:
                # Check for cancellation before executing each procedure
                if cancel_event.is_set():
                    log_emitter(f"Thread {threading.get_ident()} batch {batch_id}: Cancellation detected. Stopping batch execution.")
                    # Add error markers for remaining items in this batch? Optional.
                    # For simplicity, we just stop processing this batch here.
                    break

                proc_sql = "SET NOCOUNT ON; " + proc_data['proc']
                proc_id = proc_data['id']
                start_time = time.time()
                error_msg = None
                return_rows = 0
                execution_time = 0
                df_for_sqlite = None
                dupe_count = 0 
                
                
                try:
                    # log_emitter(f"Executing: {proc_sql[:100]}...") # Can be noisy
                    result = session.execute(text(proc_sql))
                    execution_time = time.time() - start_time
                    # Fetch results only if needed (e.g., for saving or row count)
                    # If results are large, fetchall() can consume significant memory.
                    # Consider proreturn result
                    # return_rows = result.rowcount
                    if result.returns_rows:
                        column_names = [key if key not in (None, "") else 'unknown' for key in result.keys()]
                        rows = result.fetchall()
                                                
                        return_rows = len(rows)
                        
                        df_for_sqlite = pd.DataFrame(rows, columns=column_names) 

                        dupe_count = df_for_sqlite.duplicated().sum()                        
                        if dupe_count>0:
                            df_for_sqlite.drop_duplicates(inplace = True)
                        
                        df_for_sqlite['proc_id'] = proc_id
                        df_for_sqlite['proc_sql'] = proc_sql
                        
                        
                except sqlalchemy_exc.DBAPIError as db_err: # Catch specific DB errors                    
                    error_msg = f"DBAPIError: {db_err}"
                    log_emitter(f"\nERROR executing {proc_id}: {error_msg}")
                    session.rollback() # Rollback on error within the loop for this proc
                    
                except Exception as e:                    
                    error_msg = f"GeneralError: {e}\n{traceback.format_exc()} "
                    log_emitter(f"♣\nERROR executing {proc_id}: {error_msg} \n")
                    session.rollback() # Rollback on general error
                
                output = dict(id=proc_id, proc=proc_sql, dupe_row_count = dupe_count, 
                              execution_time=execution_time, return_rows=return_rows, error=error_msg)
                if artifact_type in ("baskets","portfolios"):
                    raw_table = "raw_bask_port"
                else:
                    raw_table = "raw_data_exp"
                if savetosqlite and df_for_sqlite is not None and not df_for_sqlite.empty:
                    save_to_sqlite_buffered(
                        df=df_for_sqlite,
                        result_dict=output,
                        db_conn_path=db_file,
                        df_table_name=raw_table,
                        log_emitter=log_emitter,
                        cancel_event=cancel_event)
                elif savetosqlite and df_for_sqlite is None:
                    log_emitter(f"Skipping save to SQLite for {proc_id} as no data was returned.")
                results.append(output)
                processed_count += 1
                
    except Exception as batch_err:
        # Error opening session or other broader issue within the thread task
        log_emitter(f"ERROR: Critical error in Thread {threading.get_ident()} batch {batch_id}: {batch_err}\n{traceback.format_exc()}")
        # Add error markers for all procedures in this batch if results list is incomplete
        for i in range(len(results), len(procedures_batch)):
             proc_data = procedures_batch[i]
             results.append(dict(id=proc_data['id'], proc=proc_data['proc'], execution_time=0, return_rows=0,
                                 error=f"Batch execution failed: {batch_err}"))

    batch_duration = time.time() - batch_start_time
    log_emitter(f"Thread {threading.get_ident()} finished batch {batch_id} ({processed_count}/{len(procedures_batch)} procedures processed) in {batch_duration:.2f}s")
    return results


def execute_procs_concurrently(engine, procedures, pool_size, db_file, artifact_type,
                                savetosqlite, log_emitter, progress_emitter,
                               cancel_event, num_workers = None):
    """Execute multiple procedures concurrently using batches per thread."""
    total_procedures = len(procedures)
    if total_procedures == 0:
        log_emitter("No procedures to execute.")
        return []

    # Use a sensible number of threads, default pool_size from GUI or CPU count
    # Avoid creating excessive threads if procedure count is low
    max_workers = min(pool_size, max(1, os.cpu_count()-2), total_procedures)
    final_workers = num_workers if num_workers else max_workers
    log_emitter(f"Executing {total_procedures} procedures using {final_workers} worker threads.")

    # Divide procedures into batches for each worker
    # Aim for roughly equal batch sizes
    num_batches = final_workers
    batch_size = (total_procedures + num_batches - 1) // num_batches # Ceiling division
    procedure_batches = [procedures[i:i + batch_size] for i in range(0, total_procedures, batch_size)]
    log_emitter(f"Divided procedures into {len(procedure_batches)} batches of up to {batch_size} procedures each.")

    all_results = []
    completed_procedures = 0
    progress_emitter(completed_procedures, total_procedures) # Initial progress

    with ThreadPoolExecutor(max_workers=final_workers) as executor:
        # Submit each batch to a worker thread
        futures = {
            executor.submit(execute_proc_batch, engine, batch, f"batch_{i}",
                            db_file, artifact_type, savetosqlite,
                            log_emitter, cancel_event): batch
            for i, batch in enumerate(procedure_batches)
        }

        try:
            for future in as_completed(futures):
                # Check for cancellation while waiting for results
                if cancel_event.is_set():
                    log_emitter("Cancellation detected while waiting for procedure batches.")
                    # Attempt to cancel remaining futures (may not stop running tasks, but prevents new ones)
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break # Exit the result processing loop

                try:
                    batch_results = future.result() # Get results for the completed batch
                    all_results.extend(batch_results)
                    completed_procedures += len(batch_results) # Increment by number processed in the batch
                    # Emit progress: Use actual completed count
                    progress_emitter(completed_procedures, total_procedures)
                    # Optional: Log batch completion summary
                    # errors_in_batch = sum(1 for r in batch_results if r.get('error'))
                    # log_emitter(f"Completed batch processing: {len(batch_results)} procedures processed, {errors_in_batch} errors.")

                except Exception as exc:
                    # Catch errors from the execute_proc_batch function itself (e.g., session creation failure)
                    # Find which batch this future belonged to (might be tricky if future doesn't hold input)
                    # Need a way to map future back to input batch if specific error reporting is needed
                    log_emitter(f'ERROR: A procedure batch execution generated an exception: {exc}\n{traceback.format_exc()}')
                    # Estimate progress lost - difficult without knowing which batch failed reliably
                    # progress_emitter(completed_procedures, total_procedures) # Update progress based on known completions
            if artifact_type in ("baskets","portfolios"):
                raw_table = "raw_bask_port"
            else:
                raw_table = "raw_data_exp"
            if savetosqlite:
                final_flush_all_buffers(db_file, raw_table, "metadata")

        except KeyboardInterrupt:
            log_emitter("KeyboardInterrupt received. Attempting to cancel tasks...")
            cancel_event.set() # Signal cancellation
            # Wait briefly for running tasks to potentially see the flag
            time.sleep(1)
            # shutdown(wait=False) will try to cancel pending, but running might continue
            executor.shutdown(wait=False, cancel_futures=True)
            raise # Re-raise KeyboardInterrupt

        finally:
            # Ensure final progress update even if loop broken by cancellation
            progress_emitter(completed_procedures, total_procedures)

    log_emitter(f"Finished concurrent execution. Processed {completed_procedures}/{total_procedures} procedures.")
    if cancel_event.is_set() and completed_procedures < total_procedures:
         log_emitter("WARNING: Execution was cancelled. Results may be incomplete.")

    return all_results

# ----------------------------------------------------
# File Handling and Data Processing
# ----------------------------------------------------

def read_proc_file(path, columns_dict, sample, filtervalue, log_emitter, cancel_event):
    """Reads a batch file, processes lines, and returns a DataFrame."""
    log_emitter(f"Reading batch file: {path}")
    processed_data = []
    max_column_count = 0
    line_count = 0

    try:
        with open(path, 'r', encoding='utf-8') as file:
            for line in file:
                line_count += 1
                # Check cancellation periodically, e.g., every 100 lines
                if line_count % 100 == 0 and cancel_event.is_set():
                    log_emitter(f"Cancellation detected while reading file {path}.")
                    return pd.DataFrame() # Return empty DataFrame on cancellation

                if line.strip().startswith('#'):
                    line = line.lstrip('#').strip() # Use commented lines if they start with #
                elif line.strip() == '':
                     continue # Skip empty lines

                columns = line.strip().split('\t')
                if any(col.strip() for col in columns):
                    max_column_count = max(max_column_count, len(columns))
                    processed_data.append(columns)

    except FileNotFoundError:
        log_emitter(f"ERROR: File not found: {path}")
        return pd.DataFrame()
    except Exception as e:
        log_emitter(f"ERROR: Failed to read file {path}: {e}\n{traceback.format_exc()}")
        return pd.DataFrame()

    if not processed_data:
        log_emitter(f"Warning: No processable data found in file: {path}")
        return pd.DataFrame()

    # Standardize row length before creating DataFrame
    standardized_data = []
    for row in processed_data:
        standardized_data.append(row + [''] * (max_column_count - len(row)) if len(row) < max_column_count else row[:max_column_count])

    # Create DataFrame
    df = pd.DataFrame(standardized_data)
    
    # Rename columns based on provided dictionary
    try:
        df.rename(columns=columns_dict, inplace=True)
        # Keep only the columns specified in the dictionary's values
        required_cols = list(columns_dict.values())
        df = df[required_cols]
    except KeyError as e:
        log_emitter(f"ERROR: Missing expected column index {e} based on columns_dict in file {path}. Check batch file format and column mapping.")
        log_emitter(f"File has {df.shape[1]} columns. Expected mapping: {columns_dict}")
        return pd.DataFrame()

    # Data cleaning
    df = df.replace("", np.nan).dropna(how='all', subset=required_cols)
    if 'proc' not in df.columns:
         log_emitter(f"ERROR: 'proc' column missing after renaming in file {path}. Check columns_dict mapping.")
         return pd.DataFrame()
    df = df[df['proc'].notna() & (df['proc'].str.strip() != '')] # Ensure 'proc' column exists and is not empty

    # Combine name and ID if 'name' column exists
    if "name" in df.columns and "id" in df.columns:
        df['id_combined'] = df['name'].fillna("").astype(str) + " (" + df['id'].fillna("").astype(str) + ")"
        df.drop(['name', 'id'], axis=1, inplace=True, errors='ignore')
        df.rename(columns={'id_combined': 'id'}, inplace=True) # Use combined as the new 'id'
        df = df.drop_duplicates(subset = "id").reset_index(drop = True)

    if "id" not in df.columns:
         log_emitter(f"ERROR: 'id' column missing and 'name' column not present to create it in file {path}.")
         df["id"] = ""


    # If only 'id' exists, ensure it's used. If only 'name' exists, rename it to 'id'? Depends on expectation.
    # Assuming 'id' is mandatory either directly or via 'name'.

    # Filtering based on 'filtervalue' (case-insensitive search in 'proc' and 'id')
    if filtervalue:
        log_emitter(f"Filtering procedures containing: '{filtervalue}'")
        original_count = len(df)
        filtervalue_lower = filtervalue.strip().lower()
        # Ensure columns exist before filtering
        filter_cols = [col for col in ['proc', 'id'] if col in df.columns]
        #mask = pd.Series(False, index=df.index)
        mask = pd.Series([False] * len(df) , index=df.index)
        for col in filter_cols:
             #mask |= df[col].fillna('').str.lower().str.contains(filtervalue_lower, na=False)
             mask |= df[col].fillna('').astype(str).str.lower().str.contains(filtervalue_lower , na=False)
        df = df[mask]
        log_emitter(f"Filtered down to {len(df)} procedures from {original_count}.")

    if df.empty:
         log_emitter(f"No procedures remaining after filtering for file: {path}")
         return pd.DataFrame()

    # Sampling logic
    if sample and isinstance(sample, int) and sample > 0:
        log_emitter(f"Sampling {sample} procedures per path group...")
        original_count = len(df)
        if 'path' in df.columns:
            # Sample within each 'path' group
             df = (
                df.groupby('path', group_keys=False) # group_keys=False avoids adding path index level
                .apply(lambda x: x.sample(n=min(sample, len(x)), random_state=42))
                .reset_index(drop=True)
            )
        else:
            # If no 'path' column, sample from the whole DataFrame
            log_emitter("Warning: 'path' column not found for sampling per path. Sampling from entire dataset.")
            df = df.sample(n=min(sample, len(df)), random_state=42).reset_index(drop=True)

        log_emitter(f"Sampled down to {len(df)} procedures from {original_count}.")

    if df.empty:
         log_emitter(f"No procedures remaining after sampling for file: {path}")
         return pd.DataFrame()

    log_emitter(f"Finished reading and processing file {path}. Found {len(df)} procedures to execute.")
    return df

# ----------------------------------------------------
# Placeholder and Procedure Modification
# ----------------------------------------------------

def extract_placeholders(proc):
    """Extract placeholders (e.g., $start, $stop, $security_id) from the procedure."""
    # Regex to find $ followed by alphanumeric or underscore, avoiding $sP... patterns
    return re.findall(r'\$(?![sp][A-Z])[a-zA-Z0-9_]+', proc)

def replace_placeholders_in_proc(proc, placeholder_values, log_emitter=None):
    """
    Replace placeholders in a procedure string with actual values, mimicking
    the behavior of the simpler replace_placeholders function (removing
    surrounding single/double quotes, not adding new quotes) while retaining
    robustness features.

    Mimicked Behavior:
    - Replaces $placeholder -> str(value)
    - Replaces '$placeholder' -> str(value) (Removes single quotes)
    - Replaces "$placeholder" -> str(value) (Removes double quotes - added robustness)
    - Never adds quotes to the inserted value.

    Robustness Features Retained:
    - Handles keys in placeholder_values with or without leading '$'.
    - Sorts keys by length (desc) to handle substrings correctly ($stopDate before $stop).
    - Uses re.escape() for safe regex patterns.
    - Handles None values by converting them to the string "None" (like str(None)).
    """
    log_emitter = log_emitter or dummy_log_emitter
    original_proc = proc
    modified = False

    # Sort keys by length descending to replace longer keys first
    # This prevents replacing "$stop" inside "$stopDate", for example.
    sorted_placeholders = sorted(placeholder_values.items(), key=lambda item: len(item[0]), reverse=True)

    for placeholder, value in sorted_placeholders:
        # 1. Ensure placeholder_key used for matching always starts with $
        placeholder_key = f"${placeholder}" if not placeholder.startswith('$') else placeholder

        # 2. Escape the placeholder key for use in regex
        escaped_placeholder_key = re.escape(placeholder_key)

        # 3. Define the regex pattern:
        #    - (['\"]?) : Optionally match and capture a single or double quote (Group 1)
        #    - {escaped_placeholder_key} : Match the escaped placeholder key
        #    - \b        : Match a word boundary to avoid partial matches
        #    - \1        : Match the same quote captured in Group 1 (ensures quotes match)
        pattern = rf"(['\"]?){escaped_placeholder_key}\b\1"

        # 4. Define the replacement string:
        #    - Always use the plain string representation of the value.
        #    - This mimics the target function which uses str(value) directly.
        #    - Note: str(None) results in the string "None". If 'NULL' is desired
        #      for None, this would be a deviation from the *exact* mimicry.
        replacement_value = str(value)

        # 5. Perform the substitution
        #    re.sub replaces the *entire* pattern match with replacement_value.
        #    This effectively removes any surrounding quotes matched by the pattern.
        new_proc = re.sub(pattern, replacement_value, proc)

        if new_proc != proc:
            modified = True
            proc = new_proc # Update proc for the next iteration

    # Optional Debug Logging
    # if not modified:
    #     log_emitter(f"No placeholders found/replaced in proc: {original_proc[:100]}... with values {placeholder_values}")

    return proc

    #if not replaced_any:
    #     log_emitter(f"DEBUG: No placeholders found/replaced in proc: {original_proc[:100]}... with values {placeholder_values}")
    #elif proc == original_proc:
    #     log_emitter(f"DEBUG: Placeholders found but replacement failed? Proc: {original_proc[:100]}... Values: {placeholder_values}")

    return proc


def prepare_procs_with_placeholders(df, universe_name, universe_type, start_date, stop_date,
                                    clarifi_user, clarifi_pwd, log_emitter, cancel_event):
    """Replace placeholders in the 'proc' column using Clarifi data if necessary."""
    log_emitter("Preparing procedures by replacing placeholders...")
    start_prep_time = time.time()

    if 'proc' not in df.columns or df['proc'].isnull().all():
        log_emitter("ERROR: 'proc' column is missing or empty in the input DataFrame.")
        return pd.DataFrame()

    # Find all unique placeholders across all procedures
    unique_placeholders = set()
    for proc in df['proc'].dropna():
         unique_placeholders.update(extract_placeholders(proc))

    log_emitter(f"Found unique placeholders: {unique_placeholders}")

    # Default placeholder values
    placeholder_base_values = {
        "$start": f"'{start_date}'", # Assume dates should be quoted for SQL
        "$stop": f"'{stop_date}'"
    }

    # Identify placeholders needing data lookup (those not $start or $stop)
    lookup_placeholders = {p for p in unique_placeholders if p not in placeholder_base_values}

    universe_data_list = [] # List to hold dictionaries from universe lookup
    if lookup_placeholders and universe_name and universe_type:
        log_emitter(f"Placeholders require universe lookup: {lookup_placeholders}")
        if not clarifi_user or not clarifi_pwd:
             log_emitter("ERROR: Clarifi credentials required for universe lookup but not provided.")
             # Decide: raise error or proceed without lookup? Let's raise.
             raise ValueError("Clarifi credentials missing for required universe lookup.")

        # Check cancellation before Clarifi login/call
        if cancel_event.is_set():
            log_emitter("Cancellation detected before Clarifi lookup.")
            return pd.DataFrame()

        try:
            log_emitter("Logging into Clarifi for universe data...")
            cf.login(clarifi_user, clarifi_pwd) # Assuming login is needed per call or handled by apiClarifi internally
            # Prepare attribute list for Clarifi API
            sec_master_attr = ",".join([p for p in lookup_placeholders])
            log_emitter(f"Requesting attributes: {sec_master_attr} from {universe_type} '{universe_name}' between {start_date} and {stop_date}")

            # Call Clarifi API - This might be slow, cannot easily cancel mid-call
            universe_df = cf.basket_portfolio(
                name=universe_name,
                requestType=universe_type,
                secMasterAttr=sec_master_attr,
                startDate=start_date,
                stopDate=stop_date
            )

            if universe_df is None or not isinstance(universe_df, pd.DataFrame):
                log_emitter(f"Warning: No data returned from Clarifi for universe '{universe_name}' ({universe_type}). Proceeding without universe placeholders.")
                # Continue without universe data, placeholders won't be replaced
            elif universe_df.empty:
                 log_emitter(f"Warning: Empty DataFrame returned from Clarifi for universe '{universe_name}' ({universe_type}). Proceeding without universe placeholders.")
                 # Continue without universe data
            else:
                 log_emitter(f"Received {len(universe_df)} records from Clarifi.")
                 # Sanitize column names (replace space with underscore, keep original mapping if needed)
                 original_columns = list(universe_df.columns)
                 universe_df.columns = [col.replace(" ", "_") for col in original_columns]
                 clarifi_col_map = {col.replace(" ", "_"): col for col in original_columns} # Map sanitized back to original if needed

                 # Check if requested attributes are present (case-insensitive check might be needed)
                 available_attrs_sanitized = set(universe_df.columns)
                 requested_attrs_sanitized = {p.lstrip('$') for p in lookup_placeholders}
                 missing_attrs = requested_attrs_sanitized - available_attrs_sanitized
                 if missing_attrs:
                     log_emitter(f"Warning: Requested attributes missing from Clarifi response: {missing_attrs}")

                 # Select only the columns corresponding to placeholders
                 cols_to_keep = list(requested_attrs_sanitized.intersection(available_attrs_sanitized))
                 if not cols_to_keep:
                      log_emitter("Warning: None of the required placeholder attributes found in Clarifi response.")
                 else:
                     # Drop duplicates and sample if necessary (e.g., 50 random entries)
                     universe_df_filtered = universe_df[cols_to_keep].drop_duplicates()
                     sample_size = min(50, len(universe_df_filtered)) # Limit sample size
                     if len(universe_df_filtered) > sample_size:
                         log_emitter(f"Sampling {sample_size} unique universe entries from {len(universe_df_filtered)}.")
                         universe_df_sampled = universe_df_filtered.sample(n=sample_size, random_state=42)
                     else:
                         log_emitter(f"Using all {len(universe_df_filtered)} unique universe entries.")
                         universe_df_sampled = universe_df_filtered

                     # Convert the sampled DataFrame to a list of dictionaries
                     universe_data_list = universe_df_sampled.to_dict('records')
                     log_emitter(f"Prepared {len(universe_data_list)} universe data entries for placeholder replacement.")

        except Exception as clarifi_e:
            log_emitter(f"ERROR: Failed during Clarifi lookup for universe '{universe_name}': {clarifi_e}\n{traceback.format_exc()}")
            # Decide: raise error or continue without lookup? Let's raise.
            raise ValueError(f"Clarifi lookup failed: {clarifi_e}")

    # --- Replace placeholders ---
    updated_procs_list = []
    total_combinations = len(df) * max(1, len(universe_data_list)) # Calculate expected total
    processed_combinations = 0
    log_emitter(f"Generating procedure combinations (up to {total_combinations})...")

    # If universe data exists, create combinations; otherwise, just process original procedures
    if universe_data_list:
        # Use itertools.product for efficiency if df and universe_data_list are large
        # This can create a very large number of combinations!
        if total_combinations > 50000: # Add a safety limit
             log_emitter(f"WARNING: High number of combinations ({total_combinations}). Consider reducing sampling or filtering.")
        if total_combinations > 200000: # Add a hard limit
             raise ValueError(f"Too many procedure combinations requested ({total_combinations}). Maximum allowed is 200,000.")

        for (index, row), universe_entry in itertools.product(df.iterrows(), universe_data_list):
             processed_combinations += 1
             if processed_combinations % 1000 == 0: # Check cancellation periodically
                 if cancel_event.is_set():
                     log_emitter("Cancellation detected during procedure combination generation.")
                     return pd.DataFrame()
                 # log_emitter(f"Generating combination {processed_combinations}/{total_combinations}...") # Can be noisy

             proc = row['proc']
             current_placeholders = {**placeholder_base_values} # Start with base values ($start, $stop)
             # Add values from the current universe entry, matching placeholder names (without $)
             for placeholder_key_sanitized, value in universe_entry.items():
                  placeholder_key_with_dollar = f"${placeholder_key_sanitized}"
                  if placeholder_key_with_dollar in lookup_placeholders:
                       # Quote strings, leave numbers as is for SQL
                       current_placeholders[placeholder_key_with_dollar] = f"'{value}'" if isinstance(value, str) else str(value)

             updated_proc = replace_placeholders_in_proc(proc, current_placeholders, log_emitter)
             # Use original 'id' from the row, maybe append universe info if needed for uniqueness?
             proc_identifier = f"{row['id']}_uni{processed_combinations}" # Create a more unique ID if needed
             updated_procs_list.append({'id': row['id'], 'proc': updated_proc}) # Use original ID for now

    else:
        # No universe lookup needed or performed
        log_emitter("Processing procedures without universe data replacement.")
        for index, row in df.iterrows():
             processed_combinations += 1
             if processed_combinations % 1000 == 0: # Check cancellation periodically
                 if cancel_event.is_set():
                     log_emitter("Cancellation detected during procedure processing.")
                     return pd.DataFrame()

             proc = row['proc']
             # Only replace $start and $stop
             updated_proc = replace_placeholders_in_proc(proc, placeholder_base_values, log_emitter)
             updated_procs_list.append({'id': row['id'], 'proc': updated_proc})

    prep_duration = time.time() - start_prep_time
    log_emitter(f"Finished preparing {len(updated_procs_list)} procedures in {prep_duration:.2f}s.")

    if not updated_procs_list:
        log_emitter("Warning: No procedures generated after placeholder replacement.")
        return pd.DataFrame()

    return pd.DataFrame(updated_procs_list)


# ----------------------------------------------------
# Main Processing Pipeline
# ----------------------------------------------------

def process_single_batch_file(engine, file_path, columns_dict, batch_file_sample, filtervalue,
                              universe_name, universe_type, start_date, stop_date,
                              clarifi_user, clarifi_pwd, pool_size, # pool_size passed for execute_procs
                              artifact_type, savetosqlite,
                              log_emitter, progress_emitter, cancel_event, num_workers):
    """Processes a single batch file from reading to execution."""
    log_emitter(f"--- Starting processing for file: {file_path.name} ---")
    file_start_time = time.time()

    # Step 1: Read and process the batch file
    df = read_proc_file(file_path, columns_dict, batch_file_sample, filtervalue, log_emitter, cancel_event)
    if df.empty or cancel_event.is_set():
        log_emitter(f"Skipping file {file_path.name} due to empty data or cancellation.")
        return pd.DataFrame() # Return empty df for this file
    
    #db File
    db_path= os.path.join(os.path.expanduser("~"),"Batch_File_Testing_Results")
    os.makedirs(db_path, exist_ok = True)
    db_file = os.path.join(db_path, file_path.stem+".db")
    
    if os.path.exists(db_file):
        os.remove(db_file)    

    # Step 2: Replace placeholders in procedures
    updated_procs_df = prepare_procs_with_placeholders(
        df, universe_name, universe_type, start_date, stop_date,
        clarifi_user, clarifi_pwd, log_emitter, cancel_event
    )
    if updated_procs_df.empty or cancel_event.is_set():
        log_emitter(f"Skipping file {file_path.name} due to empty procedures after placeholder replacement or cancellation.")
        return pd.DataFrame()

    # Step 3: Execute procedures concurrently
    # Note: Progress reporting (0 to total_procedures) happens *inside* execute_procs_concurrently
    results_list = execute_procs_concurrently(
        engine,
        updated_procs_df.to_dict('records'),
        pool_size, # Pass pool_size to determine max_workers inside
        db_file, artifact_type,
        savetosqlite, log_emitter,
        progress_emitter, # Pass progress emitter down
        cancel_event, num_workers
    )

    if not results_list:
        log_emitter(f"Warning: No results obtained from executing procedures for file {file_path.name}.")
        results_df = pd.DataFrame() # Return empty df if no results
    else:
         results_df = pd.DataFrame(results_list)

    file_duration = time.time() - file_start_time
    log_emitter(f"--- Finished processing file: {file_path.name} in {file_duration:.2f}s. Got {len(results_df)} results. ---")
    return results_df


# ----------------------------------------------------
# Main Entry Point for Worker
# ----------------------------------------------------

def main_process(database_url, start_date, stop_date, path_batch,
                 batch_columns_dict, universe=None, universe_type=None,
                 pool_size=10, max_overflow=5, batch_file_sample=None, filtervalue = None,
                 clarifi_user= None, clarifi_pwd = None, num_workers = None,
                 artifact_type = None, savetosqlite = False,
                 # --- Arguments injected by Worker ---
                 log_emitter=None,
                 progress_emitter=None,
                 cancel_event=None,
                 ):
    """
    Main function adapted for worker execution. Handles single file or directory.
    Orchestrates reading, placeholder replacement, and concurrent execution.
    """
    # Use dummy emitters/event if not provided (for standalone runs)
    log_emitter = log_emitter or dummy_log_emitter
    progress_emitter = progress_emitter or dummy_progress_emitter
    _cancel_event = cancel_event or threading.Event() # Use internal name

    log_emitter("="*20 + " Batch File Processing Started " + "="*20)
    overall_start_time = time.time()

    # --- Input Validation ---
    if not database_url:
        log_emitter("ERROR: Database URL is required.")
        raise ValueError("Database URL cannot be empty.")
    if not start_date or not stop_date:
        log_emitter("ERROR: Start Date and Stop Date are required.")
        raise ValueError("Start Date and Stop Date cannot be empty.")
    if not path_batch:
        log_emitter("ERROR: Batch file path or directory is required.")
        raise ValueError("Batch file path or directory cannot be empty.")
    if not batch_columns_dict:
        log_emitter("ERROR: Batch columns dictionary mapping is required.")
        raise ValueError("Batch columns dictionary cannot be empty.")
    # Placeholder lookup requires universe info and credentials if needed
    # This is partially handled inside prepare_procs_with_placeholders

    try:
        # --- Determine files to process ---
        files_to_process = []
        path_batch_obj = Path(path_batch)

        if not path_batch_obj.exists():
             log_emitter(f"ERROR: Path does not exist: {path_batch}")
             raise FileNotFoundError(f"Path does not exist: {path_batch}")

        if path_batch_obj.is_dir() :
            log_emitter(f"Processing directory: {path_batch}")
            # Find files directly in the directory (non-recursive)
            files_to_process = [f for f in path_batch_obj.iterdir() if f.is_file()]
            if not files_to_process:
                 log_emitter(f"Warning: No files found in directory: {path_batch}")
                 return [] # Return empty list if no files
        elif path_batch_obj.is_file() :
            log_emitter(f"Processing single file: {path_batch}")
            files_to_process = [path_batch_obj]
        else:
             log_emitter(f"ERROR: Path is neither a file nor a directory: {path_batch}")
             raise ValueError(f"Path is not a valid file or directory: {path_batch}")

        log_emitter(f"Found {len(files_to_process)} file(s) to process: {[f.name for f in files_to_process]}")

        # --- Setup Database Engine ---
        # Create engine once, pass it down
        engine = create_db_engine(database_url, pool_size, max_overflow, log_emitter)
        if engine is None:
             # Error already logged by create_db_engine
             raise RuntimeError("Failed to initialize database engine.")

        # --- Process Files ---
        all_results_dfs = [] # List to store (filename, dataframe) tuples
        total_files = len(files_to_process)
        # Progress for files processed (distinct from procedure progress)
        file_progress_emitter = progress_emitter or dummy_progress_emitter
        file_progress_emitter(0, total_files) # Initial file progress

        for i, file in enumerate(files_to_process):
            # Check for cancellation before processing each file
            if _cancel_event.is_set():
                log_emitter("Cancellation detected before processing next file.")
                break

            # Use a separate progress emitter for procedures within the file processing function
            # We need a way to pass total procedures for *this file* to the inner progress emitter
            # For simplicity now, the main progress bar might reflect file progress,
            # while logs show procedure progress. Or adapt WorkerSignals for nested progress.
            # Let's assume progress_emitter passed down will handle the *procedure* progress for that file.

            file_result_df = process_single_batch_file(
                engine, file, batch_columns_dict, batch_file_sample, filtervalue,
                universe, universe_type, start_date, stop_date,
                clarifi_user, clarifi_pwd,
                pool_size, # Pass pool_size for execute_procs_concurrently
                artifact_type,
                savetosqlite,
                log_emitter,
                progress_emitter, # Pass the main progress emitter
                _cancel_event, num_workers
            )

            # # --- Accumulate and Save Results ---
            if not file_result_df.empty:
                # Store result paired with filename stem
                all_results_dfs.append((file.stem, file_result_df))

            #     # Save result for this file (optional, maybe do a combined save later)
            #     try:
            #         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            #         write_directory = os.path.join(os.path.expanduser("~"), "Batch_File_Testing_Results")
            #         os.makedirs(write_directory, exist_ok=True)
            #         # Sanitize filename stem
            #         safe_stem = re.sub(r'[<>:"/\\|?*]', '_', file.stem)
            #         write_file = os.path.join(write_directory, f"Results_{safe_stem}_{timestamp}.csv")
            #         log_emitter(f"Saving results for {file.name} to {write_file}")
            #         file_result_df.to_csv(write_file, index=False)
            #     except Exception as save_e:
            #         log_emitter(f"ERROR: Failed to save results for {file.name}: {save_e}")

            # Update file progress
            file_progress_emitter(i + 1, total_files)

        # --- Finalize ---
        log_emitter("Finished processing all specified files.")

        # Dispose the engine pool when completely done
        if engine:
            log_emitter("Disposing database engine pool.")
            engine.dispose()

        overall_duration = time.time() - overall_start_time
        log_emitter(f"Total processing time: {overall_duration:.2f} seconds.")
        log_emitter("="*20 + " Batch File Processing Finished " + "="*20)

        # Return the list of (filename_stem, DataFrame) tuples
        if not all_results_dfs and not _cancel_event.is_set():
             log_emitter("Warning: No results were generated for any processed files.")
        elif _cancel_event.is_set():
             log_emitter("Processing was cancelled. Returning potentially partial results.")

        return all_results_dfs # Return the list [(stem, df), ...]

    except (FileNotFoundError, ValueError, RuntimeError, sqlalchemy_exc.SQLAlchemyError) as e:
        # Catch specific, expected errors
        error_msg = f"ERROR in main_process: {e}\n{traceback.format_exc()}"
        log_emitter(error_msg)
        # Re-raise so the Worker's error signal is triggered
        raise
    except Exception as e:
        # Catch any other unexpected errors
        error_msg = f"UNEXPECTED ERROR in main_process: {e}\n{traceback.format_exc()}"
        log_emitter(error_msg)
        # Re-raise for the Worker
        raise
    finally:
        # Ensure engine is disposed even if errors occurred mid-process
        if 'engine' in locals() and engine:
             try:
                 engine.dispose()
                 log_emitter("Ensured database engine pool disposal in finally block.")
             except Exception as dispose_err:
                  log_emitter(f"Error during final engine disposal: {dispose_err}")



