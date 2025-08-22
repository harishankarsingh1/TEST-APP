import time
import os
import sys

try:
    import apiClarifi as cf  # Assuming this exists and works
except ImportError as e:
    print(f"Critical error: Failed to import 'apiClarifi': {e}", file=sys.stderr)
    sys.exit(1)

from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
from datetime import datetime
import re
import threading  # Added for default cancel_event
import traceback
from .dc_log_minev2 import process_log_modelstats
from .sqlite_insert import save_to_sqlite_buffered, final_flush_all_buffers


# --- Dummy Emitters for Standalone Execution ---
def dummy_log_emitter(msg):
    print(f"\r\nLOG: {msg}")


def dummy_progress_emitter(current, total):
    print(f"\r\nPROGRESS: {current}/{total} \n")

# --- Modified Functions ---


# Set max_workers globally or pass it down if needed
MAX_WORKERS_DEFAULT = max(1, os.cpu_count()-2)


def cleardcs(log_emitter=None, cancel_event=None):
    """Clears DCS logs, emits logs, and checks for cancellation."""
    log_emitter = log_emitter or dummy_log_emitter
    if cancel_event and cancel_event.is_set():
        log_emitter("Cancellation detected before clearing DCS.")
        return
    try:
        cf.clearDCS()
        stats_ = cf.status()
        clarifi_loc = stats_[stats_['server']
                             == "apiLogLocation"]["info"].iloc[0]
        path_parts = clarifi_loc.split(os.sep)
        # Handle potential OS differences in path construction more robustly
        base_directory = os.path.join(
            path_parts[0] + os.sep, path_parts[1], "DCS")

        if not os.path.exists(base_directory):
            log_emitter(f"DCS base directory not found: {base_directory}")
            return

        files = []
        log_emitter(f"Looking for DCS log files in {base_directory}")
        for file in os.listdir(base_directory):
            # Check cancellation periodically during potentially long operations
            if cancel_event and cancel_event.is_set():
                log_emitter("Cancellation detected while listing DCS files.")
                return
            if file.startswith("DCS") and file.endswith(".log"):
                files.append(os.path.join(base_directory, file))

        log_emitter(f"Found DCS log files: {files}")
        for file_path in files:
            if cancel_event and cancel_event.is_set():
                log_emitter(
                    f"Cancellation detected before processing file: {file_path}")
                break
            try:
                os.remove(file_path)
                log_emitter(f"Deleted: {file_path}")
            except Exception as e:
                log_emitter(
                    f"Couldn't delete {file_path}, attempting to clear content instead. Error: {e}")
                try:
                    with open(file_path, 'w') as file:
                        file.truncate(0)
                    log_emitter(f"Cleared contents of: {file_path}")
                except Exception as clear_e:
                    log_emitter(
                        f"Failed to clear contents of {file_path}. Error: {clear_e}")
        log_emitter("Finished clearing DCS logs.")
    except Exception as e:
        log_emitter(f"Error during cleardcs: {e}")
        # Decide if you want to re-raise or just log


def process_path(path, exp, basketName, portfolioName, startDate, stopDate, 
                 db_connect_path,  # Path for SQLite connection
                 log_emitter=None, cancel_event=None,
                 savetosqlite=False):
    """Processes a single path/expression, logs, buffers results to SQLite, and checks for cancellation."""
    log_emitter = log_emitter or dummy_log_emitter
    error_msg_str = None  # Renamed from 'error' to avoid conflict with any Exception 'e'
    rows = 0
    dupe_count = 0
    df = pd.DataFrame()  # Initialize df
    start_time = time.time()

    # Ensure exp is serializable for metadata, if it's a complex object.
    # For simplicity, we'll assume exp is a string or list of strings/dicts.
    # If exp itself is very large or complex, consider summarizing it or storing a hash.
    serializable_exp = exp
    if not isinstance(exp, (str, int, float, bool, type(None), list, dict)):
        serializable_exp = str(exp)  # Fallback to string representation

    start_formated = datetime.strptime(
        startDate, "%Y-%m-%d").strftime("%m/%d/%Y")
    stop_formated = datetime.strptime(
        stopDate, "%Y-%m-%d").strftime("%m/%d/%Y")

    # Prepare the core part of the result dictionary early
    # Note: 'expression' key in result_dict will store serializable_exp
    result_dict = {
        'path': path, 'expression': serializable_exp, 'universeName': basketName or portfolioName,
        'timeframe': f'{start_formated}-{stop_formated}', "execution_time": 0,
        # Changed 'len(df)' to 'len_df' for valid column name
        'error': None, 'len_df': 0
    }

    if cancel_event and cancel_event.is_set():
        log_emitter(f"Cancellation detected before processing path: {path}")
        result_dict['error'] = 'Cancelled before start'
        result_dict['execution_time'] = time.time() - start_time
        # Buffer this cancellation metadata
        if savetosqlite:
            save_to_sqlite_buffered(None, result_dict, db_connect_path,
                                    "raw_data",
                                    "metadata",
                                    log_emitter,
                                    cancel_event)
        return result_dict

    try:
        log_emitter(f"Starting processing for path: {path}...")
        df = cf.transforms.transforms(  # Use your actual cf.transforms.transforms
            expressions=exp,
            basketName=basketName,
            portfolioName=portfolioName,
            startDate=str(startDate),
            stopDate=str(stopDate)
        )
        
        rows = len(df.index)
        if rows > 0:
            dupe_count = df.duplicated().sum()                        
            if dupe_count>0:
                df.drop_duplicates(inplace = True)
                
            if 'expression' in df.columns:
                df = pd.melt(df, 
                             id_vars=['date', 'issueId', 'expression'], 
                             var_name='period', value_name='value'
                             )
            else:
                df = pd.melt(df, id_vars=['date', 'issueId',],
                             var_name='expression', value_name='value'
                             )
            df = df.dropna(subset = 'value')
            df['path']= path
        result_dict['dupe_count'] = dupe_count
        result_dict['len_df'] = len(df)
        log_emitter(
            f"Finished processing path: {path}. Rows: {rows}. Time: {time.time() - start_time:.2f}s")

    except Exception as e:
        error_msg_detail = f"Error processing path {path} with expression '{exp}': {e}"
        log_emitter(error_msg_detail)
        error_msg_str = str(e)  # Store error message for the result dict
        result_dict['error'] = error_msg_str

    # Check cancellation *after* the main work, before returning
    if cancel_event and cancel_event.is_set() and result_dict['error'] is None:
        result_dict['error'] = 'Cancelled after completion'
        log_emitter(f"Cancellation detected after processing path: {path}")

    execution_time = time.time() - start_time
    result_dict["execution_time"] = round(execution_time, 2)

    # Buffer the DataFrame (if not empty) and the result dictionary
    # Pass None for df if it's empty or on error, to avoid issues with empty DFs in buffer logic
    df_to_save = df if not df.empty and error_msg_str is None else None
    if savetosqlite:
        save_to_sqlite_buffered(df_to_save, result_dict, db_connect_path,
                                "raw_data_exp",
                                "metadata",
                                log_emitter, cancel_event)
    return result_dict

# --- Sampling functions (unchanged conceptually, but could add logging/cancellation if needed) ---


def sample_each_path(df, sampleSize):
    # ... (keep original logic) ...
    results = []  # Initialize the list to store results
    for path, group in df.groupby('path'):
        if sampleSize is None:
            df_to_process = group
        else:
            n_samples = min(sampleSize, len(group))
            if n_samples > 0:
                df_to_process = group.sample(n=n_samples, random_state=42)
            else:
                # Empty df if group is empty or sample size is 0
                df_to_process = pd.DataFrame(columns=df.columns)
        if not df_to_process.empty:
            chunks = [
                df_to_process.iloc[i:i + 20]
                for i in range(0, len(df_to_process), 20)
            ]
            for chunk in chunks:
                if not chunk.empty:  # Ensure chunk is not empty before processing
                    # Select only 'key' and 'dataItemId' for the output dictionary
                    chunk_data = chunk[['key', 'dataItemId']
                                       ].to_dict(orient='records')
                    results.append({path: chunk_data})
    return results


def sample_each_id(df, sampleSize):
    # ... (keep original logic) ...

    exp = (
        df.groupby(['artifactTypeName'])[df.columns]
        .apply(lambda x: x if sampleSize is None else x.sample(
            n=min(sampleSize, len(x)), random_state=42))  # Conditional sampling
        .reset_index(drop=True)
    )
    result = [
        {row['path']: [{"key": row['key'], "dataItemId": row['dataItemId']}]}
        for _, row in exp.iterrows()
    ]
    return result


def sample_across_path(df, sampleSize):
    # ... (keep original logic) ...

    shuffled_paths = np.random.permutation(df['path'].unique())
    # Ensure path_groups calculation doesn't fail if len < 6
    num_groups = max(1, len(shuffled_paths) // 6)
    path_groups = np.array_split(shuffled_paths, num_groups)
    results = []
    for group in path_groups:
        exp_ = df[df['path'].isin(group)]

        if exp_.empty:
             continue

        exp_ = (exp_.groupby(['path'])[exp_.columns]
                 .apply(lambda x: x if sampleSize is None else x.sample(
                     n=min(sampleSize, len(x)), random_state=42))
                 .reset_index(drop=True)
                 )

         # Ensure chunks don't fail if len < 20
        num_chunks = max(1, len(exp_) // 20)
        chunks = [exp_.iloc[i:i + 20]
                   for i in range(0, len(exp_), 20)]  # Split into 20-record chunks
        for chunk in chunks:
             result = {",".join(chunk['path'].unique()): chunk[
                 ['key', 'dataItemId']].to_dict(orient='records')}
             results.append(result)
    return results
# --- End Sampling Functions ---

def filter_exp_df(exp_df, artifactType=None, folderName=None, expName=None):
    folderName_list = [v.strip() for v in folderName.split(",")] if folderName else []
    exp_list = [v.strip() for v in expName.split(",")] if expName else []
    artifact_list = [v.strip() for v in artifactType.split(",")] if artifactType else []

    conditions = []

    if folderName_list:
        folder_pattern = "|".join(map(re.escape, folderName_list))
        cond = exp_df['path'].astype(str).str.contains(folder_pattern, case=False, na=False)
        conditions.append(cond)

    if exp_list:
        exp_pattern = "|".join(map(re.escape, exp_list))
        cond = exp_df['name'].astype(str).str.contains(exp_pattern, case=False, na=False)
        conditions.append(cond)

    if artifact_list:
        escaped_individual_types = [re.escape(t) for t in artifact_list]
        exact_match_pattern = f"^(?:{'|'.join(escaped_individual_types)})$"
        cond = exp_df['artifactTypeName'].astype(str).str.fullmatch(exact_match_pattern, case=False, na=False)
        conditions.append(cond)

    if conditions:
        # Combine all conditions with logical AND
        final_condition = conditions[0]
        for cond in conditions[1:]:
            final_condition &= cond
        exp_df = exp_df[final_condition]

    return exp_df
    
def prepare_expressions(artifactType, folderName, expName, sampleType, sampleSize,
                        clarifi_user, clarifi_pwd, log_emitter=None, cancel_event=None):
    """Prepares expressions, samples, logs, and checks cancellation."""
    log_emitter = log_emitter or dummy_log_emitter

    if cancel_event and cancel_event.is_set():
        log_emitter("Cancellation detected before preparing expressions.")
        return []  # Return empty list if cancelled

    try:
        # Consider if cf.login is thread-safe or if needed here vs initializer
        # log_emitter("Attempting Clarifi login (prepare_expressions)...")
        # Potentially redundant if using initializer
        cf.login(clarifi_user, clarifi_pwd)

        # Clear DCS logs (pass emitters/event)
        cleardcs(log_emitter, cancel_event)
        if cancel_event and cancel_event.is_set():
            return []

        log_emitter(
            f"Fetching data library: {artifactType}, {folderName}, {expName}")
        exp_df = cf.dataLibrary()
    
        # log_emitter(f"Columns received: {exp_df.columns.tolist()}") # Debugging

        if exp_df.empty:
            log_emitter(
                f"Warning: No data available for artifactType '{artifactType}' with specified filters.")
            # raise ValueError("No data available for artifactType") # Or just return empty
            return []
        
        exp_df =  filter_exp_df(exp_df, artifactType, folderName, expName)
        
        if exp_df.empty:
            log_emitter(
                f"Warning: No data available for artifactType '{artifactType}' with specified filters.")
            # raise ValueError("No data available for artifactType") # Or just return empty
            return [] 

        # Ensure required columns exist before renaming
        required_cols = {"path", "id", "name", "artifactTypeName"}
        if not required_cols.issubset(exp_df.columns):
            missing = required_cols - set(exp_df.columns)
            err_msg = f"Missing required columns from dataLibrary: {missing}"
            log_emitter(err_msg)
            raise ValueError(err_msg)

        # Use copy to avoid SettingWithCopyWarning
        exp_df = exp_df[["path", "id", "name", "artifactTypeName"]].copy()
        log_emitter(
            f"Number of unique paths/folders to test: {exp_df['path'].nunique()}")
        # time.sleep(5) # Remove this sleep unless absolutely necessary

        exp_df.rename(
            columns={"name": "key", "id": "dataItemId"}, inplace=True)

        log_emitter(
            f"Sampling expressions (Type: {sampleType}, Size: {sampleSize})...")
        result = []
        if sampleType == 1:
            result = sample_each_path(exp_df, sampleSize)
        elif sampleType == 2:
            result = sample_across_path(exp_df, sampleSize)
        elif sampleType == 3:
            result = sample_each_id(exp_df, sampleSize)
        else:
            log_emitter(
                f"Warning: Unknown sample type '{sampleType}'. Defaulting to sample_each_path (Type 1).")
            result = sample_each_path(exp_df, sampleSize)  # Default or error

        log_emitter(f"Prepared {len(result)} expression sets for processing.")
        return result

    except Exception as e:
        log_emitter(f"Error during prepare_expressions: {e}")
        # Re-raise the exception to be caught by the main worker loop
        raise


def save_to_csv(df_to_save, output_fileName_base, log_emitter=None, cancel_event=None):
    """Saves DataFrame to CSV, emits logs, checks cancellation."""
    log_emitter = log_emitter or dummy_log_emitter

    if cancel_event and cancel_event.is_set():
        log_emitter("Cancellation detected before saving CSV.")
        return

    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        user_dir = os.path.expanduser('~')
        # Sanitize filename base further if needed
        sanitized_base = re.sub(r'[<>:"/\\|?*]', '', output_fileName_base)
        write_directory = os.path.join(user_dir, "Batch_File_Testing_Results")
        os.makedirs(write_directory, exist_ok=True)  # Ensure directory exists

        write_file = os.path.join(
            write_directory, f"{sanitized_base}_{timestamp}.csv")

        log_emitter(f"Saving results to {write_file}...")
        df_to_save.to_csv(write_file, index=False)
        log_emitter(f"Results successfully saved to {write_file}")

    except Exception as e:
        log_emitter(f"Error saving results to CSV: {e}")
        # Decide if you want to re-raise


def initialize_worker_login(clarifi_user, clarifi_pwd, log_emitter=None):
    """Initializer function for ThreadPoolExecutor to log in each worker thread."""
    # Note: log_emitter passed here might not work as expected depending on
    # how concurrent.futures passes initializer arguments and context.
    # Direct logging from here might be unreliable.
    try:
        # print(f"Initializing worker thread {threading.get_ident()}: Logging in...") # Use print for initializer
        cf.login(clarifi_user, clarifi_pwd)
        # print(f"Worker thread {threading.get_ident()} logged in.")
    except Exception as e:
        # Use print as logging might not work reliably from initializer
        print(
            f"ERROR: Failed to log in worker thread {threading.get_ident()}: {e}")
        # This error might prevent tasks from running correctly in this thread.


def main(artifactType, folderName, clarifi_user, clarifi_pwd,
         startDate, stopDate, basketName=None, portfolioName=None,
         expName=None, sampletype: int = 1, sample: int = 10,
         # --- Added arguments for GUI integration ---
         num_workers = MAX_WORKERS_DEFAULT,
         savetosqlite= False,
         log_emitter=None,
         progress_emitter=None,
         cancel_event=None,

         ):

    # Use dummy emitters/event if not provided (for standalone runs)
    log_emitter = log_emitter or dummy_log_emitter
    progress_emitter = progress_emitter or dummy_progress_emitter
    # Create a default event if none is passed, mainly for standalone script use
    _cancel_event = cancel_event or threading.Event()

    log_emitter("Main process started.")

    try:
        # Prepare expressions (pass emitters and event)
        # Clean up filename components
        artifactType_clean = str(artifactType or "").replace(" ", "")
        folderName_clean = str(folderName or "").replace(" ", "")
        output_fileName_base = f"{artifactType_clean}_{folderName_clean}.db"
        output_fileName_base = re.sub(r'[<>:"/\\|?*]', '', output_fileName_base)  # Sanitize
        output_fileName_base = output_fileName_base.replace("None", "")

        # Create SQLITE3 DB file
        db_file_path = os.path.join(os.path.expanduser("~"), "Batch_File_Testing_Results")
        os.makedirs(db_file_path, exist_ok=True)
        db_file = os.path.join(db_file_path, output_fileName_base)
        if os.path.exists(db_file):
            os.remove(db_file)

        expressions_to_process = prepare_expressions(
            artifactType, folderName, expName, sampletype, sample,
            clarifi_user, clarifi_pwd, log_emitter, _cancel_event
        )

        if _cancel_event.is_set():
            log_emitter("Processing cancelled after preparing expressions.")
            return pd.DataFrame()  # Return empty dataframe on cancellation

        if not expressions_to_process:
            log_emitter("No expressions prepared for processing. Exiting.")
            return pd.DataFrame()

        total_tasks = len(expressions_to_process)
        log_emitter(f"Expected Number of Requests: {total_tasks}")
        progress_emitter(0, total_tasks)  # Initial progress

        final_results = []
        completed_tasks = 0

        
        # Use ThreadPoolExecutor to process each path concurrently
        # Pass login credentials to initializer for thread safety
        with ThreadPoolExecutor(
            max_workers=num_workers,
            initializer=initialize_worker_login,
            initargs=(clarifi_user, clarifi_pwd)  # Pass credentials securely
        ) as executor:
            futures = []
            for _result_item in expressions_to_process:
                # Check cancellation before submitting each task
                if _cancel_event.is_set():
                    log_emitter(
                        "Cancellation detected before submitting all tasks.")
                    break  # Stop submitting new tasks

                path, exp_data = next(iter(_result_item.items()))
                # Submit task with necessary arguments including emitters and event
                futures.append(executor.submit(
                    process_path, path, exp_data, basketName, portfolioName,
                    startDate, stopDate, db_file,  # Pass base filename
                    log_emitter, _cancel_event, savetosqlite  # Pass cancellation event
                ))
            log_emitter(f"Submitted {len(futures)} tasks to executor.")

            # Collect results as they are completed
            for future in as_completed(futures):
                # Check cancellation frequently while waiting for results
                if _cancel_event.is_set():
                    log_emitter(
                        "Cancellation detected while processing results.")
                    # Attempt to cancel remaining futures (may not stop running tasks)
                    for f in futures:
                        f.cancel()
                    break  # Exit the result processing loop

                try:
                    result = future.result()  # Get result or raise exception
                    final_results.append(result)
                    # Log errors returned *within* the result dictionary
                    if result.get('error') and 'Cancelled' not in result['error']:
                        log_emitter(
                            f"Task for path '{result.get('path')}' completed with error: {result['error']}")
                    elif result.get('error') and 'Cancelled' in result['error']:
                        log_emitter(
                            f"Task for path '{result.get('path')}' was cancelled.")

                except Exception as exc:
                    # Catch errors raised *by* the process_path function itself
                    log_emitter(f'Task generated an exception: {exc}')
                    # Optionally append an error placeholder to final_results
                    # final_results.append({'error': str(exc), ...}) # Add relevant details

                finally:
                    # Increment progress regardless of success, failure, or cancellation signal from result
                    completed_tasks += 1
                    progress_emitter(completed_tasks, total_tasks)
            if savetosqlite:
                final_flush_all_buffers(db_file, "raw_data_exp", "metadata")

        log_emitter("Finished processing all submitted tasks.")

        # Process final results if not cancelled prematurely
        if not _cancel_event.is_set() and len(final_results) > 0:
            log_emitter("Aggregating final results...")
            df_summary = pd.DataFrame(final_results)

            # Save the summary results (pass emitters and event)
            # save_to_csv(df_summary, output_fileName_base + \
            #             "_Summary", log_emitter, _cancel_event)

            if _cancel_event.is_set():
                log_emitter(
                    "Process cancelled before running log model stats.")
                return [("API Summary", df_summary)]  # Return partial results if cancelled

            log_emitter("Processing log model stats...")
            # Ensure process_log_modelstats can handle cancellation or runs quickly
            log_df_list = process_log_modelstats(
                output_fileName_base, clarifi_user, clarifi_pwd)  # Add log_emitter/cancel if needed here
            
            output = [("API Summary", df_summary)] + log_df_list
            
            for _df in output:
                if not _df[1].empty and savetosqlite:
                    save_to_sqlite_buffered(_df[1], None, db_file, _df[0], None, log_emitter, cancel_event)
                    final_flush_all_buffers(db_file, _df[0], None, log_emitter, )
                

            log_emitter("Main process finished.")
            # Combine summary dataframe with any dataframes from log processing
            return [("API Summary", df_summary)] + log_df_list

        elif _cancel_event.is_set():
            log_emitter(
                "Process was cancelled. Final results may be incomplete.")
            # Return potentially partial results collected before cancellation
            if final_results:
                 df_summary = pd.DataFrame(final_results)
                 return [("API Summary (Cancelled)", df_summary)]
            else:
                 return pd.DataFrame()  # Or None

        else:
            log_emitter("No results were generated.")
            return pd.DataFrame()  # Return empty DataFrame if no results

    except Exception as e:
        # Catch errors in the main function logic itself (e.g., prepare_expressions)
        error_msg = f"Critical error in main function: {e}\n{traceback.format_exc()}"
        log_emitter(error_msg)
        # Re-raise the exception so the worker catches it and emits process_error
        raise
