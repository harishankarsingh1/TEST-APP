import re
import os
import pandas as pd
from datetime import datetime
import apiClarifi as cf
# import numpy as np

def regex_pattern(pattern_name):
    patterns = {
        "log_pattern": re.compile(
            r'(\[\d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2},\d{3}\])'  # timestamp
            r'\s+'  # Space between timestamp and log level
            r'(\w+)'  # Log level (e.g., INFO, ERROR)
            r'\s+'  # Space between log level and source
            r'([\w\s]+)'  # Log source (e.g., filename or module)
            r'\s+\|\s+'  # Pipe separator
            r'(?:Model (\d+)\])?'  # Model number (e.g., 123)
            r'(.*?)'  # Log message content (non-greedy)
            # Next log entry or end
            r'(?=\n\[\d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2},\d{3}\]|\Z)',
            re.DOTALL
        ),
        "wf_pattern": re.compile(
            # Match the literal string 'DCSModelCacheState'
            r"DCSModelCacheState"
            # Match any character (except newlines) zero or more times
            r'.*'
            # Match the literal string 'Opened Expression Model for Workflow: '
            r"Opened Expression Model for Workflow: "
            # Capture everything inside single quotes (workflow name)
            r"'([^']+)'"
        ),
        "validation_pattern": re.compile(
            r"ModelValidater"            # Capture "ModelValidater"
            r".*Validation analyzing model for "  # Match literal string
            r"(\d+)"                       # Capture the total issues (digits)
            r" total issues for the date range: "  # Match literal string
            # Capture the date range (in format: mm/dd/yyyy - mm/dd/yyyy)
            r"([\d/]+ - [\d/]+)"
        ),
        "exp_pattern": re.compile(
            r"1\] (.*?\([\w/+=-]+\))(?:\(([\w/+=-]+)\))? of (.*?) <"
            )
        ,
        "duration_pattern": re.compile(
            # Capture the DataLoader part
            r"DataLoader"
            r".*Data\sLoading\sStats,\stotal\sduration:\s*(.*)"
        ),
        "timeseries_pattern": re.compile(
            # Capture identifier up to the newline
            r"([^\n]+)"
            # Capture the number of issues (digits)
            r"_numIssues:(\d+)"
            # Capture the expected number of points (digits)
            r"_pointsExpected:(\S+)"
            r"\s+:>"                                  # Match the ':>' separator
            # Capture the number of times invoked (digits)
            r"\sInvoked\s(\d+)\stimes,"
            # Match the literal phrase "for a total of"
            r"\sfor\sa\stotal\sof\s"
            # Capture the total time (non-whitespace characters)
            r"(\S+)"
            # Match the literal phrase ", average query time: "
            r",\saverage\squery\stime:\s"
            # Capture the average query time (non-whitespace characters)
            r"(\S+)",
        ),
        "pit_ciq_pattern": re.compile(
            # Capture everything between 'typeKey:' and ')'
            r"typeKey:\s([^)]+)"
            r"\)"                               # Match closing parenthesis after typeKey
            # Capture everything between 'for DataItem:' and ', for <digits> issues'
            r"\sfor\sDataItem:\s(.+?),\sfor\s(\d+)\sissues"
            r"\s:>"                             # Match ':>' separator
            # Capture the number of times invoked (digits)
            r"\sInvoked\s(\d+)\stimes,"
            r"\sfor\sa\stotal\sof\s"             # Match the literal phrase 'for a total of'
            # Capture the total time (can include symbols like %, ms, etc.)
            r"([^\s,]+(?:\S*)*)"
            # Match the literal phrase ', average query time:'
            r",\saverage\squery\stime:\s"
            # Capture the average query time (numeric values with optional symbols like %, ms, etc.)
            r"([^\s,]+(?:\S*)*)"
        ),
        "pit_specific_pattern": re.compile(
            # Capture the typeKey (everything between 'typeKey:' and ')')
            r"typeKey:\s([^)]+)"
            r"\)"                                          # Match closing parenthesis after typeKey
            # Capture the part between 'for' and ':> Invoked'
            r"\sfor\s([^:]+)"
            r"\s:>"                                        # Match ':>' separator
            # Capture the number of times invoked (digits)
            r"\sInvoked\s(\d+)\stimes,"
            # Match the literal phrase 'for a total of'
            r"\sfor\sa\stotal\sof\s"
            # Capture the total time (non-whitespace characters)
            r"(\S+)"
            # Match the literal phrase ', average query time:'
            r",\saverage\squery\stime:\s"
            # Capture the average query time (non-whitespace characters)
            r"(\S+)"
        ),
        "pit_currency_pattern": re.compile(
            # Capture the description with the date range
            r"PIT\sData\sload\sfor\s([^\[]+\[[^\]]+\])\s"
            # Capture the number of times invoked
            r":>\sInvoked\s(\d+)\stimes,\s"
            # Match the phrase 'for a total of'
            r"for\sa\stotal\sof\s"
            # Match 'average query time:'
            r"(\S+),\saverage\squery\stime:\s"
            # Capture the average query time
            r"(\S+)"
        ),
        "exp_exc_pattern": re.compile(
            # r"com\.clarifi\.expression.*exception.*(?:\r?\n|.)*?(?=com\.clarifi)", re.MULTILINE
            r"^(?:(?!\w+\.\w+\.).)*\b\w+\.\w+\.(?=.*expression)(?=.*exception).*",re.MULTILINE
        ),

        "SQL_query_pattern": re.compile(
            # r"query:\s*(select[\s\S]*?)(?=\s*com\.clarifi\.expression)", re.IGNORECASE
            #r"query:\s*(select[\s\S]*?)(?=\n\w+(\.\w+)+)", re.IGNORECASE|re.DOTALL
            r"query:\s*(.*?)(?=at \w+(\.\w+)+|\w+(\.\w+)+(\.\w+)+|$)", re.IGNORECASE|re.DOTALL
        ),
        "psql_pattern": re.compile(
            # re.compile(r".*Caused by: \w+(\.\w+)+(.*?)(?=\w+\.\w+)", re.DOTALL)
            r".*Caused by: (\w+(\.\w+)+): (.*?)(?=at \w+(\.\w+)+)", re.MULTILINE|re.DOTALL
        ),
        "model_start_pattern": re.compile(
            r"Successfully registered ExpressionModel.* \(dcsHandle: (\d+), dbHandle: (\d+)\)"
        ),
        "model_close_pattern": re.compile(
            r"Successfully Closed Expression Model(?:.*)\s*dcsHandle:\s*(\d+),\s*dbHandle:\s*(\d+)")
    }
    try:
        return patterns.get(pattern_name, None)
    except ValueError as e:
        raise e

def read_log_files(log_file):
    LOG_PATTERN = regex_pattern("log_pattern")
        
    with open(log_file, 'r') as file:
        log_data = file.read()
    log_final = re.findall(LOG_PATTERN, log_data)
    return log_final

def get_wf_info(log_data):
    # Initialize dictionaries for storing extracted information
    workflow_data = {}
    validation_data = {}
    workflow_begin = {}
    workflow_end = {}

    # Define regex patterns
    WF_PATTERN = regex_pattern("wf_pattern")
    VALIDATION_PATTERN = regex_pattern("validation_pattern")
    WF_BEGIN_PATTERN = regex_pattern("model_start_pattern")
    WF_END_PATTERN = regex_pattern("model_close_pattern")

    for log in log_data:
        log = tuple(item.strip() for item in log)

        model_id = log[3]
        timestamp = log[0].strip("[]").replace(",", ".")
        log_stripped = log[4]
        combined_log = log[2]+log_stripped

        # Extract workflow name
        match_ = WF_PATTERN.search(combined_log)
        if match_:
            workflow_data[model_id] = match_.group(1) if match_ else None

        # Extract validation issue details and store them
        match_ = VALIDATION_PATTERN.search(combined_log)
        if match_:
            validation_data[model_id] = (
                match_.group(1),
                match_.group(2)
            ) if match_ else None

        # Extract model begin timestamp
        match_ = WF_BEGIN_PATTERN.search(log_stripped)
        if match_:
            workflow_begin[model_id] = timestamp

        # Extract model end timestamp
        match_ = WF_END_PATTERN.search(log_stripped)
        if match_:
            workflow_end[model_id] = timestamp

    # Combine extracted data into a single list
    all_model_ids = set(workflow_data.keys()).union(
        validation_data.keys()).union(
        workflow_begin.keys()).union(
        workflow_end.keys()
    )

    aligned_data = [
        (
            model_id,
            # timestampe when model was successfully registered
            workflow_begin.get(model_id),
            # timestampe when model was successfully closed
            workflow_end.get(model_id),
            workflow_data.get(model_id),  # workflow Name
            validation_data.get(model_id, (None, None))[1],  # date_range
            validation_data.get(model_id, (None, None))[0]   # issue_count
        )
        for model_id in all_model_ids
    ]

    if not aligned_data:
        print("No Models Found")

    return pd.DataFrame(
        aligned_data,
        columns=["model_id", "execution_start", "execution_end",
            "wf_name", "date_range", "issue_count"],
    )

def get_exp_info(log_data):
    EXP_PATTERN = regex_pattern("exp_pattern")
    exp_id_pattern = re.compile(r'\(([^)]+?)\)\s*$')
    result = []

    for log in log_data:
        log = tuple(item.strip() for item in log)
        timestamp = log[0].strip("[]").replace(",", ".")
        match = EXP_PATTERN.findall(log[2] + log[4])
        if not match:
            continue

        for i , tuple_ in enumerate(match) :  # Use enumerate to get the index
            if tuple_[1] is None or tuple_[1] == "" :
                tuple_list = list(tuple_)  # Convert tuple to list for modification
                tuple_list[1] = exp_id_pattern.findall(tuple_[0])[0]
                tuple_list[0] = exp_id_pattern.sub("" , tuple_[0])
                match[i] = tuple(tuple_list)  # Update the tuple back in the match list

        match = [(timestamp, log[3],) + tuple_ for tuple_ in match]
        result.extend(match)
        
    return pd.DataFrame(
        result,
        columns=["timestamp", 'model_id', "exp_name", "exp_id", "universe"],
    ) if result else pd.DataFrame()

def get_dataloading(log_data):
    # Define regex patterns
    patterns = {
        "duration": regex_pattern("duration_pattern"),
        "timeseries": regex_pattern("timeseries_pattern"),
        "pit_ciq": regex_pattern("pit_ciq_pattern"),
        "pit_special": regex_pattern("pit_specific_pattern"),
        "pit_currency": regex_pattern("pit_currency_pattern")
    }
    # Define columns for each pattern
    pattern_columns = {
        "timeseries": ["exp_name", "num_issues", "points_expected", "times_invoked", "total_time", "average_time"],
        "pit_ciq": ["typeKey", "exp_name", "num_issues", "times_invoked", "total_time", "average_time"],
        "pit_special": ["typeKey", "exp_name", "times_invoked", "total_time", "average_time"],
        "pit_currency": ["exp_name", "times_invoked", "total_time", "average_time"]
    }

    result_data = []

    # Iterate through logs
    for log in log_data:
        log = tuple(item.strip() for item in log)
        # Strip once to avoid redundant operations
        timestamp = log[0].strip("[]").replace(",", ".")
        log_stripped = log[4]
        combined_log = log[2] + log_stripped
        # Extract the total_duration from the "duration" pattern
        duration_matches = patterns["duration"].search(combined_log)
        if not duration_matches:
            continue
        total_duration = duration_matches.group(1)
        # Process each pattern and store data in result_data
        for pattern_name, pattern in patterns.items():
            # Skip "duration" for DataFrame creation (no DataFrame needed for it)
            if pattern_name == "duration":
                continue
            columns = pattern_columns.get(pattern_name)
            matches = pattern.findall(log_stripped)
            if not matches:
                continue
            for match_ in matches:
                row_data = dict(zip(columns, match_))
                # Create a new dictionary with the additional keys at the beginning
                row_data_with_meta = {
                    'timestamp' : timestamp ,
                    'model_id' : log[3] ,  # Add modelId
                    'model_duration' : total_duration
                }
                row_data_with_meta.update({key : value.strip() for key , value in row_data.items()})
                result_data.append(row_data_with_meta)
    # Convert the accumulated data into a DataFrame and return
    return pd.DataFrame(result_data)

def get_errors(log_data):
    exception_pattern = regex_pattern("exp_exc_pattern")
    sql_pattern = regex_pattern("SQL_query_pattern")
    psql_pattern = regex_pattern("psql_pattern")

    result_data = []

    for log in log_data:
        log = tuple(item.strip() for item in log)
        timestamp = log[0].strip("[]").replace(",", ".")
        log_stripped = log[4].strip()
        expression_matches = exception_pattern.findall(log_stripped)
        
        if not expression_matches:
            continue
        exception_text = "\n".join(set(exp.split(":")[-1].strip() for exp in expression_matches))
        psql_matches = psql_pattern.findall(log_stripped)
        
        sql_match = sql_pattern.search(log_stripped)
        if sql_match:
            sql_text = sql_match[0].strip()
        else:
            sql_text = ""
        
        exception_text = exception_text.replace(sql_text, "")
        
        psql_text = "\n".join([", ".join(map(str, item[2:3])) for item in psql_matches])

        if psql_matches:
            result_data.append(
                (timestamp, sql_text, exception_text, psql_text)
            )

    return pd.DataFrame(
        result_data,
        columns=["timestamp", "sql_text", "exception_text", "psql_text"]
        ) if result_data else pd.DataFrame()

def safe_apply_datetime(df , columns, date_format):
    if not df.empty:
        try:
            df[columns] = df[columns].apply(pd.to_datetime, format=date_format, errors='coerce')
        except Exception  as e:
            print(f"Error applying pd.to_datetime to {columns}: {e}")
    else:
        print(f"DataFrame is empty, skipping datetime conversion for {columns}")

def duration_to_seconds(duration_str):
    """
    Converts a duration string in various formats (HH:MM:SS.mmm, MM:SS.mmm, SS.mmm, mmm) to seconds.

    Args:
      duration_str: A string representing the duration.

    Returns:
      The duration in seconds as a float, or None if the input is invalid.
    """

    try:
        duration_str = duration_str.strip()  # Remove leading/trailing spaces

        if ":" in duration_str:
            parts = duration_str.split(":")
            if len(parts) == 3:
                hours, minutes, seconds = map(float, parts)
                total_seconds = hours * 3600 + minutes * 60 + seconds
            elif len(parts) == 2:
                minutes, seconds = map(float, parts)
                total_seconds = minutes * 60 + seconds
            else:
                return None  # Invalid format
        else:
            if "." in duration_str:
                total_seconds = float(duration_str)
            else:
                total_seconds = float(duration_str) / 1000
        return total_seconds
    except ValueError:
        return None  # Could not convert to number
    
def process_log_modelstats(outputfile_name, clarifi_user, clarifi_pwd, hostname=None):
    """
    Process log data and return a DataFrame containing:
    - Model-related data from various sources
    - Min/max timestamps and the time difference for each model_id
    """
    cf.login(clarifi_user, clarifi_pwd)
    stats_ = cf.status()
    clarifi_loc = stats_[stats_['server']=="apiLogLocation"]["info"].iloc[0]
    path_parts = clarifi_loc.split(os.sep)

    base_directory = os.path.join(path_parts[0]+"\\", path_parts[1], "DCS")
    if not os.path.exists(base_directory):
        return []

    log_files = []
    print(f"Processing log data from {base_directory}")
    for file in os.listdir(base_directory) :
        if file.startswith("DCS") and file.endswith(".log") :
            log_files.append(os.path.join(base_directory , file))
    print(log_files)
    
    if not log_files:
        print(f"No log files found at path: {base_directory}")
        return []

    # Read and filter logs in one step
    log_data = []
    for log_file in log_files:
        log_entries = read_log_files(log_file)
        log_data.extend(log_entries)

    if not log_data:
        print("Log data is empty after filtering.")
        return []

    df_wf, df_exp, df_stats, df_errors = [pd.DataFrame() for _ in range(4)]

    df_wf = get_wf_info(log_data)
    if df_wf.empty:
        print("no workflow found")
        return []
    
    df_exp = get_exp_info(log_data)
    if df_exp.empty:
        print("no workflow found")
        return []

    df_stats = get_dataloading(log_data)
    df_errors = get_errors(log_data)
    df_stats.drop(columns=['typeKey'], errors='ignore', inplace=True)

    # convert model column to int
    if 'model_id' in df_wf.columns:
        df_wf['model_id']= pd.to_numeric(df_wf['model_id'])
        df_exp['model_id'] = pd.to_numeric(df_exp['model_id'])
        df_stats['model_id'] = pd.to_numeric(df_stats['model_id'])
    
    if not df_stats.empty:
        safe_apply_datetime(df_stats, ['timestamp'], '%d %b %Y %H:%M:%S.%f')
        df_stats['model_duration'] = df_stats['model_duration'].apply(duration_to_seconds)
        df_stats['total_time']= df_stats['total_time'].apply(duration_to_seconds)
    
        df_stats['times_invoked']= pd.to_numeric(df_stats['times_invoked'], errors='coerce').fillna(0).astype(int)
        if 'points_expected' in df_stats.columns:
            df_stats['points_expected']= pd.to_numeric(df_stats['points_expected'], errors='coerce').fillna(0).astype(int)
        df_stats['num_issues']= pd.to_numeric(df_stats['num_issues'], errors='coerce').fillna(0).astype(int)
        df_stats['total_time']= pd.to_numeric(df_stats['total_time'], errors='coerce')

        # Define the aggregation you want
        agg_spec = {
            'model_duration': ('model_duration', 'mean'),
            'num_issues': ('num_issues', 'sum'),
            'points_expected': ('points_expected', 'sum'),
            'times_invoked': ('times_invoked', 'sum'),
            'total_time': ('total_time', 'sum'),
            'count': ('model_id', 'count')  # Counting rows using a column that exists
        }

        # Filter to only include columns that exist in the DataFrame
        valid_agg_spec = {
            key: value for key, value in agg_spec.items()
            if value[0] in df_stats.columns
        }                      
        
        df_stats = df_stats.groupby(['timestamp', 'model_id', 'exp_name']).agg(**valid_agg_spec).reset_index()
        df_stats['average_time'] = df_stats['total_time'] /df_stats['times_invoked']
        
    # Convert to datetime using the correct format
    safe_apply_datetime(df_wf, ['execution_start', 'execution_end'], '%d %b %Y %H:%M:%S.%f')
    safe_apply_datetime(df_exp, ['timestamp'], '%d %b %Y %H:%M:%S.%f')    
    safe_apply_datetime(df_errors, ['timestamp'], '%d %b %Y %H:%M:%S.%f')
    
    # # Final merging of all DataFrames based on model_id and exp_name
    df_wf_final = df_exp.groupby(['universe' , 'model_id'])['exp_name'].count().reset_index()
    df_wf_final.rename(columns={"exp_name": "exp_count"}, inplace=True)
    df_wf = df_wf.merge(df_wf_final, on='model_id', how = "left")


    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    write_directory = os.path.join(os.path.expanduser("~"),"Batch_File_Testing_Results")
    os.makedirs(write_directory, exist_ok=True)
    write_file = os.path.join(write_directory, f"{outputfile_name}_{timestamp}.xlsx")
    
    with pd.ExcelWriter(write_file, engine='openpyxl') as writer:
        df_wf.to_excel(writer, sheet_name='workflow', index=False)
        df_exp.to_excel(writer, sheet_name='exp', index=False)
        df_stats.to_excel(writer, sheet_name='stats', index=False)
        df_errors.to_excel(writer, sheet_name='errors', index=False)

    return [("Workflow", df_wf),("Expressions", df_exp),
            ("stats", df_stats), ("errors", df_errors)]

if __name__ == "__main__":
   df = process_log_modelstats("SQL_CiQEstimates_Revenue","clarifi","clarifi","daredevil-srv")

