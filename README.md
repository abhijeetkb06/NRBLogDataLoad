# NRB Log Data Loader for Couchbase

This script loads NRB log data from pipe-delimited files into a Couchbase bucket. It uses the timestamp from each log entry as the document ID and converts the pipe-delimited data into JSON format with meaningful field names.

## Prerequisites

- Python 3.6 or higher
- Couchbase Server 7.1 or higher
- Couchbase Python SDK 4.4.0

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure your Couchbase server is running and accessible (e.g., http://127.0.0.1:8091/ui/index.html).
3. Make sure the target bucket (default: `NRB-Log-Data`) exists in Couchbase.

## Usage

### 1. Place your `.nrb` log files in a directory
You can use any directory, for example:
- `./nrb_logs` (default)
- `~/Downloads/nrb_log_files`

### 2. Run the script Options

#### **A. Using the default directory and settings**
If your `.nrb` files are in `./nrb_logs` and you are using the default Couchbase settings:
```bash
python nrb_log_loader.py
```

#### **B. Specify a custom directory**
If your `.nrb` files are in a different directory (e.g., `~/Downloads/nrb_log_files`):
```bash
python nrb_log_loader.py --dir ~/Downloads/nrb_test_files
```

#### **C. Specify all arguments (directory, host, username, password, bucket)**
If you want to override all settings:
```bash
python nrb_log_loader.py \
  --dir /path/to/your/nrb/files \
  --host couchbase://127.0.0.1 \
  --username Administrator \
  --password password \
  --bucket NRB-Log-Data
```

#### **D. See all available options**
```bash
python nrb_log_loader.py --help
```

## Features

- Automatically processes all `.nrb` files in the specified directory
- Uses timestamp as document ID
- Handles variable number of fields in the log entries
- Converts pipe-delimited data to JSON format with meaningful field names
- Includes error handling and logging
- Supports upsert operations (insert or update)

## Logging

The script logs its operations to the console and to a CSV file (`nrb_processing_log.csv`) in the project directory. The log file includes:
- Timestamp
- NRB Log Filename
- Status (Processed/Failed/Partially Processed)
- Error/Exception details

## Document Structure

Each document in Couchbase will have the following structure:
```json
{
  "timestamp": "1749216786471",
  "protocol": "HTTPS",
  "host": "vspc-l-hp-x-01-002",
  "direction": "IN",
  "flag1": "0",
  "flag2": "0",
  "session_id": "31148062931111",
  "auth_type": "full-auth-init",
  "device_type": "AndroidDevice",
  "value1": "0",
  "reference_id": "3114800291111",
  "decryption_info": "DEGCID=xZWXqlz5P/46wLie9rPllg6Q=",
  "message": "Received EAP-AKA request from Device;app_name=com.google.android.gm",
  "device_id": "3532433411111"
  // any extra fields will be field_1, field_2, ...
}
```

## Error Handling

The script includes comprehensive error handling for:
- Connection issues
- File reading errors
- Data parsing errors
- Couchbase operation failures

## Customization

- To change the field mapping, edit the `FIELD_NAMES` list in `nrb_log_loader.py`.
- To skip or include empty fields, adjust the logic in the `parse_nrb_line` function.

---
