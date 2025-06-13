import os
import glob
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.collection import UpsertOptions
from datetime import timedelta, datetime
import logging
import csv
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default Configuration
DEFAULT_CONFIG = {
    'host': "couchbase://127.0.0.1",     # Local Couchbase server
    'username': "Administrator",          # Your Couchbase username
    'password': "password",               # Your Couchbase password
    'bucket': "NRB-Log-Data",            # Couchbase bucket name
    'nrb_dir': "./nrb_logs",             # Directory containing .nrb files
    'log_file': "nrb_processing_log.csv"  # Processing log file
}

FIELD_NAMES = [
    "timestamp",        # 0
    "protocol",         # 1
    "host",             # 2
    "direction",        # 3
    "flag1",            # 4
    "flag2",            # 5
    "session_id",       # 6
    "auth_type",        # 7
    "device_type",      # 8
    "value1",           # 9
    "reference_id",     # 10
    "decryption_info",  # 11
    "message",          # 12
    "device_id"         # 13
]

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Load NRB log files into Couchbase')
    parser.add_argument('--dir', '-d', 
                      default=DEFAULT_CONFIG['nrb_dir'],
                      help=f'Directory containing NRB log files (default: {DEFAULT_CONFIG["nrb_dir"]})')
    parser.add_argument('--host', 
                      default=DEFAULT_CONFIG['host'],
                      help=f'Couchbase connection string (default: {DEFAULT_CONFIG["host"]})')
    parser.add_argument('--username', '-u',
                      default=DEFAULT_CONFIG['username'],
                      help=f'Couchbase username (default: {DEFAULT_CONFIG["username"]})')
    parser.add_argument('--password', '-p',
                      default=DEFAULT_CONFIG['password'],
                      help=f'Couchbase password (default: {DEFAULT_CONFIG["password"]})')
    parser.add_argument('--bucket', '-b',
                      default=DEFAULT_CONFIG['bucket'],
                      help=f'Couchbase bucket name (default: {DEFAULT_CONFIG["bucket"]})')
    return parser.parse_args()

def setup_processing_log(log_file):
    """Create or initialize the processing log file."""
    log_exists = os.path.exists(log_file)
    with open(log_file, 'a', newline='') as f:
        writer = csv.writer(f)
        if not log_exists:
            writer.writerow(['Timestamp', 'NRB Log Filename', 'Status', 'Error/Exception'])
    return log_exists

def log_processing_status(log_file, filename, status, error=None):
    """Log the processing status to CSV file."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, filename, status, error if error else ''])

def connect_to_couchbase(host, username, password, bucket_name):
    """Establish connection to Couchbase cluster."""
    try:
        cluster = Cluster(
            host,
            ClusterOptions(PasswordAuthenticator(username, password))
        )
        bucket = cluster.bucket(bucket_name)
        collection = bucket.default_collection()
        logger.info("Successfully connected to Couchbase cluster")
        return collection
    except Exception as e:
        error_msg = f"Failed to connect to Couchbase: {str(e)}"
        logger.error(error_msg)
        log_processing_status(DEFAULT_CONFIG['log_file'], "CONNECTION", "Failed", error_msg)
        raise

def parse_nrb_line(line):
    """Parse a single NRB log line into a dictionary with named fields and extra fields as field_1, field_2, ..."""
    try:
        fields = [field.strip() for field in line.strip().split('|')]
        if not fields or not fields[0]:
            return None, "Empty or invalid line"  # Skip invalid lines

        doc = {}
        for i, value in enumerate(fields):
            if i < len(FIELD_NAMES):
                if value != "" or FIELD_NAMES[i] == "timestamp":  # Always include timestamp, skip other empty fields
                    doc[FIELD_NAMES[i]] = value
            else:
                if value != "":
                    doc[f"field_{i - len(FIELD_NAMES) + 1}"] = value
        return fields[0], doc  # (timestamp, document)
    except Exception as e:
        error_msg = f"Error parsing line: {str(e)}"
        logger.error(error_msg)
        return None, error_msg

def load_nrb_files_to_couchbase(collection, directory, log_file):
    """Process all .nrb files in the specified directory."""
    try:
        nrb_files = glob.glob(os.path.join(directory, "*.nrb"))
        logger.info(f"Found {len(nrb_files)} .nrb files to process")
        
        for file_path in nrb_files:
            filename = os.path.basename(file_path)
            logger.info(f"Processing file: {filename}")
            error_count = 0
            success_count = 0
            
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line_number, line in enumerate(f, 1):
                        if line.strip():  # Skip empty lines
                            result = parse_nrb_line(line)
                            if result:
                                doc_id, doc = result
                                if isinstance(doc, str):  # Error message
                                    error_count += 1
                                    log_processing_status(log_file, filename, "Failed", f"Line {line_number}: {doc}")
                                else:
                                    try:
                                        # Upsert document (insert or replace)
                                        collection.upsert(doc_id, doc)
                                        success_count += 1
                                        logger.debug(f"Successfully upserted document with ID: {doc_id}")
                                    except Exception as e:
                                        error_count += 1
                                        error_msg = f"Failed to upsert doc_id={doc_id}: {str(e)}"
                                        logger.error(error_msg)
                                        log_processing_status(log_file, filename, "Failed", error_msg)
                
                # Log final status for the file
                if error_count == 0:
                    log_processing_status(log_file, filename, "Processed", f"Successfully processed {success_count} records")
                else:
                    log_processing_status(log_file, filename, "Partially Processed", 
                                        f"Processed {success_count} records, {error_count} errors")
                
            except Exception as e:
                error_msg = f"Error processing file: {str(e)}"
                logger.error(error_msg)
                log_processing_status(log_file, filename, "Failed", error_msg)
        
        logger.info("Completed processing all files")
    except Exception as e:
        error_msg = f"Error processing directory {directory}: {str(e)}"
        logger.error(error_msg)
        log_processing_status(log_file, "DIRECTORY", "Failed", error_msg)

def main():
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Setup processing log
        setup_processing_log(DEFAULT_CONFIG['log_file'])
        
        # Connect to Couchbase
        collection = connect_to_couchbase(
            args.host,
            args.username,
            args.password,
            args.bucket
        )
        
        # Process all NRB files
        load_nrb_files_to_couchbase(collection, args.dir, DEFAULT_CONFIG['log_file'])
        
    except Exception as e:
        error_msg = f"An error occurred: {str(e)}"
        logger.error(error_msg)
        log_processing_status(DEFAULT_CONFIG['log_file'], "MAIN", "Failed", error_msg)

if __name__ == "__main__":
    main() 