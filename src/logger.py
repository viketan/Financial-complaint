import logging
from datetime import datetime
import os
import shutil
import pandas as pd
from src.constant import TIMESTAMP  
LOG_DIR = "logs"


def get_log_file_name():
    return f"log_{TIMESTAMP}.log"


LOG_FILE_NAME = get_log_file_name()

# Remove the existing log directory if it exists
if os.path.exists(LOG_DIR):
    shutil.rmtree(LOG_DIR)

# Create a new log directory
os.makedirs(LOG_DIR, exist_ok=True)

# Define the full path for the log file
LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

# Configure the logging settings
logging.basicConfig(
    filename=LOG_FILE_PATH,
    filemode="w",
    format='[%(asctime)s] \t%(levelname)s \t%(lineno)d \t%(filename)s \t%(funcName)s() \t%(message)s',
    level=logging.INFO
)

# Create a logger instance
logger = logging.getLogger("FinanceComplaint")
