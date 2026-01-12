import logging
import os
import sys

# Configure logging
log_level = logging.INFO if os.getenv("NODE_ENV") != "test" else logging.ERROR

# Create logger
logger = logging.getLogger("uidai-api")
logger.setLevel(log_level)

# Handler
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(log_level)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)

def get_logger():
    return logger
