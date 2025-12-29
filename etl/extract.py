import os
import pandas as pd
import logging

# ------------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LOG_DIR = os.path.join(BASE_DIR, "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "etl.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# File path
# ------------------------------------------------------------------
FILE_PATH = os.path.join(BASE_DIR, "..", "data", "raw", "olist_orders_dataset.csv")


def extract_csv(file_path: str) -> pd.DataFrame:
    """
    Extract data from a CSV file into a pandas DataFrame.
    """

    logger.info("Starting extract stage")

    # 1️ Validate file existence
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"Missing source file: {file_path}")

    # 2️ Read CSV
    df = pd.read_csv(file_path)

    # 3️ Log metrics
    logger.info(
        f"Extracted {len(df)} rows and {len(df.columns)} columns "
        f"from {os.path.basename(file_path)}"
    )

    # 4️ Fail fast
    if df.empty:
        logger.error("Extracted dataset is empty")
        raise ValueError("Source dataset is empty")

    logger.info("Extract stage completed successfully")

    return df


if __name__ == "__main__":
    extract_csv(FILE_PATH)
