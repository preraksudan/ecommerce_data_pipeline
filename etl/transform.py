import pandas as pd
import logging
import sys
import os


# ==========================================================
# LOGGING CONFIGURATION
# ==========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "..", "logs")

os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "etl_transform.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True  # Overrides any existing logger settings
)

logger = logging.getLogger(__name__)
logger.info("LOGGING  INITIALIZED For Transform Script - Check etl_transform.log")

# ==========================================================
# BUSINESS CONSTANTS
# ==========================================================
REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp"
}

VALID_ORDER_STATUSES = {
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "canceled",
    "unavailable"
}

# ==========================================================
# TRANSFORM FUNCTION
# ==========================================================
def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw orders data into a clean, analytics-ready dataset.
    """

    logger.info("========== STARTING TRANSFORM STAGE ==========")

    initial_row_count = len(df)
    logger.info(f"Rows received for transform: {initial_row_count}")

    # ------------------------------------------------------
    # 1. Schema validation
    # ------------------------------------------------------
    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    # ------------------------------------------------------
    # 2. Normalize order_status
    # ------------------------------------------------------
    df["order_status"] = (
        df["order_status"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # ------------------------------------------------------
    # 3. Filter valid order statuses
    # ------------------------------------------------------
    before_status_filter = len(df)
    df = df[df["order_status"].isin(VALID_ORDER_STATUSES)]

    logger.info(
        f"Rows after status filter: {len(df)} "
        f"(dropped {before_status_filter - len(df)})"
    )

    # ------------------------------------------------------
    # 4. Cast purchase timestamp
    # ------------------------------------------------------
    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"],
        errors="coerce"
    )

    # ------------------------------------------------------
    # 5. Drop invalid keys & timestamps
    # ------------------------------------------------------
    before_dropna = len(df)
    df = df.dropna(
        subset=["order_id", "customer_id", "order_purchase_timestamp"]
    )

    logger.info(
        f"Rows after dropping nulls: {len(df)} "
        f"(dropped {before_dropna - len(df)})"
    )

    # ------------------------------------------------------
    # 6. Deduplicate orders
    # ------------------------------------------------------
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["order_id"])

    logger.info(
        f"Rows after de-duplication: {len(df)} "
        f"(dropped {before_dedup - len(df)})"
    )

    # ------------------------------------------------------
    # 7. Derived analytics columns
    # ------------------------------------------------------
    df["order_purchase_date"] = df["order_purchase_timestamp"].dt.date
    df["order_purchase_year"] = df["order_purchase_timestamp"].dt.year
    df["order_purchase_month"] = df["order_purchase_timestamp"].dt.month

    final_row_count = len(df)

    # ------------------------------------------------------
    # 8. Final validation
    # ------------------------------------------------------
    if df.empty:
        logger.error("All rows removed during transformation")
        raise ValueError("Transform resulted in empty dataset")

    logger.info(
        f"Transform completed successfully | "
        f"rows before={initial_row_count}, rows after={final_row_count}"
    )

    # ------------------------------------------------------
    # DEBUG OUTPUT (LOCAL DEVELOPMENT ONLY)
    # ------------------------------------------------------
    print("\n========== TRANSFORM OUTPUT SAMPLE ==========")
    print(df.head(5))
    print(f"\nTotal records after transform: {final_row_count}")
    print("============================================\n")

    logger.info("========== TRANSFORM STAGE COMPLETED ==========")

    return df

# ==========================================================
# MAIN (FOR LOCAL TESTING)
# ==========================================================
if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_PATH = os.path.join(
        BASE_DIR, "..", "data", "raw", "olist_orders_dataset.csv"
    )

    logger.info("Loading raw dataset for transform test")

    raw_df = pd.read_csv(DATA_PATH)
    transform_orders(raw_df)