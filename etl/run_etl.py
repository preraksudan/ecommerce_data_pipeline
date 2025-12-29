import os
import pandas as pd
from extract import extract_csv
from transform import transform_orders
from load import load_orders

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "..", "data", "raw", "olist_orders_dataset.csv")

if __name__ == "__main__":
    df_raw = extract_csv(DATA_PATH)
    df_transformed = transform_orders(df_raw)
    load_orders(df_transformed)
