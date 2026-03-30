#!/usr/bin/env python3
"""
Load sales data from local Sales_Data.xlsx into MongoDB.
Usage: MONGO_URL="mongodb://user:pass@host:port/?tls=false" DB_NAME=sales_dashboard python seed_to_mongo.py
"""

import os
import sys
from pathlib import Path

import pandas as pd
from pymongo import MongoClient

COLLECTION = "sales_data"

def get_mongo_url():
    url = os.environ.get("MONGO_URL")
    if not url:
        print("Set MONGO_URL", file=sys.stderr)
        sys.exit(1)
    return url

def main():
    mongo_url = get_mongo_url()
    db_name = os.environ.get("DB_NAME", "sales_dashboard")
    root = Path(__file__).resolve().parent.parent
    sales_file = root / "Sales_Data.xlsx"
    if not sales_file.exists():
        print("Local Excel file not found: Sales_Data.xlsx", file=sys.stderr)
        sys.exit(1)
    sheets = pd.read_excel(sales_file, sheet_name=None)
    frames = []
    for _, df in sheets.items():
        if df is None or df.empty:
            continue
        df.columns = df.columns.str.strip()
        if "NET_SALES_VALUE" in df.columns or "TRAN_ID" in df.columns or "Product" in df.columns:
            frames.append(df)
    if not frames:
        frames = [df for df in sheets.values() if df is not None and not df.empty]
    if not frames:
        print("No data rows found in Sales_Data.xlsx", file=sys.stderr)
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)
    combined.columns = combined.columns.str.strip()
    combined = combined.where(pd.notna(combined), None)
    records = combined.to_dict("records")
    for r in records:
        for k, v in list(r.items()):
            if isinstance(v, pd.Timestamp):
                r[k] = v.isoformat()
    client = MongoClient(mongo_url, serverSelectionTimeoutMS=15000)
    db = client[db_name]
    coll = db[COLLECTION]
    coll.delete_many({})
    if records:
        coll.insert_many(records)
    print(f"Loaded {len(records)} records into {db_name}.{COLLECTION}")
    client.close()

if __name__ == "__main__":
    main()
