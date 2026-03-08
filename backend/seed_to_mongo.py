#!/usr/bin/env python3
"""
Load sales data from local Excel files into MongoDB.
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
    files = [root / "Sales Data.xlsx", root / "Sales 2.xlsx"]
    if not all(f.exists() for f in files):
        print("Local Excel files not found: Sales Data.xlsx, Sales 2.xlsx", file=sys.stderr)
        sys.exit(1)
    dfs = [pd.read_excel(f) for f in files]
    combined = pd.concat(dfs, ignore_index=True)
    combined.columns = combined.columns.str.strip()
    records = combined.to_dict("records")
    for r in records:
        for k, v in list(r.items()):
            if isinstance(v, pd.Timestamp):
                r[k] = v.isoformat()
            elif pd.isna(v):
                r[k] = None
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
