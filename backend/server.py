from fastapi import FastAPI, APIRouter, HTTPException, Query
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timezone
import pandas as pd
from io import BytesIO
import httpx

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Create the main app
app = FastAPI(title="Sales Analytics Dashboard API")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Pydantic Models
class KPIResponse(BaseModel):
    net_sales_value: float
    gross_sales_value: float
    returns_value: float
    returns_rate: float
    net_sales_qty: int
    total_transactions: int
    total_customers: int
    total_products: int
    avg_transaction_value: float
    avg_revenue_per_customer: float

class TrendDataPoint(BaseModel):
    month: str
    value: float
    quantity: int
    transactions: int
    growth_pct: Optional[float] = None

class ProductPerformance(BaseModel):
    product: str
    sales_value: float
    sales_qty: int
    contribution_pct: float
    returns_value: float
    returns_rate: float
    customer_count: int
    avg_price: float
    brand: str
    division: str

class GeographyPerformance(BaseModel):
    name: str
    sales_value: float
    sales_qty: int
    contribution_pct: float
    customer_count: int
    transaction_count: int
    avg_per_customer: float

class CustomerPerformance(BaseModel):
    customer_code: str
    customer_type: str
    sales_value: float
    sales_qty: int
    transaction_count: int
    avg_transaction: float
    stop_business: str
    city: str
    state: str
    zone: str

class PricingAnalysis(BaseModel):
    product: str
    avg_ppu: float
    avg_ptr: float
    avg_mrp: float
    price_realization: float
    avg_discount: float
    total_value: float

class RiskIndicator(BaseModel):
    metric: str
    value: float
    threshold: float
    status: str
    description: str

class InsightRequest(BaseModel):
    context: Optional[str] = None
    data_summary: Optional[Dict[str, Any]] = None
    dashboard: Optional[str] = None  # e.g. "Executive Summary", "Product Intelligence"

class InsightResponse(BaseModel):
    insights: List[str]
    recommendations: List[str]
    action_items: List[str]

# Data loader endpoint
@api_router.post("/data/load")
async def load_sales_data():
    """Load sales data from Excel files into MongoDB"""
    try:
        urls = [
            "https://customer-assets.emergentagent.com/job_0882115c-ae27-43f3-a311-0e39eb3799dd/artifacts/z8jvpj4c_Sales%20Data.xlsx",
            "https://customer-assets.emergentagent.com/job_0882115c-ae27-43f3-a311-0e39eb3799dd/artifacts/hywzhcox_Sales%202.xlsx"
        ]
        
        all_data = []
        async with httpx.AsyncClient() as http_client:
            for url in urls:
                response = await http_client.get(url, timeout=60.0)
                df = pd.read_excel(BytesIO(response.content))
                all_data.append(df)
        
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Clean column names (remove leading spaces)
        combined_df.columns = combined_df.columns.str.strip()
        
        # Convert to records
        records = combined_df.to_dict('records')
        
        # Convert datetime objects to strings
        for record in records:
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.isoformat()
                elif pd.isna(value):
                    record[key] = None
        
        # Clear existing data and insert new
        await db.sales_data.delete_many({})
        if records:
            await db.sales_data.insert_many(records)
        
        return {"status": "success", "records_loaded": len(records)}
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Dashboard Overview Endpoints
@api_router.get("/dashboard/overview", response_model=KPIResponse)
async def get_dashboard_overview():
    """Get executive dashboard KPIs"""
    pipeline = [
        {
            "$group": {
                "_id": None,
                "net_sales_value": {"$sum": "$NET_SALES_VALUE"},
                "net_sales_qty": {"$sum": "$NET_SALES_QTY"},
                "transactions": {"$addToSet": "$TRAN_ID"},
                "customers": {"$addToSet": "$CUST_CODE"},
                "products": {"$addToSet": "$Product"}
            }
        }
    ]
    
    result = await db.sales_data.aggregate(pipeline).to_list(1)
    
    if not result:
        raise HTTPException(status_code=404, detail="No data found. Please load data first.")
    
    data = result[0]
    
    # Calculate gross and returns
    gross_pipeline = [
        {"$match": {"NET_SALES_VALUE": {"$gt": 0}}},
        {"$group": {"_id": None, "gross": {"$sum": "$NET_SALES_VALUE"}}}
    ]
    gross_result = await db.sales_data.aggregate(gross_pipeline).to_list(1)
    gross_value = gross_result[0]["gross"] if gross_result else 0
    
    returns_pipeline = [
        {"$match": {"NET_SALES_VALUE": {"$lt": 0}}},
        {"$group": {"_id": None, "returns": {"$sum": "$NET_SALES_VALUE"}}}
    ]
    returns_result = await db.sales_data.aggregate(returns_pipeline).to_list(1)
    returns_value = abs(returns_result[0]["returns"]) if returns_result else 0
    
    total_transactions = len(data["transactions"])
    total_customers = len(data["customers"])
    
    return KPIResponse(
        net_sales_value=data["net_sales_value"],
        gross_sales_value=gross_value,
        returns_value=returns_value,
        returns_rate=(returns_value / gross_value * 100) if gross_value > 0 else 0,
        net_sales_qty=data["net_sales_qty"],
        total_transactions=total_transactions,
        total_customers=total_customers,
        total_products=len(data["products"]),
        avg_transaction_value=data["net_sales_value"] / total_transactions if total_transactions > 0 else 0,
        avg_revenue_per_customer=data["net_sales_value"] / total_customers if total_customers > 0 else 0
    )

@api_router.get("/dashboard/trends")
async def get_monthly_trends():
    """Get monthly sales trends"""
    pipeline = [
        {
            "$addFields": {
                "doc_date_parsed": {
                    "$dateFromString": {
                        "dateString": "$DOC_DATE",
                        "onError": None,
                        "onNull": None
                    }
                }
            }
        },
        {
            "$match": {"doc_date_parsed": {"$ne": None}}
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}
                },
                "value": {"$sum": "$NET_SALES_VALUE"},
                "quantity": {"$sum": "$NET_SALES_QTY"},
                "transactions": {"$addToSet": "$TRAN_ID"}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    
    results = await db.sales_data.aggregate(pipeline).to_list(100)
    
    trends = []
    prev_value = None
    for r in results:
        growth = None
        if prev_value and prev_value > 0:
            growth = ((r["value"] - prev_value) / prev_value) * 100
        
        trends.append({
            "month": r["_id"],
            "value": r["value"],
            "quantity": r["quantity"],
            "transactions": len(r["transactions"]),
            "growth_pct": round(growth, 2) if growth else None
        })
        prev_value = r["value"]
    
    return trends

@api_router.get("/dashboard/concentration")
async def get_concentration_metrics():
    """Get revenue concentration metrics"""
    # Zone concentration
    zone_pipeline = [
        {"$group": {"_id": "$Zone_New", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}}
    ]
    zones = await db.sales_data.aggregate(zone_pipeline).to_list(100)
    total = sum(z["value"] for z in zones)
    
    zone_data = [{"name": z["_id"], "value": z["value"], "pct": round(z["value"]/total*100, 2) if total > 0 else 0} for z in zones]
    
    # State concentration
    state_pipeline = [
        {"$group": {"_id": "$State", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}},
        {"$limit": 10}
    ]
    states = await db.sales_data.aggregate(state_pipeline).to_list(10)
    state_data = [{"name": s["_id"], "value": s["value"], "pct": round(s["value"]/total*100, 2) if total > 0 else 0} for s in states]
    
    # Top 3 states contribution
    top_3_pct = sum(s["pct"] for s in state_data[:3])
    
    # Customer concentration
    cust_pipeline = [
        {"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}},
        {"$limit": 10}
    ]
    customers = await db.sales_data.aggregate(cust_pipeline).to_list(10)
    top_10_cust_pct = sum(c["value"] for c in customers) / total * 100 if total > 0 else 0
    
    return {
        "zones": zone_data,
        "top_states": state_data,
        "top_3_states_pct": round(top_3_pct, 2),
        "top_10_customers_pct": round(top_10_cust_pct, 2)
    }

# Revenue & Growth KPI drill-down (per KPI table)
_DATE_FIELDS = {"$addFields": {"_doc_date": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}}

def _build_match(filters: dict) -> Optional[dict]:
    match = {}
    if filters.get("month") and len(str(filters["month"])) >= 7:
        match["DOC_DATE"] = {"$regex": "^" + str(filters["month"])[:7]}
    if filters.get("zone"):
        match["Zone_New"] = filters["zone"]
    if filters.get("state"):
        match["State"] = filters["state"]
    if filters.get("product"):
        match["Product"] = filters["product"]
    if filters.get("customer"):
        match["CUST_CODE"] = filters["customer"]
    return match if match else None

@api_router.get("/revenue-kpi/summary")
async def get_revenue_kpi_summary():
    """Summary of all 6 Revenue & Growth KPIs for the dashboard."""
    # Net sales, gross, returns - aggregate all documents (no date filter so data always loads)
    pipeline = [
        {"$group": {
            "_id": None,
            "net_sales": {"$sum": "$NET_SALES_VALUE"},
            "gross_sales": {"$sum": {"$cond": [{"$gt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}},
            "returns_raw": {"$sum": {"$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}},
        }}
    ]
    r = await db.sales_data.aggregate(pipeline).to_list(1)
    if not r:
        return {
            "net_sales_value": 0,
            "gross_sales_value": 0,
            "returns_value": 0,
            "returns_rate_pct": 0,
            "mom_growth_pct": None,
            "revenue_concentration_pct": 0,
            "data_loaded": False,
        }
    d = r[0]
    gross = d.get("gross_sales") or 0
    returns_value = abs(d.get("returns_raw") or 0)
    net_sales = d.get("net_sales") or 0
    returns_rate = (returns_value / gross * 100) if gross > 0 else 0

    # Monthly for MoM (use same date parsing as dashboard/trends)
    month_pipe = [
        {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
        {"$match": {"doc_date_parsed": {"$ne": None}}},
        {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}, "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"_id": 1}}
    ]
    months = await db.sales_data.aggregate(month_pipe).to_list(24)
    mom_growth = None
    if len(months) >= 2:
        curr, prev = months[-1]["value"], months[-2]["value"]
        if prev and prev != 0:
            mom_growth = round((curr - prev) / prev * 100, 2)

    # Revenue concentration (top 3 states / total)
    state_pipe = [
        {"$group": {"_id": "$State", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}},
        {"$limit": 3}
    ]
    top_states = await db.sales_data.aggregate(state_pipe).to_list(3)
    total_for_conc = sum(s["value"] for s in top_states)
    all_pipe = [{"$group": {"_id": None, "total": {"$sum": "$NET_SALES_VALUE"}}}]
    all_r = await db.sales_data.aggregate(all_pipe).to_list(1)
    total_rev = (all_r[0]["total"]) if all_r else 0
    revenue_concentration = round(total_for_conc / total_rev * 100, 2) if total_rev and total_rev > 0 else 0

    return {
        "net_sales_value": round(net_sales, 2),
        "gross_sales_value": round(gross, 2),
        "returns_value": round(returns_value, 2),
        "returns_rate_pct": round(returns_rate, 2),
        "mom_growth_pct": mom_growth,
        "revenue_concentration_pct": revenue_concentration,
        "data_loaded": True,
    }

@api_router.get("/revenue-kpi/drill")
async def revenue_kpi_drill(
    kpi: str = Query(..., description="net_sales_value | gross_sales_value | returns_value | returns_rate_pct | mom_growth_pct | revenue_concentration_pct"),
    group_by: str = Query(..., description="month | zone | state | product | customer"),
    month: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    customer: Optional[str] = Query(None),
):
    """Drill-down data for Revenue & Growth KPIs at the given granularity."""
    if group_by not in ("month", "zone", "state", "product", "customer"):
        raise HTTPException(status_code=400, detail="Invalid group_by")
    filters = {"month": month, "zone": zone, "state": state, "product": product, "customer": customer}
    filters = {k: v for k, v in filters.items() if v}

    stages = []
    if group_by == "month":
        stages.append(_DATE_FIELDS)
        stages.append({"$match": {"_doc_date": {"$ne": None}}})
        stages.append({"$addFields": {"_month_str": {"$dateToString": {"format": "%Y-%m", "date": "$_doc_date"}}}})
    match = _build_match(filters)
    if match:
        stages.append({"$match": match})

    group_id = {"month": "$_month_str", "zone": "$Zone_New", "state": "$State", "product": "$Product", "customer": "$CUST_CODE"}[group_by]

    if kpi == "net_sales_value":
        stages.append({"$group": {"_id": group_id, "value": {"$sum": "$NET_SALES_VALUE"}}})
    elif kpi == "gross_sales_value":
        stages.append({"$match": {"NET_SALES_VALUE": {"$gt": 0}}})
        stages.append({"$group": {"_id": group_id, "value": {"$sum": "$NET_SALES_VALUE"}}})
    elif kpi == "returns_value":
        stages.append({"$match": {"NET_SALES_VALUE": {"$lt": 0}}})
        stages.append({"$group": {"_id": group_id, "value": {"$sum": "$NET_SALES_VALUE"}}})
    elif kpi == "returns_rate_pct":
        stages.append({"$group": {"_id": group_id, "gross": {"$sum": {"$cond": [{"$gt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}}, "returns": {"$sum": {"$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}}}})
        stages.append({"$project": {"_id": 1, "value": {"$cond": [{"$gt": ["$gross", 0]}, {"$multiply": [{"$divide": [{"$abs": "$returns"}, "$gross"]}, 100]}, 0]}}})
    elif kpi == "mom_growth_pct":
        if group_by != "month":
            stages = [_DATE_FIELDS, {"$match": {"_doc_date": {"$ne": None}}}, {"$addFields": {"_month_str": {"$dateToString": {"format": "%Y-%m", "date": "$_doc_date"}}}}] + (stages or [])
            group_id = "$_month_str"
        stages.append({"$group": {"_id": group_id, "value": {"$sum": "$NET_SALES_VALUE"}}})
        stages.append({"$sort": {"_id": 1}})
        rows = await db.sales_data.aggregate(stages).to_list(100)
        result = []
        for i, r in enumerate(rows):
            prev_val = rows[i - 1]["value"] if i > 0 else None
            growth = (round((r["value"] - prev_val) / prev_val * 100, 2) if prev_val and prev_val != 0 else None)
            result.append({"dimension": r["_id"], "value": round(r["value"], 2), "growth_pct": growth})
        return result
    elif kpi == "revenue_concentration_pct":
        if group_by == "state":
            state_pipe = stages + [{"$group": {"_id": "$State", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}, {"$limit": 3}]
            top3 = await db.sales_data.aggregate(state_pipe).to_list(3)
            total_pipe = stages + [{"$group": {"_id": None, "total": {"$sum": "$NET_SALES_VALUE"}}}]
            total_all = await db.sales_data.aggregate(total_pipe).to_list(1)
            total = total_all[0]["total"] if total_all else 0
            return [{"dimension": s["_id"], "value": round(s["value"], 2), "pct": round(s["value"] / total * 100, 2) if total else 0} for s in top3]
        stages.append({"$group": {"_id": group_id, "value": {"$sum": "$NET_SALES_VALUE"}}})
        stages.append({"$sort": {"value": -1}})
        rows = await db.sales_data.aggregate(stages).to_list(100)
        total = sum(x["value"] for x in rows)
        return [{"dimension": x["_id"], "value": round(x["value"], 2), "pct": round(x["value"] / total * 100, 2) if total else 0} for x in rows]
    else:
        raise HTTPException(status_code=400, detail="Invalid kpi")

    stages.append({"$sort": {"value": -1}})
    stages.append({"$limit": 100})
    rows = await db.sales_data.aggregate(stages).to_list(100)
    if kpi == "returns_value":
        for r in rows:
            r["value"] = abs(r["value"])
    return [{"dimension": r["_id"], "value": round(r["value"], 2)} for r in rows]

# Product Endpoints
@api_router.get("/products/performance")
async def get_product_performance():
    """Get product performance metrics"""
    pipeline = [
        {
            "$group": {
                "_id": "$Product",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                "customers": {"$addToSet": "$CUST_CODE"},
                "brand": {"$first": "$ITEM_BRAND KPMG"},
                "division": {"$first": "$Div_Code (Mapping HQ)"},
                "avg_ppu": {"$avg": "$PPU"}
            }
        },
        {"$sort": {"sales_value": -1}}
    ]
    
    products = await db.sales_data.aggregate(pipeline).to_list(100)
    total = sum(p["sales_value"] for p in products)
    
    # Get returns by product
    returns_pipeline = [
        {"$match": {"NET_SALES_VALUE": {"$lt": 0}}},
        {"$group": {"_id": "$Product", "returns": {"$sum": "$NET_SALES_VALUE"}}}
    ]
    returns_data = await db.sales_data.aggregate(returns_pipeline).to_list(100)
    returns_map = {r["_id"]: abs(r["returns"]) for r in returns_data}
    
    result = []
    for p in products:
        gross = p["sales_value"] + returns_map.get(p["_id"], 0)
        returns_val = returns_map.get(p["_id"], 0)
        result.append({
            "product": p["_id"],
            "sales_value": p["sales_value"],
            "sales_qty": p["sales_qty"],
            "contribution_pct": round(p["sales_value"] / total * 100, 2) if total > 0 else 0,
            "returns_value": returns_val,
            "returns_rate": round(returns_val / gross * 100, 2) if gross > 0 else 0,
            "customer_count": len(p["customers"]),
            "avg_price": round(p["avg_ppu"], 2) if p["avg_ppu"] else 0,
            "brand": p["brand"],
            "division": p["division"]
        })
    
    return result

@api_router.get("/products/{product_name}/details")
async def get_product_details(product_name: str):
    """Get detailed drill-down for a specific product"""
    # Monthly trend for product
    trend_pipeline = [
        {"$match": {"Product": product_name}},
        {
            "$addFields": {
                "doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None}}
            }
        },
        {"$match": {"doc_date_parsed": {"$ne": None}}},
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}},
                "value": {"$sum": "$NET_SALES_VALUE"},
                "quantity": {"$sum": "$NET_SALES_QTY"}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    trends = await db.sales_data.aggregate(trend_pipeline).to_list(100)
    
    # By zone
    zone_pipeline = [
        {"$match": {"Product": product_name}},
        {"$group": {"_id": "$Zone_New", "value": {"$sum": "$NET_SALES_VALUE"}, "qty": {"$sum": "$NET_SALES_QTY"}}},
        {"$sort": {"value": -1}}
    ]
    by_zone = await db.sales_data.aggregate(zone_pipeline).to_list(100)
    
    # By state
    state_pipeline = [
        {"$match": {"Product": product_name}},
        {"$group": {"_id": "$State", "value": {"$sum": "$NET_SALES_VALUE"}, "qty": {"$sum": "$NET_SALES_QTY"}}},
        {"$sort": {"value": -1}}
    ]
    by_state = await db.sales_data.aggregate(state_pipeline).to_list(100)
    
    # Top customers
    cust_pipeline = [
        {"$match": {"Product": product_name}},
        {"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}, "qty": {"$sum": "$NET_SALES_QTY"}}},
        {"$sort": {"value": -1}},
        {"$limit": 10}
    ]
    top_customers = await db.sales_data.aggregate(cust_pipeline).to_list(10)
    
    return {
        "product": product_name,
        "monthly_trend": [{"month": t["_id"], "value": t["value"], "quantity": t["quantity"]} for t in trends],
        "by_zone": [{"zone": z["_id"], "value": z["value"], "qty": z["qty"]} for z in by_zone],
        "by_state": [{"state": s["_id"], "value": s["value"], "qty": s["qty"]} for s in by_state],
        "top_customers": [{"customer": c["_id"], "value": c["value"], "qty": c["qty"]} for c in top_customers]
    }

# Geography Endpoints
@api_router.get("/geography/zones")
async def get_zone_performance():
    """Get zone-level performance"""
    pipeline = [
        {
            "$group": {
                "_id": "$Zone_New",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                "customers": {"$addToSet": "$CUST_CODE"},
                "transactions": {"$addToSet": "$TRAN_ID"}
            }
        },
        {"$sort": {"sales_value": -1}}
    ]
    
    zones = await db.sales_data.aggregate(pipeline).to_list(100)
    total = sum(z["sales_value"] for z in zones)
    
    return [{
        "name": z["_id"],
        "sales_value": z["sales_value"],
        "sales_qty": z["sales_qty"],
        "contribution_pct": round(z["sales_value"] / total * 100, 2) if total > 0 else 0,
        "customer_count": len(z["customers"]),
        "transaction_count": len(z["transactions"]),
        "avg_per_customer": round(z["sales_value"] / len(z["customers"]), 2) if z["customers"] else 0
    } for z in zones]

@api_router.get("/geography/zones/{zone_name}/states")
async def get_states_by_zone(zone_name: str):
    """Get state-level drill-down for a zone"""
    pipeline = [
        {"$match": {"Zone_New": zone_name}},
        {
            "$group": {
                "_id": "$State",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                "customers": {"$addToSet": "$CUST_CODE"},
                "transactions": {"$addToSet": "$TRAN_ID"}
            }
        },
        {"$sort": {"sales_value": -1}}
    ]
    
    states = await db.sales_data.aggregate(pipeline).to_list(100)
    total = sum(s["sales_value"] for s in states)
    
    return [{
        "name": s["_id"],
        "sales_value": s["sales_value"],
        "sales_qty": s["sales_qty"],
        "contribution_pct": round(s["sales_value"] / total * 100, 2) if total > 0 else 0,
        "customer_count": len(s["customers"]),
        "transaction_count": len(s["transactions"]),
        "avg_per_customer": round(s["sales_value"] / len(s["customers"]), 2) if s["customers"] else 0
    } for s in states]

@api_router.get("/geography/states/{state_name}/cities")
async def get_cities_by_state(state_name: str):
    """Get city-level drill-down for a state"""
    pipeline = [
        {"$match": {"State": state_name}},
        {
            "$group": {
                "_id": "$CITY",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                "customers": {"$addToSet": "$CUST_CODE"},
                "transactions": {"$addToSet": "$TRAN_ID"}
            }
        },
        {"$sort": {"sales_value": -1}}
    ]
    
    cities = await db.sales_data.aggregate(pipeline).to_list(200)
    total = sum(c["sales_value"] for c in cities)
    
    return [{
        "name": c["_id"],
        "sales_value": c["sales_value"],
        "sales_qty": c["sales_qty"],
        "contribution_pct": round(c["sales_value"] / total * 100, 2) if total > 0 else 0,
        "customer_count": len(c["customers"]),
        "transaction_count": len(c["transactions"]),
        "avg_per_customer": round(c["sales_value"] / len(c["customers"]), 2) if c["customers"] else 0
    } for c in cities]

# Customer Endpoints
@api_router.get("/customers/performance")
async def get_customer_performance(limit: int = Query(50, le=100)):
    """Get customer performance metrics"""
    pipeline = [
        {
            "$group": {
                "_id": "$CUST_CODE",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                "transactions": {"$addToSet": "$TRAN_ID"},
                "customer_type": {"$first": "$Customer Type (Bucket)"},
                "stop_business": {"$first": "$STOP_BUSINESS"},
                "city": {"$first": "$CITY"},
                "state": {"$first": "$State"},
                "zone": {"$first": "$Zone_New"}
            }
        },
        {"$sort": {"sales_value": -1}},
        {"$limit": limit}
    ]
    
    customers = await db.sales_data.aggregate(pipeline).to_list(limit)
    
    return [{
        "customer_code": c["_id"],
        "customer_type": c["customer_type"],
        "sales_value": c["sales_value"],
        "sales_qty": c["sales_qty"],
        "transaction_count": len(c["transactions"]),
        "avg_transaction": round(c["sales_value"] / len(c["transactions"]), 2) if c["transactions"] else 0,
        "stop_business": c["stop_business"] or "N",
        "city": c["city"],
        "state": c["state"],
        "zone": c["zone"]
    } for c in customers]

@api_router.get("/customers/concentration")
async def get_customer_concentration():
    """Get customer concentration (Pareto) analysis"""
    pipeline = [
        {
            "$group": {
                "_id": "$CUST_CODE",
                "sales_value": {"$sum": "$NET_SALES_VALUE"}
            }
        },
        {"$sort": {"sales_value": -1}}
    ]
    
    customers = await db.sales_data.aggregate(pipeline).to_list(500)
    total = sum(c["sales_value"] for c in customers)
    
    cumulative = 0
    pareto_data = []
    for i, c in enumerate(customers):
        cumulative += c["sales_value"]
        pareto_data.append({
            "rank": i + 1,
            "customer": c["_id"],
            "value": c["sales_value"],
            "pct": round(c["sales_value"] / total * 100, 2) if total > 0 else 0,
            "cumulative_pct": round(cumulative / total * 100, 2) if total > 0 else 0
        })
    
    # Key metrics
    top_10_pct = pareto_data[9]["cumulative_pct"] if len(pareto_data) >= 10 else 0
    top_20_pct = pareto_data[19]["cumulative_pct"] if len(pareto_data) >= 20 else 0
    
    return {
        "pareto_data": pareto_data[:50],  # Top 50 for visualization
        "top_10_customers_pct": top_10_pct,
        "top_20_customers_pct": top_20_pct,
        "total_customers": len(customers)
    }

@api_router.get("/customers/risk")
async def get_customer_risk():
    """Get customer risk indicators"""
    # Stop business customers
    stop_pipeline = [
        {"$match": {"STOP_BUSINESS": "Y"}},
        {
            "$group": {
                "_id": "$CUST_CODE",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "city": {"$first": "$CITY"},
                "state": {"$first": "$State"}
            }
        },
        {"$sort": {"sales_value": -1}}
    ]
    stop_customers = await db.sales_data.aggregate(stop_pipeline).to_list(100)
    
    # Total for context
    total_pipeline = [{"$group": {"_id": None, "total": {"$sum": "$NET_SALES_VALUE"}}}]
    total_result = await db.sales_data.aggregate(total_pipeline).to_list(1)
    total = total_result[0]["total"] if total_result else 0
    
    stop_value = sum(c["sales_value"] for c in stop_customers)
    
    return {
        "stop_business_customers": [{
            "customer": c["_id"],
            "value": c["sales_value"],
            "city": c["city"],
            "state": c["state"]
        } for c in stop_customers],
        "stop_business_count": len(stop_customers),
        "stop_business_value": stop_value,
        "stop_business_pct": round(stop_value / total * 100, 2) if total > 0 else 0
    }

# Pricing Endpoints
@api_router.get("/pricing/analysis")
async def get_pricing_analysis():
    """Get pricing and discount analysis"""
    pipeline = [
        {
            "$group": {
                "_id": "$Product",
                "avg_ppu": {"$avg": "$PPU"},
                "avg_ptr": {"$avg": "$PTR"},
                "avg_mrp": {"$avg": "$MRP"},
                "avg_rate": {"$avg": "$Rate (GPTS_PriceList)"},
                "avg_discount": {"$avg": "$Discount %"},
                "total_value": {"$sum": "$NET_SALES_VALUE"}
            }
        },
        {"$sort": {"total_value": -1}}
    ]
    
    products = await db.sales_data.aggregate(pipeline).to_list(100)
    
    return [{
        "product": p["_id"],
        "avg_ppu": round(p["avg_ppu"], 2) if p["avg_ppu"] else 0,
        "avg_ptr": round(p["avg_ptr"], 2) if p["avg_ptr"] else 0,
        "avg_mrp": round(p["avg_mrp"], 2) if p["avg_mrp"] else 0,
        "price_realization": round(p["avg_ppu"] / p["avg_rate"], 4) if p["avg_rate"] and p["avg_ppu"] else 1,
        "avg_discount": round(p["avg_discount"], 2) if p["avg_discount"] else 0,
        "total_value": p["total_value"]
    } for p in products]

@api_router.get("/pricing/discount-distribution")
async def get_discount_distribution():
    """Get discount distribution analysis"""
    pipeline = [
        {
            "$bucket": {
                "groupBy": "$Discount %",
                "boundaries": [0, 0.01, 5, 10, 20, 50, 100],
                "default": "Other",
                "output": {
                    "count": {"$sum": 1},
                    "value": {"$sum": "$NET_SALES_VALUE"}
                }
            }
        }
    ]
    
    distribution = await db.sales_data.aggregate(pipeline).to_list(100)
    
    labels = ["0%", "0-5%", "5-10%", "10-20%", "20-50%", "50%+", "Other"]
    result = []
    for i, d in enumerate(distribution):
        label = labels[i] if i < len(labels) else str(d["_id"])
        result.append({
            "range": label,
            "count": d["count"],
            "value": d["value"]
        })
    
    return result

# Risk & Governance Endpoints
@api_router.get("/risk/indicators")
async def get_risk_indicators():
    """Get risk governance indicators"""
    # Get total values
    total_pipeline = [
        {
            "$group": {
                "_id": None,
                "total_value": {"$sum": "$NET_SALES_VALUE"},
                "total_lines": {"$sum": 1}
            }
        }
    ]
    total_result = await db.sales_data.aggregate(total_pipeline).to_list(1)
    totals = total_result[0] if total_result else {"total_value": 0, "total_lines": 0}
    
    # Returns
    returns_pipeline = [
        {"$match": {"NET_SALES_VALUE": {"$lt": 0}}},
        {"$group": {"_id": None, "returns_value": {"$sum": "$NET_SALES_VALUE"}, "returns_count": {"$sum": 1}}}
    ]
    returns_result = await db.sales_data.aggregate(returns_pipeline).to_list(1)
    returns_data = returns_result[0] if returns_result else {"returns_value": 0, "returns_count": 0}
    
    # Gross for returns rate
    gross_pipeline = [
        {"$match": {"NET_SALES_VALUE": {"$gt": 0}}},
        {"$group": {"_id": None, "gross": {"$sum": "$NET_SALES_VALUE"}}}
    ]
    gross_result = await db.sales_data.aggregate(gross_pipeline).to_list(1)
    gross = gross_result[0]["gross"] if gross_result else 0
    
    # Zone concentration
    zone_pipeline = [
        {"$group": {"_id": "$Zone_New", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}},
        {"$limit": 1}
    ]
    top_zone = await db.sales_data.aggregate(zone_pipeline).to_list(1)
    top_zone_pct = (top_zone[0]["value"] / totals["total_value"] * 100) if top_zone and totals["total_value"] > 0 else 0
    
    # Customer concentration
    cust_pipeline = [
        {"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}},
        {"$limit": 10}
    ]
    top_customers = await db.sales_data.aggregate(cust_pipeline).to_list(10)
    top_10_cust_value = sum(c["value"] for c in top_customers)
    top_10_cust_pct = (top_10_cust_value / totals["total_value"] * 100) if totals["total_value"] > 0 else 0
    
    # Stop business count
    stop_pipeline = [
        {"$match": {"STOP_BUSINESS": "Y"}},
        {"$group": {"_id": "$CUST_CODE"}},
        {"$count": "count"}
    ]
    stop_result = await db.sales_data.aggregate(stop_pipeline).to_list(1)
    stop_count = stop_result[0]["count"] if stop_result else 0
    
    returns_rate = (abs(returns_data["returns_value"]) / gross * 100) if gross > 0 else 0
    negative_line_pct = (returns_data["returns_count"] / totals["total_lines"] * 100) if totals["total_lines"] > 0 else 0
    
    indicators = [
        {"metric": "Returns Rate", "value": round(returns_rate, 2), "threshold": 2.0, 
         "status": "warning" if returns_rate > 1.5 else "healthy", 
         "description": "Percentage of gross sales returned"},
        {"metric": "Zone Concentration", "value": round(top_zone_pct, 2), "threshold": 50.0, 
         "status": "danger" if top_zone_pct > 60 else "warning" if top_zone_pct > 50 else "healthy",
         "description": "Revenue share of top zone"},
        {"metric": "Top 10 Customer Share", "value": round(top_10_cust_pct, 2), "threshold": 40.0,
         "status": "danger" if top_10_cust_pct > 50 else "warning" if top_10_cust_pct > 40 else "healthy",
         "description": "Revenue concentration in top 10 customers"},
        {"metric": "Stop Business Count", "value": stop_count, "threshold": 10,
         "status": "danger" if stop_count > 10 else "warning" if stop_count > 5 else "healthy",
         "description": "Number of customers marked as stopped"},
        {"metric": "Negative Line Ratio", "value": round(negative_line_pct, 2), "threshold": 5.0,
         "status": "warning" if negative_line_pct > 3 else "healthy",
         "description": "Percentage of negative (return) line items"}
    ]
    
    return indicators

@api_router.get("/risk/returns-trend")
async def get_returns_trend():
    """Get monthly returns trend"""
    pipeline = [
        {"$match": {"NET_SALES_VALUE": {"$lt": 0}}},
        {
            "$addFields": {
                "doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None}}
            }
        },
        {"$match": {"doc_date_parsed": {"$ne": None}}},
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}},
                "returns_value": {"$sum": "$NET_SALES_VALUE"},
                "returns_qty": {"$sum": "$NET_SALES_QTY"}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    
    returns = await db.sales_data.aggregate(pipeline).to_list(100)
    
    # Get gross by month for rate calculation
    gross_pipeline = [
        {"$match": {"NET_SALES_VALUE": {"$gt": 0}}},
        {
            "$addFields": {
                "doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None}}
            }
        },
        {"$match": {"doc_date_parsed": {"$ne": None}}},
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}},
                "gross_value": {"$sum": "$NET_SALES_VALUE"}
            }
        }
    ]
    gross_data = await db.sales_data.aggregate(gross_pipeline).to_list(100)
    gross_map = {g["_id"]: g["gross_value"] for g in gross_data}
    
    result = []
    for r in returns:
        month = r["_id"]
        gross = gross_map.get(month, 0)
        returns_val = abs(r["returns_value"])
        rate = (returns_val / gross * 100) if gross > 0 else 0
        result.append({
            "month": month,
            "returns_value": returns_val,
            "returns_qty": abs(r["returns_qty"]),
            "returns_rate": round(rate, 2)
        })
    
    return result

# AI Insights - build data summary from real dashboard data
async def _get_insights_data_summary(dashboard: Optional[str] = None) -> Dict[str, Any]:
    """Fetch real KPIs and build a summary for insights generation. If dashboard is set, also fetch dashboard-specific data."""
    summary = {"overview": None, "trends": [], "concentration": None, "risk": None}
    try:
        overview_pipe = [{"$group": {"_id": None, "net_sales_value": {"$sum": "$NET_SALES_VALUE"}, "net_sales_qty": {"$sum": "$NET_SALES_QTY"}, "transactions": {"$addToSet": "$TRAN_ID"}, "customers": {"$addToSet": "$CUST_CODE"}, "products": {"$addToSet": "$Product"}}}]
        r = await db.sales_data.aggregate(overview_pipe).to_list(1)
        if r:
            d = r[0]
            gross_pipe = [{"$match": {"NET_SALES_VALUE": {"$gt": 0}}}, {"$group": {"_id": None, "gross": {"$sum": "$NET_SALES_VALUE"}}}]
            returns_pipe = [{"$match": {"NET_SALES_VALUE": {"$lt": 0}}}, {"$group": {"_id": None, "returns": {"$sum": "$NET_SALES_VALUE"}}}]
            gross_r = await db.sales_data.aggregate(gross_pipe).to_list(1)
            returns_r = await db.sales_data.aggregate(returns_pipe).to_list(1)
            gross = (gross_r[0].get("gross") or 0) if gross_r else 0
            returns_val = abs((returns_r[0].get("returns") or 0)) if returns_r else 0
            net_val = d.get("net_sales_value") or 0
            summary["overview"] = {
                "net_sales_value": round(float(net_val), 2),
                "gross_sales_value": round(float(gross), 2),
                "returns_value": round(float(returns_val), 2),
                "returns_rate_pct": round(returns_val / gross * 100, 2) if gross > 0 else 0,
                "total_transactions": len(d.get("transactions") or []),
                "total_customers": len(d.get("customers") or []),
                "total_products": len(d.get("products") or []),
                "net_sales_qty": int(d.get("net_sales_qty") or 0),
            }
        month_pipe = [{"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}}, {"$match": {"doc_date_parsed": {"$ne": None}}}, {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}, "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"_id": 1}}]
        trends = await db.sales_data.aggregate(month_pipe).to_list(24)
        prev = None
        for t in trends:
            growth = round((t["value"] - prev) / prev * 100, 2) if prev and prev > 0 else None
            summary["trends"].append({"month": t["_id"], "value": round(t["value"], 2), "growth_pct": growth})
            prev = t["value"]
        zone_pipe = [{"$group": {"_id": "$Zone_New", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}]
        zones = await db.sales_data.aggregate(zone_pipe).to_list(20)
        total = sum(z["value"] for z in zones)
        zone_data = [{"name": z["_id"], "value": round(z["value"], 2), "pct": round(z["value"] / total * 100, 2) if total > 0 else 0} for z in zones]
        state_pipe = [{"$group": {"_id": "$State", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}, {"$limit": 10}]
        states = await db.sales_data.aggregate(state_pipe).to_list(10)
        state_data = [{"name": s["_id"], "value": round(s["value"], 2), "pct": round(s["value"] / total * 100, 2) if total > 0 else 0} for s in states]
        top3_pct = sum(s["pct"] for s in state_data[:3])
        cust_pipe = [{"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}, {"$limit": 10}]
        custs = await db.sales_data.aggregate(cust_pipe).to_list(10)
        top10_cust_pct = sum(c["value"] for c in custs) / total * 100 if total > 0 else 0
        summary["concentration"] = {"zones": zone_data[:6], "top_states": state_data, "top_3_states_pct": round(top3_pct, 2), "top_10_customers_pct": round(top10_cust_pct, 2)}

        # Dashboard-specific data
        if dashboard == "Product Intelligence":
            prod_pipe = [{"$group": {"_id": "$Product", "sales_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"sales_value": -1}}, {"$limit": 15}]
            products = await db.sales_data.aggregate(prod_pipe).to_list(15)
            total_sales = sum(p["sales_value"] for p in products)
            ret_pipe = [{"$match": {"NET_SALES_VALUE": {"$lt": 0}}}, {"$group": {"_id": "$Product", "returns": {"$sum": "$NET_SALES_VALUE"}}}]
            rets = await db.sales_data.aggregate(ret_pipe).to_list(100)
            ret_map = {x["_id"]: abs(x["returns"]) for x in rets}
            summary["products"] = [{"name": p["_id"], "value": p["sales_value"], "pct": round(p["sales_value"] / total_sales * 100, 2) if total_sales else 0, "returns": ret_map.get(p["_id"], 0)} for p in products]
        elif dashboard == "Customer Analytics":
            cust_all = await db.sales_data.aggregate([{"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}]).to_list(500)
            total_val = sum(c["value"] for c in cust_all)
            cum = 0
            top10_pct = 0
            for i, c in enumerate(cust_all):
                cum += c["value"]
                if i == 9:
                    top10_pct = round(cum / total_val * 100, 2) if total_val else 0
                    break
            stop_r = await db.sales_data.aggregate([{"$match": {"STOP_BUSINESS": "Y"}}, {"$group": {"_id": "$CUST_CODE"}}, {"$count": "count"}]).to_list(1)
            summary["customers"] = {"top_10_pct": top10_pct, "total_customers": len(cust_all), "stop_business_count": stop_r[0]["count"] if stop_r else 0}
        elif dashboard == "Pricing & Discount":
            pricing_pipe = [{"$group": {"_id": None, "avg_discount": {"$avg": "$Discount %"}, "avg_ppu": {"$avg": "$PPU"}, "total_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$limit": 1}]
            pr = await db.sales_data.aggregate(pricing_pipe).to_list(1)
            dist_pipe = [{"$bucket": {"groupBy": "$Discount %", "boundaries": [0, 0.01, 5, 10, 20, 50, 100], "default": "Other", "output": {"count": {"$sum": 1}, "value": {"$sum": "$NET_SALES_VALUE"}}}}]
            dist = await db.sales_data.aggregate(dist_pipe).to_list(10)
            summary["pricing"] = {"avg_discount": round(pr[0]["avg_discount"], 2) if pr and pr[0].get("avg_discount") is not None else 0, "discount_buckets": dist}
        elif dashboard == "Risk & Governance":
            gross_p = [{"$match": {"NET_SALES_VALUE": {"$gt": 0}}}, {"$group": {"_id": None, "g": {"$sum": "$NET_SALES_VALUE"}}}]
            ret_p = [{"$match": {"NET_SALES_VALUE": {"$lt": 0}}}, {"$group": {"_id": None, "r": {"$sum": "$NET_SALES_VALUE"}, "lines": {"$sum": 1}}}]
            tot_p = [{"$group": {"_id": None, "total": {"$sum": "$NET_SALES_VALUE"}, "lines": {"$sum": 1}}}]
            gr = await db.sales_data.aggregate(gross_p).to_list(1)
            rr = await db.sales_data.aggregate(ret_p).to_list(1)
            tr = await db.sales_data.aggregate(tot_p).to_list(1)
            gross_val = gr[0]["g"] if gr else 0
            returns_abs = abs(rr[0]["r"]) if rr and rr[0].get("r") else 0
            total_lines = tr[0]["lines"] if tr else 0
            neg_lines = rr[0]["lines"] if rr and rr[0].get("lines") else 0
            summary["risk_indicators"] = {"returns_rate": round(returns_abs / gross_val * 100, 2) if gross_val else 0, "negative_line_pct": round(neg_lines / total_lines * 100, 2) if total_lines else 0}
            top_zone = await db.sales_data.aggregate([{"$group": {"_id": "$Zone_New", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}, {"$limit": 1}]).to_list(1)
            top_cust = await db.sales_data.aggregate([{"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}, {"$limit": 10}]).to_list(10)
            total_s = tr[0]["total"] if tr else 0
            summary["risk_indicators"]["zone_concentration_pct"] = round(top_zone[0]["value"] / total_s * 100, 2) if top_zone and total_s else 0
            summary["risk_indicators"]["top_10_customer_pct"] = round(sum(c["value"] for c in top_cust) / total_s * 100, 2) if total_s else 0
            stop_c = await db.sales_data.aggregate([{"$match": {"STOP_BUSINESS": "Y"}}, {"$count": "count"}]).to_list(1)
            summary["risk_indicators"]["stop_business_count"] = stop_c[0]["count"] if stop_c else 0
    except Exception as e:
        logger.error(f"Error building insights data summary: {e}")
    return summary


# Unicode dash characters to replace with ASCII hyphen in insight text
_INSIGHT_DASH_CHARS = ("\u2014", "\u2013", "\u2012", "\u2212", "\u2015", "\uFE58", "\uFE31")  # em dash, en dash, minus, etc.


def _normalize_dashes(s: str) -> str:
    """Replace Unicode dashes (em dash, en dash, minus sign, etc.) with ASCII hyphen."""
    if not s or not isinstance(s, str):
        return s
    for char in _INSIGHT_DASH_CHARS:
        s = s.replace(char, "-")
    return s


def _insight_response(insights: List[str], recommendations: List[str], action_items: List[str]) -> InsightResponse:
    """Build InsightResponse with all insight text normalized to use ASCII hyphen instead of Unicode dashes."""
    return InsightResponse(
        insights=[_normalize_dashes(x) for x in (insights or [])],
        recommendations=[_normalize_dashes(x) for x in (recommendations or [])],
        action_items=[_normalize_dashes(x) for x in (action_items or [])],
    )


def _rule_based_insights(summary: Dict[str, Any]) -> InsightResponse:
    """Generate Key Insights, Recommendations, and Action Items from real data using rules (Executive Summary / default)."""
    return _rule_based_insights_by_dashboard("Executive Summary", summary)


def _rule_based_insights_by_dashboard(dashboard: Optional[str], summary: Dict[str, Any]) -> InsightResponse:
    """Generate dashboard-specific Key Insights, Recommendations, and Action Items from real data."""
    ov = summary.get("overview") or {}
    conc = summary.get("concentration") or {}
    trends = summary.get("trends") or []
    net_sales = ov.get("net_sales_value") or 0
    returns_rate = ov.get("returns_rate_pct") or 0
    total_customers = ov.get("total_customers") or 0
    total_products = ov.get("total_products") or 0
    top3_pct = conc.get("top_3_states_pct") or 0
    top10_cust_pct = conc.get("top_10_customers_pct") or 0
    zones = conc.get("zones") or []
    top_zone_name = zones[0]["name"] if zones else "N/A"
    top_zone_pct = zones[0]["pct"] if zones else 0
    fallback = _insight_response(["No data available. Load sales data to generate insights."], ["Load data and refresh insights."], ["Load data and try again."])

    # Executive Summary
    if not dashboard or dashboard == "Executive Summary":
        insights, recs, actions = [], [], []
        if net_sales > 0:
            insights.append(f"Net sales total: ₹{net_sales/1e7:.2f} Cr with {total_customers} customers and {total_products} products.")
        if zones and top_zone_name and top_zone_name != "N/A":
            insights.append(f"Revenue concentration: {top_zone_name} zone leads with {top_zone_pct}% of total sales.")
        if top3_pct > 0:
            insights.append(f"Top 3 states contribute {top3_pct}% of revenue - {'high geographic concentration risk.' if top3_pct > 75 else 'moderate diversification.'}")
        if top10_cust_pct > 0:
            insights.append(f"Top 10 customers account for {top10_cust_pct:.1f}% of revenue - {'elevated dependency risk.' if top10_cust_pct > 25 else 'manageable concentration.'}")
        if returns_rate is not None:
            insights.append(f"Returns rate at {returns_rate:.2f}% is {'within acceptable range (<2%).' if returns_rate < 2 else 'above 2% - review product/quality issues.'}")
        if len(trends) >= 2 and trends[-1].get("growth_pct") is not None:
            insights.append(f"Latest month-over-month sales growth: {trends[-1]['growth_pct']:+.2f}%.")
        if top3_pct > 75:
            recs.append("Diversify revenue by expanding in underperforming states and launching targeted campaigns.")
        if top10_cust_pct > 25:
            recs.append("Implement key-account retention programs and develop a pipeline of mid-size accounts.")
        if returns_rate >= 2:
            recs.append("Investigate returns by product and state; strengthen quality controls and reverse-logistics.")
        recs.append("Use drill-downs (zone → state → product) to identify growth and return hotspots.")
        recs.append("Review pricing and promotions for products with low contribution or high returns.")
        actions.append("Schedule a monthly review of top 10 customers and top 3 states performance.")
        if returns_rate >= 2:
            actions.append("Set up automated alerts for returns rate exceeding 2% by product/state.")
        actions.append("Export Revenue & Growth KPI drill-downs for leadership decks.")
        actions.append("Define targets for geographic and customer diversification for next quarter.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    # Revenue & Growth KPIs
    if dashboard == "Revenue & Growth KPIs":
        insights, recs, actions = [], [], []
        if net_sales > 0:
            insights.append(f"Net sales: ₹{net_sales/1e7:.2f} Cr. Gross and returns drive net revenue; monitor MoM for growth targets.")
        if len(trends) >= 2:
            last = trends[-1]
            insights.append(f"Latest month net sales: ₹{last.get('value', 0)/1e7:.2f} Cr. MoM growth: {last.get('growth_pct', 0):+.2f}%." if last.get("growth_pct") is not None else f"Latest month net sales: ₹{last.get('value', 0)/1e7:.2f} Cr.")
        if top10_cust_pct > 0:
            insights.append(f"Revenue concentration: top 10 customers represent {top10_cust_pct:.1f}% of revenue - align KPI targets with key-account focus.")
        if returns_rate is not None:
            insights.append(f"Returns rate {returns_rate:.2f}% - {'within target.' if returns_rate < 2 else 'above target; factor into growth forecasts.'}")
        recs.append("Set and track Net Sales, Gross Sales, and Returns as primary KPIs with clear ownership.")
        recs.append("Use drill-down by zone/state/product to explain variance in revenue and returns.")
        if top10_cust_pct > 25:
            recs.append("Balance growth KPIs between key accounts and mid-market to reduce concentration risk.")
        actions.append("Review Revenue & Growth KPI drill-downs weekly; assign owners for underperforming segments.")
        actions.append("Export KPI summary for monthly business reviews.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    # Product Intelligence
    if dashboard == "Product Intelligence":
        insights, recs, actions = [], [], []
        products = summary.get("products") or []
        if products:
            top = products[0]
            insights.append(f"Top product by value: {top.get('name', 'N/A')} - {top.get('pct', 0)}% of product sales.")
            top3_pct_prod = sum(p.get("pct", 0) for p in products[:3])
            insights.append(f"Top 3 products contribute {top3_pct_prod:.1f}% of product-level revenue.")
            high_returns = [p for p in products if p.get("returns", 0) > 0]
            if high_returns:
                insights.append(f"{len(high_returns)} product(s) have notable returns; review quality and channel mix.")
        if total_products > 0:
            insights.append(f"Portfolio spans {total_products} products; use contribution and returns rate to prioritize focus.")
        recs.append("Focus promotion and inventory on top contributors; phase or improve high-return products.")
        recs.append("Use product drill-down (zone, state, customer) to identify growth and return hotspots.")
        actions.append("Review top 10 products monthly; set targets for contribution and returns rate by product.")
        actions.append("Export product performance for brand and supply chain planning.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    # Geography Intelligence
    if dashboard == "Geography Intelligence":
        insights, recs, actions = [], [], []
        if zones:
            insights.append(f"Leading zone: {top_zone_name} with {top_zone_pct}% of sales.")
        states = conc.get("top_states") or []
        if states:
            top_states_str = ", ".join(s.get("name", "") for s in states[:3])
            insights.append(f"Top 3 states by revenue: {top_states_str} - combined {top3_pct}% of total.")
        if top3_pct > 75:
            insights.append("High geographic concentration; consider expansion in underpenetrated regions.")
        recs.append("Use zone → state → city drill-down to allocate field force and promotions.")
        if top3_pct > 75:
            recs.append("Build territory plans for lower-contribution states to diversify revenue.")
        actions.append("Review geography performance monthly; align territory targets with zone/state contribution.")
        actions.append("Export geography views for regional leadership and S&OP.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    # Customer Analytics
    if dashboard == "Customer Analytics":
        insights, recs, actions = [], [], []
        cust_data = summary.get("customers") or {}
        top10_pct = cust_data.get("top_10_pct") or top10_cust_pct
        stop_count = cust_data.get("stop_business_count", 0)
        total_cust = cust_data.get("total_customers") or total_customers
        if top10_pct > 0:
            insights.append(f"Top 10 customers represent {top10_pct}% of revenue - {'high concentration; prioritize retention.' if top10_pct > 25 else 'moderate spread.'}")
        if total_cust > 0:
            insights.append(f"Total active customers: {total_cust}. Use concentration and risk views for segmentation.")
        if stop_count > 0:
            insights.append(f"Stop-business customers: {stop_count}. Review list and win-back or closure plans.")
        recs.append("Segment customers by value and risk; tailor retention and growth programs.")
        if stop_count > 5:
            recs.append("Address stop-business list: reassign or formalize exit to protect service levels.")
        actions.append("Review customer concentration and risk dashboards monthly; assign key-account owners.")
        actions.append("Export customer analytics for sales and credit teams.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    # Pricing & Discount
    if dashboard == "Pricing & Discount":
        insights, recs, actions = [], [], []
        pricing = summary.get("pricing") or {}
        avg_disc = pricing.get("avg_discount", 0)
        if avg_disc is not None:
            insights.append(f"Average discount across transactions: {avg_disc:.2f}%. Monitor by product and channel for margin impact.")
        buckets = pricing.get("discount_buckets") or []
        if buckets:
            high_disc = [b for b in buckets if isinstance(b.get("_id"), (int, float)) and b.get("_id", 0) >= 20]
            if high_disc:
                insights.append("Significant volume in higher discount buckets; review approval norms and margin.")
        recs.append("Use pricing and discount distribution to align list price, schemes, and net realization.")
        recs.append("Flag products or customers with unusually high discounts for review.")
        actions.append("Review average discount and distribution monthly; set discount caps by segment.")
        actions.append("Export pricing analysis for finance and sales operations.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    # Risk & Governance
    if dashboard == "Risk & Governance":
        insights, recs, actions = [], [], []
        risk = summary.get("risk_indicators") or {}
        ret_rate = risk.get("returns_rate", returns_rate)
        zone_conc = risk.get("zone_concentration_pct", top_zone_pct)
        cust_conc = risk.get("top_10_customer_pct", top10_cust_pct)
        stop_count = risk.get("stop_business_count", 0)
        neg_pct = risk.get("negative_line_pct", 0)
        insights.append(f"Returns rate: {ret_rate:.2f}% - {'within governance threshold.' if ret_rate < 2 else 'above threshold; escalate to quality/commercial.'}")
        insights.append(f"Zone concentration: top zone {zone_conc:.2f}%; customer concentration: top 10 at {cust_conc:.2f}%.")
        if stop_count > 0:
            insights.append(f"Stop-business customers: {stop_count}. Maintain governance list and credit/collections alignment.")
        if neg_pct > 0:
            insights.append(f"Negative line ratio: {neg_pct:.2f}% of lines are returns - track for process and fraud checks.")
        recs.append("Monitor returns rate, zone and customer concentration, and stop-business list against thresholds.")
        if ret_rate >= 2:
            recs.append("Conduct returns deep-dive by product and geography; tighten approval and reverse-logistics.")
        actions.append("Review risk indicators weekly; trigger escalation when metrics breach thresholds.")
        actions.append("Export risk and returns trend for compliance and leadership.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    return fallback


@api_router.post("/insights/generate", response_model=InsightResponse)
async def generate_insights(request: InsightRequest):
    """Generate dashboard-specific Key Insights, Recommendations, and Action Items from real data. Pass dashboard name for page-specific insights."""
    import json
    import re
    dashboard = (request.dashboard or request.context or "").strip() or None
    if request.context and not dashboard:
        dashboard = request.context
    data_summary = {"overview": None, "trends": [], "concentration": None}
    try:
        data_summary = await _get_insights_data_summary(dashboard)
    except Exception as e:
        logger.error(f"Error fetching insights data: {e}")

    context = request.context or (dashboard or "Dashboard Analysis")

    try:
        try:
            from emergentintegrations.llm.chat import LlmChat, UserMessage
        except ImportError:
            return _rule_based_insights_by_dashboard(dashboard or "Executive Summary", data_summary)

        api_key = os.environ.get("EMERGENT_LLM_KEY")
        if not api_key or str(api_key).strip().startswith("sk-your-"):
            return _rule_based_insights_by_dashboard(dashboard or "Executive Summary", data_summary)

        data_for_prompt = json.dumps({"context": context, "dashboard": dashboard, "overview": data_summary.get("overview"), "trends": data_summary.get("trends"), "concentration": data_summary.get("concentration"), "products": data_summary.get("products"), "customers": data_summary.get("customers"), "pricing": data_summary.get("pricing"), "risk_indicators": data_summary.get("risk_indicators")}, indent=2)
        system_prompt = """You are a Senior Business Intelligence Analyst specializing in pharma sales analytics.
Analyze the provided REAL data for the given dashboard context and generate ONLY from that data:
1. KEY INSIGHTS (3-5 bullet points) - Specific facts from the numbers (values, %, trends). Quote actual figures. Make insights relevant to the dashboard (e.g. Product Intelligence = product-level insights, Geography = zones/states, etc.).
2. RECOMMENDATIONS (3-4 bullet points) - Strategic suggestions based on the insights, specific to this dashboard.
3. ACTION ITEMS (3-4 bullet points) - Concrete, actionable next steps.

Be concise and data-driven. Use the exact numbers from the data. Output valid JSON only with keys: insights, recommendations, action_items (each an array of strings)."""
        user_prompt = f"""Context: {context}\nDashboard: {dashboard or 'Executive Summary'}\n\nReal data summary:\n{data_for_prompt}\n\nGenerate dashboard-specific insights, recommendations, and action items in JSON format."""

        chat = LlmChat(api_key=api_key, session_id=str(uuid.uuid4()), system_message=system_prompt).with_model("openai", "gpt-4o")
        response = await chat.send_message(UserMessage(text=user_prompt))
        response_text = str(response) if response is not None else ""
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if json_match:
            parsed = json.loads(json_match.group())
            return _insight_response(parsed.get("insights", []), parsed.get("recommendations", []), parsed.get("action_items", []))
    except Exception as e:
        logger.error(f"Error generating insights: {e}")

    return _rule_based_insights_by_dashboard(dashboard or "Executive Summary", data_summary)


@api_router.get("/insights/generate", response_model=InsightResponse)
async def generate_insights_get(dashboard: Optional[str] = Query(None, description="Dashboard name for page-specific insights")):
    """GET: generate dashboard-specific Key Insights, Recommendations, and Action Items. Pass ?dashboard=... for page-specific insights."""
    data_summary = {"overview": None, "trends": [], "concentration": None}
    try:
        data_summary = await _get_insights_data_summary(dashboard)
    except Exception as e:
        logger.error(f"Error fetching insights data: {e}")
    return _rule_based_insights_by_dashboard(dashboard or "Executive Summary", data_summary)


# Promotion Analysis
@api_router.get("/promotions/analysis")
async def get_promotion_analysis():
    """Get promotion effectiveness analysis"""
    pipeline = [
        {
            "$group": {
                "_id": "$Promoted/non promoted",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                "avg_ppu": {"$avg": "$PPU"},
                "customers": {"$addToSet": "$CUST_CODE"},
                "line_count": {"$sum": 1}
            }
        }
    ]
    
    results = await db.sales_data.aggregate(pipeline).to_list(10)
    total = sum(r["sales_value"] for r in results)
    
    return [{
        "promotion_type": r["_id"],
        "sales_value": r["sales_value"],
        "sales_qty": r["sales_qty"],
        "contribution_pct": round(r["sales_value"] / total * 100, 2) if total > 0 else 0,
        "avg_ppu": round(r["avg_ppu"], 2) if r["avg_ppu"] else 0,
        "customer_count": len(r["customers"]),
        "line_count": r["line_count"]
    } for r in results]

# Division/Brand Analysis
@api_router.get("/divisions/performance")
async def get_division_performance():
    """Get division and brand performance"""
    pipeline = [
        {
            "$group": {
                "_id": {
                    "division": "$Div_Code (Mapping HQ)",
                    "brand": "$ITEM_BRAND KPMG"
                },
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                "products": {"$addToSet": "$Product"},
                "customers": {"$addToSet": "$CUST_CODE"}
            }
        },
        {"$sort": {"sales_value": -1}}
    ]
    
    results = await db.sales_data.aggregate(pipeline).to_list(100)
    total = sum(r["sales_value"] for r in results)
    
    return [{
        "division": r["_id"]["division"],
        "brand": r["_id"]["brand"],
        "sales_value": r["sales_value"],
        "sales_qty": r["sales_qty"],
        "contribution_pct": round(r["sales_value"] / total * 100, 2) if total > 0 else 0,
        "product_count": len(r["products"]),
        "customer_count": len(r["customers"])
    } for r in results]

# Health check
@api_router.get("/")
async def root():
    return {"message": "Sales Analytics Dashboard API", "status": "healthy"}

@api_router.get("/health")
async def health_check():
    # Check MongoDB connection
    try:
        await db.command("ping")
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    
    # Check data availability
    count = await db.sales_data.count_documents({})
    
    return {
        "status": "healthy",
        "database": db_status,
        "data_records": count
    }

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
