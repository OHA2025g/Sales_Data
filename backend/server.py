from fastapi import FastAPI, APIRouter, HTTPException, Query, UploadFile, File
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Annotated

try:
    from copilot_responses import build_copilot_response, format_copilot_response
except ImportError:
    build_copilot_response = None
    format_copilot_response = None
import uuid
from datetime import datetime, timezone
from copy import deepcopy
import asyncio
import math
import time

try:
    from sales_insight_engine import SalesInsightEngine
except Exception:
    SalesInsightEngine = None


async def _try_hf_sales_insight_engine(
    dashboard: str,
    data_summary: Dict[str, Any],
) -> Optional["InsightResponse"]:
    """
    Try Hugging Face-based SalesInsightEngine when configured.
    Returns InsightResponse on success, else None (caller should fallback).
    """
    try:
        if SalesInsightEngine is None:
            return None
        if not (os.environ.get("HF_TOKEN") or "").strip():
            return None
        if os.environ.get("USE_HF_INSIGHTS", "1").strip() in ("0", "false", "False", "no", "NO"):
            return None

        engine = SalesInsightEngine()

        def _run_sync():
            return engine.generate(dashboard=dashboard, data_summary=data_summary)

        out = await asyncio.wait_for(asyncio.to_thread(_run_sync), timeout=25.0)
        resp = InsightResponse(
            insights=out.get("insights") or [],
            recommendations=out.get("recommendations") or [],
            action_items=out.get("action_items") or [],
        )
        return resp if _hf_insight_response_usable(resp) else None
    except Exception as e:
        logger.warning(f"HF SalesInsightEngine skipped: {e}")
        return None

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# ETL state for async /data/load
_ETL_STATE: Dict[str, Any] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "records_loaded": 0,
    "source_file": None,
    "error": None,
}
_ETL_LOCK = asyncio.Lock()

# ETL state for async /data/load/incentives
_INC_ETL_STATE: Dict[str, Any] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "records_loaded": 0,
    "source_file": None,
    "error": None,
}
_INC_ETL_LOCK = asyncio.Lock()

def _parse_iso_ts(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None

# Create the main app
app = FastAPI(title="Sales Analytics Dashboard API")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Short-lived in-memory cache for expensive dashboard/insight queries.
CACHE_TTL_SECONDS = int(os.environ.get("API_CACHE_TTL_SECONDS", "600"))
_API_CACHE: Dict[str, Dict[str, Any]] = {}

# Persisted cache to avoid slow cold-start after container restart.
PERSIST_CACHE = os.environ.get("PERSIST_API_CACHE", "1").strip() not in ("0", "false", "False", "no", "NO")
# Keep persisted cache long-lived so "first load after restart" stays fast.
PERSIST_CACHE_TTL_SECONDS = int(os.environ.get("PERSIST_API_CACHE_TTL_SECONDS", "604800"))
_PERSIST_COLL = "api_cache"


def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def _sanitize_json_value(value: Any):
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return 0.0
        return value
    if isinstance(value, list):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _sanitize_json_value(v) for k, v in value.items()}
    return value


async def _persist_cache_get(key: str):
    if not PERSIST_CACHE:
        return None
    try:
        doc = await db[_PERSIST_COLL].find_one({"_id": key}, {"value": 1, "expires_at": 1, "created_at": 1})
        if not doc:
            return None
        # If an ETL has started since this cache was created, treat it as stale.
        etl_started = _parse_iso_ts(_ETL_STATE.get("started_at"))
        created_at_raw = doc.get("created_at")
        created_at = float(created_at_raw or 0)
        # Old cache entries (before we started tracking created_at) are treated as stale.
        if created_at_raw is None:
            return None
        if etl_started and created_at < etl_started:
            return None
        if _now_ts() > float(doc.get("expires_at") or 0):
            # Best-effort cleanup
            await db[_PERSIST_COLL].delete_one({"_id": key})
            return None
        return _sanitize_json_value(deepcopy(doc.get("value")))
    except Exception:
        return None


async def _persist_cache_set(key: str, value: Any, ttl_seconds: int = PERSIST_CACHE_TTL_SECONDS):
    if not PERSIST_CACHE:
        return
    try:
        await db[_PERSIST_COLL].update_one(
            {"_id": key},
            {"$set": {"value": _sanitize_json_value(deepcopy(value)), "expires_at": _now_ts() + ttl_seconds, "created_at": _now_ts()}},
            upsert=True,
        )
    except Exception:
        return


async def _persist_cache_clear_all():
    if not PERSIST_CACHE:
        return
    try:
        await db[_PERSIST_COLL].delete_many({})
    except Exception:
        return


async def _ensure_persist_cache_indexes():
    if not PERSIST_CACHE:
        return
    try:
        await db[_PERSIST_COLL].create_index("expires_at")
    except Exception:
        pass


def _cache_get(key: str):
    entry = _API_CACHE.get(key)
    if not entry:
        return None
    if datetime.now(timezone.utc).timestamp() > entry["expires_at"]:
        _API_CACHE.pop(key, None)
        return None
    return _sanitize_json_value(deepcopy(entry["value"]))


def _cache_set(key: str, value: Any, ttl_seconds: int = CACHE_TTL_SECONDS):
    _API_CACHE[key] = {
        "value": _sanitize_json_value(deepcopy(value)),
        "expires_at": _now_ts() + ttl_seconds,
    }


def _cache_clear():
    _API_CACHE.clear()
    # Do not clear persisted cache by default; it is used to speed up cold starts.


async def _warm_core_caches():
    """Precompute heavy dashboard payloads for faster first paint."""
    try:
        async def _safe(name: str, coro, timeout_s: float = 120.0):
            try:
                await asyncio.wait_for(coro, timeout=timeout_s)
            except Exception as e:
                logger.warning(f"Cache warm-up step skipped: {name}: {e}")

        # Warm key dashboard APIs in parallel so first user hit is fast even after restart.
        data_warm_tasks = [
            _safe("dashboard/overview", get_dashboard_overview()),
            _safe("dashboard/trends", get_monthly_trends()),
            _safe("dashboard/concentration", get_concentration_metrics()),
            _safe("revenue-growth/visuals", get_revenue_growth_visuals()),
            _safe("products/performance", get_product_performance()),
            _safe("geography/zones", get_zone_performance()),
            _safe("customers/performance", get_customer_performance()),
            _safe("customers/concentration", get_customer_concentration()),
            _safe("customers/risk", get_customer_risk()),
            _safe("risk/indicators", get_risk_indicators()),
            _safe("risk/returns-trend", get_returns_trend()),
            _safe("risk/anomalies", get_risk_anomalies()),
            _safe("pricing/analysis", get_pricing_analysis()),
            _safe("pricing/discount-distribution", get_discount_distribution()),
        ]
        await asyncio.gather(*data_warm_tasks, return_exceptions=True)

        # Warm AI insights for all major sections requested by users.
        insight_warm_tasks = [
            _safe("insights (Executive Summary)", generate_insights_get("Executive Summary")),
            _safe("insights (Revenue & Growth KPIs)", generate_insights_get("Revenue & Growth KPIs")),
            _safe("insights (Product Intelligence)", generate_insights_get("Product Intelligence")),
            _safe("insights (Geography Intelligence)", generate_insights_get("Geography Intelligence")),
            _safe("insights (Customer Analytics)", generate_insights_get("Customer Analytics")),
            _safe("insights (Pricing & Discount)", generate_insights_get("Pricing & Discount")),
            _safe("insights (Incentive Analytics)", generate_insights_get("Incentive Analytics")),
            _safe("insights (Risk & Governance)", generate_insights_get("Risk & Governance")),
        ]
        await asyncio.gather(*insight_warm_tasks, return_exceptions=True)
        logger.info("Core API caches warmed successfully.")
    except Exception as e:
        logger.warning(f"Cache warm-up skipped due to error: {e}")

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


def _hf_insight_response_usable(resp: Optional[InsightResponse]) -> bool:
    """HF sometimes returns empty arrays; treat that as failure so rule-based / LLM fallback runs."""
    if resp is None:
        return False

    def _has_text(lst):
        return any(isinstance(x, str) and x.strip() for x in (lst or []))

    return _has_text(resp.insights) or _has_text(resp.recommendations) or _has_text(resp.action_items)


class SalesInsightEngineRequest(BaseModel):
    dashboard: str = Field(..., min_length=1)
    question: Optional[str] = None
    force: bool = False


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, description="User question about data or insights")


class ChatResponse(BaseModel):
    answer: str


# Data loader endpoint
@api_router.post("/data/load")
async def load_sales_data():
    """Start async load of sales data from Excel into MongoDB."""
    try:
        # Lazy import: avoids blocking API startup on pandas import issues.
        import pandas as pd
        sales_file = os.environ.get("SALES_DATA_FILE", "/app/Sales_Data.xlsx")
        sales_path = Path(sales_file)
        if not sales_path.exists():
            raise FileNotFoundError(f"Sales file not found: {sales_path}")

        async with _ETL_LOCK:
            if _ETL_STATE.get("running"):
                return {"status": "running", **_ETL_STATE}

            _ETL_STATE.update({
                "running": True,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
                "records_loaded": 0,
                "source_file": str(sales_path),
                "error": None,
            })
            # Immediately clear persisted cache so we don't serve stale insights during ETL.
            await _persist_cache_clear_all()

        def _read_excel_records_sync(xlsx_path: Path):
            # Some Docker-for-Mac file sharing layers can intermittently throw:
            # OSError: [Errno 35] Resource deadlock avoided
            import shutil
            last_err = None
            for attempt in range(5):
                try:
                    # Copy to container-local fs first to avoid osxfs deadlocks on large reads.
                    tmp_path = Path("/tmp/Sales_Data_copy.xlsx")
                    with open(xlsx_path, "rb") as src, open(tmp_path, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)

                    sheets_local = pd.read_excel(tmp_path, sheet_name=None)
                    frames_local = []
                    for _, df in sheets_local.items():
                        if df is None or df.empty:
                            continue
                        df.columns = df.columns.str.strip()
                        if "NET_SALES_VALUE" in df.columns or "TRAN_ID" in df.columns or "Product" in df.columns:
                            frames_local.append(df)
                    if not frames_local:
                        frames_local = [df for df in sheets_local.values() if df is not None and not df.empty]
                    if not frames_local:
                        raise ValueError(f"No data rows found in workbook: {xlsx_path}")

                    combined = pd.concat(frames_local, ignore_index=True)
                    combined.columns = combined.columns.str.strip()
                    combined = combined.where(pd.notna(combined), None)
                    recs = combined.to_dict("records")
                    for r in recs:
                        for k, v in list(r.items()):
                            if isinstance(v, pd.Timestamp):
                                r[k] = v.isoformat()
                    return recs
                except OSError as e:
                    last_err = e
                    if getattr(e, "errno", None) == 35:
                        time.sleep(0.25 * (attempt + 1))
                        continue
                    raise
            raise last_err or RuntimeError("Failed to read Excel after retries")

        # Run sync pandas/openpyxl work off the event loop.
        async def _run_etl():
            try:
                records = await asyncio.to_thread(_read_excel_records_sync, sales_path)

                # Clear existing data and insert new in batches to avoid timeouts.
        await db.sales_data.delete_many({})
        if records:
                    batch_size = 5000
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        await db.sales_data.insert_many(batch, ordered=False)
                        if i == 0 or (i // batch_size) % 10 == 0:
                            _ETL_STATE["records_loaded"] = min(i + len(batch), len(records))

                    # Helpful indexes for dashboard and drill filters.
                    await db.sales_data.create_index("DOC_DATE")
                    await db.sales_data.create_index("Zone_New")
                    await db.sales_data.create_index("State")
                    await db.sales_data.create_index("Product")
                    await db.sales_data.create_index("CUST_CODE")
                    await db.sales_data.create_index("TRAN_ID")
                    await db.sales_data.create_index("Div_Code (Mapping HQ)")
                    await db.sales_data.create_index("Promoted/non promoted")

                _cache_clear()
                # Persisted cache contains computed payloads/insights; clear it so UI reflects latest Excel.
                await _persist_cache_clear_all()
                asyncio.create_task(_warm_core_caches())

                _ETL_STATE.update({
                    "running": False,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "records_loaded": len(records),
                    "error": None,
                })
    except Exception as e:
                logger.exception(f"Error loading data: {e}")
                _ETL_STATE.update({
                    "running": False,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "error": str(e),
                })

        asyncio.create_task(_run_etl())
        return {"status": "started", **_ETL_STATE}
    except Exception as e:
        logger.exception(f"Error loading data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/data/load/status")
async def data_load_status():
    return {"status": "running" if _ETL_STATE.get("running") else "idle", **_ETL_STATE}


@api_router.post("/data/load/incentives")
async def load_incentive_data(file: UploadFile | None = File(default=None)):
    """
    Start async load of incentive data from Excel into MongoDB.

    - If a multipart `file` is provided, it will be read from the upload.
    - Otherwise, reads from `INCENTIVE_DATA_FILE` env (default: /app/Incentive Data1.xlsx).
    """
    try:
        import pandas as pd

        upload_bytes: bytes | None = None
        if file is not None:
            # UploadFile stream is closed once the request finishes; read it now.
            upload_bytes = await file.read()

        async with _INC_ETL_LOCK:
            if _INC_ETL_STATE.get("running"):
                return {"status": "running", **_INC_ETL_STATE}

            source_file = None
            if file is not None:
                source_file = getattr(file, "filename", None) or "uploaded.xlsx"
            else:
                inc_file = os.environ.get("INCENTIVE_DATA_FILE", "/app/Incentive Data1.xlsx")
                inc_path = Path(inc_file)
                if not inc_path.exists():
                    raise FileNotFoundError(
                        f"Incentive file not found: {inc_path}. "
                        f"Either set INCENTIVE_DATA_FILE inside the container or upload a file to this endpoint."
                    )
                source_file = str(inc_path)

            _INC_ETL_STATE.update({
                "running": True,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": None,
                "records_loaded": 0,
                "source_file": source_file,
                "error": None,
            })

        def _read_excel_records_sync_from_path(xlsx_path: Path):
            import shutil
            last_err = None
            for attempt in range(5):
                try:
                    tmp_path = Path("/tmp/Incentive_Data_copy.xlsx")
                    with open(xlsx_path, "rb") as src, open(tmp_path, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)
                    sheets_local = pd.read_excel(tmp_path, sheet_name=None)
                    frames_local = [df for df in sheets_local.values() if df is not None and not df.empty]
                    if not frames_local:
                        raise ValueError(f"No data rows found in workbook: {xlsx_path}")
                    combined = pd.concat(frames_local, ignore_index=True)
                    combined.columns = combined.columns.str.strip()
                    combined = combined.where(pd.notna(combined), None)
                    recs = combined.to_dict("records")
                    for r in recs:
                        for k, v in list(r.items()):
                            if isinstance(v, pd.Timestamp):
                                r[k] = v.isoformat()
                            elif isinstance(v, float):
                                # Avoid Mongo NaN propagation in aggregations
                                if math.isnan(v) or math.isinf(v):
                                    r[k] = None
                    return recs
                except OSError as e:
                    last_err = e
                    if getattr(e, "errno", None) == 35:
                        time.sleep(0.25 * (attempt + 1))
                        continue
                    raise
            raise last_err or RuntimeError("Failed to read Excel after retries")

        def _read_excel_records_sync_from_upload_bytes(b: bytes):
            import io
            sheets_local = pd.read_excel(io.BytesIO(b), sheet_name=None)
            frames_local = [df for df in sheets_local.values() if df is not None and not df.empty]
            if not frames_local:
                raise ValueError("No data rows found in uploaded workbook.")
            combined = pd.concat(frames_local, ignore_index=True)
            combined.columns = combined.columns.str.strip()
            combined = combined.where(pd.notna(combined), None)
            recs = combined.to_dict("records")
            for r in recs:
                for k, v in list(r.items()):
                    if isinstance(v, pd.Timestamp):
                        r[k] = v.isoformat()
                    elif isinstance(v, float):
                        if math.isnan(v) or math.isinf(v):
                            r[k] = None
            return recs

        async def _run_inc_etl():
            try:
                if upload_bytes is not None:
                    records = await asyncio.to_thread(_read_excel_records_sync_from_upload_bytes, upload_bytes)
                else:
                    inc_path = Path(os.environ.get("INCENTIVE_DATA_FILE", "/app/Incentive Data1.xlsx"))
                    records = await asyncio.to_thread(_read_excel_records_sync_from_path, inc_path)

                coll = db.incentive_data
                await coll.delete_many({})
                if records:
                    batch_size = 5000
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        await coll.insert_many(batch, ordered=False)
                        if i == 0 or (i // batch_size) % 10 == 0:
                            _INC_ETL_STATE["records_loaded"] = min(i + len(batch), len(records))

                    # Best-effort indexes on common columns; ignore failures (unknown schema).
                    for idx in ["DOC_DATE", "Month", "month", "Zone_New", "Zone", "Product", "CUST_CODE", "Emp Code", "EMP_CODE"]:
                        try:
                            await coll.create_index(idx)
                        except Exception:
                            pass

                # Invalidate caches so dashboards reflect latest incentive upload.
                _cache_clear()
                await _persist_cache_clear_all()

                _INC_ETL_STATE.update({
                    "running": False,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "records_loaded": len(records),
                    "error": None,
                })
            except Exception as e:
                logger.exception(f"Error loading incentive data: {e}")
                _INC_ETL_STATE.update({
                    "running": False,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "error": str(e),
                })

        asyncio.create_task(_run_inc_etl())
        return {"status": "started", **_INC_ETL_STATE}
    except Exception as e:
        logger.exception(f"Error starting incentive ETL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/data/load/incentives/status")
async def incentive_load_status():
    return {"status": "running" if _INC_ETL_STATE.get("running") else "idle", **_INC_ETL_STATE}


def _mongo_num(field_name: str):
    """Mongo aggregation expression to safely coerce a field to double."""
    return {
        "$convert": {
            "input": f"${field_name}",
            "to": "double",
            "onError": 0.0,
            "onNull": 0.0,
        }
    }


@api_router.get("/incentives/overview")
async def incentives_overview():
    """Executive incentive KPIs from incentive_data."""
    cached = _cache_get("incentives:overview")
    if cached is not None:
        return cached
    persisted = await _persist_cache_get("incentives:overview")
    if persisted is not None:
        _cache_set("incentives:overview", persisted, ttl_seconds=300)
        return persisted

    coll = db.incentive_data
    # Fast check: if empty, return zeros (avoid throwing on dashboards)
    any_doc = await coll.find_one({}, {"_id": 1})
    if not any_doc:
        payload = {
            "total_incentive_paid": 0.0,
            "total_potential_incentive": 0.0,
            "payout_ratio_pct": 0.0,
            "total_budget": 0.0,
            "total_actual_sales": 0.0,
            "achievement_pct": 0.0,
            "incentive_cost_pct": 0.0,
            "revenue_per_incentive": 0.0,
            "employees_total": 0,
            "employees_eligible": 0,
            "employees_eligible_pct": 0.0,
        }
        _cache_set("incentives:overview", payload, ttl_seconds=300)
        await _persist_cache_set("incentives:overview", payload)
        return payload

    pipeline = [
        {
            "$group": {
                "_id": None,
                "total_incentive_paid": {"$sum": _mongo_num("Final Incentive")},
                "total_potential_incentive": {"$sum": _mongo_num("Potential Incentive")},
                "total_budget": {"$sum": _mongo_num("Budget")},
                "total_actual_sales": {"$sum": _mongo_num("Actual (Sales)")},
                "employees_total_set": {"$addToSet": "$Emp Id"},
                "employees_eligible_set": {
                    "$addToSet": {
                        "$cond": [
                            {"$gt": [_mongo_num("Final Incentive"), 0]},
                            "$Emp Id",
                            None,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "total_incentive_paid": {"$round": ["$total_incentive_paid", 2]},
                "total_potential_incentive": {"$round": ["$total_potential_incentive", 2]},
                "total_budget": {"$round": ["$total_budget", 2]},
                "total_actual_sales": {"$round": ["$total_actual_sales", 2]},
                "employees_total": {
                    "$size": {
                        "$filter": {
                            "input": "$employees_total_set",
                            "as": "e",
                            "cond": {"$ne": ["$$e", None]},
                        }
                    }
                },
                "employees_eligible": {
                    "$size": {
                        "$filter": {
                            "input": "$employees_eligible_set",
                            "as": "e",
                            "cond": {"$ne": ["$$e", None]},
                        }
                    }
                },
            }
        },
    ]
    out = await coll.aggregate(pipeline).to_list(1)
    row = (out or [{}])[0]

    total_paid = float(row.get("total_incentive_paid") or 0.0)
    total_potential = float(row.get("total_potential_incentive") or 0.0)
    total_budget = float(row.get("total_budget") or 0.0)
    total_actual = float(row.get("total_actual_sales") or 0.0)
    employees_total = int(row.get("employees_total") or 0)
    employees_eligible = int(row.get("employees_eligible") or 0)

    payout_ratio_pct = (total_paid / total_potential * 100.0) if total_potential > 0 else 0.0
    achievement_pct = (total_actual / total_budget * 100.0) if total_budget > 0 else 0.0
    incentive_cost_pct = (total_paid / total_actual * 100.0) if total_actual > 0 else 0.0
    revenue_per_incentive = (total_actual / total_paid) if total_paid > 0 else 0.0
    employees_eligible_pct = (employees_eligible / employees_total * 100.0) if employees_total > 0 else 0.0

    payload = {
        "total_incentive_paid": round(total_paid, 2),
        "total_potential_incentive": round(total_potential, 2),
        "payout_ratio_pct": round(payout_ratio_pct, 2),
        "total_budget": round(total_budget, 2),
        "total_actual_sales": round(total_actual, 2),
        "achievement_pct": round(achievement_pct, 2),
        "incentive_cost_pct": round(incentive_cost_pct, 2),
        "revenue_per_incentive": round(revenue_per_incentive, 4),
        "employees_total": employees_total,
        "employees_eligible": employees_eligible,
        "employees_eligible_pct": round(employees_eligible_pct, 2),
    }
    _cache_set("incentives:overview", payload, ttl_seconds=300)
    await _persist_cache_set("incentives:overview", payload)
    return payload


@api_router.get("/incentives/trend")
async def incentives_trend():
    """Cycle-wise trend for sales & incentives from incentive_data."""
    cached = _cache_get("incentives:trend")
    if cached is not None:
        return cached
    persisted = await _persist_cache_get("incentives:trend")
    if persisted is not None:
        _cache_set("incentives:trend", persisted, ttl_seconds=300)
        return persisted

    coll = db.incentive_data
    pipeline = [
        {
            "$group": {
                "_id": {"fy": "$FY", "cycle": "$Cycle"},
                "budget": {"$sum": _mongo_num("Budget")},
                "actual": {"$sum": _mongo_num("Actual (Sales)")},
                "incentive": {"$sum": _mongo_num("Final Incentive")},
                "potential": {"$sum": _mongo_num("Potential Incentive")},
            }
        },
        {
            "$project": {
                "_id": 0,
                "fy": "$_id.fy",
                "cycle": "$_id.cycle",
                "budget": {"$round": ["$budget", 2]},
                "actual": {"$round": ["$actual", 2]},
                "incentive": {"$round": ["$incentive", 2]},
                "potential": {"$round": ["$potential", 2]},
            }
        },
    ]
    rows = await coll.aggregate(pipeline).to_list(5000)

    def _cycle_num(c):
        try:
            s = str(c or "").strip().upper()
            return int(s[1:]) if s.startswith("C") and s[1:].isdigit() else 999
        except Exception:
            return 999

    rows.sort(key=lambda r: (str(r.get("fy") or ""), _cycle_num(r.get("cycle"))))

    for r in rows:
        budget = float(r.get("budget") or 0)
        actual = float(r.get("actual") or 0)
        inc = float(r.get("incentive") or 0)
        pot = float(r.get("potential") or 0)
        r["achievement_pct"] = round((actual / budget * 100.0) if budget > 0 else 0.0, 2)
        r["incentive_cost_pct"] = round((inc / actual * 100.0) if actual > 0 else 0.0, 2)
        r["payout_ratio_pct"] = round((inc / pot * 100.0) if pot > 0 else 0.0, 2)

    _cache_set("incentives:trend", rows, ttl_seconds=300)
    await _persist_cache_set("incentives:trend", rows)
    return rows


@api_router.get("/incentives/zone-division")
async def incentives_zone_division():
    """Zone × Division heatmap data (sales + incentive)."""
    cached = _cache_get("incentives:zone_division")
    if cached is not None:
        return cached

    coll = db.incentive_data
    pipeline = [
        {
            "$group": {
                "_id": {
                    "zone": "$Zone",
                    "division": "$Division",
                },
                "actual": {"$sum": _mongo_num("Actual (Sales)")},
                "incentive": {"$sum": _mongo_num("Final Incentive")},
            }
        },
        {
            "$project": {
                "_id": 0,
                "zone": "$_id.zone",
                "division": "$_id.division",
                "actual": {"$round": ["$actual", 2]},
                "incentive": {"$round": ["$incentive", 2]},
            }
        },
    ]
    rows = await coll.aggregate(pipeline).to_list(100000)
    rows = [r for r in rows if r.get("zone") and r.get("division")]
    _cache_set("incentives:zone_division", rows, ttl_seconds=300)
    return rows


@api_router.get("/incentives/distribution")
async def incentives_distribution(buckets: int = Query(12, ge=5, le=30)):
    """Histogram buckets for final incentive distribution."""
    cached = _cache_get(f"incentives:dist:{buckets}")
    if cached is not None:
        return cached

    coll = db.incentive_data
    pipeline = [
        {"$project": {"_v": _mongo_num("Final Incentive")}},
        {"$match": {"_v": {"$gt": 0}}},
        {"$bucketAuto": {"groupBy": "$_v", "buckets": int(buckets), "output": {"count": {"$sum": 1}}}},
        {"$project": {"_id": 0, "min": "$_id.min", "max": "$_id.max", "count": 1}},
    ]
    rows = await coll.aggregate(pipeline).to_list(100)
    for r in rows:
        r["min"] = float(r.get("min") or 0)
        r["max"] = float(r.get("max") or 0)
        r["label"] = f"{int(round(r['min']))}–{int(round(r['max']))}"
        r["count"] = int(r.get("count") or 0)
    _cache_set(f"incentives:dist:{buckets}", rows, ttl_seconds=300)
    return rows


@api_router.get("/incentives/employee-scatter")
async def incentives_employee_scatter(limit: int = Query(400, ge=50, le=1500)):
    """Employee-level scatter: sales vs incentive (plus achievement, role, zone, division)."""
    cached = _cache_get(f"incentives:employee_scatter:{limit}")
    if cached is not None:
        return cached

    coll = db.incentive_data
    pipeline = [
        {
            "$group": {
                "_id": "$Emp Id",
                "actual": {"$sum": _mongo_num("Actual (Sales)")},
                "budget": {"$sum": _mongo_num("Budget")},
                "incentive": {"$sum": _mongo_num("Final Incentive")},
                "potential": {"$sum": _mongo_num("Potential Incentive")},
                "zone": {"$first": "$Zone"},
                "division": {"$first": "$Division"},
                "role": {"$first": "$Role_Final"},
                "hq": {"$first": "$HQ_Final"},
            }
        },
        {"$match": {"_id": {"$ne": None}}},
        {
            "$project": {
                "_id": 0,
                "emp_id": "$_id",
                "actual": {"$round": ["$actual", 2]},
                "budget": {"$round": ["$budget", 2]},
                "incentive": {"$round": ["$incentive", 2]},
                "potential": {"$round": ["$potential", 2]},
                "zone": 1,
                "division": 1,
                "role": 1,
                "hq": 1,
            }
        },
    ]
    rows = await coll.aggregate(pipeline).to_list(50000)
    for r in rows:
        actual = float(r.get("actual") or 0.0)
        budget = float(r.get("budget") or 0.0)
        inc = float(r.get("incentive") or 0.0)
        pot = float(r.get("potential") or 0.0)
        r["achievement_pct"] = round((actual / budget * 100.0) if budget > 0 else 0.0, 2)
        r["incentive_cost_pct"] = round((inc / actual * 100.0) if actual > 0 else 0.0, 4)
        r["payout_ratio_pct"] = round((inc / pot * 100.0) if pot > 0 else 0.0, 2)

    # Reduce payload size: choose highest-sales employees + some high-incentive outliers
    rows_sorted_sales = sorted(rows, key=lambda x: float(x.get("actual") or 0), reverse=True)
    rows_sorted_inc = sorted(rows, key=lambda x: float(x.get("incentive") or 0), reverse=True)
    picked = []
    seen = set()
    for r in (rows_sorted_sales[: int(limit * 0.75)] + rows_sorted_inc[: int(limit * 0.25)]):
        eid = r.get("emp_id")
        if eid in seen:
            continue
        seen.add(eid)
        picked.append(r)
        if len(picked) >= limit:
            break

    _cache_set(f"incentives:employee_scatter:{limit}", picked, ttl_seconds=300)
    return picked


@api_router.get("/incentives/anomalies")
async def incentives_anomalies(limit: int = Query(25, ge=5, le=100)):
    """
    Incentive anomalies: high incentive with low sales OR low achievement.
    Heuristic:
    - "high incentive" = top incentive_cost_pct (incentive/actual) with minimum sales floor
    - plus "low achievement + paid incentive" cases
    """
    cached = _cache_get(f"incentives:anomalies:{limit}")
    if cached is not None:
        return cached

    coll = db.incentive_data
    pipeline = [
        {
            "$group": {
                "_id": "$Emp Id",
                "actual": {"$sum": _mongo_num("Actual (Sales)")},
                "budget": {"$sum": _mongo_num("Budget")},
                "incentive": {"$sum": _mongo_num("Final Incentive")},
                "potential": {"$sum": _mongo_num("Potential Incentive")},
                "zone": {"$first": "$Zone"},
                "division": {"$first": "$Division"},
                "role": {"$first": "$Role_Final"},
                "hq": {"$first": "$HQ_Final"},
            }
        },
        {"$match": {"_id": {"$ne": None}}},
        {
            "$project": {
                "_id": 0,
                "emp_id": "$_id",
                "actual": "$actual",
                "budget": "$budget",
                "incentive": "$incentive",
                "potential": "$potential",
                "zone": 1,
                "division": 1,
                "role": 1,
                "hq": 1,
            }
        },
    ]
    rows = await coll.aggregate(pipeline).to_list(50000)

    enriched = []
    for r in rows:
        actual = float(r.get("actual") or 0.0)
        budget = float(r.get("budget") or 0.0)
        inc = float(r.get("incentive") or 0.0)
        pot = float(r.get("potential") or 0.0)
        if inc <= 0:
            continue
        ach = (actual / budget * 100.0) if budget > 0 else 0.0
        cost = (inc / actual * 100.0) if actual > 0 else 0.0
        payout = (inc / pot * 100.0) if pot > 0 else 0.0
        enriched.append(
            {
                "emp_id": r.get("emp_id"),
                "zone": r.get("zone"),
                "division": r.get("division"),
                "role": r.get("role"),
                "hq": r.get("hq"),
                "actual": round(actual, 2),
                "budget": round(budget, 2),
                "incentive": round(inc, 2),
                "potential": round(pot, 2),
                "achievement_pct": round(ach, 2),
                "incentive_cost_pct": round(cost, 4),
                "payout_ratio_pct": round(payout, 2),
            }
        )

    # Sales floor: focus on meaningful sales to avoid tiny denominators dominating.
    enriched_sorted_sales = sorted(enriched, key=lambda x: x["actual"], reverse=True)
    sales_floor = enriched_sorted_sales[int(len(enriched_sorted_sales) * 0.8)]["actual"] if enriched_sorted_sales else 0.0
    sales_floor = max(sales_floor, 100000.0)  # at least ₹1L

    high_cost = [r for r in enriched if r["actual"] >= sales_floor]
    high_cost.sort(key=lambda x: x["incentive_cost_pct"], reverse=True)

    low_ach = [r for r in enriched if r["achievement_pct"] > 0 and r["achievement_pct"] < 80]
    low_ach.sort(key=lambda x: (x["achievement_pct"], -x["incentive"]), reverse=False)

    combined = []
    seen = set()
    for r in high_cost[:limit]:
        if r["emp_id"] in seen:
            continue
        seen.add(r["emp_id"])
        r["reason"] = "High incentive cost vs sales"
        combined.append(r)
    for r in low_ach[:limit]:
        if r["emp_id"] in seen:
            continue
        seen.add(r["emp_id"])
        r["reason"] = "Low achievement with incentive paid"
        combined.append(r)
        if len(combined) >= limit:
            break

    _cache_set(f"incentives:anomalies:{limit}", combined, ttl_seconds=300)
    return combined


@api_router.get("/incentives/employee-drill")
async def incentives_employee_drill(emp_id: int = Query(..., description="Employee id (Emp Id in incentive data)")):
    """Cycle-wise totals and product×division mix for one employee (incentive_data)."""
    coll = db.incentive_data
    emp_match = {"$in": [emp_id, float(emp_id)]}
    sample = await coll.find_one(
        {"Emp Id": emp_match},
        {"Zone": 1, "Division": 1, "Role_Final": 1, "HQ_Final": 1, "Role": 1},
    )
    if not sample:
        raise HTTPException(status_code=404, detail=f"No incentive rows found for employee {emp_id}.")

    meta = {
        "emp_id": emp_id,
        "zone": sample.get("Zone"),
        "division": sample.get("Division"),
        "role": sample.get("Role_Final") or sample.get("Role"),
        "hq": sample.get("HQ_Final"),
    }

    def _cycle_sort_key(r):
        c = str(r.get("cycle") or "").strip().upper()
        try:
            n = int(c[1:]) if c.startswith("C") and c[1:].isdigit() else 999
        except Exception:
            n = 999
        return (str(r.get("fy") or ""), n)

    cycle_pipe = [
        {"$match": {"Emp Id": emp_match}},
        {
            "$group": {
                "_id": {"fy": "$FY", "cycle": "$Cycle"},
                "budget": {"$sum": _mongo_num("Budget")},
                "actual": {"$sum": _mongo_num("Actual (Sales)")},
                "incentive": {"$sum": _mongo_num("Final Incentive")},
                "potential": {"$sum": _mongo_num("Potential Incentive")},
                "lines": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "fy": "$_id.fy",
                "cycle": "$_id.cycle",
                "budget": {"$round": ["$budget", 2]},
                "actual": {"$round": ["$actual", 2]},
                "incentive": {"$round": ["$incentive", 2]},
                "potential": {"$round": ["$potential", 2]},
                "lines": 1,
            }
        },
    ]
    cycles = await coll.aggregate(cycle_pipe).to_list(500)
    by_cycle = []
    for r in cycles:
        budget = float(r.get("budget") or 0)
        actual = float(r.get("actual") or 0)
        inc = float(r.get("incentive") or 0)
        pot = float(r.get("potential") or 0)
        by_cycle.append({
            "fy": r.get("fy"),
            "cycle": r.get("cycle"),
            "budget": r.get("budget"),
            "actual": r.get("actual"),
            "incentive": r.get("incentive"),
            "potential": r.get("potential"),
            "lines": int(r.get("lines") or 0),
            "achievement_pct": round((actual / budget * 100.0) if budget > 0 else 0.0, 2),
            "incentive_cost_pct": round((inc / actual * 100.0) if actual > 0 else 0.0, 4),
            "payout_ratio_pct": round((inc / pot * 100.0) if pot > 0 else 0.0, 2),
        })
    by_cycle.sort(key=_cycle_sort_key)

    mix_pipe = [
        {"$match": {"Emp Id": emp_match}},
        {
            "$group": {
                "_id": {"product": "$Product", "division": "$Division"},
                "actual": {"$sum": _mongo_num("Actual (Sales)")},
                "incentive": {"$sum": _mongo_num("Final Incentive")},
                "lines": {"$sum": 1},
            }
        },
        {"$sort": {"actual": -1}},
        {"$limit": 60},
        {
            "$project": {
                "_id": 0,
                "product": "$_id.product",
                "division": "$_id.division",
                "actual": {"$round": ["$actual", 2]},
                "incentive": {"$round": ["$incentive", 2]},
                "lines": 1,
            }
        },
    ]
    mix_raw = await coll.aggregate(mix_pipe).to_list(60)
    by_product_division = []
    for r in mix_raw:
        actual = float(r.get("actual") or 0)
        inc = float(r.get("incentive") or 0)
        by_product_division.append({
            "product": r.get("product"),
            "division": r.get("division"),
            "actual": r.get("actual"),
            "incentive": r.get("incentive"),
            "lines": int(r.get("lines") or 0),
            "incentive_cost_pct": round((inc / actual * 100.0) if actual > 0 else 0.0, 4),
        })

    return {"meta": meta, "by_cycle": by_cycle, "by_product_division": by_product_division}


# Dashboard Overview Endpoints
@api_router.get("/dashboard/overview", response_model=KPIResponse)
async def get_dashboard_overview():
    """Get executive dashboard KPIs"""
    cached = _cache_get("dashboard:overview")
    if cached is not None:
        return KPIResponse(**cached)
    persisted = await _persist_cache_get("dashboard:overview")
    if persisted is not None:
        _cache_set("dashboard:overview", persisted)
        return KPIResponse(**persisted)

    pipeline = [
        {
            "$facet": {
                "totals": [
        {
            "$group": {
                "_id": None,
                "net_sales_value": {"$sum": "$NET_SALES_VALUE"},
                "net_sales_qty": {"$sum": "$NET_SALES_QTY"},
                        }
                    }
                ],
                "transactions": [{"$group": {"_id": "$TRAN_ID"}}, {"$count": "count"}],
                "customers": [{"$group": {"_id": "$CUST_CODE"}}, {"$count": "count"}],
                "products": [{"$group": {"_id": "$Product"}}, {"$count": "count"}],
                "gross": [
                    {"$match": {"NET_SALES_VALUE": {"$gt": 0}}},
                    {"$group": {"_id": None, "gross": {"$sum": "$NET_SALES_VALUE"}}},
                ],
                "returns": [
                    {"$match": {"NET_SALES_VALUE": {"$lt": 0}}},
                    {"$group": {"_id": None, "returns": {"$sum": "$NET_SALES_VALUE"}}},
                ],
            }
        }
    ]
    
    result = await db.sales_data.aggregate(pipeline).to_list(1)
    
    if not result:
        raise HTTPException(status_code=404, detail="No data found. Please load data first.")
    
    data = result[0]
    totals = (data.get("totals") or [{}])[0]
    gross_row = (data.get("gross") or [{}])[0]
    returns_row = (data.get("returns") or [{}])[0]

    net_sales_value = float(totals.get("net_sales_value") or 0)
    net_sales_qty = int(round(totals.get("net_sales_qty") or 0))
    gross_value = float(gross_row.get("gross") or 0)
    returns_value = abs(float(returns_row.get("returns") or 0))
    total_transactions = int(((data.get("transactions") or [{}])[0]).get("count") or 0)
    total_customers = int(((data.get("customers") or [{}])[0]).get("count") or 0)
    total_products = int(((data.get("products") or [{}])[0]).get("count") or 0)

    payload = {
        "net_sales_value": net_sales_value,
        "gross_sales_value": gross_value,
        "returns_value": returns_value,
        "returns_rate": (returns_value / gross_value * 100) if gross_value > 0 else 0,
        "net_sales_qty": net_sales_qty,
        "total_transactions": total_transactions,
        "total_customers": total_customers,
        "total_products": total_products,
        "avg_transaction_value": net_sales_value / total_transactions if total_transactions > 0 else 0,
        "avg_revenue_per_customer": net_sales_value / total_customers if total_customers > 0 else 0,
    }
    _cache_set("dashboard:overview", payload)
    await _persist_cache_set("dashboard:overview", payload)
    return KPIResponse(**payload)

@api_router.get("/dashboard/trends")
async def get_monthly_trends():
    """Get monthly sales trends"""
    cached = _cache_get("dashboard:trends")
    if cached is not None:
        return cached
    persisted = await _persist_cache_get("dashboard:trends")
    if persisted is not None:
        _cache_set("dashboard:trends", persisted)
        return persisted

    pipeline = [
        {
            "$addFields": {"_month": {"$substrBytes": ["$DOC_DATE", 0, 7]}}
        },
        {
            "$match": {"_month": {"$ne": None}}
        },
        {
            "$group": {
                "_id": "$_month",
                "value": {"$sum": "$NET_SALES_VALUE"},
                "quantity": {"$sum": "$NET_SALES_QTY"},
                # Count line-items (fast). This may differ from distinct TRAN_ID counts.
                "transactions": {"$sum": 1},
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
            "transactions": int(r.get("transactions") or 0),
            "growth_pct": round(growth, 2) if growth else None
        })
        prev_value = r["value"]
    
    _cache_set("dashboard:trends", trends)
    await _persist_cache_set("dashboard:trends", trends)
    return trends

@api_router.get("/dashboard/concentration")
async def get_concentration_metrics():
    """Get revenue concentration metrics"""
    cached = _cache_get("dashboard:concentration")
    if cached is not None:
        return cached
    persisted = await _persist_cache_get("dashboard:concentration")
    if persisted is not None:
        _cache_set("dashboard:concentration", persisted)
        return persisted

    # Zone concentration
    zone_pipeline = [
        {"$group": {"_id": "$Zone_New", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}}
    ]
    zones = await db.sales_data.aggregate(zone_pipeline).to_list(100)
    total = sum(_safe_float(z.get("value")) for z in zones)

    zone_data = [
        {
            "name": z.get("_id"),
            "value": _safe_float(z.get("value")),
            "pct": round((_safe_float(z.get("value")) / total) * 100, 2) if total > 0 else 0,
        }
        for z in zones
        if z.get("_id") is not None
    ]
    
    # State concentration
    state_pipeline = [
        {"$group": {"_id": "$State", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}},
        {"$limit": 10}
    ]
    states = await db.sales_data.aggregate(state_pipeline).to_list(10)
    state_data = [
        {
            "name": s.get("_id"),
            "value": _safe_float(s.get("value")),
            "pct": round((_safe_float(s.get("value")) / total) * 100, 2) if total > 0 else 0,
        }
        for s in states
        if s.get("_id") is not None
    ]
    
    # Top 3 states contribution
    top_3_pct = sum(s["pct"] for s in state_data[:3])
    
    # Customer concentration
    cust_pipeline = [
        {"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}},
        {"$limit": 10}
    ]
    customers = await db.sales_data.aggregate(cust_pipeline).to_list(10)
    top_10_cust_value = sum(_safe_float(c.get("value")) for c in customers)
    top_10_cust_pct = (top_10_cust_value / total * 100) if total > 0 else 0
    top_customers_data = [
        {
            "name": c.get("_id"),
            "value": round(_safe_float(c.get("value")), 2),
            "pct": round((_safe_float(c.get("value")) / total) * 100, 2) if total > 0 else 0,
        }
        for c in customers
        if c.get("_id") is not None
    ]
    
    payload = {
        "zones": zone_data,
        "top_states": state_data,
        "top_3_states_pct": round(top_3_pct, 2),
        "top_10_customers_pct": round(_safe_float(top_10_cust_pct), 2),
        "top_customers": top_customers_data,
    }
    _cache_set("dashboard:concentration", payload)
    await _persist_cache_set("dashboard:concentration", payload)
    return payload

@api_router.get("/dashboard/drill")
async def dashboard_drill(
    metric: str = Query(..., description="transactions | customers | products"),
    group_by: str = Query(..., description="month | zone | state"),
):
    """Drill-down counts by dimension for Executive dashboard KPIs (transactions, customers, products)."""
    if metric not in ("transactions", "customers", "products"):
        raise HTTPException(status_code=400, detail="Invalid metric")
    if group_by not in ("month", "zone", "state"):
        raise HTTPException(status_code=400, detail="Invalid group_by")
    add_doc_date = {"$addFields": {"_doc_date": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}}
    match_valid_date = {"$match": {"_doc_date": {"$ne": None}}} if group_by == "month" else {"$match": {}}
    month_id = {"$dateToString": {"format": "%Y-%m", "date": "$_doc_date"}}
    group_id_expr = {"month": month_id, "zone": "$Zone_New", "state": "$State"}[group_by]
    if metric == "transactions":
        pipeline = [
            add_doc_date,
            match_valid_date,
            {"$group": {"_id": group_id_expr, "value": {"$addToSet": "$TRAN_ID"}}},
            {"$addFields": {"value": {"$size": "$value"}}},
            {"$sort": {"_id": 1 if group_by == "month" else -1, "value": -1 if group_by != "month" else 1}},
            {"$limit": 100},
        ]
    elif metric == "customers":
        pipeline = [
            add_doc_date,
            match_valid_date,
            {"$group": {"_id": group_id_expr, "value": {"$addToSet": "$CUST_CODE"}}},
            {"$addFields": {"value": {"$size": "$value"}}},
            {"$sort": {"_id": 1 if group_by == "month" else -1, "value": -1 if group_by != "month" else 1}},
            {"$limit": 100},
        ]
    else:
        pipeline = [
            add_doc_date,
            match_valid_date,
            {"$group": {"_id": group_id_expr, "value": {"$addToSet": "$Product"}}},
            {"$addFields": {"value": {"$size": "$value"}}},
            {"$sort": {"_id": 1 if group_by == "month" else -1, "value": -1 if group_by != "month" else 1}},
            {"$limit": 100},
        ]
    rows = await db.sales_data.aggregate(pipeline).to_list(100)
    return [{"dimension": r["_id"], "value": r["value"]} for r in rows if r.get("_id") is not None]
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

    if group_by == "month":
        stages.append({"$sort": {"_id": 1}})
    else:
        stages.append({"$sort": {"value": -1}})
    stages.append({"$limit": 100})
    rows = await db.sales_data.aggregate(stages).to_list(100)
    if kpi == "returns_value":
        for r in rows:
            r["value"] = abs(r["value"])
    return [{"dimension": r["_id"], "value": round(r["value"], 2)} for r in rows if r.get("_id") is not None]


@api_router.get("/revenue-growth/visuals")
async def get_revenue_growth_visuals():
    """Visual-ready KPI payload for Revenue & Growth dashboard."""
    cache_key = "revenue-growth:visuals"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    persisted = await _persist_cache_get(cache_key)
    if persisted is not None:
        _cache_set(cache_key, persisted, ttl_seconds=300)
        return persisted

    trends = await get_monthly_trends()
    months = [t.get("month") for t in trends if t.get("month")]
    latest_month = months[-1] if months else None
    prev_month = months[-2] if len(months) >= 2 else None

    last_3 = trends[-3:] if len(trends) >= 3 else trends
    prev_3 = trends[-6:-3] if len(trends) >= 6 else trends[:-3]
    run_rate = sum(_safe_float(x.get("value")) for x in last_3) / len(last_3) if last_3 else 0
    run_rate_target = sum(_safe_float(x.get("value")) for x in prev_3) / len(prev_3) if prev_3 else run_rate
    run_rate_vs_target_pct = ((run_rate - run_rate_target) / run_rate_target * 100) if run_rate_target > 0 else 0

    cagr_3m_pct = 0.0
    if len(last_3) >= 3:
        first_v = _safe_float(last_3[0].get("value"))
        last_v = _safe_float(last_3[-1].get("value"))
        if first_v > 0 and last_v > 0:
            periods = len(last_3) - 1
            cagr_3m_pct = ((last_v / first_v) ** (12 / periods) - 1) * 100

    async def _growth_contrib(dim_field: str, limit: int = 10):
        if not latest_month or not prev_month:
            return []
        pipe = [
            {"$addFields": {"_month": {"$substrBytes": ["$DOC_DATE", 0, 7]}}},
            {"$match": {"_month": {"$in": [prev_month, latest_month]}}},
            {"$group": {
                "_id": f"${dim_field}",
                "latest_value": {"$sum": {"$cond": [{"$eq": ["$_month", latest_month]}, "$NET_SALES_VALUE", 0]}},
                "prev_value": {"$sum": {"$cond": [{"$eq": ["$_month", prev_month]}, "$NET_SALES_VALUE", 0]}},
            }},
            {"$match": {"_id": {"$ne": None}}},
            {"$project": {
                "_id": 0,
                "dimension": "$_id",
                "latest_value": {"$round": ["$latest_value", 2]},
                "prev_value": {"$round": ["$prev_value", 2]},
                "delta_value": {"$round": [{"$subtract": ["$latest_value", "$prev_value"]}, 2]},
            }},
            {"$sort": {"delta_value": -1}},
            {"$limit": limit},
        ]
        return await db.sales_data.aggregate(pipe).to_list(limit)

    zone_contrib = await _growth_contrib("Zone_New", 8)
    state_contrib = await _growth_contrib("State", 8)
    product_contrib = await _growth_contrib("Product", 8)

    new_existing = []
    if latest_month and prev_month:
        new_existing_pipe = [
            {"$addFields": {"_month": {"$substrBytes": ["$DOC_DATE", 0, 7]}}},
            {"$group": {
                "_id": "$CUST_CODE",
                "first_month": {"$min": "$_month"},
                "latest_value": {"$sum": {"$cond": [{"$eq": ["$_month", latest_month]}, "$NET_SALES_VALUE", 0]}},
                "prev_value": {"$sum": {"$cond": [{"$eq": ["$_month", prev_month]}, "$NET_SALES_VALUE", 0]}},
            }},
            {"$project": {
                "_id": 0,
                "segment": {"$cond": [{"$eq": ["$first_month", latest_month]}, "New", "Existing"]},
                "latest_value": "$latest_value",
                "prev_value": "$prev_value",
            }},
            {"$group": {
                "_id": "$segment",
                "latest_value": {"$sum": "$latest_value"},
                "prev_value": {"$sum": "$prev_value"},
            }},
            {"$project": {
                "_id": 0,
                "segment": "$_id",
                "latest_value": {"$round": ["$latest_value", 2]},
                "prev_value": {"$round": ["$prev_value", 2]},
                "growth_pct": {
                    "$round": [
                        {"$cond": [
                            {"$gt": ["$prev_value", 0]},
                            {"$multiply": [{"$divide": [{"$subtract": ["$latest_value", "$prev_value"]}, "$prev_value"]}, 100]},
                            0,
                        ]},
                        2,
                    ]
                },
            }},
        ]
        new_existing = await db.sales_data.aggregate(new_existing_pipe).to_list(2)

    recovery = {"gross_growth_pct": 0, "net_growth_pct": 0, "returns_drag_pct": 0, "returns_rate_change_pct": 0}
    if latest_month and prev_month:
        rec_pipe = [
            {"$addFields": {"_month": {"$substrBytes": ["$DOC_DATE", 0, 7]}}},
            {"$match": {"_month": {"$in": [prev_month, latest_month]}}},
            {"$group": {
                "_id": "$_month",
                "gross_value": {"$sum": {"$cond": [{"$gt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}},
                "net_value": {"$sum": "$NET_SALES_VALUE"},
                "returns_value": {"$sum": {"$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, {"$abs": "$NET_SALES_VALUE"}, 0]}},
            }},
        ]
        rec_rows = await db.sales_data.aggregate(rec_pipe).to_list(2)
        rec_map = {r.get("_id"): r for r in rec_rows}
        pr = rec_map.get(prev_month) or {}
        lr = rec_map.get(latest_month) or {}
        prev_gross = _safe_float(pr.get("gross_value"))
        latest_gross = _safe_float(lr.get("gross_value"))
        prev_net = _safe_float(pr.get("net_value"))
        latest_net = _safe_float(lr.get("net_value"))
        prev_ret = _safe_float(pr.get("returns_value"))
        latest_ret = _safe_float(lr.get("returns_value"))
        gross_growth = ((latest_gross - prev_gross) / prev_gross * 100) if prev_gross > 0 else 0
        net_growth = ((latest_net - prev_net) / prev_net * 100) if prev_net > 0 else 0
        prev_ret_rate = (prev_ret / prev_gross * 100) if prev_gross > 0 else 0
        latest_ret_rate = (latest_ret / latest_gross * 100) if latest_gross > 0 else 0
        recovery = {
            "gross_growth_pct": round(gross_growth, 2),
            "net_growth_pct": round(net_growth, 2),
            "returns_drag_pct": round(gross_growth - net_growth, 2),
            "returns_rate_change_pct": round(latest_ret_rate - prev_ret_rate, 2),
        }

    payload = _sanitize_json_value({
        "periods": {"previous_month": prev_month, "latest_month": latest_month},
        "kpis": {
            "cagr_3m_pct": round(cagr_3m_pct, 2),
            "run_rate": round(run_rate, 2),
            "run_rate_target": round(run_rate_target, 2),
            "run_rate_vs_target_pct": round(run_rate_vs_target_pct, 2),
            "recovery_growth_return_adjusted_pct": round(recovery.get("net_growth_pct", 0) - max(recovery.get("returns_rate_change_pct", 0), 0), 2),
        },
        "run_rate_vs_target": [
            {"name": "Run-rate", "value": round(run_rate, 2)},
            {"name": "Target", "value": round(run_rate_target, 2)},
        ],
        "growth_contribution": {
            "zone": zone_contrib,
            "state": state_contrib,
            "product": product_contrib,
        },
        "new_vs_existing_growth": new_existing,
        "recovery_breakdown": [
            {"name": "Gross growth %", "value": recovery.get("gross_growth_pct", 0)},
            {"name": "Net growth %", "value": recovery.get("net_growth_pct", 0)},
            {"name": "Returns drag %", "value": recovery.get("returns_drag_pct", 0)},
            {"name": "Return rate delta %", "value": recovery.get("returns_rate_change_pct", 0)},
        ],
    })
    _cache_set(cache_key, payload, ttl_seconds=300)
    await _persist_cache_set(cache_key, payload)
    return payload

# Product Endpoints
@api_router.get("/products/performance")
async def get_product_performance():
    """Get product performance metrics"""
    cached = _cache_get("products:performance")
    if cached is not None:
        return cached
    persisted = await _persist_cache_get("products:performance")
    if persisted is not None:
        _cache_set("products:performance", persisted)
        return persisted

    # Avoid large $addToSet arrays (slow/memory heavy). Compute distinct customer counts via grouping.
    pipeline = [
        {
            "$facet": {
                "totals": [
        {
            "$group": {
                "_id": "$Product",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                "brand": {"$first": "$ITEM_BRAND KPMG"},
                "division": {"$first": "$Div_Code (Mapping HQ)"},
                            "avg_ppu": {"$avg": "$PPU"},
                        }
                    },
                    {"$sort": {"sales_value": -1}},
                    {"$limit": 100},
                ],
                "customer_counts": [
                    {"$group": {"_id": {"product": "$Product", "cust": "$CUST_CODE"}}},
                    {"$group": {"_id": "$_id.product", "customer_count": {"$sum": 1}}},
                ],
                "returns": [
                    {"$match": {"NET_SALES_VALUE": {"$lt": 0}}},
                    {"$group": {"_id": "$Product", "returns": {"$sum": "$NET_SALES_VALUE"}}},
                ],
            }
        }
    ]

    agg = await db.sales_data.aggregate(pipeline).to_list(1)
    if not agg:
        _cache_set("products:performance", [])
        await _persist_cache_set("products:performance", [])
        return []
    data = agg[0]
    products = data.get("totals") or []
    total = sum(p.get("sales_value") or 0 for p in products)

    returns_map = {r["_id"]: abs(r.get("returns") or 0) for r in (data.get("returns") or []) if r.get("_id") is not None}
    cust_map = {c["_id"]: int(c.get("customer_count") or 0) for c in (data.get("customer_counts") or []) if c.get("_id") is not None}
    
    result = []
    for p in products:
        pid = p.get("_id")
        if pid is None:
            continue
        sales_value = float(p.get("sales_value") or 0)
        returns_val = float(returns_map.get(pid) or 0)
        gross = sales_value + returns_val
        result.append({
            "product": pid,
            "sales_value": sales_value,
            "sales_qty": float(p.get("sales_qty") or 0),
            "contribution_pct": round(sales_value / total * 100, 2) if total > 0 else 0,
            "returns_value": returns_val,
            "returns_rate": round(returns_val / gross * 100, 2) if gross > 0 else 0,
            "customer_count": cust_map.get(pid, 0),
            "avg_price": round(float(p.get("avg_ppu") or 0), 2) if p.get("avg_ppu") is not None else 0,
            "brand": p.get("brand"),
            "division": p.get("division"),
        })

    _cache_set("products:performance", result)
    await _persist_cache_set("products:performance", result)
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
    cache_key = "geography:zones"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    persisted = await _persist_cache_get(cache_key)
    if persisted is not None:
        _cache_set(cache_key, persisted)
        return persisted
    pipeline = [{
        "$facet": {
            "totals": [
                {"$group": {
                "_id": "$Zone_New",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                    # Fast proxy for activity volume (line items).
                    "transaction_count": {"$sum": 1},
                }},
                {"$sort": {"sales_value": -1}},
            ],
            "customer_counts": [
                {"$group": {"_id": {"zone": "$Zone_New", "cust": "$CUST_CODE"}}},
                {"$group": {"_id": "$_id.zone", "customer_count": {"$sum": 1}}},
            ],
        }
    }]
    agg = await db.sales_data.aggregate(pipeline).to_list(1)
    data = agg[0] if agg else {}
    zones = data.get("totals") or []
    cust_map = {c.get("_id"): int(c.get("customer_count") or 0) for c in (data.get("customer_counts") or []) if c.get("_id") is not None}
    total = sum(_safe_float(z.get("sales_value")) for z in zones)

    rows = [{
        "name": z.get("_id"),
        "sales_value": _safe_float(z.get("sales_value")),
        "sales_qty": _safe_float(z.get("sales_qty")),
        "contribution_pct": round((_safe_float(z.get("sales_value")) / total) * 100, 2) if total > 0 else 0,
        "customer_count": int(cust_map.get(z.get("_id"), 0)),
        "transaction_count": int(z.get("transaction_count") or 0),
        "avg_per_customer": round(_safe_float(z.get("sales_value")) / int(cust_map.get(z.get("_id"), 0)), 2) if int(cust_map.get(z.get("_id"), 0)) > 0 else 0
    } for z in zones if z.get("_id") is not None]
    rows = _sanitize_json_value(rows)
    _cache_set(cache_key, rows)
    await _persist_cache_set(cache_key, rows)
    return rows

@api_router.get("/geography/zones/{zone_name}/states")
async def get_states_by_zone(zone_name: str):
    """Get state-level drill-down for a zone"""
    cache_key = f"geography:states:{zone_name}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    persisted = await _persist_cache_get(cache_key)
    if persisted is not None:
        _cache_set(cache_key, persisted)
        return persisted
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
    total = sum(_safe_float(s.get("sales_value")) for s in states)

    rows = [{
        "name": s.get("_id"),
        "sales_value": _safe_float(s.get("sales_value")),
        "sales_qty": _safe_float(s.get("sales_qty")),
        "contribution_pct": round((_safe_float(s.get("sales_value")) / total) * 100, 2) if total > 0 else 0,
        "customer_count": len(s.get("customers") or []),
        "transaction_count": len(s.get("transactions") or []),
        "avg_per_customer": round(_safe_float(s.get("sales_value")) / len(s.get("customers") or []), 2) if (s.get("customers") or []) else 0
    } for s in states if s.get("_id") is not None]
    rows = _sanitize_json_value(rows)
    _cache_set(cache_key, rows)
    await _persist_cache_set(cache_key, rows)
    return rows

@api_router.get("/geography/states/{state_name}/cities")
async def get_cities_by_state(state_name: str):
    """Get city-level drill-down for a state"""
    cache_key = f"geography:cities:{state_name}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    persisted = await _persist_cache_get(cache_key)
    if persisted is not None:
        _cache_set(cache_key, persisted)
        return persisted
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
    total = sum(_safe_float(c.get("sales_value")) for c in cities)

    rows = [{
        "name": c.get("_id"),
        "sales_value": _safe_float(c.get("sales_value")),
        "sales_qty": _safe_float(c.get("sales_qty")),
        "contribution_pct": round((_safe_float(c.get("sales_value")) / total) * 100, 2) if total > 0 else 0,
        "customer_count": len(c.get("customers") or []),
        "transaction_count": len(c.get("transactions") or []),
        "avg_per_customer": round(_safe_float(c.get("sales_value")) / len(c.get("customers") or []), 2) if (c.get("customers") or []) else 0
    } for c in cities if c.get("_id") is not None]
    rows = _sanitize_json_value(rows)
    _cache_set(cache_key, rows)
    await _persist_cache_set(cache_key, rows)
    return rows

# Customer Endpoints
@api_router.get("/customers/performance")
async def get_customer_performance(
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
):
    """Get customer performance metrics (plain int default so internal callers work)."""
    cache_key = f"customers:performance:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    persisted = await _persist_cache_get(cache_key)
    if persisted is not None:
        _cache_set(cache_key, persisted)
        return persisted
    pipeline = [
        {
            "$group": {
                "_id": "$CUST_CODE",
                "sales_value": {"$sum": "$NET_SALES_VALUE"},
                "sales_qty": {"$sum": "$NET_SALES_QTY"},
                # Use line-item count for speed instead of large distinct set.
                "transaction_count": {"$sum": 1},
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
    
    rows = [{
        "customer_code": c.get("_id"),
        "customer_type": c.get("customer_type"),
        "sales_value": _safe_float(c.get("sales_value")),
        "sales_qty": _safe_float(c.get("sales_qty")),
        "transaction_count": int(c.get("transaction_count") or 0),
        "avg_transaction": round(_safe_float(c.get("sales_value")) / int(c.get("transaction_count") or 0), 2) if int(c.get("transaction_count") or 0) > 0 else 0,
        "stop_business": c.get("stop_business") or "N",
        "city": c.get("city"),
        "state": c.get("state"),
        "zone": c.get("zone")
    } for c in customers if c.get("_id") is not None]
    rows = _sanitize_json_value(rows)
    _cache_set(cache_key, rows)
    await _persist_cache_set(cache_key, rows)
    return rows

@api_router.get("/customers/concentration")
async def get_customer_concentration():
    """Get customer concentration (Pareto) analysis"""
    cache_key = "customers:concentration"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    persisted = await _persist_cache_get(cache_key)
    if persisted is not None:
        _cache_set(cache_key, persisted)
        return persisted
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
    
    payload = {
        "pareto_data": pareto_data[:50],  # Top 50 for visualization
        "top_10_customers_pct": top_10_pct,
        "top_20_customers_pct": top_20_pct,
        "total_customers": len(customers)
    }
    payload = _sanitize_json_value(payload)
    _cache_set(cache_key, payload)
    await _persist_cache_set(cache_key, payload)
    return payload

@api_router.get("/customers/risk")
async def get_customer_risk():
    """Get customer risk indicators"""
    cache_key = "customers:risk"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    persisted = await _persist_cache_get(cache_key)
    if persisted is not None:
        _cache_set(cache_key, persisted)
        return persisted
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
    
    payload = {
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
    payload = _sanitize_json_value(payload)
    _cache_set(cache_key, payload)
    await _persist_cache_set(cache_key, payload)
    return payload

# Pricing Endpoints
@api_router.get("/pricing/analysis")
async def get_pricing_analysis():
    """Get pricing and discount analysis"""
    cached = _cache_get("pricing:analysis")
    if cached is not None:
        return cached
    persisted = await _persist_cache_get("pricing:analysis")
    if persisted is not None:
        _cache_set("pricing:analysis", persisted)
        return persisted

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
    
    result = [{
        "product": p.get("_id"),
        "avg_ppu": round(_safe_float(p.get("avg_ppu")), 2),
        "avg_ptr": round(_safe_float(p.get("avg_ptr")), 2),
        "avg_mrp": round(_safe_float(p.get("avg_mrp")), 2),
        "price_realization": round((_safe_float(p.get("avg_ppu")) / _safe_float(p.get("avg_rate"))) if _safe_float(p.get("avg_rate")) > 0 else 1, 4),
        "avg_discount": round(_safe_float(p.get("avg_discount")), 2),
        "total_value": _safe_float(p.get("total_value"))
    } for p in products if p.get("_id") is not None]
    result = _sanitize_json_value(result)
    _cache_set("pricing:analysis", result)
    await _persist_cache_set("pricing:analysis", result)
    return result

@api_router.get("/pricing/discount-distribution")
async def get_discount_distribution():
    """Get discount distribution analysis"""
    cache_key = "pricing:discount_distribution"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    persisted = await _persist_cache_get(cache_key)
    if persisted is not None:
        _cache_set(cache_key, persisted)
        return persisted
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
    
    result = _sanitize_json_value(result)
    _cache_set(cache_key, result)
    await _persist_cache_set(cache_key, result)
    return result

# Risk & Governance Endpoints
@api_router.get("/risk/indicators")
async def get_risk_indicators():
    """Get risk governance indicators"""
    cached = _cache_get("risk:indicators")
    if cached is not None:
        return cached
    persisted = await _persist_cache_get("risk:indicators")
    if persisted is not None:
        _cache_set("risk:indicators", persisted)
        return persisted

    # Consolidate into a single aggregation to avoid multiple slow roundtrips.
    pipeline = [
        {
            "$facet": {
                "totals": [
                    {"$group": {"_id": None, "total_value": {"$sum": "$NET_SALES_VALUE"}, "total_lines": {"$sum": 1}}},
                ],
                "returns": [
        {"$match": {"NET_SALES_VALUE": {"$lt": 0}}},
                    {"$group": {"_id": None, "returns_value": {"$sum": "$NET_SALES_VALUE"}, "returns_count": {"$sum": 1}}},
                ],
                "gross": [
        {"$match": {"NET_SALES_VALUE": {"$gt": 0}}},
                    {"$group": {"_id": None, "gross": {"$sum": "$NET_SALES_VALUE"}}},
                ],
                "top_zone": [
        {"$group": {"_id": "$Zone_New", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}},
                    {"$limit": 1},
                ],
                "top_customers": [
        {"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}},
        {"$sort": {"value": -1}},
                    {"$limit": 10},
                ],
                "stop": [
        {"$match": {"STOP_BUSINESS": "Y"}},
        {"$group": {"_id": "$CUST_CODE"}},
                    {"$count": "count"},
                ],
            }
        }
    ]

    agg = await db.sales_data.aggregate(pipeline).to_list(1)
    data = agg[0] if agg else {}
    totals = (data.get("totals") or [{}])[0]
    returns_data = (data.get("returns") or [{}])[0]
    gross_row = (data.get("gross") or [{}])[0]
    top_zone = (data.get("top_zone") or [])
    top_customers = (data.get("top_customers") or [])
    stop_row = (data.get("stop") or [{}])[0]

    total_value = float(totals.get("total_value") or 0)
    total_lines = float(totals.get("total_lines") or 0)
    gross = float(gross_row.get("gross") or 0)
    top_zone_pct = (float(top_zone[0].get("value") or 0) / total_value * 100) if top_zone and total_value > 0 else 0
    top_10_cust_value = sum(float(c.get("value") or 0) for c in top_customers)
    top_10_cust_pct = (top_10_cust_value / total_value * 100) if total_value > 0 else 0
    stop_count = int(stop_row.get("count") or 0)

    returns_rate = (abs(float(returns_data.get("returns_value") or 0)) / gross * 100) if gross > 0 else 0
    negative_line_pct = (float(returns_data.get("returns_count") or 0) / total_lines * 100) if total_lines > 0 else 0
    
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
    
    _cache_set("risk:indicators", indicators)
    await _persist_cache_set("risk:indicators", indicators)
    return indicators

@api_router.get("/risk/returns-trend")
async def get_returns_trend():
    """Get monthly returns trend"""
    cached = _cache_get("risk:returns_trend")
    if cached is not None:
        return cached
    persisted = await _persist_cache_get("risk:returns_trend")
    if persisted is not None:
        _cache_set("risk:returns_trend", persisted)
        return persisted

    # Fast month extraction from DOC_DATE string: "YYYY-MM..."
    pipeline = [
        {"$addFields": {"_month": {"$substrBytes": ["$DOC_DATE", 0, 7]}}},
        {"$match": {"_month": {"$ne": None}}},
        {
            "$group": {
                "_id": "$_month",
                "gross_value": {
                    "$sum": {
                        "$cond": [{"$gt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]
                    }
                },
                "returns_value": {
                    "$sum": {
                        "$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]
                    }
                },
                "returns_qty": {
                    "$sum": {
                        "$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_QTY", 0]
                    }
                },
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "month": "$_id",
                "returns_value": {"$round": [{"$abs": "$returns_value"}, 2]},
                "returns_qty": {"$abs": "$returns_qty"},
                "returns_rate": {
                    "$round": [
                        {
                            "$cond": [
                                {"$gt": ["$gross_value", 0]},
                                {"$multiply": [{"$divide": [{"$abs": "$returns_value"}, "$gross_value"]}, 100]},
                                0,
                            ]
                        },
                        2,
                    ]
                },
            }
        },
    ]

    result = await db.sales_data.aggregate(pipeline).to_list(200)
    _cache_set("risk:returns_trend", result, ttl_seconds=600)
    await _persist_cache_set("risk:returns_trend", result)
    return result


@api_router.get("/risk/anomalies")
async def get_risk_anomalies():
    """Risk analytics KPIs:
    1) Zones with sudden high increase in recent months
    2) Products with unusually high recent sales vs history
    3) Promoted vs Non-promoted sales trend (monthly)
    4) Same product+zone price variance outliers
    5) Division+zone sales outliers (recent vs history)
    """
    cached = _cache_get("risk:anomalies")
    if cached is not None:
        return cached
    persisted = await _persist_cache_get("risk:anomalies")
    if persisted is not None:
        _cache_set("risk:anomalies", persisted, ttl_seconds=300)
        return persisted

    # Prefer fast month extraction from string (avoids $dateFromString on huge collections)
    # DOC_DATE is stored like "YYYY-MM-DD..." in the loaded dataset.
    add_month_str = {"$addFields": {"_month": {"$substrBytes": ["$DOC_DATE", 0, 7]}}}
    match_month = {"$match": {"_month": {"$ne": None}}}

    # Determine last 12 months from cached dashboard trends (fast path)
    trends_cached = _cache_get("dashboard:trends")
    if trends_cached is None:
        trends_cached = await get_monthly_trends()
    months = [t.get("month") for t in (trends_cached or []) if t.get("month")]
    months = months[-12:]
    recent_months = months[-3:] if len(months) >= 3 else months
    hist_months = months[:-3] if len(months) > 3 else []
    last_month = months[-1] if months else None
    prev_month = months[-2] if len(months) >= 2 else None

    # 1) Zone spikes: last vs previous month growth (do in Mongo)
    zone_spikes = []
    if last_month and prev_month:
        zone_spike_pipe = [
            add_month_str,
            match_month,
            {"$match": {"_month": {"$in": [prev_month, last_month]}}},
            {"$group": {
                "_id": "$Zone_New",
                "last_value": {"$sum": {"$cond": [{"$eq": ["$_month", last_month]}, "$NET_SALES_VALUE", 0]}},
                "prev_value": {"$sum": {"$cond": [{"$eq": ["$_month", prev_month]}, "$NET_SALES_VALUE", 0]}},
            }},
            {"$match": {"_id": {"$ne": None}, "prev_value": {"$gt": 0}}},
            {"$project": {
                "_id": 0,
                "zone": "$_id",
                "month": {"$literal": last_month},
                "value": {"$round": ["$last_value", 2]},
                "prev_value": {"$round": ["$prev_value", 2]},
                "growth_pct": {
                    "$round": [
                        {"$multiply": [{"$divide": [{"$subtract": ["$last_value", "$prev_value"]}, "$prev_value"]}, 100]},
                        2,
                    ]
                },
            }},
            {"$sort": {"growth_pct": -1}},
            {"$limit": 10},
        ]
        zone_spikes = await db.sales_data.aggregate(zone_spike_pipe).to_list(10)

    # 2) Product surge: avg recent 3 months vs avg history (do in Mongo)
    product_surges = []
    if months and recent_months and hist_months:
        prod_surge_pipe = [
            add_month_str,
            match_month,
            {"$match": {"_month": {"$in": months}}},
            {"$group": {
                "_id": "$Product",
                "recent_sum": {"$sum": {"$cond": [{"$in": ["$_month", recent_months]}, "$NET_SALES_VALUE", 0]}},
                "recent_n": {"$sum": {"$cond": [{"$in": ["$_month", recent_months]}, 1, 0]}},
                "hist_sum": {"$sum": {"$cond": [{"$in": ["$_month", hist_months]}, "$NET_SALES_VALUE", 0]}},
                "hist_n": {"$sum": {"$cond": [{"$in": ["$_month", hist_months]}, 1, 0]}},
            }},
            {"$match": {"_id": {"$ne": None}, "hist_sum": {"$gt": 0}, "hist_n": {"$gt": 0}, "recent_n": {"$gt": 0}}},
            {"$project": {
                "_id": 0,
                "product": "$_id",
                "recent_avg": {"$divide": ["$recent_sum", "$recent_n"]},
                "history_avg": {"$divide": ["$hist_sum", "$hist_n"]},
            }},
            {"$addFields": {
                "lift_pct": {"$multiply": [{"$divide": [{"$subtract": ["$recent_avg", "$history_avg"]}, "$history_avg"]}, 100]}
            }},
            {"$match": {"lift_pct": {"$gt": 0}}},
            {"$project": {
                "product": 1,
                "recent_avg": {"$round": ["$recent_avg", 2]},
                "history_avg": {"$round": ["$history_avg", 2]},
                "lift_pct": {"$round": ["$lift_pct", 2]},
            }},
            {"$sort": {"lift_pct": -1}},
            {"$limit": 10},
        ]
        product_surges = await db.sales_data.aggregate(prod_surge_pipe).to_list(10)

    # 3) Promoted vs non-promoted trend
    promo_trend = []
    if months:
        promo_pipe = [
            add_month_str,
            match_month,
            {"$match": {"_month": {"$in": months}}},
            {"$group": {"_id": {"month": "$_month", "promo": "$Promoted/non promoted"}, "value": {"$sum": "$NET_SALES_VALUE"}}},
        ]
        pr = await db.sales_data.aggregate(promo_pipe).to_list(5000)
        by_month = {m: {"month": m, "promoted": 0.0, "non_promoted": 0.0} for m in months}
        for r in pr:
            rid = r.get("_id") or {}
            month = rid.get("month") if isinstance(rid, dict) else None
            promo = rid.get("promo") if isinstance(rid, dict) else None
            if not month or month not in by_month:
                continue
            val = float(r.get("value") or 0)
            promo_str = (str(promo).strip().lower() if promo is not None else "")
            if "non" in promo_str:
                by_month[month]["non_promoted"] += val
            elif promo_str:
                by_month[month]["promoted"] += val
        promo_trend = [by_month[m] for m in months]

    # 4) Price variance outliers: product+zone min/max/avg PPU ratio
    price_outliers = []
    price_months = months[-6:] if len(months) >= 6 else months
    price_pipe = [
        add_month_str,
        match_month,
        {"$match": {"_month": {"$in": price_months}, "PPU": {"$ne": None}}},
        {"$group": {
            "_id": {"product": "$Product", "zone": "$Zone_New"},
            "avg_ppu": {"$avg": "$PPU"},
            "min_ppu": {"$min": "$PPU"},
            "max_ppu": {"$max": "$PPU"},
            "count": {"$sum": 1},
        }},
        {"$match": {"count": {"$gte": 10}, "avg_ppu": {"$gt": 0}, "_id.product": {"$ne": None}, "_id.zone": {"$ne": None}}},
    ]
    po = await db.sales_data.aggregate(price_pipe).to_list(50000)
    for r in po:
        rid = r.get("_id") or {}
        prod = rid.get("product") if isinstance(rid, dict) else None
        zone = rid.get("zone") if isinstance(rid, dict) else None
        if prod is None or zone is None:
            continue
        avg_ppu = float(r.get("avg_ppu") or 0)
        min_ppu = float(r.get("min_ppu") or 0)
        max_ppu = float(r.get("max_ppu") or 0)
        variance_pct = ((max_ppu - min_ppu) / avg_ppu * 100) if avg_ppu > 0 else 0
        if variance_pct >= 20:  # configurable threshold for “outlier”
            price_outliers.append({
                "product": prod,
                "zone": zone,
                "avg_ppu": round(avg_ppu, 2),
                "min_ppu": round(min_ppu, 2),
                "max_ppu": round(max_ppu, 2),
                "variance_pct": round(variance_pct, 2),
                "lines": int(r.get("count") or 0),
            })
    price_outliers.sort(key=lambda x: x["variance_pct"], reverse=True)
    price_outliers = price_outliers[:15]

    # 5) Division+zone outliers: last month vs avg of prior 6 months (do in Mongo)
    div_zone_outliers = []
    if last_month and len(months) >= 7:
        prior = months[-7:-1]  # 6 months before last
        dz_base_pipe = [
            add_month_str,
            match_month,
            {"$match": {"_month": {"$in": prior + [last_month]}}},
            {"$group": {
                "_id": {"division": "$Div_Code (Mapping HQ)", "zone": "$Zone_New"},
                "last_value": {"$sum": {"$cond": [{"$eq": ["$_month", last_month]}, "$NET_SALES_VALUE", 0]}},
                "prior_sum": {"$sum": {"$cond": [{"$in": ["$_month", prior]}, "$NET_SALES_VALUE", 0]}},
                "prior_n": {"$sum": {"$cond": [{"$in": ["$_month", prior]}, 1, 0]}},
            }},
            {"$match": {"_id.division": {"$ne": None}, "_id.zone": {"$ne": None}, "prior_sum": {"$gt": 0}, "prior_n": {"$gt": 0}}},
            {"$project": {
                "_id": 0,
                "division": "$_id.division",
                "zone": "$_id.zone",
                "month": {"$literal": last_month},
                "value": {"$round": ["$last_value", 2]},
                "prior_avg": {"$divide": ["$prior_sum", "$prior_n"]},
            }},
            {"$addFields": {
                "lift_pct": {
                    "$multiply": [{"$divide": [{"$subtract": ["$value", "$prior_avg"]}, "$prior_avg"]}, 100]
                }
            }},
            {"$project": {
                "division": 1,
                "zone": 1,
                "month": 1,
                "value": 1,
                "prior_avg": {"$round": ["$prior_avg", 2]},
                "lift_pct": {"$round": ["$lift_pct", 2]},
            }},
        ]
        # Fetch both positive and negative outliers
        dz_pos_pipe = dz_base_pipe + [{"$match": {"lift_pct": {"$gte": 50}}}]
        dz_neg_pipe = dz_base_pipe + [{"$match": {"lift_pct": {"$lte": -50}}}]
        neg = await db.sales_data.aggregate(dz_neg_pipe).to_list(200)
        pos = await db.sales_data.aggregate(dz_pos_pipe).to_list(200)
        combined = (pos or []) + (neg or [])
        combined.sort(key=lambda x: abs(float(x.get("lift_pct") or 0)), reverse=True)
        div_zone_outliers = combined[:15]

    payload = {
        "months": months,
        "zone_spikes": zone_spikes,
        "product_surges": product_surges,
        "promo_trend": promo_trend,
        "price_outliers": price_outliers,
        "division_zone_outliers": div_zone_outliers,
    }
    _cache_set("risk:anomalies", payload, ttl_seconds=300)
    await _persist_cache_set("risk:anomalies", payload)
    return payload

# AI Insights - build data summary from real dashboard data
async def _get_insights_data_summary(dashboard: Optional[str] = None) -> Dict[str, Any]:
    """Fetch real KPIs and build a summary for insights generation. If dashboard is set, also fetch dashboard-specific data."""
    summary = {"overview": None, "trends": [], "concentration": None, "risk": None}
    try:
        ov_cached = _cache_get("dashboard:overview")
        if ov_cached is None:
            ov_cached = (await get_dashboard_overview()).model_dump()
        summary["overview"] = {
            "net_sales_value": round(float(ov_cached.get("net_sales_value") or 0), 2),
            "gross_sales_value": round(float(ov_cached.get("gross_sales_value") or 0), 2),
            "returns_value": round(float(ov_cached.get("returns_value") or 0), 2),
            "returns_rate_pct": round(float(ov_cached.get("returns_rate") or 0), 2),
            "total_transactions": int(ov_cached.get("total_transactions") or 0),
            "total_customers": int(ov_cached.get("total_customers") or 0),
            "total_products": int(ov_cached.get("total_products") or 0),
            "net_sales_qty": int(ov_cached.get("net_sales_qty") or 0),
        }

        trends_cached = _cache_get("dashboard:trends")
        if trends_cached is None:
            trends_cached = await get_monthly_trends()
        summary["trends"] = [
            {
                "month": t.get("month"),
                "value": round(float(t.get("value") or 0), 2),
                "growth_pct": t.get("growth_pct"),
            }
            for t in trends_cached
        ]

        conc_cached = _cache_get("dashboard:concentration")
        if conc_cached is None:
            conc_cached = await get_concentration_metrics()
        summary["concentration"] = {
            "zones": conc_cached.get("zones", [])[:6],
            "top_states": conc_cached.get("top_states", []),
            "top_3_states_pct": round(float(conc_cached.get("top_3_states_pct") or 0), 2),
            "top_10_customers_pct": round(float(conc_cached.get("top_10_customers_pct") or 0), 2),
        }

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
            try:
                pricing_pipe = [{"$group": {"_id": None, "avg_discount": {"$avg": "$Discount %"}, "avg_ppu": {"$avg": "$PPU"}, "total_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$limit": 1}]
                pr = await db.sales_data.aggregate(pricing_pipe).to_list(1)
                dist_pipe = [{"$bucket": {"groupBy": "$Discount %", "boundaries": [0, 0.01, 5, 10, 20, 50, 100], "default": "Other", "output": {"count": {"$sum": 1}, "value": {"$sum": "$NET_SALES_VALUE"}}}}]
                dist = await db.sales_data.aggregate(dist_pipe).to_list(10)
                avg_discount = round(_safe_float((pr[0] or {}).get("avg_discount") if pr else 0), 2)
                top_disc = await db.sales_data.aggregate([
                    {"$group": {"_id": "$Product", "avg_discount": {"$avg": "$Discount %"}, "total_value": {"$sum": "$NET_SALES_VALUE"}}},
                    {"$match": {"avg_discount": {"$ne": None}}},
                    {"$sort": {"avg_discount": -1}},
                    {"$limit": 5},
                ]).to_list(5)
                summary["pricing"] = {
                    "avg_discount": avg_discount,
                    "discount_buckets": _sanitize_json_value(dist),
                    "top_discount_products": [
                        {"product": x.get("_id"), "avg_discount": round(_safe_float(x.get("avg_discount")), 2), "total_value": round(_safe_float(x.get("total_value")), 2)}
                        for x in top_disc if x.get("_id") is not None
                    ],
                }
            except Exception as pe:
                logger.warning(f"Pricing insights summary aggregation failed: {pe}")
                summary["pricing"] = {"avg_discount": 0.0, "discount_buckets": [], "top_discount_products": []}
        elif dashboard == "Incentive Analytics":
            try:
                inc_ov = await incentives_overview()
                inc_tr = await incentives_trend()
                inc_anom = await incentives_anomalies(limit=15)
                tr_list = inc_tr if isinstance(inc_tr, list) else []
                an_list = inc_anom if isinstance(inc_anom, list) else []
                summary["incentives"] = {
                    **(inc_ov if isinstance(inc_ov, dict) else {}),
                    "recent_cycles": tr_list[-10:] if tr_list else [],
                    "anomaly_samples": an_list[:12],
                }
            except Exception as ie:
                logger.warning(f"Incentive insights summary failed: {ie}")
                summary["incentives"] = {"employees_total": 0, "recent_cycles": [], "anomaly_samples": []}
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

            # Add anomalies/outliers summary for AI Insights (fast path via cache; fallback to short timeout fetch).
            anomalies = _cache_get("risk:anomalies")
            if anomalies is None:
                try:
                    anomalies = await asyncio.wait_for(get_risk_anomalies(), timeout=2.5)
                except Exception:
                    anomalies = None
            if isinstance(anomalies, dict):
                zone_spikes = anomalies.get("zone_spikes") or []
                product_surges = anomalies.get("product_surges") or []
                price_outliers = anomalies.get("price_outliers") or []
                div_zone = anomalies.get("division_zone_outliers") or []
                promo_trend = anomalies.get("promo_trend") or []

                def _safe0(lst):
                    return lst[0] if isinstance(lst, list) and lst else None

                last_promo = promo_trend[-1] if isinstance(promo_trend, list) and promo_trend else None
                promoted = float(last_promo.get("promoted") or 0) if isinstance(last_promo, dict) else 0
                non_promoted = float(last_promo.get("non_promoted") or 0) if isinstance(last_promo, dict) else 0
                promo_share = round((promoted / (promoted + non_promoted) * 100), 2) if (promoted + non_promoted) > 0 else 0

                summary["risk_anomalies"] = {
                    "zone_spikes_count": len(zone_spikes),
                    "top_zone_spike": _safe0(zone_spikes),
                    "product_surges_count": len(product_surges),
                    "top_product_surge": _safe0(product_surges),
                    "price_outliers_count": len(price_outliers),
                    "top_price_outlier": _safe0(price_outliers),
                    "division_zone_outliers_count": len(div_zone),
                    "top_division_zone_outlier": _safe0(div_zone),
                    "promo_share_pct_latest": promo_share,
                }
    except Exception as e:
        logger.error(f"Error building insights data summary: {e}")
    return summary


async def _get_chat_data_summary() -> Dict[str, Any]:
    """Fetch full data context for chatbot: overview, trends, concentration, products, top customers, quarterly, brand growth, zones (for underperforming)."""
    summary = {"overview": None, "trends": [], "concentration": None}
    try:
        summary = await _get_insights_data_summary(None)
        # Include full anomalies payload for fast Risk & Governance Q&A
        try:
            summary["risk_anomalies_payload"] = await get_risk_anomalies()
        except Exception:
            summary["risk_anomalies_payload"] = None
        # Products (top 15 by revenue)
        prod_pipe = [{"$group": {"_id": "$Product", "sales_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"sales_value": -1}}, {"$limit": 15}]
        products = await db.sales_data.aggregate(prod_pipe).to_list(15)
        total_sales = sum(p["sales_value"] for p in products)
        if total_sales:
            summary["products"] = [{"name": p["_id"], "value": p["sales_value"], "pct": round(p["sales_value"] / total_sales * 100, 2)} for p in products]
        else:
            summary["products"] = []
        # Top 10 customers by revenue (key account analysis)
        cust_pipe = [{"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}, {"$limit": 10}]
        top_cust = await db.sales_data.aggregate(cust_pipe).to_list(10)
        summary["top_customers"] = [{"customer": c["_id"], "value": round(c["value"], 2)} for c in top_cust]
        # Quarterly totals from monthly trends (Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec)
        trends = summary.get("trends") or []
        q_sums = {"Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0}
        for t in trends:
            month = t.get("month") or t.get("_id") or ""
            if len(month) >= 7:
                m = int(month.split("-")[1])
                if 1 <= m <= 3:
                    q_sums["Q1"] += t.get("value", 0)
                elif 4 <= m <= 6:
                    q_sums["Q2"] += t.get("value", 0)
                elif 7 <= m <= 9:
                    q_sums["Q3"] += t.get("value", 0)
                elif 10 <= m <= 12:
                    q_sums["Q4"] += t.get("value", 0)
        summary["quarterly"] = q_sums
        # Brand-level growth: sales by brand by month, then growth (last month vs previous month)
        brand_month_pipe = [
            {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
        {"$match": {"doc_date_parsed": {"$ne": None}}},
            {"$group": {"_id": {"brand": "$ITEM_BRAND KPMG", "month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            {"$sort": {"_id.month": -1}},
        ]
        brand_months = await db.sales_data.aggregate(brand_month_pipe).to_list(500)
        by_brand = {}
        for row in brand_months:
            bid = row["_id"]
            brand_name = (bid.get("brand") or "Unknown") if isinstance(bid, dict) else "Unknown"
            month = bid.get("month") if isinstance(bid, dict) else None
            if brand_name not in by_brand:
                by_brand[brand_name] = []
            by_brand[brand_name].append({"month": month, "value": row["value"]})
        brand_growth = []
        for brand_name, months in by_brand.items():
            months_sorted = sorted([m for m in months if m.get("month")], key=lambda x: x["month"], reverse=True)
            if len(months_sorted) >= 2:
                last_val = months_sorted[0]["value"]
                prev_val = months_sorted[1]["value"]
                growth = round((last_val - prev_val) / prev_val * 100, 2) if prev_val and prev_val != 0 else None
                if growth is not None:
                    brand_growth.append({"brand": brand_name, "growth_pct": growth, "last_value": last_val})
        brand_growth.sort(key=lambda x: x["growth_pct"], reverse=True)
        summary["brand_growth"] = brand_growth[:10]
        # Zones already in concentration; ensure we have full list for underperforming (lowest %)
        conc = summary.get("concentration") or {}
        zones = conc.get("zones") or []
        summary["zones_sorted_low"] = sorted(zones, key=lambda z: z.get("pct", 0)) if zones else []
        # Product return rate (highest return rate = product quality risk)
        ret_pipe = [
            {"$group": {"_id": "$Product", "gross": {"$sum": {"$cond": [{"$gt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}}, "returns_abs": {"$sum": {"$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, {"$abs": "$NET_SALES_VALUE"}, 0]}}}},
            {"$match": {"gross": {"$gt": 0}}},
            {"$addFields": {"return_rate_pct": {"$multiply": [{"$divide": ["$returns_abs", "$gross"]}, 100]}}},
            {"$sort": {"return_rate_pct": -1}},
            {"$limit": 10},
        ]
        ret_list = await db.sales_data.aggregate(ret_pipe).to_list(10)
        summary["product_return_rates"] = [{"name": r["_id"], "return_rate_pct": round(r["return_rate_pct"], 2), "returns_abs": round(r["returns_abs"], 2)} for r in ret_list]
        # Product MoM growth (growth SKUs)
        prod_month_pipe = [
            {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
            {"$match": {"doc_date_parsed": {"$ne": None}}},
            {"$group": {"_id": {"product": "$Product", "month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            {"$sort": {"_id.month": -1}},
        ]
        prod_months = await db.sales_data.aggregate(prod_month_pipe).to_list(1000)
        by_product = {}
        for row in prod_months:
            pid = row["_id"]
            pname = (pid.get("product") or "Unknown") if isinstance(pid, dict) else "Unknown"
            month = pid.get("month") if isinstance(pid, dict) else None
            if pname not in by_product:
                by_product[pname] = []
            by_product[pname].append({"month": month, "value": row["value"]})
        product_growth = []
        for pname, months in by_product.items():
            months_sorted = sorted([m for m in months if m.get("month")], key=lambda x: x["month"], reverse=True)
            if len(months_sorted) >= 2:
                last_val = months_sorted[0]["value"]
                prev_val = months_sorted[1]["value"]
                growth = round((last_val - prev_val) / prev_val * 100, 2) if prev_val and prev_val != 0 else None
                if growth is not None:
                    product_growth.append({"product": pname, "growth_pct": growth, "last_value": last_val})
        product_growth.sort(key=lambda x: x["growth_pct"], reverse=True)
        summary["product_growth_skus"] = product_growth[:10]
        # Average selling price per product (PPU)
        price_pipe = [{"$group": {"_id": "$Product", "avg_ppu": {"$avg": "$PPU"}, "total_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$match": {"avg_ppu": {"$ne": None, "$gt": 0}}}, {"$sort": {"total_value": -1}}, {"$limit": 15}]
        price_list = await db.sales_data.aggregate(price_pipe).to_list(15)
        summary["product_avg_price"] = [{"name": p["_id"], "avg_ppu": round(p["avg_ppu"], 2), "total_value": round(p["total_value"], 2)} for p in price_list]
        # Product penetration (distinct customers per product - most widely distributed)
        pen_pipe = [{"$group": {"_id": "$Product", "customer_count": {"$addToSet": "$CUST_CODE"}}}, {"$addFields": {"count": {"$size": "$customer_count"}}}, {"$sort": {"count": -1}}, {"$limit": 10}]
        pen_list = await db.sales_data.aggregate(pen_pipe).to_list(10)
        summary["product_penetration"] = [{"name": p["_id"], "customer_count": p["count"]} for p in pen_list]
        # Product geographic concentration (products dependent on few states = highest top-state %)
        state_by_prod = await db.sales_data.aggregate([{"$group": {"_id": {"product": "$Product", "state": "$State"}, "value": {"$sum": "$NET_SALES_VALUE"}}}]).to_list(2000)
        prod_state_totals = {}
        prod_state_values = {}
        for row in state_by_prod:
            pid = row["_id"]
            pname = pid.get("product") if isinstance(pid, dict) else None
            state = pid.get("state") if isinstance(pid, dict) else None
            if not pname:
                continue
            prod_state_totals[pname] = prod_state_totals.get(pname, 0) + row["value"]
            if pname not in prod_state_values:
                prod_state_values[pname] = []
            prod_state_values[pname].append({"state": state, "value": row["value"]})
        product_geo = []
        for pname, total in prod_state_totals.items():
            vals = prod_state_values.get(pname) or []
            if not total or total <= 0:
                continue
            vals_sorted = sorted(vals, key=lambda x: x["value"], reverse=True)
            top_pct = round(vals_sorted[0]["value"] / total * 100, 2) if vals_sorted else 0
            state_count = len(vals)
            product_geo.append({"product": pname, "top_state_pct": top_pct, "state_count": state_count})
        product_geo.sort(key=lambda x: x["top_state_pct"], reverse=True)
        summary["product_geo_concentration"] = product_geo[:10]
        # State return rates (supply chain issues)
        state_ret_pipe = [
            {"$group": {"_id": "$State", "gross": {"$sum": {"$cond": [{"$gt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}}, "returns_abs": {"$sum": {"$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, {"$abs": "$NET_SALES_VALUE"}, 0]}}}},
            {"$match": {"gross": {"$gt": 0}, "_id": {"$ne": None}}},
            {"$addFields": {"return_rate_pct": {"$multiply": [{"$divide": ["$returns_abs", "$gross"]}, 100]}}},
            {"$sort": {"return_rate_pct": -1}},
            {"$limit": 10},
        ]
        state_ret_list = await db.sales_data.aggregate(state_ret_pipe).to_list(10)
        summary["state_return_rates"] = [{"name": r["_id"], "return_rate_pct": round(r["return_rate_pct"], 2), "returns_abs": round(r["returns_abs"], 2)} for r in state_ret_list]
        # City-level MoM growth (expansion opportunity)
        city_month_pipe = [
            {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
            {"$match": {"doc_date_parsed": {"$ne": None}}},
            {"$group": {"_id": {"city": "$CITY", "month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            {"$sort": {"_id.month": -1}},
        ]
        city_months = await db.sales_data.aggregate(city_month_pipe).to_list(800)
        by_city = {}
        for row in city_months:
            cid = row["_id"]
            city_name = (cid.get("city") or "Unknown") if isinstance(cid, dict) else "Unknown"
            month = cid.get("month") if isinstance(cid, dict) else None
            if city_name not in by_city:
                by_city[city_name] = []
            by_city[city_name].append({"month": month, "value": row["value"]})
        city_growth = []
        for city_name, months in by_city.items():
            months_sorted = sorted([m for m in months if m.get("month")], key=lambda x: x["month"], reverse=True)
            if len(months_sorted) >= 2:
                last_val = months_sorted[0]["value"]
                prev_val = months_sorted[1]["value"]
                growth = round((last_val - prev_val) / prev_val * 100, 2) if prev_val and prev_val != 0 else None
                if growth is not None:
                    city_growth.append({"city": city_name, "growth_pct": growth, "last_value": last_val})
        city_growth.sort(key=lambda x: x["growth_pct"], reverse=True)
        summary["city_growth"] = city_growth[:10]
        # Zone-level MoM growth (declining = territory performance)
        zone_month_pipe = [
            {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
            {"$match": {"doc_date_parsed": {"$ne": None}}},
            {"$group": {"_id": {"zone": "$Zone_New", "month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            {"$sort": {"_id.month": -1}},
        ]
        zone_months = await db.sales_data.aggregate(zone_month_pipe).to_list(200)
        by_zone = {}
        for row in zone_months:
            zid = row["_id"]
            zone_name = (zid.get("zone") or "Unknown") if isinstance(zid, dict) else "Unknown"
            month = zid.get("month") if isinstance(zid, dict) else None
            if zone_name not in by_zone:
                by_zone[zone_name] = []
            by_zone[zone_name].append({"month": month, "value": row["value"]})
        zone_growth_list = []
        for zone_name, months in by_zone.items():
            months_sorted = sorted([m for m in months if m.get("month")], key=lambda x: x["month"], reverse=True)
            if len(months_sorted) >= 2:
                last_val = months_sorted[0]["value"]
                prev_val = months_sorted[1]["value"]
                growth = round((last_val - prev_val) / prev_val * 100, 2) if prev_val and prev_val != 0 else None
                if growth is not None:
                    zone_growth_list.append({"zone": zone_name, "growth_pct": growth, "last_value": last_val})
        summary["zone_growth"] = zone_growth_list
        # Revenue per customer by state (market strength)
        state_cust_pipe = [
            {"$group": {"_id": "$State", "total_value": {"$sum": "$NET_SALES_VALUE"}, "customers": {"$addToSet": "$CUST_CODE"}}},
            {"$match": {"_id": {"$ne": None}}},
            {"$addFields": {"customer_count": {"$size": "$customers"}}},
            {"$addFields": {"revenue_per_customer": {"$cond": [{"$gt": ["$customer_count", 0]}, {"$divide": ["$total_value", "$customer_count"]}, 0]}}},
            {"$sort": {"total_value": -1}},
            {"$limit": 15},
        ]
        state_cust_list = await db.sales_data.aggregate(state_cust_pipe).to_list(15)
        summary["state_revenue_per_customer"] = [{"name": s["_id"], "revenue_per_customer": round(s["revenue_per_customer"], 2), "total_value": round(s["total_value"], 2), "customer_count": s["customer_count"]} for s in state_cust_list]
        # Stop business customers (customer attrition)
        stop_pipe = [{"$match": {"STOP_BUSINESS": "Y"}}, {"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}, {"$limit": 20}]
        stop_list = await db.sales_data.aggregate(stop_pipe).to_list(20)
        summary["stop_business_customers"] = [{"customer": s["_id"], "value": round(s["value"], 2)} for s in stop_list]
        # Customer return rates (distributor risk)
        cust_ret_pipe = [
            {"$group": {"_id": "$CUST_CODE", "gross": {"$sum": {"$cond": [{"$gt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}}, "returns_abs": {"$sum": {"$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, {"$abs": "$NET_SALES_VALUE"}, 0]}}}},
            {"$match": {"gross": {"$gt": 0}, "_id": {"$ne": None}}},
            {"$addFields": {"return_rate_pct": {"$multiply": [{"$divide": ["$returns_abs", "$gross"]}, 100]}}},
            {"$sort": {"return_rate_pct": -1}},
            {"$limit": 10},
        ]
        cust_ret_list = await db.sales_data.aggregate(cust_ret_pipe).to_list(10)
        summary["customer_return_rates"] = [{"name": r["_id"], "return_rate_pct": round(r["return_rate_pct"], 2), "returns_abs": round(r["returns_abs"], 2)} for r in cust_ret_list]
        # Customers who buy the most products (cross-selling opportunity)
        cust_prod_pipe = [{"$group": {"_id": "$CUST_CODE", "products": {"$addToSet": "$Product"}}}, {"$addFields": {"product_count": {"$size": "$products"}}}, {"$sort": {"product_count": -1}}, {"$limit": 10}]
        cust_prod_list = await db.sales_data.aggregate(cust_prod_pipe).to_list(10)
        summary["customer_product_count"] = [{"customer": p["_id"], "product_count": p["product_count"]} for p in cust_prod_list]
        # Pareto 80%: customers that contribute 80% of revenue
        all_cust = await db.sales_data.aggregate([{"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}]).to_list(5000)
        total_rev = sum(c["value"] for c in all_cust)
        cum = 0
        pareto_80 = []
        for c in all_cust:
            cum += c["value"]
            pareto_80.append({"customer": c["_id"], "value": round(c["value"], 2)})
            if total_rev and total_rev > 0 and cum >= 0.8 * total_rev:
                break
        summary["pareto_80_customers"] = pareto_80
        summary["pareto_80_count"] = len(pareto_80)
        summary["pareto_80_pct"] = round(cum / total_rev * 100, 2) if total_rev else 0
        # Customer MoM growth (high-value accounts)
        cust_month_pipe = [
            {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
            {"$match": {"doc_date_parsed": {"$ne": None}}},
            {"$group": {"_id": {"customer": "$CUST_CODE", "month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            {"$sort": {"_id.month": -1}},
        ]
        cust_months = await db.sales_data.aggregate(cust_month_pipe).to_list(2000)
        by_cust = {}
        for row in cust_months:
            cid = row["_id"]
            cust_code = (cid.get("customer") or "Unknown") if isinstance(cid, dict) else "Unknown"
            month = cid.get("month") if isinstance(cid, dict) else None
            if cust_code not in by_cust:
                by_cust[cust_code] = []
            by_cust[cust_code].append({"month": month, "value": row["value"]})
        customer_growth = []
        for cust_code, months in by_cust.items():
            months_sorted = sorted([m for m in months if m.get("month")], key=lambda x: x["month"], reverse=True)
            if len(months_sorted) >= 2:
                last_val = months_sorted[0]["value"]
                prev_val = months_sorted[1]["value"]
                growth = round((last_val - prev_val) / prev_val * 100, 2) if prev_val and prev_val != 0 else None
                if growth is not None:
                    customer_growth.append({"customer": cust_code, "growth_pct": growth, "last_value": last_val})
        customer_growth.sort(key=lambda x: x["growth_pct"], reverse=True)
        summary["customer_growth"] = customer_growth[:10]
        # Average discount across products (pricing control)
        avg_disc_pipe = [{"$group": {"_id": None, "avg_discount": {"$avg": "$Discount %"}}}, {"$limit": 1}]
        avg_disc_r = await db.sales_data.aggregate(avg_disc_pipe).to_list(1)
        summary["avg_discount_overall"] = round(avg_disc_r[0]["avg_discount"], 2) if avg_disc_r and avg_disc_r[0].get("avg_discount") is not None else None
        # Products with highest discounts (margin leakage)
        prod_disc_pipe = [{"$group": {"_id": "$Product", "avg_discount": {"$avg": "$Discount %"}, "total_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$match": {"avg_discount": {"$ne": None}}}, {"$sort": {"avg_discount": -1}}, {"$limit": 10}]
        prod_disc_list = await db.sales_data.aggregate(prod_disc_pipe).to_list(10)
        summary["product_highest_discount"] = [{"name": p["_id"], "avg_discount": round(p["avg_discount"], 2), "total_value": round(p["total_value"], 2)} for p in prod_disc_list]
        # Zones with lowest price realization (pricing discipline). Price realization = PPU / Price list.
        zone_pr_pipe = [
            {"$group": {"_id": "$Zone_New", "avg_ppu": {"$avg": "$PPU"}, "avg_rate": {"$avg": "$Rate (GPTS_PriceList)"}, "total_value": {"$sum": "$NET_SALES_VALUE"}}},
            {"$match": {"_id": {"$ne": None}, "avg_rate": {"$gt": 0}}},
            {"$addFields": {"price_realization_pct": {"$multiply": [{"$divide": ["$avg_ppu", "$avg_rate"]}, 100]}}},
            {"$sort": {"price_realization_pct": 1}},
            {"$limit": 10},
        ]
        zone_pr_list = await db.sales_data.aggregate(zone_pr_pipe).to_list(10)
        summary["zone_price_realization"] = [{"name": z["_id"], "price_realization_pct": round(z["price_realization_pct"], 2), "avg_ppu": round(z["avg_ppu"], 2)} for z in zone_pr_list]
        # Overall price realization vs price list (revenue leakage)
        overall_pr_pipe = [{"$group": {"_id": None, "avg_ppu": {"$avg": "$PPU"}, "avg_rate": {"$avg": "$Rate (GPTS_PriceList)"}, "total_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$limit": 1}]
        overall_pr_r = await db.sales_data.aggregate(overall_pr_pipe).to_list(1)
        if overall_pr_r and overall_pr_r[0].get("avg_rate") and overall_pr_r[0]["avg_rate"] > 0:
            d = overall_pr_r[0]
            realization_pct = round(d["avg_ppu"] / d["avg_rate"] * 100, 2)
            summary["overall_price_realization"] = {"avg_ppu": round(d["avg_ppu"], 2), "avg_list_rate": round(d["avg_rate"], 2), "realization_pct": realization_pct}
        else:
            summary["overall_price_realization"] = None
        # Customers with highest discounts (governance)
        cust_disc_pipe = [{"$group": {"_id": "$CUST_CODE", "avg_discount": {"$avg": "$Discount %"}, "total_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$match": {"avg_discount": {"$ne": None}}}, {"$sort": {"avg_discount": -1}}, {"$limit": 10}]
        cust_disc_list = await db.sales_data.aggregate(cust_disc_pipe).to_list(10)
        summary["customer_highest_discount"] = [{"customer": c["_id"], "avg_discount": round(c["avg_discount"], 2), "total_value": round(c["total_value"], 2)} for c in cust_disc_list]
        # Promotion: % sales from promoted (marketing impact), promoted vs non (effectiveness), top promoted products (campaign ROI), zones (regional strategy)
        promo_pipe = [{"$group": {"_id": "$Promoted/non promoted", "sales_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$match": {"_id": {"$ne": None}}}]
        promo_list = await db.sales_data.aggregate(promo_pipe).to_list(10)
        total_promo_sales = sum(p["sales_value"] for p in promo_list)
        promoted_value = 0
        non_promoted_value = 0
        for p in promo_list:
            pid = (p["_id"] or "").strip().lower()
            if "promoted" in pid and "non" not in pid:
                promoted_value = p["sales_value"]
            else:
                non_promoted_value += p["sales_value"]
        summary["promoted_sales_pct"] = round(promoted_value / total_promo_sales * 100, 2) if total_promo_sales and total_promo_sales > 0 else 0
        summary["promoted_vs_non"] = {"promoted_value": round(promoted_value, 2), "non_promoted_value": round(non_promoted_value, 2), "total": round(total_promo_sales, 2)}
        # Top promoted products (match promoted type; field value may vary)
        promo_match_val = None
        for p in promo_list:
            if (p["_id"] or "").strip().lower().startswith("promoted") and "non" not in (p["_id"] or "").strip().lower():
                promo_match_val = p["_id"]
                break
        if promo_match_val is not None:
            top_promoted_pipe = [{"$match": {"Promoted/non promoted": promo_match_val}}, {"$group": {"_id": "$Product", "sales_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"sales_value": -1}}, {"$limit": 10}]
            top_promoted = await db.sales_data.aggregate(top_promoted_pipe).to_list(10)
            summary["top_promoted_products"] = [{"name": p["_id"], "sales_value": round(p["sales_value"], 2)} for p in top_promoted]
        else:
            summary["top_promoted_products"] = []
        # By zone: % or value from promoted (regions that respond best)
        zone_promo_pipe = [{"$group": {"_id": {"zone": "$Zone_New", "promo": "$Promoted/non promoted"}, "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$match": {"_id.zone": {"$ne": None}}}]
        zone_promo_raw = await db.sales_data.aggregate(zone_promo_pipe).to_list(100)
        zone_totals = {}
        zone_promoted = {}
        for row in zone_promo_raw:
            zid = row["_id"]
            zname = zid.get("zone") if isinstance(zid, dict) else None
            promo_type = (zid.get("promo") or "").strip().lower() if isinstance(zid, dict) else ""
            if not zname:
                continue
            zone_totals[zname] = zone_totals.get(zname, 0) + row["value"]
            if "promoted" in promo_type and "non" not in promo_type:
                zone_promoted[zname] = zone_promoted.get(zname, 0) + row["value"]
        region_promo = [{"name": z, "promoted_pct": round(zone_promoted.get(z, 0) / zone_totals[z] * 100, 2) if zone_totals.get(z) else 0, "promoted_value": round(zone_promoted.get(z, 0), 2)} for z in zone_totals]
        region_promo.sort(key=lambda x: x["promoted_pct"], reverse=True)
        summary["zone_promotion_response"] = region_promo[:10]
        # Risk / early warning: customers >10% revenue (dependency risk), high-discount transactions (fraud/policy), zones declining (early warning)
        all_cust_rev = await db.sales_data.aggregate([{"$group": {"_id": "$CUST_CODE", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"value": -1}}]).to_list(5000)
        total_rev_cust = sum(c["value"] for c in all_cust_rev)
        summary["high_dependency_customers"] = [{"customer": c["_id"], "value": round(c["value"], 2), "pct": round(c["value"] / total_rev_cust * 100, 2)} for c in all_cust_rev if total_rev_cust and (c["value"] / total_rev_cust * 100) > 10]
        # Transactions with unusually high discounts (e.g. avg discount > 50% or top by discount)
        tran_disc_pipe = [{"$group": {"_id": "$TRAN_ID", "avg_discount": {"$avg": "$Discount %"}, "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$match": {"avg_discount": {"$gt": 30}}}, {"$sort": {"avg_discount": -1}}, {"$limit": 15}]
        tran_disc_list = await db.sales_data.aggregate(tran_disc_pipe).to_list(15)
        summary["high_discount_transactions"] = [{"tran_id": t["_id"], "avg_discount": round(t["avg_discount"], 2), "value": round(t["value"], 2)} for t in tran_disc_list]
        # Zones with declining trends (negative MoM growth)
        zone_growth_list = summary.get("zone_growth") or []
        summary["zones_declining"] = [z for z in zone_growth_list if z.get("growth_pct") is not None and z["growth_pct"] < 0]
        # Sales productivity / demand pattern / order size / market activity
        ov = summary.get("overview") or {}
        total_txn = ov.get("total_transactions") or 0
        net_sales_val = ov.get("net_sales_value") or 0
        net_qty = ov.get("net_sales_qty") or 0
        summary["avg_transaction_value"] = round(net_sales_val / total_txn, 2) if total_txn else None
        summary["avg_qty_per_transaction"] = round(net_qty / total_txn, 2) if total_txn else None
        # Transactions per month (distinct TRAN_ID)
        txn_month_pipe = [
            {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
            {"$match": {"doc_date_parsed": {"$ne": None}}},
            {"$group": {"_id": {"month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}, "txns": {"$addToSet": "$TRAN_ID"}}},
            {"$addFields": {"transaction_count": {"$size": "$txns"}}},
            {"$sort": {"_id.month": 1}},
        ]
        txn_month = await db.sales_data.aggregate(txn_month_pipe).to_list(24)
        summary["transactions_per_month"] = [{"month": r["_id"]["month"], "transactions": r["transaction_count"]} for r in txn_month][-12:]
        # Transactions by region (zone)
        txn_zone_pipe = [
            {"$group": {"_id": "$Zone_New", "txns": {"$addToSet": "$TRAN_ID"}}},
            {"$match": {"_id": {"$ne": None}}},
            {"$addFields": {"transaction_count": {"$size": "$txns"}}},
            {"$sort": {"transaction_count": -1}},
            {"$limit": 10},
        ]
        txn_zone = await db.sales_data.aggregate(txn_zone_pipe).to_list(10)
        summary["transactions_by_zone"] = [{"zone": r["_id"], "transactions": r["transaction_count"]} for r in txn_zone]

        # Totals for contribution-style checks (used by narrative alerts)
        total_sales_pipe = [{"$group": {"_id": None, "total_sales": {"$sum": "$NET_SALES_VALUE"}}}, {"$limit": 1}]
        total_sales_r = await db.sales_data.aggregate(total_sales_pipe).to_list(1)
        summary["total_sales_value"] = round(total_sales_r[0]["total_sales"], 2) if total_sales_r else 0

        # Product contribution lookup (top 200 products by revenue)
        prod_all_pipe = [{"$group": {"_id": "$Product", "sales_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"sales_value": -1}}, {"$limit": 200}]
        prod_all = await db.sales_data.aggregate(prod_all_pipe).to_list(200)
        total_sales_val = summary.get("total_sales_value") or 0
        summary["product_contribution_map"] = {
            (p["_id"] or ""): {
                "value": round(p["sales_value"], 2),
                "pct": round(p["sales_value"] / total_sales_val * 100, 2) if total_sales_val else 0,
            }
            for p in prod_all
        }

        # Customer contribution lookup (top 200 customers by revenue)
        cust_all_pipe = [{"$group": {"_id": "$CUST_CODE", "sales_value": {"$sum": "$NET_SALES_VALUE"}}}, {"$sort": {"sales_value": -1}}, {"$limit": 200}]
        cust_all = await db.sales_data.aggregate(cust_all_pipe).to_list(200)
        summary["customer_contribution_map"] = {
            (c["_id"] or ""): {
                "value": round(c["sales_value"], 2),
                "pct": round(c["sales_value"] / total_sales_val * 100, 2) if total_sales_val else 0,
            }
            for c in cust_all
        }

        # Latest month helper (used for "this month" narrative alerts)
        trends = summary.get("trends") or []
        summary["latest_month"] = (trends[-1]["month"] if trends else None)
        summary["prev_month"] = (trends[-2]["month"] if len(trends) >= 2 else None)

        # Returns MoM by state: compute returns_abs by state for latest and previous month
        if summary.get("latest_month") and summary.get("prev_month"):
            state_ret_mom_pipe = [
                {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
                {"$match": {"doc_date_parsed": {"$ne": None}}},
                {"$addFields": {"month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}},
                {"$match": {"month": {"$in": [summary["latest_month"], summary["prev_month"]]}}},
                {"$addFields": {"returns_abs": {"$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, {"$abs": "$NET_SALES_VALUE"}, 0]}}},
                {"$group": {"_id": {"state": "$State", "month": "$month"}, "returns_abs": {"$sum": "$returns_abs"}}},
            ]
            state_ret_rows = await db.sales_data.aggregate(state_ret_mom_pipe).to_list(5000)
            by_state = {}
            for row in state_ret_rows:
                sid = row["_id"]
                state = sid.get("state") if isinstance(sid, dict) else None
                month = sid.get("month") if isinstance(sid, dict) else None
                if not state or not month:
                    continue
                if state not in by_state:
                    by_state[state] = {}
                by_state[state][month] = row["returns_abs"]
            mom = {}
            for state, m in by_state.items():
                curr = m.get(summary["latest_month"], 0) or 0
                prev = m.get(summary["prev_month"], 0) or 0
                if prev > 0:
                    mom[state] = round((curr - prev) / prev * 100, 2)
            summary["state_returns_mom_pct"] = mom

        # --- Advanced analytics support ---
        # Month-over-month revenue change (latest vs previous)
        if summary.get("latest_month") and summary.get("prev_month"):
            lm = summary["latest_month"]
            pm = summary["prev_month"]
            trend_map = {t.get("month"): t.get("value") for t in (summary.get("trends") or []) if t.get("month")}
            latest_val = float(trend_map.get(lm) or 0)
            prev_val = float(trend_map.get(pm) or 0)
            mom_pct = round((latest_val - prev_val) / prev_val * 100, 2) if prev_val else None
            summary["revenue_mom"] = {"latest_month": lm, "prev_month": pm, "latest_value": round(latest_val, 2), "prev_value": round(prev_val, 2), "mom_pct": mom_pct}

            # Root cause breakdown: zone/state/product deltas between pm and lm
            # Zone deltas
            zone_delta_pipe = [
                {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
                {"$match": {"doc_date_parsed": {"$ne": None}}},
                {"$addFields": {"month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}},
                {"$match": {"month": {"$in": [lm, pm]}}},
                {"$group": {"_id": {"zone": "$Zone_New", "month": "$month"}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            ]
            zone_rows = await db.sales_data.aggregate(zone_delta_pipe).to_list(5000)
            zmap: Dict[str, Dict[str, float]] = {}
            for r in zone_rows:
                zid = r["_id"]
                z = (zid.get("zone") or "Unknown") if isinstance(zid, dict) else "Unknown"
                m = zid.get("month") if isinstance(zid, dict) else None
                if not m:
                    continue
                if z not in zmap:
                    zmap[z] = {}
                zmap[z][m] = float(r["value"] or 0)
            zone_deltas = []
            for z, mv in zmap.items():
                dv = (mv.get(lm, 0) or 0) - (mv.get(pm, 0) or 0)
                if dv != 0:
                    zone_deltas.append({"zone": z, "delta_value": round(dv, 2)})
            zone_deltas.sort(key=lambda x: x["delta_value"])
            summary["root_cause_zone_deltas"] = zone_deltas[:10]

            # State deltas
            state_delta_pipe = [
                {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
                {"$match": {"doc_date_parsed": {"$ne": None}}},
                {"$addFields": {"month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}},
                {"$match": {"month": {"$in": [lm, pm]}}},
                {"$group": {"_id": {"state": "$State", "month": "$month"}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            ]
            state_rows = await db.sales_data.aggregate(state_delta_pipe).to_list(10000)
            smap: Dict[str, Dict[str, float]] = {}
            for r in state_rows:
                sid = r["_id"]
                s = (sid.get("state") or "Unknown") if isinstance(sid, dict) else "Unknown"
                m = sid.get("month") if isinstance(sid, dict) else None
                if not m:
                    continue
                if s not in smap:
                    smap[s] = {}
                smap[s][m] = float(r["value"] or 0)
            state_deltas = []
            for s, mv in smap.items():
                dv = (mv.get(lm, 0) or 0) - (mv.get(pm, 0) or 0)
                if dv != 0:
                    state_deltas.append({"state": s, "delta_value": round(dv, 2)})
            state_deltas.sort(key=lambda x: x["delta_value"])
            summary["root_cause_state_deltas"] = state_deltas[:10]

            # Product deltas (limit to top movers by absolute delta)
            prod_delta_pipe = [
                {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
                {"$match": {"doc_date_parsed": {"$ne": None}}},
                {"$addFields": {"month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}},
                {"$match": {"month": {"$in": [lm, pm]}}},
                {"$group": {"_id": {"product": "$Product", "month": "$month"}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            ]
            prod_rows = await db.sales_data.aggregate(prod_delta_pipe).to_list(20000)
            pmap: Dict[str, Dict[str, float]] = {}
            for r in prod_rows:
                pid = r["_id"]
                p = (pid.get("product") or "Unknown") if isinstance(pid, dict) else "Unknown"
                m = pid.get("month") if isinstance(pid, dict) else None
                if not m:
                    continue
                if p not in pmap:
                    pmap[p] = {}
                pmap[p][m] = float(r["value"] or 0)
            prod_deltas = []
            for p, mv in pmap.items():
                dv = (mv.get(lm, 0) or 0) - (mv.get(pm, 0) or 0)
                if dv != 0:
                    prod_deltas.append({"product": p, "delta_value": round(dv, 2), "abs_delta": abs(dv)})
            prod_deltas.sort(key=lambda x: x["abs_delta"], reverse=True)
            summary["root_cause_product_deltas"] = [{"product": x["product"], "delta_value": x["delta_value"]} for x in prod_deltas[:12]]

            # Territory analytics: underperforming zones/states = negative delta and/or negative MoM growth
            under_zones = [z for z in (summary.get("zone_growth") or []) if z.get("growth_pct") is not None and z["growth_pct"] < 0]
            under_zones_sorted = sorted(under_zones, key=lambda x: x.get("growth_pct", 0))
            summary["underperforming_zones"] = under_zones_sorted[:10]

        # Recommendation support: richer product maps (top 200)
        total_sales_val = summary.get("total_sales_value") or 0
        # Product return rate map (top 200 by revenue)
        prod_rr_pipe = [
            {"$group": {"_id": "$Product", "gross": {"$sum": {"$cond": [{"$gt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}}, "returns_abs": {"$sum": {"$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, {"$abs": "$NET_SALES_VALUE"}, 0]}}, "sales_value": {"$sum": "$NET_SALES_VALUE"}, "avg_discount": {"$avg": "$Discount %"}}},
            {"$sort": {"sales_value": -1}},
            {"$limit": 200},
        ]
        rr_rows = await db.sales_data.aggregate(prod_rr_pipe).to_list(200)
        prm = {}
        for r in rr_rows:
            gross = float(r.get("gross") or 0)
            rr = round(float(r.get("returns_abs") or 0) / gross * 100, 2) if gross > 0 else None
            prm[r["_id"] or ""] = {
                "return_rate_pct": rr,
                "avg_discount": round(float(r.get("avg_discount") or 0), 2) if r.get("avg_discount") is not None else None,
                "sales_value": round(float(r.get("sales_value") or 0), 2),
                "sales_pct": round(float(r.get("sales_value") or 0) / total_sales_val * 100, 2) if total_sales_val else 0,
            }
        summary["product_risk_pricing_map"] = prm

        # Churn proxy: customers inactive in latest month or sharp decline (no supervised label available)
        if summary.get("latest_month") and summary.get("prev_month"):
            lm = summary["latest_month"]
            pm = summary["prev_month"]
            cust_month_pipe = [
                {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
                {"$match": {"doc_date_parsed": {"$ne": None}}},
                {"$addFields": {"month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}},
                {"$match": {"month": {"$in": [lm, pm]}}},
                {"$group": {"_id": {"customer": "$CUST_CODE", "month": "$month"}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            ]
            cust_rows = await db.sales_data.aggregate(cust_month_pipe).to_list(20000)
            cmap: Dict[str, Dict[str, float]] = {}
            for r in cust_rows:
                cid = r["_id"]
                c = (cid.get("customer") or "Unknown") if isinstance(cid, dict) else "Unknown"
                m = cid.get("month") if isinstance(cid, dict) else None
                if not m:
                    continue
                if c not in cmap:
                    cmap[c] = {}
                cmap[c][m] = float(r["value"] or 0)
            churn_candidates = []
            for c, mv in cmap.items():
                curr = mv.get(lm, 0) or 0
                prev = mv.get(pm, 0) or 0
                if prev <= 0:
                    continue
                if curr == 0:
                    churn_candidates.append({"customer": c, "reason": "No sales in latest month", "prev_value": round(prev, 2), "latest_value": 0, "drop_pct": 100.0})
                else:
                    drop_pct = round((curr - prev) / prev * 100, 2)
                    if drop_pct <= -50:
                        churn_candidates.append({"customer": c, "reason": "Sales dropped sharply", "prev_value": round(prev, 2), "latest_value": round(curr, 2), "drop_pct": drop_pct})
            churn_candidates.sort(key=lambda x: (x["drop_pct"], -x.get("prev_value", 0)))
            summary["churn_risk_candidates"] = churn_candidates[:15]

        # Daily-ish "today" sales: use latest available DOC_DATE in data (not system date)
        latest_date_pipe = [
            {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
            {"$match": {"doc_date_parsed": {"$ne": None}}},
            {"$sort": {"doc_date_parsed": -1}},
            {"$limit": 1},
            {"$project": {"doc_date_parsed": 1}},
        ]
        latest_date_r = await db.sales_data.aggregate(latest_date_pipe).to_list(1)
        latest_date = latest_date_r[0]["doc_date_parsed"] if latest_date_r else None
        summary["latest_doc_date"] = latest_date.isoformat() if latest_date else None
        if latest_date:
            day_str = latest_date.strftime("%Y-%m-%d")
            today_sales_pipe = [
                {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
                {"$match": {"doc_date_parsed": {"$ne": None}}},
                {"$match": {"$expr": {"$eq": [{"$dateToString": {"format": "%Y-%m-%d", "date": "$doc_date_parsed"}}, day_str]}}},
                {"$group": {"_id": None, "net_sales_value": {"$sum": "$NET_SALES_VALUE"}, "net_sales_qty": {"$sum": "$NET_SALES_QTY"}, "transactions": {"$addToSet": "$TRAN_ID"}}},
                {"$limit": 1},
            ]
            today_sales_r = await db.sales_data.aggregate(today_sales_pipe).to_list(1)
            if today_sales_r:
                d = today_sales_r[0]
                summary["today_sales"] = {
                    "date": day_str,
                    "net_sales_value": round(float(d.get("net_sales_value") or 0), 2),
                    "net_sales_qty": int(d.get("net_sales_qty") or 0),
                    "transactions": len(d.get("transactions") or []),
                }
            else:
                summary["today_sales"] = {"date": day_str, "net_sales_value": 0, "net_sales_qty": 0, "transactions": 0}

        # Return rate for latest month
        if summary.get("latest_month"):
            lm = summary["latest_month"]
            month_returns_pipe = [
                {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
                {"$match": {"doc_date_parsed": {"$ne": None}}},
                {"$addFields": {"month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}},
                {"$match": {"month": lm}},
                {"$group": {
                    "_id": None,
                    "gross": {"$sum": {"$cond": [{"$gt": ["$NET_SALES_VALUE", 0]}, "$NET_SALES_VALUE", 0]}},
                    "returns_abs": {"$sum": {"$cond": [{"$lt": ["$NET_SALES_VALUE", 0]}, {"$abs": "$NET_SALES_VALUE"}, 0]}},
                }},
                {"$limit": 1},
            ]
            mr = await db.sales_data.aggregate(month_returns_pipe).to_list(1)
            if mr:
                gross = float(mr[0].get("gross") or 0)
                ret = float(mr[0].get("returns_abs") or 0)
                summary["return_rate_latest_month"] = {"month": lm, "returns_abs": round(ret, 2), "gross": round(gross, 2), "return_rate_pct": round(ret / gross * 100, 2) if gross > 0 else 0}
            else:
                summary["return_rate_latest_month"] = {"month": lm, "returns_abs": 0, "gross": 0, "return_rate_pct": 0}

        # Products declining in sales (MoM) - bottom movers
        # Reuse product month data computed earlier if available; otherwise compute quickly
        if summary.get("latest_month") and summary.get("prev_month"):
            lm = summary["latest_month"]
            pm = summary["prev_month"]
            prod_month_pipe2 = [
                {"$addFields": {"doc_date_parsed": {"$dateFromString": {"dateString": "$DOC_DATE", "onError": None, "onNull": None}}}},
                {"$match": {"doc_date_parsed": {"$ne": None}}},
                {"$addFields": {"month": {"$dateToString": {"format": "%Y-%m", "date": "$doc_date_parsed"}}}},
                {"$match": {"month": {"$in": [lm, pm]}}},
                {"$group": {"_id": {"product": "$Product", "month": "$month"}, "value": {"$sum": "$NET_SALES_VALUE"}}},
            ]
            rows = await db.sales_data.aggregate(prod_month_pipe2).to_list(50000)
            mby: Dict[str, Dict[str, float]] = {}
            for r in rows:
                pid = r["_id"]
                p = (pid.get("product") or "Unknown") if isinstance(pid, dict) else "Unknown"
                m = pid.get("month") if isinstance(pid, dict) else None
                if not m:
                    continue
                if p not in mby:
                    mby[p] = {}
                mby[p][m] = float(r["value"] or 0)
            declining = []
            for p, mv in mby.items():
                curr = mv.get(lm, 0) or 0
                prev = mv.get(pm, 0) or 0
                if prev and prev != 0:
                    growth = round((curr - prev) / prev * 100, 2)
                    if growth < 0:
                        declining.append({"product": p, "growth_pct": growth, "latest_value": round(curr, 2), "prev_value": round(prev, 2)})
            declining.sort(key=lambda x: x["growth_pct"])
            summary["declining_products"] = declining[:15]

        # Zones growing fastest (MoM)
        zg = summary.get("zone_growth") or []
        zg_sorted = sorted([z for z in zg if z.get("growth_pct") is not None], key=lambda x: x["growth_pct"], reverse=True)
        summary["zones_growing_fastest"] = zg_sorted[:10]

        # Top 10 cities by sales (overall)
        city_sales_pipe = [{"$group": {"_id": "$CITY", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$match": {"_id": {"$ne": None}}}, {"$sort": {"value": -1}}, {"$limit": 10}]
        city_sales = await db.sales_data.aggregate(city_sales_pipe).to_list(10)
        summary["top_cities_by_sales"] = [{"city": c["_id"], "value": round(c["value"], 2)} for c in city_sales]

        # Brands perform best (overall revenue)
        brand_sales_pipe = [{"$group": {"_id": "$ITEM_BRAND KPMG", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$match": {"_id": {"$ne": None}}}, {"$sort": {"value": -1}}, {"$limit": 10}]
        brand_sales = await db.sales_data.aggregate(brand_sales_pipe).to_list(10)
        summary["top_brands_by_sales"] = [{"brand": b["_id"], "value": round(b["value"], 2)} for b in brand_sales]

        # Revenue distribution across divisions
        div_sales_pipe = [{"$group": {"_id": "$Div_Code (Mapping HQ)", "value": {"$sum": "$NET_SALES_VALUE"}}}, {"$match": {"_id": {"$ne": None}}}, {"$sort": {"value": -1}}]
        div_sales = await db.sales_data.aggregate(div_sales_pipe).to_list(100)
        total_div = sum(d["value"] for d in div_sales) if div_sales else 0
        summary["division_distribution"] = [{"division": d["_id"], "value": round(d["value"], 2), "pct": round(d["value"] / total_div * 100, 2) if total_div else 0} for d in div_sales]
    except Exception as e:
        logger.error(f"Error building chat data summary: {e}")
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


_DEFAULT_INSIGHT = "No data available. Load sales data to generate insights."
_DEFAULT_REC = "Load data and refresh insights."
_DEFAULT_ACTION = "Load data and try again."


def _insight_response(insights: List[str], recommendations: List[str], action_items: List[str]) -> InsightResponse:
    """Build InsightResponse; never return empty lists so the UI always has something to show."""
    def _norm(lst: List[str], default: str) -> List[str]:
        out = [_normalize_dashes(x) for x in (lst or []) if x]
        return out if out else [default]
    return InsightResponse(
        insights=_norm(insights, _DEFAULT_INSIGHT),
        recommendations=_norm(recommendations, _DEFAULT_REC),
        action_items=_norm(action_items, _DEFAULT_ACTION),
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
        avg_disc = _safe_float(pricing.get("avg_discount", 0))
        insights.append(f"Average discount across transactions: {avg_disc:.2f}%. Monitor by product and channel for margin impact.")
        top_dp = pricing.get("top_discount_products") or []
        if top_dp:
            top_s = ", ".join(
                f"{p.get('product','?')} ({_safe_float(p.get('avg_discount')):.1f}% avg)"
                for p in top_dp[:3]
                if p.get("product")
            )
            if top_s:
                insights.append(f"Highest average discount products (sample): {top_s}.")
        buckets = pricing.get("discount_buckets") or []
        if buckets:
            high_disc = [b for b in buckets if isinstance(b.get("_id"), (int, float)) and b.get("_id", 0) >= 20]
            if high_disc:
                insights.append("Significant volume in higher discount buckets; review approval norms and margin.")
        if net_sales > 0:
            insights.append(f"Net sales context: ₹{net_sales/1e7:.2f} Cr — tie discount discipline to realization and returns ({returns_rate:.2f}% returns rate).")
        recs.append("Use pricing and discount distribution to align list price, schemes, and net realization.")
        recs.append("Flag products or customers with unusually high discounts for review.")
        actions.append("Review average discount and distribution monthly; set discount caps by segment.")
        actions.append("Export pricing analysis for finance and sales operations.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    # Incentive Analytics
    if dashboard == "Incentive Analytics":
        insights, recs, actions = [], [], []
        inc = summary.get("incentives") or {}
        total_paid = _safe_float(inc.get("total_incentive_paid"))
        total_act = _safe_float(inc.get("total_actual_sales"))
        total_bud = _safe_float(inc.get("total_budget"))
        ach = _safe_float(inc.get("achievement_pct"))
        cost_pct = _safe_float(inc.get("incentive_cost_pct"))
        payout = _safe_float(inc.get("payout_ratio_pct"))
        rev_per = _safe_float(inc.get("revenue_per_incentive"))
        elig = _safe_float(inc.get("employees_eligible_pct"))
        etot = int(inc.get("employees_total") or 0)
        if total_paid > 0 or total_act > 0:
            insights.append(
                f"Incentive payout: ₹{total_paid/1e7:.2f} Cr on incentive-basis actual sales ₹{total_act/1e7:.2f} Cr vs budget ₹{total_bud/1e7:.2f} Cr "
                f"(achievement {ach:.2f}%). Incentive cost {cost_pct:.3f}% of sales; revenue per ₹ incentive ~{rev_per:.1f}."
            )
            if payout > 0:
                insights.append(f"Potential vs paid: payout ratio ~{payout:.2f}% of total potential incentive pool.")
            if etot > 0:
                insights.append(f"Coverage: {elig:.1f}% of employees ({inc.get('employees_eligible', 0)}/{etot}) received incentive.")
        cycles = inc.get("recent_cycles") or []
        if cycles:
            last = cycles[-1]
            insights.append(
                f"Latest cycle {last.get('cycle','?')} ({last.get('fy','')}): incentive ₹{_safe_float(last.get('incentive'))/1e7:.2f} Cr, "
                f"cost {_safe_float(last.get('incentive_cost_pct')):.3f}% of sales, achievement {_safe_float(last.get('achievement_pct')):.2f}%."
            )
        an = inc.get("anomaly_samples") or []
        if an:
            first = an[0]
            insights.append(
                f"Incentive anomaly sample: Emp {first.get('emp_id')} ({first.get('zone') or '—'} / {first.get('division') or '—'}) — "
                f"{first.get('reason','flagged')}; cost {_safe_float(first.get('incentive_cost_pct')):.3f}% of sales, achievement {_safe_float(first.get('achievement_pct')):.2f}%."
            )
            recs.append("Review flagged employees for target setting, payout rules, and data quality (zone/HQ mapping).")
        if not insights:
            insights.append("No incentive rows found. Load Incentive Data via /api/data/load/incentives to enable insights.")
        recs.append("Track incentive cost % and payout ratio by cycle; align with budget attainment and role mix.")
        recs.append("Use zone×division heatmaps and employee scatter to find misaligned high payout / low sales cases.")
        actions.append("Reconcile incentive actuals vs finance payroll monthly; investigate top anomaly cases.")
        actions.append("Share incentive efficiency (sales per ₹ incentive) with leadership for scheme design.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    # Risk & Governance
    if dashboard == "Risk & Governance":
        insights, recs, actions = [], [], []
        risk = summary.get("risk_indicators") or {}
        anomalies = summary.get("risk_anomalies") or {}
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

        # Anomalies & outliers (new KPIs)
        if isinstance(anomalies, dict) and anomalies:
            if anomalies.get("zone_spikes_count"):
                topz = anomalies.get("top_zone_spike") or {}
                try:
                    insights.append(
                        f"Zone spike alert: {topz.get('zone','N/A')} is {float(topz.get('growth_pct') or 0):+.2f}% MoM in {topz.get('month','latest')} (₹{float(topz.get('value') or 0)/1e7:.2f} Cr)."
                    )
                except Exception:
                    pass
            if anomalies.get("product_surges_count"):
                topp = anomalies.get("top_product_surge") or {}
                try:
                    insights.append(
                        f"Product surge: {topp.get('product','N/A')} recent avg ₹{float(topp.get('recent_avg') or 0)/1e7:.2f} Cr vs history avg ₹{float(topp.get('history_avg') or 0)/1e7:.2f} Cr ({float(topp.get('lift_pct') or 0):+.2f}% lift)."
                    )
                except Exception:
                    pass
            if anomalies.get("promo_share_pct_latest") is not None:
                insights.append(f"Promotion mix (latest month): promoted contributes ~{float(anomalies.get('promo_share_pct_latest') or 0):.2f}% of sales value.")
            if anomalies.get("price_outliers_count"):
                to = anomalies.get("top_price_outlier") or {}
                try:
                    insights.append(
                        f"Price variance outlier: {to.get('product','N/A')} in {to.get('zone','N/A')} shows ~{float(to.get('variance_pct') or 0):.2f}% PPU variance (min {to.get('min_ppu','-')}, max {to.get('max_ppu','-')})."
                    )
                except Exception:
                    pass
            if anomalies.get("division_zone_outliers_count"):
                td = anomalies.get("top_division_zone_outlier") or {}
                try:
                    insights.append(
                        f"Division×zone outlier: {td.get('division','N/A')} × {td.get('zone','N/A')} is {float(td.get('lift_pct') or 0):+.2f}% vs prior average in {td.get('month','latest')}."
                    )
                except Exception:
                    pass

        recs.append("Monitor returns rate, zone and customer concentration, and stop-business list against thresholds.")
        if isinstance(anomalies, dict) and anomalies:
            if anomalies.get("zone_spikes_count"):
                recs.append("Investigate zone spikes for one-time institutional orders, channel stuffing, or pricing changes; validate sustainability.")
            if anomalies.get("product_surges_count"):
                recs.append("Validate product surges against promotion calendars and stock availability; ensure growth is not return-driven.")
            if anomalies.get("price_outliers_count"):
                recs.append("Enforce PTR/PPU guardrails for product×zone price variance outliers; require approvals for exceptions.")
        if ret_rate >= 2:
            recs.append("Conduct returns deep-dive by product and geography; tighten approval and reverse-logistics.")
        actions.append("Review risk indicators weekly; trigger escalation when metrics breach thresholds.")
        if isinstance(anomalies, dict) and anomalies:
            actions.append("Set automated alerts for zone MoM spikes, product surges vs history, and extreme product×zone PPU variance.")
            actions.append("Audit the top price variance and division×zone outliers; document root cause and corrective action.")
        actions.append("Export risk and returns trend for compliance and leadership.")
        return _insight_response(insights or fallback.insights, recs or fallback.recommendations, actions or fallback.action_items)

    return fallback


def _summary_to_context_text(summary: Dict[str, Any]) -> str:
    """Turn data summary into a short text context for chat."""
    parts = []
    ov = summary.get("overview") or {}
    if ov:
        parts.append(
            f"Overview: Net sales {ov.get('net_sales_value')}, gross sales {ov.get('gross_sales_value')}, "
            f"returns {ov.get('returns_value')} ({ov.get('returns_rate_pct')}% return rate). "
            f"Transactions: {ov.get('total_transactions')}, Customers: {ov.get('total_customers')}, "
            f"Products: {ov.get('total_products')}, Net quantity: {ov.get('net_sales_qty')}."
        )
    trends = summary.get("trends") or []
    if trends:
        last3 = trends[-3:]
        parts.append("Recent monthly trend: " + ", ".join(f"{t['month']}: {t['value']}" + (f" ({t.get('growth_pct')}% MoM)" if t.get("growth_pct") is not None else "") for t in last3))
    conc = summary.get("concentration") or {}
    if conc.get("top_states"):
        parts.append("Top states by sales: " + ", ".join(f"{s['name']} ({s['pct']}%)" for s in conc["top_states"][:5]))
    if conc.get("zones"):
        parts.append("Zones: " + ", ".join(f"{z['name']} ({z['pct']}%)" for z in conc["zones"][:5]))
    products = summary.get("products") or []
    if products:
        parts.append("Top products: " + ", ".join(f"{p['name']} ({p['pct']}%)" for p in products[:8]))
    cust = summary.get("customers")
    if isinstance(cust, dict):
        parts.append(f"Customers: top 10 represent {cust.get('top_10_pct')}% of sales; total customers {cust.get('total_customers')}; stop business count {cust.get('stop_business_count')}.")
    pricing = summary.get("pricing")
    if isinstance(pricing, dict):
        parts.append(f"Pricing: avg discount {pricing.get('avg_discount')}%.")
    inc = summary.get("incentives")
    if isinstance(inc, dict) and (inc.get("total_incentive_paid") or inc.get("total_actual_sales")):
        parts.append(
            f"Incentives: paid {inc.get('total_incentive_paid')}, achievement {inc.get('achievement_pct')}%, "
            f"cost {inc.get('incentive_cost_pct')}%, eligible employees {inc.get('employees_eligible_pct')}%."
        )
    risk = summary.get("risk_indicators")
    if isinstance(risk, dict):
        parts.append(f"Risk: returns rate {risk.get('returns_rate')}%, negative line % {risk.get('negative_line_pct')}, zone concentration {risk.get('zone_concentration_pct')}%, top 10 customer % {risk.get('top_10_customer_pct')}, stop business {risk.get('stop_business_count')}.")
    return " ".join(parts) if parts else "No data loaded."


def _answer_question_from_data(question: str, summary: Dict[str, Any]) -> str:
    """Answer the user question using the data summary. Maps prompts to specific insights (Net sales KPI, Revenue growth, etc.)."""
    q = (question or "").strip().lower()
    if not q:
        return "Please ask a specific question about the sales data or insights."
    ctx = _summary_to_context_text(summary)
    ov = summary.get("overview") or {}
    conc = summary.get("concentration") or {}
    trends = summary.get("trends") or []
    products = summary.get("products") or []
    top_customers = summary.get("top_customers") or []
    cust = summary.get("customers")
    pricing = summary.get("pricing")
    risk = summary.get("risk_indicators")
    quarterly = summary.get("quarterly") or {}
    brand_growth = summary.get("brand_growth") or []
    zones_sorted_low = summary.get("zones_sorted_low") or []
    product_return_rates = summary.get("product_return_rates") or []
    product_growth_skus = summary.get("product_growth_skus") or []
    product_avg_price = summary.get("product_avg_price") or []
    product_penetration = summary.get("product_penetration") or []
    product_geo_concentration = summary.get("product_geo_concentration") or []
    state_return_rates = summary.get("state_return_rates") or []
    city_growth = summary.get("city_growth") or []
    zone_growth = summary.get("zone_growth") or []
    state_revenue_per_customer = summary.get("state_revenue_per_customer") or []
    stop_business_customers = summary.get("stop_business_customers") or []
    customer_return_rates = summary.get("customer_return_rates") or []
    customer_product_count = summary.get("customer_product_count") or []
    pareto_80_customers = summary.get("pareto_80_customers") or []
    pareto_80_count = summary.get("pareto_80_count") or 0
    pareto_80_pct = summary.get("pareto_80_pct") or 0
    customer_growth = summary.get("customer_growth") or []
    avg_discount_overall = summary.get("avg_discount_overall")
    product_highest_discount = summary.get("product_highest_discount") or []
    zone_price_realization = summary.get("zone_price_realization") or []
    overall_price_realization = summary.get("overall_price_realization")
    customer_highest_discount = summary.get("customer_highest_discount") or []
    promoted_sales_pct = summary.get("promoted_sales_pct") or 0
    promoted_vs_non = summary.get("promoted_vs_non") or {}
    top_promoted_products = summary.get("top_promoted_products") or []
    zone_promotion_response = summary.get("zone_promotion_response") or []
    high_dependency_customers = summary.get("high_dependency_customers") or []
    high_discount_transactions = summary.get("high_discount_transactions") or []
    zones_declining = summary.get("zones_declining") or []
    avg_transaction_value = summary.get("avg_transaction_value")
    avg_qty_per_transaction = summary.get("avg_qty_per_transaction")
    transactions_per_month = summary.get("transactions_per_month") or []
    transactions_by_zone = summary.get("transactions_by_zone") or []
    revenue_mom = summary.get("revenue_mom") or {}
    root_cause_zone_deltas = summary.get("root_cause_zone_deltas") or []
    root_cause_state_deltas = summary.get("root_cause_state_deltas") or []
    root_cause_product_deltas = summary.get("root_cause_product_deltas") or []
    underperforming_zones = summary.get("underperforming_zones") or []
    product_risk_pricing_map = summary.get("product_risk_pricing_map") or {}
    churn_risk_candidates = summary.get("churn_risk_candidates") or []
    today_sales = summary.get("today_sales") or None
    return_rate_latest_month = summary.get("return_rate_latest_month") or {}
    declining_products = summary.get("declining_products") or []
    zones_growing_fastest = summary.get("zones_growing_fastest") or []
    top_cities_by_sales = summary.get("top_cities_by_sales") or []
    top_brands_by_sales = summary.get("top_brands_by_sales") or []
    division_distribution = summary.get("division_distribution") or []
    latest_month = summary.get("latest_month")
    state_returns_mom_pct = summary.get("state_returns_mom_pct") or {}
    product_contribution_map = summary.get("product_contribution_map") or {}
    customer_contribution_map = summary.get("customer_contribution_map") or {}

    # --- Narrative alert mechanism (Alert/Insight/Warning/Opportunity/Risk) ---
    # Accepts statements like:
    # - "Alert: Returns increased by 18% in Karnataka this month."
    # - "Insight: Product_5 contributes 19% of total revenue."
    # - "Warning: 72% of revenue comes from the South zone."
    # - "Opportunity: Coimbatore sales grew 34% month-over-month."
    # - "Risk: Customer XYZ accounts for 12% of total revenue."
    import re

    nq = (question or "").strip()
    nq_lower = nq.lower()
    if any(nq_lower.startswith(pfx) for pfx in ["alert:", "insight:", "warning:", "opportunity:", "risk:"]):
        # 1) Returns increased by X% in <State> this month
        m = re.search(r"returns\s+increased\s+by\s+(\d+(?:\.\d+)?)%\s+in\s+(.+?)\s+this\s+month", nq_lower)
        if m:
            stated = float(m.group(1))
            state_raw = m.group(2).strip()
            # Try to match state name by case-insensitive key lookup
            matched_state = None
            for s in state_returns_mom_pct.keys():
                if str(s).strip().lower() == state_raw:
                    matched_state = s
                    break
            if matched_state and latest_month:
                actual = state_returns_mom_pct.get(matched_state)
                if actual is None:
                    return f"**Operational risk alert**\n\nI found state returns data for {matched_state}, but MoM change for {latest_month} could not be computed (previous month returns may be zero)."
                return (
                    "**Operational risk alert**\n\n"
                    f"Claim: Returns increased by {stated}% in {matched_state} this month.\n"
                    f"Data check ({latest_month} vs {summary.get('prev_month')}): {matched_state} returns MoM change is {actual}%."
                )
            return "**Operational risk alert**\n\nI couldn't verify that yet (missing month/state match). Please ensure the state name matches the dataset and that at least 2 months of data are available."

        # 2) Product contributes X% of total revenue
        m = re.search(r"([a-z0-9_\\- ]+?)\s+contributes\s+(\d+(?:\.\d+)?)%\s+of\s+total\s+revenue", nq_lower)
        if m:
            prod_raw = m.group(1).strip()
            stated = float(m.group(2))
            # Find product key by case-insensitive match
            matched_prod = None
            for p in product_contribution_map.keys():
                if str(p).strip().lower() == prod_raw:
                    matched_prod = p
                    break
            if matched_prod:
                actual = product_contribution_map[matched_prod]["pct"]
                return (
                    "**Revenue contribution insight**\n\n"
                    f"Claim: {matched_prod} contributes {stated}% of total revenue.\n"
                    f"Data check: {matched_prod} contributes {actual}% (value {product_contribution_map[matched_prod]['value']:,.2f})."
                )
            return "**Revenue contribution insight**\n\nI couldn’t find that product name in the current dataset (top products). Try using the exact product name as in the data."

        # 3) X% of revenue comes from the <Zone> zone
        m = re.search(r"(\d+(?:\.\d+)?)%\s+of\s+revenue\s+comes\s+from\s+the\s+(.+?)\s+zone", nq_lower)
        if m:
            stated = float(m.group(1))
            zone_raw = m.group(2).strip()
            zones = (conc.get("zones") or [])
            matched_zone = None
            for z in zones:
                if str(z.get("name", "")).strip().lower() == zone_raw:
                    matched_zone = z
                    break
            if matched_zone:
                return (
                    "**Regional concentration warning**\n\n"
                    f"Claim: {stated}% of revenue comes from {matched_zone['name']} zone.\n"
                    f"Data check: {matched_zone['name']} zone contributes {matched_zone.get('pct', 0)}% (value {matched_zone.get('value', 0):,.2f})."
                )
            return "**Regional concentration warning**\n\nI couldn’t match that zone name. Please use the exact zone label from the dashboard (e.g., `South`, `North`, etc.)."

        # 4) <City> sales grew X% month-over-month
        m = re.search(r"(.+?)\s+sales\s+grew\s+(\d+(?:\.\d+)?)%\s+month[- ]over[- ]month", nq_lower)
        if m:
            city_raw = m.group(1).strip()
            stated = float(m.group(2))
            matched_city = None
            for c in (city_growth or []):
                if str(c.get("city", "")).strip().lower() == city_raw:
                    matched_city = c
                    break
            if matched_city:
                return (
                    "**Expansion opportunity**\n\n"
                    f"Claim: {matched_city['city']} sales grew {stated}% MoM.\n"
                    f"Data check: {matched_city['city']} MoM growth is {matched_city.get('growth_pct')}%."
                )
            return "**Expansion opportunity**\n\nI couldn’t verify that city (it may not be in the top-growing list). Try using the exact city name as in the data."

        # 5) Customer accounts for X% of total revenue
        m = re.search(r"customer\s+(.+?)\s+accounts\s+for\s+(\d+(?:\.\d+)?)%\s+of\s+total\s+revenue", nq_lower)
        if m:
            cust_raw = m.group(1).strip()
            stated = float(m.group(2))
            matched_cust = None
            for c in customer_contribution_map.keys():
                if str(c).strip().lower() == cust_raw:
                    matched_cust = c
                    break
            if matched_cust:
                actual = customer_contribution_map[matched_cust]["pct"]
                return (
                    "**Dependency risk**\n\n"
                    f"Claim: Customer {matched_cust} accounts for {stated}% of total revenue.\n"
                    f"Data check: Customer {matched_cust} accounts for {actual}% (value {customer_contribution_map[matched_cust]['value']:,.2f})."
                )
            return "**Dependency risk**\n\nI couldn’t find that customer code in the current top customers list. Use the exact `CUST_CODE` from the data."

        return "**Noted**\n\nI received the alert/insight text, but I couldn’t parse it into a supported pattern yet. Try one of these formats:\n- Returns increased by X% in <State> this month\n- <Product> contributes X% of total revenue\n- X% of revenue comes from the <Zone> zone\n- <City> sales grew X% month-over-month\n- Customer <CUST_CODE> accounts for X% of total revenue"

    # --- Prompt-to-insight mapping ---
    # 1. Total sales revenue for current period -> Net sales KPI
    if any(phrase in q for phrase in ["total sales revenue", "total revenue", "sales revenue for the current period", "current period revenue"]):
        v = ov.get("net_sales_value")
        if v is not None:
            return (
                "**Net sales KPI**\n\n"
                f"Total net sales value (current period): {v:,.2f}. "
                f"Gross sales: {ov.get('gross_sales_value', 0):,.2f}. "
                f"Returns: {ov.get('returns_value', 0):,.2f}. "
                f"Return rate: {ov.get('returns_rate_pct', 0)}%. "
                f"Transactions: {ov.get('total_transactions', 0):,}; Customers: {ov.get('total_customers', 0):,}; Products: {ov.get('total_products', 0):,}."
            )
        return "No sales data loaded for the current period."
    # 2. Monthly revenue trend -> Revenue growth
    if any(phrase in q for phrase in ["monthly revenue trend", "monthly trend", "revenue trend", "show monthly revenue"]):
        if len(trends) >= 2:
            lines = [f"{t['month']}: {t['value']:,.2f}" + (f" ({t.get('growth_pct')}% MoM)" if t.get("growth_pct") is not None else "") for t in trends[-8:]]
            return "**Revenue growth**\n\nMonthly revenue trend (recent): " + "; ".join(lines)
        return "Insufficient trend data for revenue growth."
    # 3. States contribute most -> Geographic concentration
    if any(phrase in q for phrase in ["states contribute", "which states", "states contribute the most", "most to sales"]):
        states = conc.get("top_states") or []
        if states:
            lines = [f"{s['name']}: {s['pct']}% of sales" for s in states[:10]]
            top3 = conc.get("top_3_states_pct")
            return "**Geographic concentration**\n\nStates contributing most to sales: " + "; ".join(lines) + (f". Top 3 states represent {top3}% of total sales." if top3 is not None else "")
        return "No state-level data available."
    # 4. Products drive highest revenue -> Product contribution
    if any(phrase in q for phrase in ["products drive", "highest revenue", "which products", "product contribution", "top products by revenue"]):
        if products:
            lines = [f"{p['name']}: {p['pct']}% (value {p['value']:,.2f})" for p in products[:10]]
            return "**Product contribution**\n\nProducts driving the highest revenue: " + "; ".join(lines)
        return "No product-level data available."
    # 5. Zones underperforming -> Regional performance
    if any(phrase in q for phrase in ["zones underperforming", "underperforming zones", "which zones are underperforming", "regional performance"]):
        if zones_sorted_low:
            under = zones_sorted_low[:5]
            lines = [f"{z.get('name', '')}: {z.get('pct', 0)}% of sales" for z in under]
            return "**Regional performance**\n\nZones with lowest share (underperforming vs others): " + "; ".join(lines) + ". Consider focus on these regions to improve performance."
        zones = conc.get("zones") or []
        if zones:
            sorted_z = sorted(zones, key=lambda z: z.get("pct", 0))
            lines = [f"{z['name']}: {z['pct']}%" for z in sorted_z[:5]]
            return "**Regional performance**\n\nZones with lowest contribution: " + "; ".join(lines)
        return "No zone-level data available."
    # 6. Current return rate -> Operational health
    if any(phrase in q for phrase in ["return rate", "current return rate", "operational health", "returns rate"]):
        rate = ov.get("returns_rate_pct")
        r_val = ov.get("returns_value")
        if rate is not None:
            return (
                "**Operational health**\n\n"
                f"Current return rate: {rate}% of gross sales. "
                f"Returns value: {r_val:,.2f}. "
                "Monitor returns by product and channel to protect margin."
            )
        return "Return rate not available."
    # 7. Customers contribute most revenue -> Key account analysis
    if any(phrase in q for phrase in ["customers contribute", "most revenue", "which customers", "key account", "top customers by revenue"]):
        if top_customers:
            lines = [f"{c['customer']}: {c['value']:,.2f}" for c in top_customers[:10]]
            total_top = sum(c["value"] for c in top_customers)
            return "**Key account analysis**\n\nCustomers contributing the most revenue: " + "; ".join(lines) + f". Combined top 10: {total_top:,.2f}."
        if isinstance(cust, dict):
            return f"**Key account analysis**\n\nTop 10 customers represent {cust.get('top_10_pct')}% of sales. Total customers: {cust.get('total_customers')}."
        return "No customer-level data available."
    # 8. Brands growing fastest -> Portfolio growth
    if any(phrase in q for phrase in ["brands growing", "fastest growing", "which brands", "portfolio growth", "brand growth"]):
        if brand_growth:
            lines = [f"{b['brand']}: {b['growth_pct']}% growth (recent period)" for b in brand_growth[:8]]
            return "**Portfolio growth**\n\nBrands growing fastest (MoM): " + "; ".join(lines)
        return "Insufficient brand-level trend data for portfolio growth."
    # 9. Compare Q1 vs Q2 -> Period comparison
    if any(phrase in q for phrase in ["q1 vs q2", "q1 vs q2 performance", "compare q1", "compare q2", "period comparison", "quarterly comparison"]):
        q1 = quarterly.get("Q1", 0)
        q2 = quarterly.get("Q2", 0)
        if q1 or q2:
            change = round((q2 - q1) / q1 * 100, 2) if q1 and q1 != 0 else None
            msg = f"**Period comparison**\n\nQ1 total sales: {q1:,.2f}. Q2 total sales: {q2:,.2f}."
            if change is not None:
                msg += f" Q2 vs Q1: {change:+.2f}%."
            return msg
        return "Insufficient quarterly data for Q1 vs Q2 comparison."

    # 10. Products generate highest sales revenue -> Top SKUs
    if any(phrase in q for phrase in ["products generate the highest sales revenue", "highest sales revenue", "top skus", "top sku"]):
        if products:
            lines = [f"{p['name']}: {p['value']:,.2f} ({p['pct']}%)" for p in products[:10]]
            return "**Top SKUs**\n\nProducts generating the highest sales revenue: " + "; ".join(lines)
        return "No product-level data available."
    # 11. Products highest return rate -> Product quality risk
    if any(phrase in q for phrase in ["products have the highest return rate", "highest return rate", "product quality risk", "which products have the highest return"]):
        if product_return_rates:
            lines = [f"{r['name']}: {r['return_rate_pct']}% return rate (returns {r['returns_abs']:,.2f})" for r in product_return_rates[:8]]
            return "**Product quality risk**\n\nProducts with the highest return rate: " + "; ".join(lines) + ". Review quality and channel for these SKUs."
        return "No product return-rate data available."
    # 12. Products growing fastest MoM -> Growth SKUs
    if any(phrase in q for phrase in ["products are growing fastest", "growing fastest month-over-month", "growth skus", "growth sku", "month-over-month growth"]):
        if product_growth_skus:
            lines = [f"{g['product']}: {g['growth_pct']}% MoM growth" for g in product_growth_skus[:8]]
            return "**Growth SKUs**\n\nProducts growing fastest month-over-month: " + "; ".join(lines)
        return "Insufficient product-level trend data for growth SKUs."
    # 13. Average selling price per product -> Pricing insight
    if any(phrase in q for phrase in ["average selling price", "average price for each product", "pricing insight", "selling price for each product"]):
        if product_avg_price:
            lines = [f"{p['name']}: avg price {p['avg_ppu']:,.2f} (total value {p['total_value']:,.2f})" for p in product_avg_price[:10]]
            return "**Pricing insight**\n\nAverage selling price (PPU) by product: " + "; ".join(lines)
        return "No product-level pricing data available."
    # 14. Most widely distributed across customers -> Product penetration
    if any(phrase in q for phrase in ["most widely distributed", "distributed across customers", "product penetration", "widely distributed across customers"]):
        if product_penetration:
            lines = [f"{p['name']}: {p['customer_count']} distinct customers" for p in product_penetration[:8]]
            return "**Product penetration**\n\nProducts most widely distributed across customers: " + "; ".join(lines)
        return "No product-customer distribution data available."
    # 15. Products dependent on a few states -> Geographic concentration (product)
    if any(phrase in q for phrase in ["products are dependent on a few states", "dependent on a few states", "products dependent on", "geographic concentration by product"]):
        if product_geo_concentration:
            lines = [f"{g['product']}: top state {g['top_state_pct']}% of product sales ({g['state_count']} states)" for g in product_geo_concentration[:8]]
            return "**Geographic concentration**\n\nProducts most dependent on a few states (highest single-state share): " + "; ".join(lines)
        return "No product-state concentration data available."

    # 16. States generate highest sales revenue -> State ranking
    if any(phrase in q for phrase in ["states generate the highest sales revenue", "which states generate", "state ranking", "highest sales revenue by state"]):
        states = conc.get("top_states") or []
        if states:
            lines = [f"{s['name']}: {s['value']:,.2f} ({s['pct']}%)" for s in states[:10]]
            return "**State ranking**\n\nStates generating the highest sales revenue: " + "; ".join(lines)
        return "No state-level data available."
    # 17. Sales distribution across zones -> Regional mix
    if any(phrase in q for phrase in ["sales distribution across zones", "distribution across zones", "regional mix", "show sales distribution"]):
        zones = conc.get("zones") or []
        if zones:
            lines = [f"{z['name']}: {z['pct']}% (value {z['value']:,.2f})" for z in zones]
            return "**Regional mix**\n\nSales distribution across zones: " + "; ".join(lines)
        return "No zone-level data available."
    # 18. Cities fastest growth -> Expansion opportunity
    if any(phrase in q for phrase in ["cities have the fastest growth", "which cities", "fastest growth", "expansion opportunity", "city growth"]):
        if city_growth:
            lines = [f"{c['city']}: {c['growth_pct']}% MoM growth" for c in city_growth[:8]]
            return "**Expansion opportunity**\n\nCities with the fastest growth (MoM): " + "; ".join(lines)
        return "Insufficient city-level trend data for expansion opportunity."
    # 19. States highest return rates -> Supply chain issues
    if any(phrase in q for phrase in ["states have the highest return rates", "highest return rates by state", "supply chain issues", "which states have the highest return"]):
        if state_return_rates:
            lines = [f"{r['name']}: {r['return_rate_pct']}% return rate (returns {r['returns_abs']:,.2f})" for r in state_return_rates[:8]]
            return "**Supply chain issues**\n\nStates with the highest return rates: " + "; ".join(lines) + ". Review logistics and quality in these states."
        return "No state return-rate data available."
    # 20. Zones declining sales -> Territory performance
    if any(phrase in q for phrase in ["zones have declining sales", "declining sales", "territory performance", "which zones have declining"]):
        if zone_growth:
            declining = sorted(zone_growth, key=lambda x: x["growth_pct"])[:8]
            lines = [f"{z['zone']}: {z['growth_pct']}% MoM" for z in declining]
            return "**Territory performance**\n\nZones with declining sales (lowest MoM growth): " + "; ".join(lines)
        return "Insufficient zone-level trend data for territory performance."
    # 21. Revenue per customer in each state -> Market strength
    if any(phrase in q for phrase in ["revenue per customer in each state", "revenue per customer", "market strength", "revenue per customer by state"]):
        if state_revenue_per_customer:
            lines = [f"{s['name']}: {s['revenue_per_customer']:,.2f} per customer ({s['customer_count']} customers)" for s in state_revenue_per_customer[:10]]
            return "**Market strength**\n\nRevenue per customer by state: " + "; ".join(lines)
        return "No state-level revenue-per-customer data available."

    # 22. Top 10 customers by revenue -> Key accounts
    if any(phrase in q for phrase in ["top 10 customers by revenue", "top 10 customers", "key accounts", "who are the top 10 customers"]):
        if top_customers:
            lines = [f"{c['customer']}: {c['value']:,.2f}" for c in top_customers[:10]]
            total_top = sum(c["value"] for c in top_customers)
            return "**Key accounts**\n\nTop 10 customers by revenue: " + "; ".join(lines) + f". Combined: {total_top:,.2f}."
        return "No customer revenue data available."
    # 23. Customers stopped business recently -> Customer attrition
    if any(phrase in q for phrase in ["customers have stopped business", "stopped business recently", "customer attrition", "stop business"]):
        if stop_business_customers:
            lines = [f"{c['customer']}: {c['value']:,.2f} (historical)" for c in stop_business_customers[:15]]
            return "**Customer attrition**\n\nCustomers with stopped business flag: " + "; ".join(lines)
        return "No stop-business customer data available."
    # 24. Customers highest return rates -> Distributor risk
    if any(phrase in q for phrase in ["customers have the highest return rates", "highest return rates", "distributor risk", "which customers have the highest return"]):
        if customer_return_rates:
            lines = [f"{r['name']}: {r['return_rate_pct']}% return rate (returns {r['returns_abs']:,.2f})" for r in customer_return_rates[:8]]
            return "**Distributor risk**\n\nCustomers with the highest return rates: " + "; ".join(lines) + ". Review distribution and terms for these accounts."
        return "No customer return-rate data available."
    # 25. Customers buy the most products -> Cross-selling opportunity
    if any(phrase in q for phrase in ["customers buy the most products", "buy the most products", "cross-selling opportunity", "most products"]):
        if customer_product_count:
            lines = [f"{p['customer']}: {p['product_count']} distinct products" for p in customer_product_count[:8]]
            return "**Cross-selling opportunity**\n\nCustomers who buy the most products (by variety): " + "; ".join(lines)
        return "No product-mix-by-customer data available."
    # 26. Customers contribute 80% of revenue -> Pareto analysis
    if any(phrase in q for phrase in ["customers contribute 80%", "contribute 80% of revenue", "pareto analysis", "80% of revenue"]):
        if pareto_80_customers:
            lines = [f"{c['customer']}: {c['value']:,.2f}" for c in pareto_80_customers[:15]]
            return f"**Pareto analysis**\n\nCustomers that contribute ~80% of revenue (top {pareto_80_count} = {pareto_80_pct}%): " + "; ".join(lines)
        return "No Pareto customer data available."
    # 27. Customers growing fastest -> High-value accounts
    if any(phrase in q for phrase in ["customers are growing fastest", "customers growing fastest", "high-value accounts", "which customers are growing"]):
        if customer_growth:
            lines = [f"{g['customer']}: {g['growth_pct']}% MoM growth" for g in customer_growth[:8]]
            return "**High-value accounts**\n\nCustomers growing fastest (MoM): " + "; ".join(lines)
        return "Insufficient customer-level trend data for growth."

    # 28. Average discount across products -> Pricing control
    if any(phrase in q for phrase in ["average discount offered across products", "average discount", "pricing control", "discount offered across"]):
        if avg_discount_overall is not None:
            return "**Pricing control**\n\nAverage discount offered across products: " + str(avg_discount_overall) + "%. Use this as a baseline to monitor pricing discipline and margin."
        return "No average discount data available."
    # 29. Products with highest discounts -> Margin leakage
    if any(phrase in q for phrase in ["products have the highest discounts", "highest discounts", "margin leakage", "which products have the highest discount"]):
        if product_highest_discount:
            lines = [f"{p['name']}: {p['avg_discount']}% avg discount (value {p['total_value']:,.2f})" for p in product_highest_discount[:8]]
            return "**Margin leakage**\n\nProducts with the highest discounts: " + "; ".join(lines) + ". Review discount policy for these SKUs."
        return "No product-level discount data available."
    # 30. Regions with lowest price realization -> Pricing discipline
    if any(phrase in q for phrase in ["regions have the lowest price realization", "lowest price realization", "pricing discipline", "which regions"]):
        if zone_price_realization:
            lines = [f"{z['name']}: {z['price_realization_pct']}% realization (avg PPU {z['avg_ppu']:,.2f})" for z in zone_price_realization[:8]]
            return "**Pricing discipline**\n\nRegions (zones) with the lowest price realization vs list: " + "; ".join(lines)
        return "No zone-level price realization data available."
    # 31. Price realization vs price list -> Revenue leakage
    if any(phrase in q for phrase in ["price realization vs price list", "price realization vs", "revenue leakage", "realization vs price list"]):
        if overall_price_realization:
            r = overall_price_realization
            return (
                "**Revenue leakage**\n\n"
                f"Price realization vs price list: {r['realization_pct']}% (avg selling price {r['avg_ppu']:,.2f} vs avg list rate {r['avg_list_rate']:,.2f}). "
                "Gap vs 100% indicates revenue left on the table."
            )
        return "No overall price realization data available."
    # 32. Customers with highest discounts -> Governance
    if any(phrase in q for phrase in ["customers receive the highest discounts", "which customers receive the highest discounts", "which customers receive", "customers receive highest discounts"]):
        if customer_highest_discount:
            lines = [f"{c['customer']}: {c['avg_discount']}% avg discount (value {c['total_value']:,.2f})" for c in customer_highest_discount[:8]]
            return "**Governance**\n\nCustomers receiving the highest discounts: " + "; ".join(lines) + ". Review approval and compliance for these accounts."
        return "No customer-level discount data available."

    # 33. % sales from promoted products -> Marketing impact
    if any(phrase in q for phrase in ["percentage of sales from promoted products", "sales comes from promoted", "marketing impact", "promoted products share"]):
        return "**Marketing impact**\n\n" + str(promoted_sales_pct) + "% of sales come from promoted products. Use this to gauge the role of promotions in overall revenue."
    # 34. Do promoted generate higher revenue? -> Promotion effectiveness
    if any(phrase in q for phrase in ["promoted products generate higher revenue", "do promoted products", "promotion effectiveness", "promoted vs non promoted"]):
        pv = promoted_vs_non.get("promoted_value", 0)
        nv = promoted_vs_non.get("non_promoted_value", 0)
        total = promoted_vs_non.get("total", 0)
        pct = round(pv / total * 100, 2) if total else 0
        higher = "Yes" if pv > nv else "No"
        return "**Promotion effectiveness**\n\nPromoted products revenue: " + f"{pv:,.2f}. Non-promoted: {nv:,.2f}. Promoted share: {pct}%. Do promoted generate higher revenue? {higher}."
    # 35. Which promoted products perform best? -> Campaign ROI
    if any(phrase in q for phrase in ["promoted products perform best", "which promoted products", "campaign roi", "best promoted products"]):
        if top_promoted_products:
            lines = [f"{p['name']}: {p['sales_value']:,.2f}" for p in top_promoted_products[:8]]
            return "**Campaign ROI**\n\nPromoted products that perform best by revenue: " + "; ".join(lines)
        return "No promoted-product performance data available."
    # 36. Which regions respond best to promotions? -> Regional marketing strategy
    if any(phrase in q for phrase in ["regions respond best to promotions", "which regions respond", "regional marketing strategy", "regions respond best"]):
        if zone_promotion_response:
            lines = [f"{z['name']}: {z['promoted_pct']}% of sales from promoted (value {z['promoted_value']:,.2f})" for z in zone_promotion_response[:8]]
            return "**Regional marketing strategy**\n\nRegions that respond best to promotions (highest % from promoted): " + "; ".join(lines)
        return "No zone-level promotion data available."

    # 37. States with unusually high return rates -> Operational risk
    if any(phrase in q for phrase in ["states have unusually high return rates", "unusually high return rates", "operational risk", "states unusually high return"]):
        if state_return_rates:
            lines = [f"{r['name']}: {r['return_rate_pct']}% return rate (returns {r['returns_abs']:,.2f})" for r in state_return_rates[:8]]
            return "**Operational risk**\n\nStates with unusually high return rates: " + "; ".join(lines) + ". Review logistics and quality in these states."
        return "No state return-rate data available."
    # 38. Products depend heavily on a single state -> Portfolio risk
    if any(phrase in q for phrase in ["products depend heavily on a single state", "depend heavily on a single state", "portfolio risk", "products depend heavily"]):
        if product_geo_concentration:
            lines = [f"{g['product']}: top state {g['top_state_pct']}% of product sales ({g['state_count']} states)" for g in product_geo_concentration[:8]]
            return "**Portfolio risk**\n\nProducts that depend heavily on a single state (high geographic concentration): " + "; ".join(lines)
        return "No product-state concentration data available."
    # 39. Customers contributing >10% of revenue -> Dependency risk
    if any(phrase in q for phrase in ["customers contribute more than 10%", "contribute more than 10% of revenue", "dependency risk", "more than 10% of revenue"]):
        if high_dependency_customers:
            lines = [f"{c['customer']}: {c['pct']}% of revenue (value {c['value']:,.2f})" for c in high_dependency_customers[:10]]
            return "**Dependency risk**\n\nCustomers that contribute more than 10% of revenue: " + "; ".join(lines) + ". Consider diversification."
        return "No customers exceed 10% of total revenue."
    # 40. Transactions with unusually high discounts -> Fraud / policy breach
    if any(phrase in q for phrase in ["transactions have unusually high discounts", "unusually high discounts", "fraud", "policy breach", "high discount transactions"]):
        if high_discount_transactions:
            lines = [f"Tran {t['tran_id']}: {t['avg_discount']}% avg discount (value {t['value']:,.2f})" for t in high_discount_transactions[:10]]
            return "**Fraud / policy breach**\n\nTransactions with unusually high discounts (>30% avg): " + "; ".join(lines) + ". Review for policy compliance and potential abuse."
        return "No transactions with unusually high discounts found."
    # 41. Zones showing declining trends -> Early warning
    if any(phrase in q for phrase in ["zones show declining trends", "which zones show declining", "early warning", "zones declining"]):
        if zones_declining:
            declining_sorted = sorted(zones_declining, key=lambda x: x.get("growth_pct", 0))
            lines = [f"{z['zone']}: {z['growth_pct']}% MoM" for z in declining_sorted[:8]]
            return "**Early warning**\n\nZones showing declining sales trends (negative MoM): " + "; ".join(lines) + ". Prioritize intervention in these territories."
        return "No zones with declining trends in the current period."

    # 42. Average transaction value -> Sales productivity
    if any(phrase in q for phrase in ["average transaction value", "avg transaction value", "sales productivity"]):
        if avg_transaction_value is not None:
            return f"**Sales productivity**\n\nAverage transaction value: {avg_transaction_value:,.2f}."
        return "Average transaction value is not available."

    # 43. Transactions per month -> Demand pattern
    if any(phrase in q for phrase in ["how many transactions occur per month", "transactions per month", "demand pattern"]):
        if transactions_per_month:
            lines = [f"{m['month']}: {m['transactions']:,}" for m in transactions_per_month[-12:]]
            return "**Demand pattern**\n\nTransactions per month (recent): " + "; ".join(lines)
        return "Monthly transaction counts are not available."

    # 44. Average quantity per transaction -> Order size
    if any(phrase in q for phrase in ["average quantity per transaction", "avg quantity per transaction", "order size"]):
        if avg_qty_per_transaction is not None:
            return f"**Order size**\n\nAverage quantity per transaction: {avg_qty_per_transaction:,.2f} units."
        return "Average quantity per transaction is not available."

    # 45. Regions generating most transactions -> Market activity
    if any(phrase in q for phrase in ["regions generate the most transactions", "most transactions", "market activity", "transactions by region"]):
        if transactions_by_zone:
            lines = [f"{r['zone']}: {r['transactions']:,} transactions" for r in transactions_by_zone[:8]]
            return "**Market activity**\n\nRegions (zones) generating the most transactions: " + "; ".join(lines)
        return "Region-wise transaction counts are not available."

    # 46. Explain why revenue dropped last month -> Root cause analysis
    if any(phrase in q for phrase in ["explain why revenue dropped last month", "why revenue dropped last month", "root cause analysis"]):
        if not revenue_mom or revenue_mom.get("mom_pct") is None:
            return "**Root cause analysis**\n\nI need at least two months of valid date data to explain the drop."
        lm = revenue_mom.get("latest_month")
        pm = revenue_mom.get("prev_month")
        mom = revenue_mom.get("mom_pct")
        direction = "dropped" if mom < 0 else "increased"
        lines = [f"Revenue {direction} {abs(mom)}% ({pm}: {revenue_mom.get('prev_value', 0):,.2f} → {lm}: {revenue_mom.get('latest_value', 0):,.2f})."]
        if mom < 0:
            if root_cause_zone_deltas:
                z = [f"{x['zone']}: {x['delta_value']:,.2f}" for x in root_cause_zone_deltas[:5]]
                lines.append("Top zone contributors to the change: " + "; ".join(z))
            if root_cause_state_deltas:
                s = [f"{x['state']}: {x['delta_value']:,.2f}" for x in root_cause_state_deltas[:5]]
                lines.append("Top state contributors to the change: " + "; ".join(s))
            if root_cause_product_deltas:
                p = [f"{x['product']}: {x['delta_value']:,.2f}" for x in root_cause_product_deltas[:6]]
                lines.append("Top product movers: " + "; ".join(p))
        else:
            lines.append("Revenue did not drop in the latest month based on the data.")
        return "**Root cause analysis**\n\n" + "\n".join(lines)

    # 47. Forecast next month's sales -> Time series prediction
    if any(phrase in q for phrase in ["forecast next month's sales", "forecast next month sales", "time series prediction", "predict next month's sales", "revenue forecast for next month", "forecast for next month"]):
        if not trends or len(trends) < 4:
            return "**Time series prediction**\n\nNot enough monthly history to forecast (need at least 4 months)."
        # Simple linear trend on last N months (works offline; no external model)
        series = [float(t.get("value") or 0) for t in trends if t.get("value") is not None]
        series = series[-12:] if len(series) > 12 else series
        n = len(series)
        if n < 4:
            return "**Time series prediction**\n\nNot enough monthly history to forecast."
        xs = list(range(n))
        x_mean = sum(xs) / n
        y_mean = sum(series) / n
        denom = sum((x - x_mean) ** 2 for x in xs)
        slope = (sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, series)) / denom) if denom else 0
        intercept = y_mean - slope * x_mean
        forecast = intercept + slope * n
        forecast = max(0, forecast)
        last_month = (trends[-1].get("month") if trends else None) or "latest month"
        last_val = series[-1] if series else 0
        return (
            "**Time series prediction**\n\n"
            f"Forecast for next month (based on linear trend over last {n} months): {forecast:,.2f}.\n"
            f"Latest month ({last_month}) sales: {last_val:,.2f}."
        )

    # 48. Identify underperforming territories -> Territory analytics
    if any(phrase in q for phrase in ["identify underperforming territories", "underperforming territories", "territory analytics", "which territories are underperforming", "territories are underperforming"]):
        if underperforming_zones:
            lines = [f"{z['zone']}: {z['growth_pct']}% MoM (latest value {z.get('last_value', 0):,.2f})" for z in underperforming_zones[:8]]
            return "**Territory analytics**\n\nUnderperforming zones (negative MoM): " + "; ".join(lines)
        return "**Territory analytics**\n\nNo zones show negative MoM growth in the current period."

    # 49. Suggest products to promote -> AI recommendation
    if any(phrase in q for phrase in ["suggest products to promote", "products to promote", "ai recommendation", "recommend products to promote", "which products should we promote", "products should we promote"]):
        # Use growth SKUs, filter out high return-rate and extremely discounted products if we have info
        candidates = []
        for g in (product_growth_skus or []):
            pname = g.get("product")
            if not pname:
                continue
            meta = product_risk_pricing_map.get(pname) or {}
            rr = meta.get("return_rate_pct")
            disc = meta.get("avg_discount")
            share = meta.get("sales_pct")
            # Prefer: high growth, reasonable returns, not already huge share
            if rr is not None and rr > 5:
                continue
            candidates.append({
                "product": pname,
                "growth_pct": g.get("growth_pct"),
                "return_rate_pct": rr,
                "avg_discount": disc,
                "sales_pct": share,
            })
        candidates.sort(key=lambda x: (x.get("growth_pct") or 0, -(x.get("sales_pct") or 0)), reverse=True)
        top = candidates[:8]
        if top:
            lines = []
            for c in top:
                extra = []
                if c.get("return_rate_pct") is not None:
                    extra.append(f"returns {c['return_rate_pct']}%")
                if c.get("avg_discount") is not None:
                    extra.append(f"avg discount {c['avg_discount']}%")
                if c.get("sales_pct") is not None:
                    extra.append(f"share {c['sales_pct']}%")
                lines.append(f"{c['product']}: {c.get('growth_pct')}% MoM" + (f" ({', '.join(extra)})" if extra else ""))
            return "**AI recommendation**\n\nSuggested products to promote (growth + risk/pricing filters): " + "; ".join(lines)
        return "**AI recommendation**\n\nI couldn't compute product recommendations (need product growth + pricing/returns data)."

    # 50. Predict customer churn -> Retention model
    if any(phrase in q for phrase in ["predict customer churn", "customer churn", "retention model", "predict churn"]):
        if churn_risk_candidates:
            lines = [f"{c['customer']}: {c['reason']} (prev {c['prev_value']:,.2f} → latest {c['latest_value']:,.2f}, change {c['drop_pct']}%)" for c in churn_risk_candidates[:10]]
            return "**Retention model**\n\nChurn risk signals (heuristic, from last 2 months): " + "; ".join(lines)
        return "**Retention model**\n\nNo churn risk candidates detected from last-2-month activity (or insufficient date history)."

    # --- Quick “operator” prompts (exact-style questions) ---
    # 1) Today's sales numbers
    if any(phrase in q for phrase in ["today's sales numbers", "todays sales numbers", "today sales numbers", "today's sales"]):
        if today_sales:
            return (
                "**Today's sales numbers**\n\n"
                f"Date (latest in data): {today_sales.get('date')}.\n"
                f"Net sales value: {today_sales.get('net_sales_value', 0):,.2f}.\n"
                f"Net sales quantity: {today_sales.get('net_sales_qty', 0):,}.\n"
                f"Transactions: {today_sales.get('transactions', 0):,}."
            )
        return "**Today's sales numbers**\n\nDaily sales could not be computed (no valid `DOC_DATE` parsed)."

    # 2) Which products are driving revenue?
    if any(phrase in q for phrase in ["which products are driving revenue", "products driving revenue"]):
        if products:
            lines = [f"{p['name']}: {p['value']:,.2f} ({p['pct']}%)" for p in products[:10]]
            return "**Products driving revenue**\n\nTop products by revenue: " + "; ".join(lines)
        return "**Products driving revenue**\n\nNo product revenue data available."

    # 3) Which states contribute the most revenue?
    if any(phrase in q for phrase in ["which states contribute the most revenue", "states contribute the most revenue"]):
        states = conc.get("top_states") or []
        if states:
            lines = [f"{s['name']}: {s['value']:,.2f} ({s['pct']}%)" for s in states[:10]]
            return "**States contributing most revenue**\n\nTop states: " + "; ".join(lines)
        return "**States contributing most revenue**\n\nNo state revenue data available."

    # 4) Which customers contribute the most sales?
    if any(phrase in q for phrase in ["which customers contribute the most sales", "customers contribute the most sales"]):
        if top_customers:
            lines = [f"{c['customer']}: {c['value']:,.2f}" for c in top_customers[:10]]
            return "**Top customers by sales**\n\nCustomers contributing the most sales: " + "; ".join(lines)
        return "**Top customers by sales**\n\nNo customer sales data available."

    # 5) Which products are declining in sales?
    if any(phrase in q for phrase in ["which products are declining in sales", "products declining in sales"]):
        if declining_products:
            lines = [f"{p['product']}: {p['growth_pct']}% MoM (prev {p['prev_value']:,.2f} → latest {p['latest_value']:,.2f})" for p in declining_products[:10]]
            return "**Declining products**\n\nProducts declining month-over-month: " + "; ".join(lines)
        return "**Declining products**\n\nNo declining products detected (or insufficient month history)."

    # 6) Return rate this month?
    if any(phrase in q for phrase in ["what is the return rate this month", "return rate this month"]):
        if return_rate_latest_month:
            return (
                "**Return rate (this month)**\n\n"
                f"Month: {return_rate_latest_month.get('month')}.\n"
                f"Return rate: {return_rate_latest_month.get('return_rate_pct', 0)}%.\n"
                f"Returns value: {return_rate_latest_month.get('returns_abs', 0):,.2f}.\n"
                f"Gross sales: {return_rate_latest_month.get('gross', 0):,.2f}."
            )
        return "**Return rate (this month)**\n\nReturn rate could not be computed."

    # 7) Zones growing fastest
    if any(phrase in q for phrase in ["which zones are growing fastest", "zones are growing fastest"]):
        if zones_growing_fastest:
            lines = [f"{z['zone']}: {z['growth_pct']}% MoM" for z in zones_growing_fastest[:8]]
            return "**Fastest-growing zones**\n\nZones growing fastest month-over-month: " + "; ".join(lines)
        return "**Fastest-growing zones**\n\nNo zone growth data available."

    # 8) Products with highest discounts
    if any(phrase in q for phrase in ["which products have the highest discounts", "products have the highest discounts"]):
        if product_highest_discount:
            lines = [f"{p['name']}: {p['avg_discount']}% avg discount (value {p['total_value']:,.2f})" for p in product_highest_discount[:10]]
            return "**Highest product discounts**\n\nProducts with the highest discounts: " + "; ".join(lines)
        return "**Highest product discounts**\n\nNo product discount data available."

    # 9) Customers at risk of churn
    if any(phrase in q for phrase in ["which customers are at risk of churn", "customers at risk of churn"]):
        if churn_risk_candidates:
            lines = [f"{c['customer']}: {c['reason']} (prev {c['prev_value']:,.2f} → latest {c['latest_value']:,.2f}, change {c['drop_pct']}%)" for c in churn_risk_candidates[:10]]
            return "**Customers at risk of churn**\n\n" + "; ".join(lines)
        return "**Customers at risk of churn**\n\nNo churn risk candidates detected (or insufficient month history)."

    # 10) Revenue forecast next month
    if any(phrase in q for phrase in ["what is the revenue forecast for next month", "revenue forecast for next month"]):
        # Delegate to the forecast handler by reusing existing computed logic above
        # (fall through to #47 if user uses either phrasing)
        pass

    # 11) Products should we promote
    if any(phrase in q for phrase in ["which products should we promote", "products should we promote"]):
        # Delegate to existing recommendation handler (#49) via fallthrough
        pass

    # 12) Territories underperforming
    if any(phrase in q for phrase in ["which territories are underperforming", "territories are underperforming"]):
        # Delegate to existing territory analytics handler (#48) via fallthrough
        pass

    # 13) Top 10 cities by sales
    if any(phrase in q for phrase in ["what are the top 10 cities by sales", "top 10 cities by sales"]):
        if top_cities_by_sales:
            lines = [f"{c['city']}: {c['value']:,.2f}" for c in top_cities_by_sales]
            return "**Top 10 cities by sales**\n\n" + "; ".join(lines)
        return "**Top 10 cities by sales**\n\nNo city sales data available."

    # 14) Which brands perform best?
    if any(phrase in q for phrase in ["which brands perform best", "brands perform best"]):
        if top_brands_by_sales:
            lines = [f"{b['brand']}: {b['value']:,.2f}" for b in top_brands_by_sales]
            return "**Top brands**\n\nBest-performing brands by revenue: " + "; ".join(lines)
        return "**Top brands**\n\nNo brand sales data available."

    # 15) Revenue distribution across divisions
    if any(phrase in q for phrase in ["show revenue distribution across divisions", "revenue distribution across divisions"]):
        if division_distribution:
            lines = [f"{d['division']}: {d['value']:,.2f} ({d['pct']}%)" for d in division_distribution[:10]]
            return "**Division revenue distribution**\n\nRevenue distribution across divisions: " + "; ".join(lines)
        return "**Division revenue distribution**\n\nNo division distribution data available."

    # Overview / KPIs (legacy)
    if any(w in q for w in ["net sales", "total sales", "revenue", "sales value"]):
        v = ov.get("net_sales_value")
        if v is not None:
            return f"Net sales value is {v:,.2f}. Gross sales is {ov.get('gross_sales_value', 0):,.2f} and returns are {ov.get('returns_value', 0):,.2f} ({ov.get('returns_rate_pct', 0)}% return rate)."
    if any(w in q for w in ["customer", "customers", "how many customer"]):
        n = ov.get("total_customers")
        if n is not None:
            return f"There are {n:,} unique customers in the data."
    if any(w in q for w in ["transaction", "transactions"]):
        n = ov.get("total_transactions")
        if n is not None:
            return f"Total number of transactions is {n:,}."
    if any(w in q for w in ["product", "products", "how many product"]):
        n = ov.get("total_products")
        if n is not None:
            return f"There are {n:,} unique products."
    if any(w in q for w in ["return", "returns", "return rate"]):
        r = ov.get("returns_value")
        rate = ov.get("returns_rate_pct")
        if r is not None:
            return f"Returns value is {r:,.2f} and the return rate is {rate}% of gross sales."
    if any(w in q for w in ["quantity", "qty", "units sold"]):
        n = ov.get("net_sales_qty")
        if n is not None:
            return f"Net sales quantity (units sold) is {n:,}."

    # Geography
    if any(w in q for w in ["zone", "zones", "region", "geography"]):
        zones = conc.get("zones") or []
        if zones:
            lines = [f"{z['name']}: {z['pct']}%" for z in zones[:6]]
            return "Sales by zone: " + "; ".join(lines)
        return "No zone breakdown available."
    if any(w in q for w in ["state", "states", "top state"]):
        states = conc.get("top_states") or []
        if states:
            lines = [f"{s['name']} ({s['pct']}%)" for s in states[:5]]
            return "Top states by sales: " + "; ".join(lines)
        return "No state breakdown available."

    # Trends
    if any(w in q for w in ["trend", "monthly", "growth", "mom", "month"]):
        if len(trends) >= 2:
            last = trends[-1]
            prev = trends[-2]
            return f"Latest month {last['month']}: net sales {last['value']:,.2f}" + (f" ({last.get('growth_pct')}% MoM growth)" if last.get("growth_pct") is not None else "") + f". Previous month {prev['month']}: {prev['value']:,.2f}."
        return "Insufficient trend data."
    # Top products
    if any(w in q for w in ["top product", "best product", "leading product"]):
        if products:
            lines = [f"{p['name']} ({p['pct']}%)" for p in products[:5]]
            return "Top products by sales: " + "; ".join(lines)
        return "No product breakdown in context. Ask about product intelligence for more detail."
    # Customer concentration
    if any(w in q for w in ["customer concentration", "top customer", "stop business"]):
        if isinstance(cust, dict):
            return f"Top 10 customers represent {cust.get('top_10_pct')}% of sales. Total customers: {cust.get('total_customers')}. Customers with stop business flag: {cust.get('stop_business_count')}."
        return "Customer analytics not in current context."
    # Pricing
    if any(w in q for w in ["discount", "pricing", "price"]):
        if isinstance(pricing, dict):
            return f"Average discount across the data is {pricing.get('avg_discount')}%."
        return "Pricing data not in current context."
    # Risk
    if any(w in q for w in ["risk", "governance", "concentration"]):
        if isinstance(risk, dict):
            return (
                f"Risk indicators: returns rate {risk.get('returns_rate')}%; "
                f"negative line % {risk.get('negative_line_pct')}; zone concentration {risk.get('zone_concentration_pct')}%; "
                f"top 10 customer concentration {risk.get('top_10_customer_pct')}%; stop business count {risk.get('stop_business_count')}."
            )
        return "Risk data not in current context."

    # Generic: return a short summary so the user gets value
    return (
        "Here’s a quick summary from the data: " + ctx[:600]
        + ("..." if len(ctx) > 600 else "")
        + " You can ask: total sales revenue, monthly trend, top states, top products, underperforming zones, return rate, top customers, fastest-growing brands, Q1 vs Q2, top SKUs, product return rate, growth SKUs, average selling price, product penetration, products dependent on few states, state ranking, sales distribution across zones, cities fastest growth, state return rates, zones declining sales, revenue per customer by state, key accounts, customer attrition, distributor risk, cross-selling opportunity, pareto 80% revenue, customers growing fastest, average discount, products highest discounts, regions lowest price realization, price realization vs list, customers highest discounts, percentage sales from promoted, promoted products revenue, best promoted products, regions respond best to promotions, states unusually high return rates, products depend on single state, customers over 10% revenue, high discount transactions, zones declining trends, average transaction value, transactions per month, average quantity per transaction, or regions with most transactions."
    )


@api_router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Answer a question about the sales data and insights. Uses real data from the database."""
    question = (request.message or "").strip()
    if not question:
        return ChatResponse(answer="Please ask a question about the sales data or insights.")

    # Fast-path: Risk & Governance anomaly questions should answer from cached anomalies
    # without building the full (slow) chat summary.
    ql = question.lower()
    is_risk_anomaly_q = any(
        s in ql
        for s in [
            "sudden high increase",
            "sudden increase in sales",
            "zone spike",
            "zones with sudden",
            "product with high sales in recent",
            "recent months than in history",
            "product surge",
            "non promoted products vs",
            "non-promoted vs promoted",
            "promoted product sales trend",
            "promoted vs non",
            "price variance outliers",
            "price outlier",
            "ppu outlier",
            "same product",
            "same zone",
            "division code",
            "division zone sales outlier",
            "division-zone outlier",
        ]
    )

    if is_risk_anomaly_q and build_copilot_response and format_copilot_response:
        try:
            anomalies_payload = await get_risk_anomalies()
            fast_summary = {"risk_anomalies_payload": anomalies_payload}
            structured = build_copilot_response(question, fast_summary)
            if structured:
                return ChatResponse(answer=_normalize_dashes(format_copilot_response(structured)))
        except Exception as e:
            logger.error(f"Risk anomaly fast-path error: {e}")

    summary = {"overview": None, "trends": [], "concentration": None}
    try:
        summary = await _get_chat_data_summary()
    except Exception as e:
        logger.error(f"Error fetching data for chat: {e}")

    # AI Sales Copilot: intent-based structured response for 150-prompt library
    if build_copilot_response and format_copilot_response:
        try:
            structured = build_copilot_response(question, summary)
            if structured:
                return ChatResponse(answer=_normalize_dashes(format_copilot_response(structured)))
        except Exception as e:
            logger.error(f"Copilot response error: {e}")

    # Optional: use LLM if available (same pattern as insights)
    try:
        try:
            from emergentintegrations.llm.chat import LlmChat, UserMessage
        except ImportError:
            return ChatResponse(answer=_answer_question_from_data(question, summary))

        api_key = os.environ.get("EMERGENT_LLM_KEY")
        if not api_key or str(api_key).strip().startswith("sk-your-"):
            return ChatResponse(answer=_answer_question_from_data(question, summary))

        context_text = _summary_to_context_text(summary)
        system_prompt = (
            "You are a helpful sales analytics assistant. Answer the user's question using ONLY the following data context. "
            "Be concise and cite numbers. If the data does not contain the answer, say so briefly."
        )
        user_prompt = f"Data context:\n{context_text}\n\nUser question: {question}"

        chat = LlmChat(api_key=api_key, session_id=str(uuid.uuid4()), system_message=system_prompt).with_model("openai", "gpt-4o")
        response = await chat.send_message(UserMessage(text=user_prompt))
        answer = str(response).strip() if response else ""
        if answer:
            return ChatResponse(answer=_normalize_dashes(answer))
    except Exception as e:
        logger.error(f"Error in chat LLM: {e}")

    return ChatResponse(answer=_answer_question_from_data(question, summary))


@api_router.post("/insights/generate", response_model=InsightResponse)
async def generate_insights(request: InsightRequest):
    """Generate dashboard-specific Key Insights, Recommendations, and Action Items from real data. Pass dashboard name for page-specific insights."""
    import json
    import re
    dashboard = (request.dashboard or request.context or "").strip() or None
    if request.context and not dashboard:
        dashboard = request.context
    cache_key = f"insights:post:{dashboard or 'Executive Summary'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return InsightResponse(**cached)
    persisted = await _persist_cache_get(cache_key)
    if persisted is not None:
        _cache_set(cache_key, persisted, ttl_seconds=180)
        return InsightResponse(**persisted)
    data_summary = {"overview": None, "trends": [], "concentration": None}
    try:
        data_summary = await _get_insights_data_summary(dashboard)
    except Exception as e:
        logger.error(f"Error fetching insights data: {e}")

    context = request.context or (dashboard or "Dashboard Analysis")

    # Prefer Hugging Face Sales Insight Engine when configured.
    hf_resp = await _try_hf_sales_insight_engine(dashboard or "Executive Summary", data_summary)
    if hf_resp is not None:
        if data_summary.get("overview") is not None:
            _cache_set(cache_key, hf_resp.model_dump(), ttl_seconds=180)
            await _persist_cache_set(cache_key, hf_resp.model_dump())
        return hf_resp

    try:
    try:
        from emergentintegrations.llm.chat import LlmChat, UserMessage
        except ImportError:
            rb = _rule_based_insights_by_dashboard(dashboard or "Executive Summary", data_summary)
            _cache_set(cache_key, rb.model_dump(), ttl_seconds=180)
            await _persist_cache_set(cache_key, rb.model_dump())
            return rb
        
        api_key = os.environ.get("EMERGENT_LLM_KEY")
        if not api_key or str(api_key).strip().startswith("sk-your-"):
            rb = _rule_based_insights_by_dashboard(dashboard or "Executive Summary", data_summary)
            _cache_set(cache_key, rb.model_dump(), ttl_seconds=180)
            await _persist_cache_set(cache_key, rb.model_dump())
            return rb

        data_for_prompt = json.dumps({
            "context": context,
            "dashboard": dashboard,
            "overview": data_summary.get("overview"),
            "trends": data_summary.get("trends"),
            "concentration": data_summary.get("concentration"),
            "products": data_summary.get("products"),
            "customers": data_summary.get("customers"),
            "pricing": data_summary.get("pricing"),
            "incentives": data_summary.get("incentives"),
            "risk_indicators": data_summary.get("risk_indicators"),
            "risk_anomalies": data_summary.get("risk_anomalies"),
        }, indent=2)
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
            parsed_resp = _insight_response(parsed.get("insights", []), parsed.get("recommendations", []), parsed.get("action_items", []))
            _cache_set(cache_key, parsed_resp.model_dump(), ttl_seconds=180)
            await _persist_cache_set(cache_key, parsed_resp.model_dump())
            return parsed_resp
    except Exception as e:
        logger.error(f"Error generating insights: {e}")

    rb = _rule_based_insights_by_dashboard(dashboard or "Executive Summary", data_summary)
    if data_summary.get("overview") is not None:
        _cache_set(cache_key, rb.model_dump(), ttl_seconds=180)
        await _persist_cache_set(cache_key, rb.model_dump())
    return rb


@api_router.get("/insights/generate", response_model=InsightResponse)
async def generate_insights_get(
    dashboard: Optional[str] = Query(None, description="Dashboard name for page-specific insights"),
    force: bool = Query(False, description="If true, bypass cache and recompute from latest data."),
):
    """GET: generate dashboard-specific Key Insights, Recommendations, and Action Items. Pass ?dashboard=... for page-specific insights."""
    cache_key = f"insights:get:{dashboard or 'Executive Summary'}"
    if not force:
        cached = _cache_get(cache_key)
        if cached is not None:
            return InsightResponse(**cached)
        persisted = await _persist_cache_get(cache_key)
        if persisted is not None:
            _cache_set(cache_key, persisted, ttl_seconds=180)
            return InsightResponse(**persisted)

    data_summary = {"overview": None, "trends": [], "concentration": None}
    try:
        data_summary = await _get_insights_data_summary(dashboard)
    except Exception as e:
        logger.error(f"Error fetching insights data: {e}")

    # Prefer Hugging Face Sales Insight Engine when configured.
    hf_resp = await _try_hf_sales_insight_engine(dashboard or "Executive Summary", data_summary)
    resp = hf_resp or _rule_based_insights_by_dashboard(dashboard or "Executive Summary", data_summary)
    # Only cache if we successfully built a real summary; avoid caching "0.00%" fallbacks during transient DB reconnects.
    if data_summary.get("overview") is not None:
        _cache_set(cache_key, resp.model_dump(), ttl_seconds=180)
        await _persist_cache_set(cache_key, resp.model_dump())
    return resp


@api_router.post("/sales-insight-engine/generate", response_model=InsightResponse)
async def sales_insight_engine_generate(req: SalesInsightEngineRequest):
    """
    Hugging Face powered insight generation.
    Requires env var HF_TOKEN. Uses same dashboard data summary as built-in insights.
    """
    if SalesInsightEngine is None:
        raise HTTPException(status_code=500, detail="SalesInsightEngine not available in this build.")
    if not os.environ.get("HF_TOKEN"):
        raise HTTPException(status_code=400, detail="HF_TOKEN is not set on the backend.")

    dashboard = (req.dashboard or "").strip() or "Executive Summary"
    cache_key = f"hf_insights:{dashboard}"
    if not req.force:
        cached = _cache_get(cache_key)
        if cached is not None:
            return InsightResponse(**cached)

    data_summary = await _get_insights_data_summary(dashboard)
    engine = SalesInsightEngine()
    try:
        out = engine.generate(dashboard=dashboard, data_summary=data_summary)
    except Exception as e:
        logger.error(f"HF insight engine error: {e}")
        raise HTTPException(status_code=500, detail="HF insight generation failed.")

    resp = InsightResponse(
        insights=out.get("insights") or [],
        recommendations=out.get("recommendations") or [],
        action_items=out.get("action_items") or [],
    )
    if not _hf_insight_response_usable(resp):
        rb = _rule_based_insights_by_dashboard(dashboard, data_summary)
        _cache_set(cache_key, rb.model_dump(), ttl_seconds=300)
        return rb
    _cache_set(cache_key, resp.model_dump(), ttl_seconds=300)
    return resp


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


@app.on_event("startup")
async def startup_warm_cache():
    # Run warm-up in background to avoid blocking API startup.
    asyncio.create_task(_ensure_persist_cache_indexes())
    asyncio.create_task(_warm_core_caches())

# CORS: allow frontend domain; CORS_ORIGINS env can add more (comma-separated)
_cors_origins_raw = os.environ.get('CORS_ORIGINS', '*').strip()
if _cors_origins_raw == '*':
    _cors_origins = ['*']
else:
    _cors_origins = [o.strip() for o in _cors_origins_raw.split(',') if o.strip()]
    _sales_domain = 'https://sales.demo.agrayianailabs.com'
    if _sales_domain not in _cors_origins:
        _cors_origins.append(_sales_domain)
    if 'http://sales.demo.agrayianailabs.com' not in _cors_origins:
        _cors_origins.append('http://sales.demo.agrayianailabs.com')

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=_cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
