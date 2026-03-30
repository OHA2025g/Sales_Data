"""
AI Sales Copilot response mechanism for the 150-prompt library.
Maps user questions to intent, fetches real data from summary, and returns
structured responses (Answer, Key Metrics, Breakdown, Insight, Suggested Drilldowns, Best Visual).
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

# Synonym map: user phrase -> canonical (for optional normalization)
SYNONYM_MAP = {
    "sales revenue": "net sales value",
    "total sales": "net sales value",
    "billing": "gross sales value",
    "returns %": "return rate",
    "order value": "avg transaction value",
    "top regions": "top zones",
    "best-selling products": "top products by revenue",
    "product mix": "division contribution",
    "customer concentration": "top customer share",
    "promoted sales": "promoted revenue share",
    "pricing discipline": "price realization",
    "discount leakage": "high discount",
    "churn risk": "stop business",
}

# Intent patterns: (list of phrase substrings, intent_key). Order matters: more specific first.
INTENT_PATTERNS: List[Tuple[List[str], str]] = [
    # Risk & Governance anomaly queries (fast, from /risk/anomalies) - keep at top to win ties
    (["regions and zones with sudden high increase", "zones with sudden high increase", "sudden high increase in sales", "sudden spike in sales", "zone spikes", "zone spike"], "risk_zone_spikes"),
    (["product with high sales in recent months", "products with high sales in recent months", "high sales in recent months than in history", "recent months than in history", "product surges", "product surge"], "risk_product_surges"),
    (["non promoted products vs promoted product sales trend", "non promoted vs promoted sales trend", "non-promoted vs promoted sales trend", "promoted vs non promoted sales trend", "promoted vs non-promoted sales trend", "promoted/non promoted sales trend"], "risk_promo_trend"),
    (["same product , same zone, price variance outliers", "same product, same zone, price variance outliers", "price variance outliers", "ppu outliers", "price outlier"], "risk_price_outliers"),
    (["division code, zone sales outlier", "division code zone sales outlier", "division-zone sales outlier", "division × zone sales outlier", "division zone outlier"], "risk_division_zone_outliers"),

    # Executive Performance (1-20)
    (["total net sales revenue", "total net sales", "net sales revenue for the selected period"], "total_net_sales"),
    (["monthly revenue trends", "show monthly revenue", "monthly trend"], "monthly_revenue_trend"),
    (["compare revenue between q1 and q2", "q1 and q2", "q1 vs q2"], "compare_q1_q2"),
    (["overall return rate", "return rate", "returns rate"], "return_rate"),
    (["average transaction value", "avg transaction value", "order value"], "avg_transaction_value"),
    (["zones generate the highest revenue", "which zones", "highest revenue zone"], "top_zones_revenue"),
    (["states contribute the most", "which states", "states contribute most sales"], "top_states_revenue"),
    (["products contribute the most revenue", "which products contribute", "top products by revenue"], "top_products_revenue"),
    (["percentage of revenue from top 10 customers", "top 10 customers share", "revenue from top 10"], "top10_customer_share"),
    (["regions showing declining sales", "declining sales", "which regions declining"], "regions_declining"),
    (["revenue distribution by division", "division distribution", "by division"], "revenue_by_division"),
    (["growth rate compared to previous month", "growth rate", "mom growth"], "growth_rate_mom"),
    (["brand generates the most revenue", "top brand", "which brand"], "top_brand_revenue"),
    (["zone has the highest growth rate", "highest growth zone", "zone growth"], "zone_highest_growth"),
    (["total quantity sold this month", "quantity sold", "total quantity"], "total_quantity_sold"),
    (["revenue per customer", "revenue per customer"], "revenue_per_customer"),
    (["products dominate the portfolio", "dominant products", "portfolio dominance"], "products_dominate"),
    (["regions are underperforming", "underperforming regions", "underperforming"], "underperforming_regions"),
    (["top 10 cities by sales", "top cities", "cities by sales"], "top_cities_sales"),
    (["revenue from promoted products", "promoted products revenue", "how much from promoted"], "promoted_revenue"),
    # Revenue & Growth (21-40)
    (["revenue growth month-over-month", "mom revenue", "month over month growth"], "revenue_mom_growth"),
    (["states have the fastest revenue growth", "fastest growth states", "state growth"], "states_fastest_growth"),
    (["products show declining sales", "declining products", "products declining"], "products_declining"),
    (["revenue trend by product", "product revenue trend"], "revenue_trend_product"),
    (["brands are growing the fastest", "fastest growing brands", "brand growth"], "brands_fastest_growth"),
    (["revenue trend across zones", "zone revenue trend"], "revenue_trend_zone"),
    (["product revenue growth over time", "product growth over time"], "product_revenue_growth"),
    (["city has the highest sales growth", "city highest growth", "city growth"], "city_highest_growth"),
    (["contribution of each zone to growth", "zone contribution to growth"], "zone_contribution_growth"),
    (["quarterly revenue growth", "quarterly growth rate"], "quarterly_growth"),
    (["customers are driving growth", "customers driving growth"], "customers_driving_growth"),
    (["revenue contribution by product brand", "contribution by brand"], "revenue_contribution_brand"),
    # Product Intelligence (41-60)
    (["products generate the highest sales revenue", "highest sales revenue products"], "products_highest_revenue"),
    (["products generate the highest sales volume", "highest volume", "sales volume products"], "products_highest_volume"),
    (["products have the highest return rate", "highest return rate products", "product return rate"], "products_highest_return_rate"),
    (["products sold across the most states", "most states", "product state coverage"], "products_most_states"),
    (["average selling price for each product", "asp", "average selling price"], "avg_selling_price_product"),
    (["products have the highest discounts", "highest discount products"], "products_highest_discounts"),
    (["products are most profitable", "most profitable products"], "products_most_profitable"),
    (["products are growing fastest", "fastest growing products"], "products_growing_fastest"),
    (["products losing market share", "losing market share"], "products_losing_share"),
    (["products have the most stable sales", "stable sales products"], "products_stable_sales"),
    (["products depend on a single state", "product single state", "dependent on one state"], "products_single_state"),
    (["products sell the most in each zone", "product by zone"], "products_by_zone"),
    (["products generate the highest revenue per customer", "revenue per customer product"], "products_revenue_per_customer"),
    (["products have the highest price realization", "price realization product"], "products_price_realization"),
    (["products are sold across the most cities", "products most cities"], "products_most_cities"),
    # Geography (61-80)
    (["states generate the most revenue", "which states most revenue"], "states_most_revenue"),
    (["revenue distribution by state", "by state distribution"], "revenue_by_state"),
    (["zones contribute the most sales", "which zones contribute"], "zones_contribute_sales"),
    (["cities generate the highest sales volume", "cities sales volume"], "cities_highest_volume"),
    (["regions have the highest return rates", "high return regions"], "regions_highest_returns"),
    (["zones show the highest growth", "zone highest growth"], "zones_highest_growth"),
    (["states are underperforming", "underperforming states"], "states_underperforming"),
    (["cities have declining sales", "cities declining"], "cities_declining"),
    (["zones have the highest average selling price", "zone asp"], "zones_highest_asp"),
    (["states generate the highest revenue per customer", "state revenue per customer"], "states_revenue_per_customer"),
    (["cities have the most customers", "cities most customers"], "cities_most_customers"),
    (["zones have the highest product diversity", "zone product diversity"], "zones_product_diversity"),
    (["states have the lowest price realization", "state low realization"], "states_lowest_realization"),
    (["zones generate the highest sales per transaction", "zone sales per transaction"], "zones_sales_per_txn"),
    # Customer (81-100)
    (["top 10 customers by revenue", "who are the top 10 customers"], "top10_customers"),
    (["customers generate the highest sales volume", "customers highest volume"], "customers_highest_volume"),
    (["customers purchase the most products", "customers most products", "product variety customer"], "customers_most_products"),
    (["customers have the highest return rate", "customer return rate"], "customers_highest_return_rate"),
    (["customers receive the highest discounts", "customers highest discounts"], "customers_highest_discounts"),
    (["customers generate the highest revenue per transaction", "customer revenue per transaction"], "customers_revenue_per_txn"),
    (["customers have stopped doing business", "stop business", "stopped business"], "customers_stopped_business"),
    (["customers are growing fastest", "fastest growing customers"], "customers_growing_fastest"),
    (["customers contribute 80%", "pareto 80", "80% revenue customers"], "customers_pareto_80"),
    (["customers have declining purchase trends", "declining purchase", "customer declining"], "customers_declining_purchase"),
    (["customers buy the widest product variety", "widest variety", "product variety"], "customers_wide_variety"),
    (["customers have the highest transaction frequency", "transaction frequency customer"], "customers_transaction_frequency"),
    (["customers have the lowest return rates", "lowest return customers"], "customers_lowest_returns"),
    (["customers represent the biggest revenue risk", "biggest revenue risk customer"], "customers_revenue_risk"),
    # Pricing (101-115)
    (["average discount across products", "avg discount", "what is the average discount"], "avg_discount"),
    (["products have the highest discounts", "highest discount products"], "products_highest_discount_list"),
    (["regions have the highest discount levels", "region discount", "highest discount region"], "regions_highest_discount"),
    (["customers receive the largest discounts", "largest discount customers"], "customers_largest_discount"),
    (["price realization index", "price realization across", "realization index"], "price_realization_index"),
    (["states have the lowest price realization", "state lowest realization"], "states_low_realization"),
    (["products are sold above price list", "above list price", "at or above list"], "products_above_list_price"),
    (["transactions have unusually high discounts", "unusually high discount", "suspicious discount"], "high_discount_transactions"),
    (["average ppu by product", "ppu by product"], "avg_ppu_product"),
    (["regions maintain the best price discipline", "best price discipline"], "regions_price_discipline"),
    # Promotion (116-125)
    (["percentage of sales from promoted", "sales from promoted", "promoted sales share"], "promoted_sales_pct"),
    (["promoted products generate the most revenue", "top promoted products"], "promoted_products_revenue"),
    (["regions respond best to promotions", "respond best to promotions"], "regions_promotion_response"),
    (["products sell best without promotion", "without promotion", "non promoted"], "products_without_promotion"),
    (["revenue difference between promoted and non-promoted", "promoted vs non"], "promoted_vs_non_revenue"),
    (["customers buy promoted products most", "customers promoted most"], "customers_promoted_most"),
    (["zones rely heavily on promotions", "zones rely on promotions"], "zones_promotion_rely"),
    # Operational (126-135)
    (["average transaction value", "avg transaction value"], "avg_txn_value"),
    (["how many transactions each month", "transactions each month", "transaction count month"], "transactions_per_month"),
    (["average quantity per transaction", "quantity per transaction", "basket size"], "avg_qty_per_transaction"),
    (["regions have the highest transaction volume", "highest transaction volume"], "regions_transaction_volume"),
    (["products generate the most transactions", "most transactions product"], "products_most_transactions"),
    (["customers place the most orders", "most orders customer"], "customers_most_orders"),
    (["transaction growth trend", "transaction trend"], "transaction_growth_trend"),
    (["zones have the highest sales productivity", "sales productivity zone"], "zones_sales_productivity"),
    (["average basket size", "basket size"], "avg_basket_size"),
    (["products drive the largest order sizes", "largest order size"], "products_largest_orders"),
    # Risk (136-150)
    (["regions and zones with sudden high increase", "zone spikes", "zones with sudden", "sudden increase in sales", "sudden spike in sales", "zone spike"], "risk_zone_spikes"),
    (["product with high sales in recent months", "product surges", "products with high recent sales", "recent months than in history", "surge vs history", "product surge"], "risk_product_surges"),
    (["non promoted products vs", "promoted vs non", "non-promoted vs promoted", "promoted vs non promoted", "promotion trend", "promoted/non promoted sales trend"], "risk_promo_trend"),
    (["same product", "same zone", "price variance outliers", "ppu outliers", "price outlier", "price variance"], "risk_price_outliers"),
    (["division code", "division × zone", "division zone sales outlier", "division-zone outlier", "division outlier"], "risk_division_zone_outliers"),
    (["states have unusually high return rates", "unusually high return state"], "states_high_returns"),
    (["products depend heavily on one region", "product depend one region"], "products_depend_one_region"),
    (["customers contribute more than 10%", "customers over 10%", "more than 10% revenue"], "customers_over_10_pct"),
    (["transactions have suspicious discounts", "suspicious discount"], "suspicious_discounts"),
    (["zones show sudden sales decline", "sudden decline zone"], "zones_sudden_decline"),
    (["products have volatile sales", "volatile sales product"], "products_volatile_sales"),
    (["customers show declining purchase trends", "customer declining trend"], "customers_declining_trends"),
    (["regions have unstable sales growth", "unstable growth region"], "regions_unstable_growth"),
    (["products have inconsistent pricing", "inconsistent pricing"], "products_inconsistent_pricing"),
    (["states show unusual revenue fluctuations", "unusual fluctuation state"], "states_unusual_fluctuation"),
    (["customers have increasing return rates", "increasing return rate customer"], "customers_increasing_returns"),
    (["zones overdependent on single product", "zone single product dependent"], "zones_single_product"),
    (["customers have stopped purchasing recently", "stopped purchasing", "churn"], "customers_stopped_recently"),
    (["products have the highest sales volatility", "highest volatility product"], "products_highest_volatility"),
    (["territories pose the biggest revenue risk", "biggest revenue risk", "territory risk"], "territories_revenue_risk"),
    # Fallbacks / generic
    (["show sales", "total sales", "sales value"], "total_net_sales"),
    (["top products", "best products"], "top_products_revenue"),
    (["growth", "revenue growth"], "revenue_mom_growth"),
    (["best region", "best zone", "top region"], "top_zones_revenue"),
    (["performance", "overall performance"], "total_net_sales"),
]


def _fmt_cr(v: float) -> str:
    """Format as Crores/Lakhs/K."""
    if v is None:
        return "-"
    if abs(v) >= 1e7:
        return f"₹{(v / 1e7):.2f} Cr"
    if abs(v) >= 1e5:
        return f"₹{(v / 1e5):.2f} L"
    if abs(v) >= 1e3:
        return f"₹{(v / 1e3):.1f} K"
    return f"₹{v:,.2f}"


def _match_intent(question: str) -> Optional[str]:
    q = (question or "").strip().lower()
    if not q:
        return None
    for patterns, intent_key in INTENT_PATTERNS:
        for p in patterns:
            if p in q:
                return intent_key
    return None


def _build_response(intent_key: str, summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Build structured response for the given intent from summary data."""
    ov = summary.get("overview") or {}
    conc = summary.get("concentration") or {}
    trends = summary.get("trends") or []
    products = summary.get("products") or []
    top_customers = summary.get("top_customers") or []
    quarterly = summary.get("quarterly") or {}
    brand_growth = summary.get("brand_growth") or []
    zones_sorted_low = summary.get("zones_sorted_low") or []
    zone_growth = summary.get("zone_growth") or []
    product_return_rates = summary.get("product_return_rates") or []
    product_growth_skus = summary.get("product_growth_skus") or []
    product_avg_price = summary.get("product_avg_price") or []
    product_geo_concentration = summary.get("product_geo_concentration") or []
    state_return_rates = summary.get("state_return_rates") or []
    city_growth = summary.get("city_growth") or []
    state_revenue_per_customer = summary.get("state_revenue_per_customer") or []
    stop_business_customers = summary.get("stop_business_customers") or []
    customer_return_rates = summary.get("customer_return_rates") or []
    customer_product_count = summary.get("customer_product_count") or []
    pareto_80_customers = summary.get("pareto_80_customers") or []
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
    underperforming_zones = summary.get("underperforming_zones") or []
    top_cities_by_sales = summary.get("top_cities_by_sales") or []
    top_brands_by_sales = summary.get("top_brands_by_sales") or []
    division_distribution = summary.get("division_distribution") or []
    declining_products = summary.get("declining_products") or []
    zones_growing_fastest = summary.get("zones_growing_fastest") or []
    churn_risk_candidates = summary.get("churn_risk_candidates") or []
    pareto_80_pct = summary.get("pareto_80_pct") or 0
    zones = conc.get("zones") or []
    top_states = conc.get("top_states") or []
    top_3_states_pct = conc.get("top_3_states_pct")
    top_10_customers_pct = conc.get("top_10_customers_pct")

    anomalies_payload = summary.get("risk_anomalies_payload") or {}

    def _top_rows(rows: Any, n: int = 8) -> List[Dict[str, Any]]:
        if not isinstance(rows, list):
            return []
        return [r for r in rows if isinstance(r, dict)][:n]

    # --- Risk anomaly Q&A (fast, from cached /risk/anomalies) ---
    if intent_key == "risk_zone_spikes":
        zone_spikes = _top_rows(anomalies_payload.get("zone_spikes"), 10)
        if not zone_spikes:
            return {
                "direct_answer": "No zone spike alerts found for the latest month vs previous month (or insufficient history).",
                "key_metrics": [],
                "breakdown": [],
                "insight": ["If you expect spikes, verify DOC_DATE coverage and Zone_New mapping in the source file."],
                "suggested_drilldowns": ["Zone → State → Product", "Month → Zone"],
                "best_visual": "Ranked table + bar chart",
            }
        topz = zone_spikes[0]
        return {
            "direct_answer": f"Top zone spike: {topz.get('zone','N/A')} is {float(topz.get('growth_pct') or 0):+.2f}% MoM in {topz.get('month','latest')} (value {_fmt_cr(float(topz.get('value') or 0))}).",
            "key_metrics": [f"{r.get('zone','N/A')}: {float(r.get('growth_pct') or 0):+.2f}% MoM ({_fmt_cr(float(r.get('value') or 0))})" for r in zone_spikes[:6]],
            "breakdown": [f"{r.get('zone','N/A')} | prev {_fmt_cr(float(r.get('prev_value') or 0))} → last {_fmt_cr(float(r.get('value') or 0))}" for r in zone_spikes[:6]],
            "insight": ["Validate whether spikes are one-time institutional orders, channel loading, pricing changes, or genuine demand expansion."],
            "suggested_drilldowns": ["Zone spike month → Top customers", "Zone spike month → Top products"],
            "best_visual": "Bar chart (MoM growth%) + table",
        }

    if intent_key == "risk_product_surges":
        surges = _top_rows(anomalies_payload.get("product_surges"), 10)
        if not surges:
            return {
                "direct_answer": "No product surge alerts found (recent 3-month average vs historical average).",
                "key_metrics": [],
                "breakdown": [],
                "insight": ["If you expect surges, ensure you have at least ~6–12 months of history in DOC_DATE."],
                "suggested_drilldowns": ["Product → Zone", "Product → Customer", "Product monthly trend"],
                "best_visual": "Ranked table",
            }
        top_p = surges[0]
        return {
            "direct_answer": f"Top product surge: {top_p.get('product','N/A')} is {float(top_p.get('uplift_pct') or 0):+.2f}% vs history (recent avg {_fmt_cr(float(top_p.get('recent_avg') or 0))}).",
            "key_metrics": [f"{r.get('product','N/A')}: {float(r.get('uplift_pct') or 0):+.2f}% (recent {_fmt_cr(float(r.get('recent_avg') or 0))} vs hist {_fmt_cr(float(r.get('hist_avg') or 0))})" for r in surges[:6]],
            "breakdown": [],
            "insight": ["Check if the surge is promotion-led, distribution expansion, stocking, or substitution from competing SKUs."],
            "suggested_drilldowns": ["Product → Zone (recent months)", "Product → Top customers (recent months)"],
            "best_visual": "Table + sparkline per product",
        }

    if intent_key == "risk_promo_trend":
        promo = anomalies_payload.get("promo_trend") or {}
        series = promo.get("series") if isinstance(promo, dict) else None
        if not isinstance(series, list) or not series:
            return {
                "direct_answer": "Promotion trend data is not available (missing promoted/non-promoted flag or insufficient dates).",
                "key_metrics": [],
                "breakdown": [],
                "insight": ["Confirm the column `Promoted/non promoted` exists and is populated consistently."],
                "suggested_drilldowns": ["Month → Promoted vs Non-promoted by zone", "Top promoted products"],
                "best_visual": "Stacked line / stacked area",
            }
        last = series[-1]
        return {
            "direct_answer": f"Latest month {last.get('month','latest')}: promoted {_fmt_cr(float(last.get('promoted_value') or 0))}, non-promoted {_fmt_cr(float(last.get('non_promoted_value') or 0))} (promo share {float(last.get('promo_share_pct') or 0):.2f}%).",
            "key_metrics": [f"{r.get('month')}: promo share {float(r.get('promo_share_pct') or 0):.2f}%" for r in series[-6:]],
            "breakdown": [f"{r.get('month')}: promoted {_fmt_cr(float(r.get('promoted_value') or 0))} | non-promoted {_fmt_cr(float(r.get('non_promoted_value') or 0))}" for r in series[-6:]],
            "insight": ["Rising promo share can signal discount-led growth; monitor price realization and returns impact."],
            "suggested_drilldowns": ["Promo share by zone", "Promo share by product"],
            "best_visual": "Two-line trend + promo share line",
        }

    if intent_key == "risk_price_outliers":
        outliers = _top_rows(anomalies_payload.get("price_outliers"), 12)
        if not outliers:
            return {
                "direct_answer": "No price variance outliers found for product×zone in the recent window.",
                "key_metrics": [],
                "breakdown": [],
                "insight": ["If you expect outliers, ensure `PPU` is present and numeric; also verify the recent-month window has enough transactions."],
                "suggested_drilldowns": ["Product×Zone → transaction list", "Customer pricing dispersion"],
                "best_visual": "Box plot per product×zone",
            }
        top_o = outliers[0]
        return {
            "direct_answer": f"Top price outlier: {top_o.get('product','N/A')} in {top_o.get('zone','N/A')} has high PPU variance (std {float(top_o.get('ppu_std') or 0):.2f}) over {int(top_o.get('n') or 0)} lines.",
            "key_metrics": [f"{r.get('product','N/A')} | {r.get('zone','N/A')}: std {float(r.get('ppu_std') or 0):.2f}, avg {float(r.get('ppu_avg') or 0):.2f}, n {int(r.get('n') or 0)}" for r in outliers[:6]],
            "breakdown": [],
            "insight": ["Investigate pricing overrides, customer-specific contracts, channel mix, and data entry issues."],
            "suggested_drilldowns": ["Product×Zone → Top customers", "Product×Zone → Invoice-level prices"],
            "best_visual": "Scatter/box plot",
        }

    if intent_key == "risk_division_zone_outliers":
        dz = _top_rows(anomalies_payload.get("division_zone_outliers"), 12)
        if not dz:
            return {
                "direct_answer": "No division×zone sales outliers found (recent vs history).",
                "key_metrics": [],
                "breakdown": [],
                "insight": ["If you expect outliers, verify `Div_Code (Mapping HQ)` values are populated."],
                "suggested_drilldowns": ["Division×Zone → Top products", "Division×Zone → Top customers"],
                "best_visual": "Heatmap",
            }
        top_d = dz[0]
        return {
            "direct_answer": f"Top division×zone outlier: {top_d.get('division','N/A')} × {top_d.get('zone','N/A')} is {float(top_d.get('uplift_pct') or 0):+.2f}% vs history.",
            "key_metrics": [f"{r.get('division','N/A')}×{r.get('zone','N/A')}: {float(r.get('uplift_pct') or 0):+.2f}% (recent {_fmt_cr(float(r.get('recent_avg') or 0))} vs hist {_fmt_cr(float(r.get('hist_avg') or 0))})" for r in dz[:6]],
            "breakdown": [],
            "insight": ["Validate if this is driven by new launches, coverage changes, tender wins, or distributor stocking."],
            "suggested_drilldowns": ["Division×Zone → Monthly trend", "Division×Zone → Product mix"],
            "best_visual": "Heatmap + table",
        }

    net_sales = ov.get("net_sales_value") or 0
    gross_sales = ov.get("gross_sales_value") or 0
    returns_value = ov.get("returns_value") or 0
    returns_rate = ov.get("returns_rate_pct") or 0
    total_txn = ov.get("total_transactions") or 0
    total_cust = ov.get("total_customers") or 0
    total_prod = ov.get("total_products") or 0
    net_qty = ov.get("net_sales_qty") or 0

    # --- Response builders by intent ---
    if intent_key == "total_net_sales":
        return {
            "direct_answer": f"The total net sales revenue for the selected period is {_fmt_cr(net_sales)}.",
            "key_metrics": [
                f"Net Sales Revenue: {_fmt_cr(net_sales)}",
                f"Total Quantity Sold: {net_qty:,} units",
                f"Total Transactions: {total_txn:,}",
                f"Active Customers: {total_cust:,}",
            ],
            "breakdown": [
                f"Top Zone: {zones[0]['name']} contributing {zones[0].get('pct', 0)}%" if zones else "N/A",
                f"Top State: {top_states[0]['name']} contributing {top_states[0].get('pct', 0)}%" if top_states else "N/A",
                f"Top Product: {products[0]['name']} contributing {products[0].get('pct', 0)}%" if products else "N/A",
            ],
            "insight": [
                f"Revenue concentration: top 3 states represent {top_3_states_pct}% of sales." if top_3_states_pct is not None else "Review zone and product mix for diversification.",
            ],
            "suggested_drilldowns": ["Monthly revenue trend", "Revenue by zone", "Revenue by product", "Revenue by top customers"],
            "best_visual": "KPI card + bar chart + pareto",
        }

    if intent_key == "monthly_revenue_trend":
        peak = max(trends, key=lambda t: t.get("value") or 0) if trends else {}
        low = min(trends, key=lambda t: t.get("value") or float("inf")) if trends else {}
        last = trends[-1] if trends else {}
        return {
            "direct_answer": f"Monthly revenue shows {'increasing' if (last.get('growth_pct') or 0) >= 0 else 'declining'} performance. Peak month: {peak.get('month', 'N/A')} with {_fmt_cr(peak.get('value', 0))}." if trends else "No trend data available.",
            "key_metrics": [
                f"Peak Month: {peak.get('month', 'N/A')} with {_fmt_cr(peak.get('value', 0))}" if peak else "N/A",
                f"Latest Month: {last.get('month', 'N/A')} with {_fmt_cr(last.get('value', 0))}" if last else "N/A",
                f"MoM Growth (Latest): {last.get('growth_pct', 0):+.2f}%" if last.get("growth_pct") is not None else "N/A",
            ],
            "breakdown": [f"{t['month']}: {_fmt_cr(t.get('value', 0))} ({t.get('growth_pct', 0):+.2f}% MoM)" if t.get("growth_pct") is not None else f"{t['month']}: {_fmt_cr(t.get('value', 0))}" for t in trends[-8:]],
            "insight": ["Revenue trend is driven by top states and products; drill by zone and product for drivers."],
            "suggested_drilldowns": ["Month → Zone", "Month → Product", "Month → Customer"],
            "best_visual": "Line chart",
        }

    if intent_key == "compare_q1_q2":
        q1 = quarterly.get("Q1") or 0
        q2 = quarterly.get("Q2") or 0
        diff_pct = round((q2 - q1) / q1 * 100, 2) if q1 else None
        return {
            "direct_answer": f"Revenue in Q2 was {_fmt_cr(q2)} compared to {_fmt_cr(q1)} in Q1, a change of {diff_pct:+.2f}%." if q1 and diff_pct is not None else "Quarterly data not available.",
            "key_metrics": [f"Q1 Revenue: {_fmt_cr(q1)}", f"Q2 Revenue: {_fmt_cr(q2)}", f"Difference: {_fmt_cr(q2 - q1)}", f"Growth: {diff_pct}%" if diff_pct is not None else "N/A"],
            "breakdown": [],
            "insight": ["Compare zone and product deltas for root cause of quarter-on-quarter change."],
            "suggested_drilldowns": ["Quarter by zone", "Quarter by product"],
            "best_visual": "Clustered bar chart / waterfall",
        }

    if intent_key == "return_rate":
        return {
            "direct_answer": f"The overall return rate for the selected period is {returns_rate:.2f}%.",
            "key_metrics": [
                f"Returns Value: {_fmt_cr(returns_value)}",
                f"Gross Sales Value: {_fmt_cr(gross_sales)}",
                f"Net Sales Value: {_fmt_cr(net_sales)}",
            ],
            "breakdown": [f"{r['name']}: {r.get('return_rate_pct', 0):.2f}%" for r in product_return_rates[:5]] if product_return_rates else [],
            "insight": ["Return rate is relatively controlled; review by product and state for localized issues."],
            "suggested_drilldowns": ["Return rate by product", "Return rate by state", "Monthly return trend"],
            "best_visual": "Gauge + trend line",
        }

    if intent_key == "avg_transaction_value" or intent_key == "avg_txn_value":
        atv = avg_transaction_value or (net_sales / total_txn if total_txn else None)
        return {
            "direct_answer": f"The average transaction value is {_fmt_cr(atv) if atv is not None else 'N/A'}." if atv is not None else "No transaction data available.",
            "key_metrics": [f"ATV: {_fmt_cr(atv)}" if atv else "N/A", f"Total Transactions: {total_txn:,}", f"Total Revenue: {_fmt_cr(net_sales)}"],
            "breakdown": [f"{z['zone']}: {z['transactions']:,} txns" for z in transactions_by_zone[:5]] if transactions_by_zone else [],
            "insight": ["Basket size and ATV indicate purchase behavior; compare across zones and products."],
            "suggested_drilldowns": ["Zone", "Product", "Customer"],
            "best_visual": "KPI card",
        }

    if intent_key == "top_zones_revenue":
        top_z = zones[:5] if zones else []
        return {
            "direct_answer": f"The highest revenue-generating zones are: {', '.join(z['name'] + ' (' + _fmt_cr(z['value']) + ')' for z in top_z)}." if top_z else "No zone data available.",
            "key_metrics": [f"{i+1}. {z['name']}: {_fmt_cr(z['value'])} ({z.get('pct', 0)}%)" for i, z in enumerate(top_z)],
            "breakdown": [f"{z['name']}: {z.get('pct', 0)}%" for z in top_z],
            "insight": ["Revenue is concentrated in top zones; consider dependency risk and expansion in low-share zones."],
            "suggested_drilldowns": ["Zone → State → City", "Zone → Product", "Zone → Customer"],
            "best_visual": "Ranked horizontal bar chart",
        }

    if intent_key == "top_states_revenue" or intent_key == "states_most_revenue":
        return {
            "direct_answer": f"The states contributing the most to sales are: {', '.join(s['name'] + ' (' + str(s.get('pct', 0)) + '%)' for s in top_states[:5])}." if top_states else "No state data available.",
            "key_metrics": [f"{i+1}. {s['name']}: {_fmt_cr(s['value'])} ({s.get('pct', 0)}%)" for i, s in enumerate(top_states[:5])],
            "breakdown": [f"Top 3 states share: {top_3_states_pct}%" if top_3_states_pct is not None else ""],
            "insight": [f"Top 3 states represent {top_3_states_pct}% of revenue - {'high geographic concentration.' if top_3_states_pct and top_3_states_pct > 75 else 'moderate diversification.'}" if top_3_states_pct is not None else ""],
            "suggested_drilldowns": ["State → City", "State → Product", "State → Customer"],
            "best_visual": "Map / horizontal bar",
        }

    if intent_key == "top_products_revenue" or intent_key == "products_highest_revenue":
        top_p = products[:5] if products else []
        return {
            "direct_answer": f"The top revenue-contributing products are: {', '.join(p['name'] + ' (' + str(p.get('pct', 0)) + '%)' for p in top_p)}." if top_p else "No product data available.",
            "key_metrics": [f"{i+1}. {p['name']}: {_fmt_cr(p['value'])} ({p.get('pct', 0)}%)" for i, p in enumerate(top_p)],
            "breakdown": [],
            "insight": ["A few products dominate the portfolio; monitor concentration risk and growth in tail products."],
            "suggested_drilldowns": ["Product → State", "Product → Customer", "Product → Returns"],
            "best_visual": "Pareto chart",
        }

    if intent_key == "top10_customer_share":
        return {
            "direct_answer": f"The top 10 customers contribute {top_10_customers_pct:.1f}% of total revenue." if top_10_customers_pct is not None else "Concentration data not available.",
            "key_metrics": [f"Top 10 share: {top_10_customers_pct}%", f"Total revenue: {_fmt_cr(net_sales)}", f"Top 10 revenue: {_fmt_cr(net_sales * (top_10_customers_pct or 0) / 100)}"],
            "breakdown": [f"{c.get('customer', c.get('name', 'N/A'))}: {_fmt_cr(c.get('value', 0))}" for c in top_customers[:10]],
            "insight": ["High top-10 share indicates strong account concentration and possible revenue dependency risk."],
            "suggested_drilldowns": ["Customer ranking", "Top 10 by zone", "Top 10 by product mix"],
            "best_visual": "Pareto chart",
        }

    if intent_key == "regions_declining" or intent_key == "underperforming_regions" or intent_key == "zones_sudden_decline":
        decl = zones_declining or underperforming_zones or zones_sorted_low[:5]
        return {
            "direct_answer": f"The underperforming/declining regions are: {', '.join((d.get('zone') or d.get('name', '')) for d in decl[:5])}." if decl else "No declining zone data available.",
            "key_metrics": [f"{d.get('zone', d.get('name', 'N/A'))}: {d.get('growth_pct', d.get('pct', 0))}%" for d in decl[:5]],
            "breakdown": [],
            "insight": ["These regions need deeper review for demand, coverage, pricing, or distributor health."],
            "suggested_drilldowns": ["Zone → State", "Product mix", "Customer list"],
            "best_visual": "Heatmap / risk matrix",
        }

    if intent_key == "revenue_by_division":
        div = division_distribution[:5] if division_distribution else []
        return {
            "direct_answer": f"Revenue distribution by division: {'; '.join(d.get('division', d.get('name', 'N/A')) + ' ' + str(d.get('pct', 0)) + '%' for d in div)}." if div else "No division data available.",
            "key_metrics": [f"{d.get('division', d.get('name', 'N/A'))}: {_fmt_cr(d.get('value', 0))} ({d.get('pct', 0)}%)" for d in div],
            "breakdown": [],
            "insight": ["Use division mix to align resource allocation and growth targets."],
            "suggested_drilldowns": ["Division by zone", "Division by product"],
            "best_visual": "Bar chart",
        }

    if intent_key == "growth_rate_mom" or intent_key == "revenue_mom_growth":
        mom = revenue_mom.get("mom_pct") if revenue_mom else (trends[-1].get("growth_pct") if trends else None)
        return {
            "direct_answer": f"Revenue growth compared to previous month is {mom:+.2f}%." if mom is not None else "Insufficient data for MoM growth.",
            "key_metrics": [
                f"Latest month: {revenue_mom.get('latest_month', 'N/A')} - {_fmt_cr(revenue_mom.get('latest_value', 0))}" if revenue_mom else "N/A",
                f"Previous month: {revenue_mom.get('prev_month', 'N/A')} - {_fmt_cr(revenue_mom.get('prev_value', 0))}" if revenue_mom else "N/A",
                f"MoM Growth: {mom}%" if mom is not None else "N/A",
            ],
            "breakdown": [],
            "insight": ["Drivers: check zone, state, and product deltas for root cause."],
            "suggested_drilldowns": ["Month → Zone", "Month → Product", "Month → Customer"],
            "best_visual": "Line chart",
        }

    if intent_key == "top_brand_revenue":
        top_b = top_brands_by_sales[:3] if top_brands_by_sales else []
        return {
            "direct_answer": f"The brand generating the most revenue is {top_b[0]['brand']} with {_fmt_cr(top_b[0]['value'])}." if top_b else "No brand data available.",
            "key_metrics": [f"{b['brand']}: {_fmt_cr(b['value'])}" for b in top_b],
            "breakdown": [],
            "insight": ["Brand mix drives portfolio strategy; track growth by brand."],
            "suggested_drilldowns": ["Brand by zone", "Brand by product"],
            "best_visual": "Bar chart",
        }

    if intent_key == "zone_highest_growth":
        zg = zones_growing_fastest or [z for z in zone_growth if (z.get("growth_pct") or 0) > 0][:5]
        zg_sorted = sorted(zg, key=lambda x: x.get("growth_pct") or 0, reverse=True) if zg else []
        top_g = zg_sorted[0] if zg_sorted else {}
        return {
            "direct_answer": f"The zone with the highest growth rate is {top_g.get('zone', 'N/A')} with {top_g.get('growth_pct', 0):+.2f}% MoM." if top_g else "No zone growth data available.",
            "key_metrics": [f"{z.get('zone', z.get('name', 'N/A'))}: {z.get('growth_pct', 0):+.2f}%" for z in zg_sorted[:5]],
            "breakdown": [],
            "insight": ["High-growth zones are expansion opportunities; replicate best practices."],
            "suggested_drilldowns": ["Zone → State", "Zone → Product"],
            "best_visual": "Bar chart",
        }

    if intent_key == "total_quantity_sold":
        return {
            "direct_answer": f"The total quantity sold in the selected period is {net_qty:,} units.",
            "key_metrics": [f"Total Quantity: {net_qty:,}", f"Net Sales Value: {_fmt_cr(net_sales)}", f"Avg per transaction: {round(net_qty / total_txn, 1) if total_txn else 0}"],
            "breakdown": [],
            "insight": ["Volume and value together indicate mix and pricing; drill by product and zone."],
            "suggested_drilldowns": ["Product", "Zone", "Month"],
            "best_visual": "KPI card",
        }

    if intent_key == "revenue_per_customer":
        rpc = net_sales / total_cust if total_cust else None
        return {
            "direct_answer": f"Revenue per customer is {_fmt_cr(rpc)}." if rpc is not None else "No customer data available.",
            "key_metrics": [f"Revenue per customer: {_fmt_cr(rpc)}", f"Total revenue: {_fmt_cr(net_sales)}", f"Total customers: {total_cust:,}"],
            "breakdown": [f"{s['name']}: {_fmt_cr(s.get('revenue_per_customer', 0))}" for s in state_revenue_per_customer[:5]] if state_revenue_per_customer else [],
            "insight": ["Higher RPC states indicate strong account penetration; focus retention and cross-sell."],
            "suggested_drilldowns": ["State", "Zone", "Product"],
            "best_visual": "Bar chart",
        }

    if intent_key == "top_cities_sales":
        cities = top_cities_by_sales[:10] if top_cities_by_sales else []
        return {
            "direct_answer": f"Top 10 cities by sales: {', '.join(c.get('city', c.get('name', 'N/A')) + ' ' + _fmt_cr(c.get('value', 0)) for c in cities)}." if cities else "No city data available.",
            "key_metrics": [f"{i+1}. {c.get('city', c.get('name', 'N/A'))}: {_fmt_cr(c.get('value', 0))}" for i, c in enumerate(cities)],
            "breakdown": [],
            "insight": ["City ranking supports territory and coverage planning."],
            "suggested_drilldowns": ["City → Customer", "City → Product"],
            "best_visual": "Bar chart / map",
        }

    if intent_key == "promoted_revenue" or intent_key == "promoted_sales_pct":
        return {
            "direct_answer": f"Promoted products contribute {promoted_sales_pct}% of total revenue ({_fmt_cr(promoted_vs_non.get('promoted_value', 0))}).",
            "key_metrics": [f"Promoted revenue share: {promoted_sales_pct}%", f"Promoted value: {_fmt_cr(promoted_vs_non.get('promoted_value', 0))}", f"Non-promoted: {_fmt_cr(promoted_vs_non.get('non_promoted_value', 0))}"],
            "breakdown": [f"{p['name']}: {_fmt_cr(p.get('sales_value', 0))}" for p in top_promoted_products[:5]] if top_promoted_products else [],
            "insight": ["Promotion dependence indicates campaign-driven revenue; balance with organic pull."],
            "suggested_drilldowns": ["Product", "Zone", "Customer"],
            "best_visual": "Donut + stacked bar",
        }

    if intent_key == "products_highest_return_rate":
        top_r = product_return_rates[:5] if product_return_rates else []
        return {
            "direct_answer": f"Products with the highest return rate: {', '.join(r['name'] + ' (' + str(r.get('return_rate_pct', 0)) + '%)' for r in top_r)}." if top_r else "No return rate data by product.",
            "key_metrics": [f"{r['name']}: {r.get('return_rate_pct', 0):.2f}%" for r in top_r],
            "breakdown": [],
            "insight": ["High return rate products need quality or channel review."],
            "suggested_drilldowns": ["Product → State", "Product → Customer"],
            "best_visual": "Ranked bar chart",
        }

    if intent_key == "avg_selling_price_product" or intent_key == "avg_ppu_product":
        asp = product_avg_price[:5] if product_avg_price else []
        return {
            "direct_answer": f"Average selling price (PPU) by product: {'; '.join(p['name'] + ' ₹' + str(p.get('avg_ppu', 0)) for p in asp)}." if asp else "No ASP data available.",
            "key_metrics": [f"{p['name']}: ₹{p.get('avg_ppu', 0):,.2f}" for p in asp],
            "breakdown": [],
            "insight": ["ASP by product supports pricing and margin analysis."],
            "suggested_drilldowns": ["Product", "Zone", "Customer"],
            "best_visual": "Bar chart",
        }

    if intent_key == "avg_discount" or intent_key == "products_highest_discount_list" or intent_key == "products_highest_discounts":
        disc_list = product_highest_discount[:5] if product_highest_discount else []
        return {
            "direct_answer": f"The average discount across products is {avg_discount_overall:.2f}%." if avg_discount_overall is not None else "Discount data not available. Highest discount products: " + ", ".join(d["name"] + " " + str(d.get("avg_discount", 0)) + "%" for d in disc_list),
            "key_metrics": [f"Overall avg discount: {avg_discount_overall}%" if avg_discount_overall is not None else "N/A"] + [f"{d['name']}: {d.get('avg_discount', 0)}%" for d in disc_list],
            "breakdown": [],
            "insight": ["Monitor discount by product and channel for margin impact."],
            "suggested_drilldowns": ["Product", "Zone", "Customer"],
            "best_visual": "Bar chart",
        }

    if intent_key == "price_realization_index":
        pr = overall_price_realization
        val = pr.get("realization_pct") if isinstance(pr, dict) else (pr if isinstance(pr, (int, float)) else None)
        return {
            "direct_answer": f"The average price realization index across products is {val}%." if val is not None else "Price realization data not available.",
            "key_metrics": [f"Realization: {val}%", f"Avg PPU: ₹{pr.get('avg_ppu', 0):,.2f}" if isinstance(pr, dict) else "N/A"],
            "breakdown": [f"{z['name']}: {z.get('price_realization_pct', 0):.2f}%" for z in zone_price_realization[:5]] if zone_price_realization else [],
            "insight": ["Products below 100% realization may indicate discount leakage or pricing pressure."],
            "suggested_drilldowns": ["Product", "Zone", "Customer"],
            "best_visual": "Box plot / ranked bar",
        }

    if intent_key == "top10_customers":
        return {
            "direct_answer": f"Top 10 customers by revenue: {', '.join(str(c.get('customer', c.get('name', 'N/A'))) + ' ' + _fmt_cr(c.get('value', 0)) for c in top_customers[:10])}.",
            "key_metrics": [f"{i+1}. {c.get('customer', c.get('name', 'N/A'))}: {_fmt_cr(c.get('value', 0))}" for i, c in enumerate(top_customers[:10])],
            "breakdown": [f"Top 10 share: {top_10_customers_pct}%"],
            "insight": ["Key account concentration; prioritize retention and growth in these accounts."],
            "suggested_drilldowns": ["Customer → Product", "Customer → Zone"],
            "best_visual": "Pareto chart",
        }

    if intent_key == "customers_stopped_business" or intent_key == "customers_stopped_recently":
        stop = stop_business_customers[:10] if stop_business_customers else []
        return {
            "direct_answer": f"{len(stop_business_customers)} customers have stopped doing business. Top by historical value: {', '.join(s.get('customer', 'N/A') + ' ' + _fmt_cr(s.get('value', 0)) for s in stop)}." if stop else "No stop-business customers found.",
            "key_metrics": [f"Stop business count: {len(stop_business_customers)}", f"Value at risk: {_fmt_cr(sum(s.get('value', 0) for s in stop_business_customers))}"],
            "breakdown": [f"{s.get('customer', 'N/A')}: {_fmt_cr(s.get('value', 0))}" for s in stop],
            "insight": ["Review stop-business list for win-back or closure plans."],
            "suggested_drilldowns": ["Customer list", "By zone", "By product"],
            "best_visual": "Table",
        }

    if intent_key == "customers_pareto_80":
        return {
            "direct_answer": f"Customers contributing 80% of total revenue: {pareto_80_pct}% of revenue comes from {len(pareto_80_customers)} customers.",
            "key_metrics": [f"Pareto 80% revenue share: {pareto_80_pct}%", f"Number of customers: {len(pareto_80_customers)}"],
            "breakdown": [f"{c.get('customer', 'N/A')}: {_fmt_cr(c.get('value', 0))}" for c in pareto_80_customers[:10]],
            "insight": ["Concentration in few accounts; diversify and grow mid-tier accounts."],
            "suggested_drilldowns": ["Customer ranking", "By zone", "By product"],
            "best_visual": "Pareto chart",
        }

    if intent_key == "customers_over_10_pct" or intent_key == "territories_revenue_risk":
        high = high_dependency_customers[:10] if high_dependency_customers else []
        return {
            "direct_answer": f"Customers contributing more than 10% of revenue: {', '.join(c.get('customer', 'N/A') + ' (' + str(c.get('pct', 0)) + '%)' for c in high)}." if high else "No single customer exceeds 10% of revenue.",
            "key_metrics": [f"{c.get('customer', 'N/A')}: {c.get('pct', 0)}% ({_fmt_cr(c.get('value', 0))})" for c in high],
            "breakdown": [],
            "insight": ["High dependency increases revenue risk; key account management and diversification recommended."],
            "suggested_drilldowns": ["Customer", "Product mix", "Zone"],
            "best_visual": "Risk scorecard / table",
        }

    if intent_key == "states_high_returns":
        sr = state_return_rates[:5] if state_return_rates else []
        return {
            "direct_answer": f"States with unusually high return rates: {', '.join(s['name'] + ' (' + str(s.get('return_rate_pct', 0)) + '%)' for s in sr)}." if sr else "No state return rate data.",
            "key_metrics": [f"{s['name']}: {s.get('return_rate_pct', 0):.2f}%" for s in sr],
            "breakdown": [],
            "insight": ["High return states may indicate supply chain or quality issues; investigate by product."],
            "suggested_drilldowns": ["State → Product", "State → Customer"],
            "best_visual": "Bar chart / heatmap",
        }

    if intent_key == "products_depend_one_region" or intent_key == "product_geo_concentration":
        geo = product_geo_concentration[:5] if product_geo_concentration else []
        return {
            "direct_answer": f"Products most dependent on a single state (highest top-state %): {', '.join(g.get('product', 'N/A') + ' (' + str(g.get('top_state_pct', 0)) + '%)' for g in geo)}." if geo else "No product geo concentration data.",
            "key_metrics": [f"{g.get('product', 'N/A')}: {g.get('top_state_pct', 0)}% in top state" for g in geo],
            "breakdown": [],
            "insight": ["Single-region dependency increases portfolio risk; expand distribution."],
            "suggested_drilldowns": ["Product → State", "Product → Zone"],
            "best_visual": "Bar chart",
        }

    if intent_key == "high_discount_transactions" or intent_key == "suspicious_discounts":
        high_t = high_discount_transactions[:5] if high_discount_transactions else []
        return {
            "direct_answer": f"Transactions with unusually high discounts: {len(high_discount_transactions)} found. Sample: {', '.join(str(t.get('tran_id', 'N/A')) + ' (' + str(t.get('avg_discount', 0)) + '%)' for t in high_t)}." if high_t else "No high-discount transactions flagged.",
            "key_metrics": [f"Transaction: {t.get('tran_id', 'N/A')}, Discount: {t.get('avg_discount', 0)}%, Value: {_fmt_cr(t.get('value', 0))}" for t in high_t],
            "breakdown": [],
            "insight": ["Review approval norms and policy for high-discount transactions."],
            "suggested_drilldowns": ["Transaction detail", "Customer", "Product"],
            "best_visual": "Table",
        }

    if intent_key == "avg_qty_per_transaction" or intent_key == "avg_basket_size":
        aq = avg_qty_per_transaction
        return {
            "direct_answer": f"The average quantity per transaction (basket size) is {aq:,.2f} units." if aq is not None else "No data available.",
            "key_metrics": [f"Avg quantity per txn: {aq}", f"Total quantity: {net_qty:,}", f"Total transactions: {total_txn:,}"],
            "breakdown": [],
            "insight": ["Basket size indicates purchase behavior; use for assortment and promotion planning."],
            "suggested_drilldowns": ["Product", "Zone", "Customer"],
            "best_visual": "KPI card",
        }

    if intent_key == "transactions_per_month":
        tpm = transactions_per_month[-6:] if transactions_per_month else []
        return {
            "direct_answer": f"Transaction count by month (recent): {', '.join(t.get('month', 'N/A') + ': ' + str(t.get('transactions', 0)) for t in tpm)}." if tpm else "No monthly transaction data.",
            "key_metrics": [f"{t.get('month', 'N/A')}: {t.get('transactions', 0):,} txns" for t in tpm],
            "breakdown": [],
            "insight": ["Transaction trend reflects demand and coverage; compare with revenue trend."],
            "suggested_drilldowns": ["Month → Zone", "Month → Product"],
            "best_visual": "Line chart",
        }

    if intent_key == "declining_products" or intent_key == "products_declining":
        decl_p = declining_products[:5] if declining_products else []
        return {
            "direct_answer": f"Products showing declining sales (MoM): {', '.join(d.get('product', 'N/A') + ' (' + str(d.get('growth_pct', 0)) + '%)' for d in decl_p)}." if decl_p else "No declining products in recent period.",
            "key_metrics": [f"{d.get('product', 'N/A')}: {d.get('growth_pct', 0)}% MoM" for d in decl_p],
            "breakdown": [],
            "insight": ["Declining products need demand or distribution review."],
            "suggested_drilldowns": ["Product → State", "Product → Customer"],
            "best_visual": "Bar chart",
        }

    if intent_key == "customers_growing_fastest":
        cg = (summary.get("customer_growth") or [])[:5]
        return {
            "direct_answer": f"Fastest-growing customers (MoM): {', '.join(c.get('customer', 'N/A') + ' (' + str(c.get('growth_pct', 0)) + '%)' for c in cg)}." if cg else "No customer growth data.",
            "key_metrics": [f"{c.get('customer', 'N/A')}: {c.get('growth_pct', 0):+.2f}%" for c in cg],
            "breakdown": [],
            "insight": ["High-growth accounts are expansion opportunities; replicate success drivers."],
            "suggested_drilldowns": ["Customer → Product", "Customer → Zone"],
            "best_visual": "Bar chart",
        }

    if intent_key == "regions_promotion_response" or intent_key == "zone_promotion_response":
        zpr = zone_promotion_response[:5] if zone_promotion_response else []
        return {
            "direct_answer": f"Regions that respond best to promotions (by promoted %): {', '.join(z.get('name', 'N/A') + ' (' + str(z.get('promoted_pct', 0)) + '%)' for z in zpr)}." if zpr else "No promotion response by zone.",
            "key_metrics": [f"{z.get('name', 'N/A')}: {z.get('promoted_pct', 0)}% promoted" for z in zpr],
            "breakdown": [],
            "insight": ["Use promotion response by region for campaign targeting."],
            "suggested_drilldowns": ["Zone", "Product", "Customer"],
            "best_visual": "Bar chart",
        }

    if intent_key == "products_above_list_price":
        pr = overall_price_realization
        val = pr.get("realization_pct") if isinstance(pr, dict) else (pr if isinstance(pr, (int, float)) else None)
        zones_above = [z for z in (zone_price_realization or []) if (z.get("price_realization_pct") or 0) >= 100][:5]
        return {
            "direct_answer": f"Overall price realization is {val}% (PPU vs list rate). Zones at or above list: {', '.join(z['name'] for z in zones_above)}." if val is not None else "Price realization data not available. Use Pricing & Discount dashboard for product-level view.",
            "key_metrics": [f"Overall realization: {val}%"] + [f"{z['name']}: {z.get('price_realization_pct', 0):.2f}%" for z in zones_above],
            "breakdown": [],
            "insight": ["Strong price realization indicates pricing discipline; monitor below-list products and zones."],
            "suggested_drilldowns": ["Product", "Zone", "Customer"],
            "best_visual": "Bar chart",
        }

    if intent_key == "customers_most_products" or intent_key == "customers_wide_variety":
        cpc = customer_product_count[:5] if customer_product_count else []
        return {
            "direct_answer": f"Customers purchasing the most products (widest variety): {', '.join(c.get('customer', 'N/A') + ' (' + str(c.get('product_count', 0)) + ' products)' for c in cpc)}." if cpc else "No data available.",
            "key_metrics": [f"{c.get('customer', 'N/A')}: {c.get('product_count', 0)} products" for c in cpc],
            "breakdown": [],
            "insight": ["Wide variety indicates cross-sell potential and stickiness."],
            "suggested_drilldowns": ["Customer → Product", "Customer → Zone"],
            "best_visual": "Bar chart",
        }

    if intent_key == "revenue_by_state":
        return {
            "direct_answer": f"Revenue distribution by state: Top states {', '.join(s['name'] + ' ' + str(s.get('pct', 0)) + '%' for s in top_states[:5])}." if top_states else "No state data.",
            "key_metrics": [f"{s['name']}: {_fmt_cr(s['value'])} ({s.get('pct', 0)}%)" for s in top_states[:5]],
            "breakdown": [],
            "insight": ["State mix supports territory planning and resource allocation."],
            "suggested_drilldowns": ["State → City", "State → Product"],
            "best_visual": "Map / bar",
        }

    if intent_key == "zones_contribute_sales":
        return {
            "direct_answer": f"Zones contributing the most sales: {', '.join(z['name'] + ' ' + str(z.get('pct', 0)) + '%' for z in zones[:5])}." if zones else "No zone data.",
            "key_metrics": [f"{z['name']}: {_fmt_cr(z['value'])} ({z.get('pct', 0)}%)" for z in zones[:5]],
            "breakdown": [],
            "insight": ["Zone contribution guides regional strategy."],
            "suggested_drilldowns": ["Zone → State", "Zone → Product"],
            "best_visual": "Bar chart",
        }

    if intent_key == "quarterly_growth":
        q1, q2 = quarterly.get("Q1") or 0, quarterly.get("Q2") or 0
        g = round((q2 - q1) / q1 * 100, 2) if q1 else None
        return {
            "direct_answer": f"Quarterly revenue: Q1 {_fmt_cr(q1)}, Q2 {_fmt_cr(q2)}. Q2 vs Q1 growth: {g}%." if g is not None else "Quarterly data not available.",
            "key_metrics": [f"Q1: {_fmt_cr(q1)}", f"Q2: {_fmt_cr(q2)}", f"Growth: {g}%"],
            "breakdown": [],
            "insight": ["Quarter-on-quarter trend indicates momentum; drill by zone and product."],
            "suggested_drilldowns": ["Quarter by zone", "Quarter by product"],
            "best_visual": "Clustered bar",
        }

    if intent_key == "states_fastest_growth":
        state_growth = summary.get("root_cause_state_deltas") or []
        pos = [s for s in state_growth if (s.get("delta_value") or 0) > 0][:5]
        return {
            "direct_answer": f"States with the fastest revenue growth (positive delta): {', '.join(s.get('state', 'N/A') + ' ' + _fmt_cr(s.get('delta_value', 0)) for s in pos)}." if pos else "State growth/delta data not available.",
            "key_metrics": [f"{s.get('state', 'N/A')}: {_fmt_cr(s.get('delta_value', 0))}" for s in pos],
            "breakdown": [],
            "insight": ["Growing states are expansion priorities."],
            "suggested_drilldowns": ["State → City", "State → Product"],
            "best_visual": "Bar chart",
        }

    if intent_key == "city_highest_growth":
        cg = city_growth[:5] if city_growth else []
        return {
            "direct_answer": f"City with the highest sales growth: {cg[0].get('city', 'N/A')} with {cg[0].get('growth_pct', 0):+.2f}% MoM." if cg else "No city growth data.",
            "key_metrics": [f"{c.get('city', 'N/A')}: {c.get('growth_pct', 0):+.2f}%" for c in cg],
            "breakdown": [],
            "insight": ["High-growth cities are expansion opportunities."],
            "suggested_drilldowns": ["City → Customer", "City → Product"],
            "best_visual": "Bar chart",
        }

    if intent_key == "customers_highest_return_rate":
        cr = customer_return_rates[:5] if customer_return_rates else []
        return {
            "direct_answer": f"Customers with the highest return rate: {', '.join(r.get('name', 'N/A') + ' (' + str(r.get('return_rate_pct', 0)) + '%)' for r in cr)}." if cr else "No customer return rate data.",
            "key_metrics": [f"{r.get('name', 'N/A')}: {r.get('return_rate_pct', 0):.2f}%" for r in cr],
            "breakdown": [],
            "insight": ["High return customers may indicate distributor or quality issues."],
            "suggested_drilldowns": ["Customer → Product", "Customer → State"],
            "best_visual": "Bar chart",
        }

    if intent_key == "customers_highest_discounts" or intent_key == "customers_largest_discount":
        cd = customer_highest_discount[:5] if customer_highest_discount else []
        return {
            "direct_answer": f"Customers receiving the highest discounts: {', '.join(c.get('customer', 'N/A') + ' (' + str(c.get('avg_discount', 0)) + '%)' for c in cd)}." if cd else "No customer discount data.",
            "key_metrics": [f"{c.get('customer', 'N/A')}: {c.get('avg_discount', 0)}% avg discount" for c in cd],
            "breakdown": [],
            "insight": ["Review discount norms for key accounts."],
            "suggested_drilldowns": ["Customer → Product", "Customer → Zone"],
            "best_visual": "Bar chart",
        }

    if intent_key == "states_low_realization" or intent_key == "states_lowest_realization":
        zpr_low = sorted(zone_price_realization, key=lambda x: x.get("price_realization_pct") or 0)[:5] if zone_price_realization else []
        return {
            "direct_answer": f"Regions with the lowest price realization: {', '.join(z.get('name', 'N/A') + ' (' + str(z.get('price_realization_pct', 0)) + '%)' for z in zpr_low)}." if zpr_low else "No price realization by zone.",
            "key_metrics": [f"{z.get('name', 'N/A')}: {z.get('price_realization_pct', 0):.2f}%" for z in zpr_low],
            "breakdown": [],
            "insight": ["Low realization may indicate discount leakage or pricing pressure."],
            "suggested_drilldowns": ["Zone → Product", "Zone → Customer"],
            "best_visual": "Bar chart",
        }

    if intent_key == "churn_risk_candidates" or intent_key == "customers_declining_purchase" or intent_key == "customers_declining_trends":
        churn = churn_risk_candidates[:5] if churn_risk_candidates else []
        return {
            "direct_answer": f"Customers with declining or stopped purchases (churn risk): {len(churn_risk_candidates)} identified. Sample: {', '.join(c.get('customer', 'N/A') + ' (' + c.get('reason', '') + ')' for c in churn)}." if churn else "No churn risk candidates in recent period.",
            "key_metrics": [f"{c.get('customer', 'N/A')}: {c.get('reason', 'N/A')} (prev {_fmt_cr(c.get('prev_value', 0))})" for c in churn],
            "breakdown": [],
            "insight": ["Prioritize win-back or exit decisions for at-risk accounts."],
            "suggested_drilldowns": ["Customer list", "By product", "By zone"],
            "best_visual": "Table",
        }

    if intent_key == "products_dominate" or intent_key == "products_most_profitable":
        top3_pct = sum(p.get("pct", 0) for p in products[:3]) if products else 0
        return {
            "direct_answer": f"The top products dominating the portfolio contribute {top3_pct:.1f}% of product revenue. Lead product: {products[0]['name']} ({products[0].get('pct', 0)}%)." if products else "No product data.",
            "key_metrics": [f"{p['name']}: {p.get('pct', 0)}%" for p in products[:5]],
            "breakdown": [],
            "insight": ["Portfolio concentration in few products; balance growth in tail SKUs."],
            "suggested_drilldowns": ["Product → Zone", "Product → Customer"],
            "best_visual": "Pareto chart",
        }

    if intent_key == "transactions_by_zone" or intent_key == "regions_transaction_volume":
        tz = transactions_by_zone[:5] if transactions_by_zone else []
        return {
            "direct_answer": f"Regions with the highest transaction volume: {', '.join(t.get('zone', 'N/A') + ' (' + str(t.get('transactions', 0)) + ' txns)' for t in tz)}." if tz else "No transaction-by-zone data.",
            "key_metrics": [f"{t.get('zone', 'N/A')}: {t.get('transactions', 0):,} transactions" for t in tz],
            "breakdown": [],
            "insight": ["Transaction volume indicates market activity and coverage."],
            "suggested_drilldowns": ["Zone → State", "Zone → Product"],
            "best_visual": "Bar chart",
        }

    # Default: generic executive summary
    return _build_response("total_net_sales", summary)


def build_copilot_response(question: str, summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Classify question, build structured response from summary. Returns None if no data."""
    intent = _match_intent(question)
    if not intent:
        return None
    return _build_response(intent, summary)


def format_copilot_response(structured: Dict[str, Any]) -> str:
    """Format structured copilot response as standard text for chat UI."""
    lines = []
    if structured.get("direct_answer"):
        lines.append("**Answer:**\n" + structured["direct_answer"])
    if structured.get("key_metrics"):
        lines.append("\n**Key Metrics:**\n" + "\n".join("- " + m for m in structured["key_metrics"] if m))
    if structured.get("breakdown"):
        lines.append("\n**Breakdown:**\n" + "\n".join("- " + b for b in structured["breakdown"] if b))
    if structured.get("insight"):
        lines.append("\n**Insight:**\n" + "\n".join("- " + i for i in structured["insight"] if i))
    if structured.get("suggested_drilldowns"):
        lines.append("\n**Suggested Drilldowns:**\n" + "\n".join("- " + d for d in structured["suggested_drilldowns"] if d))
    if structured.get("best_visual"):
        lines.append("\n**Best Visual:** " + structured["best_visual"])
    return "\n".join(lines).strip() if lines else ""
