import { useState, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import axios from "axios";
import { ArrowLeft, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  BarChart,
  Bar,
  Cell,
} from "recharts";

import { API } from "@/apiConfig";

const DIMENSION_LABELS = {
  month: "Month",
  zone: "Zone",
  state: "State",
  product: "Product",
  customer: "Customer",
};
const COLORS = ["#D63384", "#0F172A", "#10B981", "#F59E0B", "#3B82F6", "#8B5CF6"];

const formatCurrency = (value) => {
  if (value == null) return "-";
  const v = Number(value);
  if (v >= 10000000) return `₹${(v / 10000000).toFixed(2)}Cr`;
  if (v >= 100000) return `₹${(v / 100000).toFixed(2)}L`;
  if (v >= 1000) return `₹${(v / 1000).toFixed(1)}K`;
  return `₹${v.toFixed(0)}`;
};

const formatRowValue = (row, valueFormat) => {
  const v = row.value;
  if (valueFormat === "currency") return formatCurrency(v);
  if (valueFormat === "percent")
    return row.pct != null
      ? `${Number(row.pct).toFixed(2)}%`
      : v != null
        ? `${Number(v).toFixed(2)}%`
        : "-";
  if (valueFormat === "number") return v != null ? Number(v).toLocaleString() : "-";
  return v != null ? String(v) : "-";
};

const formatDetailCell = (col, raw) => {
  if (raw == null || raw === "") return "-";
  const t = col.type || "text";
  if (t === "currency") return formatCurrency(raw);
  if (t === "percent") return `${Number(raw).toFixed(2)}%`;
  if (t === "number")
    return Number(raw).toLocaleString(undefined, { maximumFractionDigits: 2 });
  return String(raw);
};

export default function DrillDownPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const config = location.state || {};

  const [groupBy, setGroupBy] = useState(
    config.groupByOptions?.[0]?.value ?? "month"
  );
  const [rows, setRows] = useState(config.staticRows ?? []);
  const [loading, setLoading] = useState(false);
  const [netSalesKpis, setNetSalesKpis] = useState(null);
  const [netSalesVisuals, setNetSalesVisuals] = useState({ trends: [], topStates: [] });

  const {
    title = "Drill-down",
    subtitle,
    type,
    parentPath = "/",
    parentLabel = "Executive Summary",
    valueFormat = "number",
    groupByOptions = [],
    showPctColumn = false,
    detailColumns = [],
    detailRows: detailRowsProp,
    detailTables: detailTablesProp,
  } = config;
  const detailRows = Array.isArray(detailRowsProp) ? detailRowsProp : [];
  const detailTables = Array.isArray(detailTablesProp) ? detailTablesProp : [];
  const isMultiDetail =
    type === "static" &&
    detailTables.length > 0 &&
    detailTables.every(
      (t) =>
        t &&
        Array.isArray(t.columns) &&
        t.columns.length > 0 &&
        Array.isArray(t.rows)
    );
  const isDetailTable =
    type === "static" &&
    !isMultiDetail &&
    Array.isArray(detailColumns) &&
    detailColumns.length > 0 &&
    Array.isArray(detailRowsProp);
  const isNetSalesDrill =
    config.kpiKey === "net_sales_value" || config.kpi === "net_sales_value";

  const fetchData = async () => {
    if (!type || type === "static") return;
    setLoading(true);
    try {
      if (type === "revenue-kpi") {
        const res = await axios.get(`${API}/revenue-kpi/drill`, {
          params: { kpi: config.kpi, group_by: groupBy },
        });
        setRows((res.data || []).filter((r) => r?.dimension != null || r?.name != null));
      } else if (type === "dashboard") {
        const res = await axios.get(`${API}/dashboard/drill`, {
          params: { metric: config.metric, group_by: groupBy },
        });
        setRows((res.data || []).filter((r) => r?.dimension != null || r?.name != null));
      } else {
        setRows([]);
      }
    } catch (err) {
      setRows([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const c = location.state || {};
    if (!c.type || c.type === "static") {
      const tables = Array.isArray(c.detailTables) ? c.detailTables : [];
      const hasMulti =
        tables.length > 0 &&
        tables.every(
          (t) =>
            t &&
            Array.isArray(t.columns) &&
            t.columns.length > 0 &&
            Array.isArray(t.rows)
        );
      const hasSingle =
        Array.isArray(c.detailColumns) &&
        c.detailColumns.length > 0 &&
        Array.isArray(c.detailRows);
      setRows(hasMulti || hasSingle ? [] : c.staticRows ?? []);
      return;
    }
    fetchData();
  }, [location.state, type, groupBy, config.kpi, config.metric]);

  useEffect(() => {
    if (!isNetSalesDrill) return;
    const fetchNetSalesKpis = async () => {
      try {
        const [overviewRes, concentrationRes, trendsRes] = await Promise.allSettled([
          axios.get(`${API}/dashboard/overview`),
          axios.get(`${API}/dashboard/concentration`),
          axios.get(`${API}/dashboard/trends`),
        ]);

        const ov =
          overviewRes.status === "fulfilled" ? overviewRes.value.data || {} : {};
        const conc =
          concentrationRes.status === "fulfilled"
            ? concentrationRes.value.data || {}
            : {};
        const tr =
          trendsRes.status === "fulfilled" ? trendsRes.value.data || [] : [];
        const latest = tr.length ? tr[tr.length - 1] : null;

        setNetSalesKpis({
          netSalesValue: ov.net_sales_value ?? null,
          avgTransactionValue: ov.avg_transaction_value ?? null,
          avgRevenuePerCustomer: ov.avg_revenue_per_customer ?? null,
          top3StatesShare: conc.top_3_states_pct ?? null,
          top10CustomersShare: conc.top_10_customers_pct ?? null,
          latestGrowthPct: latest?.growth_pct ?? null,
        });
        setNetSalesVisuals({
          trends: tr.map((x) => ({ month: x.month, value: Number(x.value || 0) })),
          topStates: (conc.top_states || []).slice(0, 10).map((x) => ({
            name: x.name,
            value: Number(x.value || 0),
          })),
        });
      } catch {
        setNetSalesKpis(null);
        setNetSalesVisuals({ trends: [], topStates: [] });
      }
    };
    fetchNetSalesKpis();
  }, [isNetSalesDrill]);

  const goBack = () => navigate(parentPath);

  if (!location.state) {
    return (
      <div className="p-8 text-center">
        <p className="text-slate-500 mb-4">No drill-down context. Open a KPI from a dashboard.</p>
        <Button onClick={() => navigate("/")}>Go to Dashboard</Button>
      </div>
    );
  }

  const dimensionLabel =
    DIMENSION_LABELS[groupBy] || groupByOptions.find((o) => o.value === groupBy)?.label || "Dimension";
  const totalValue =
    !isDetailTable && !isMultiDetail && valueFormat === "currency" && rows.length
      ? rows.reduce((sum, r) => sum + Number(r.value || 0), 0)
      : null;
  const totalCount =
    !isDetailTable && !isMultiDetail && valueFormat === "number" && rows.length
      ? rows.reduce((sum, r) => sum + Number(r.value || 0), 0)
      : null;
  const multiDetailRowCount = isMultiDetail
    ? detailTables.reduce((sum, t) => sum + (t.rows?.length || 0), 0)
    : 0;
  const displayRowCount = isMultiDetail
    ? multiDetailRowCount
    : isDetailTable
      ? detailRows.length
      : rows.length;

  return (
    <div className="space-y-6 animate-fade-in" data-testid="drill-down-page">
      {/* Back + Title */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div className="flex items-center gap-3">
          <Button
            variant="outline"
            size="sm"
            onClick={goBack}
            className="gap-2 shrink-0"
            data-testid="drill-back-btn"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to {parentLabel}
          </Button>
        </div>
        <div className="min-w-0 text-right sm:text-right flex-1">
          <h1 className="text-2xl font-bold text-slate-900 font-['Manrope'] truncate">{title}</h1>
          {subtitle ? (
            <p className="text-sm text-slate-500 mt-1 truncate" title={subtitle}>
              {subtitle}
            </p>
          ) : null}
        </div>
      </div>

      {/* Summary KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
          <p className="text-xs font-medium text-slate-500 mb-1">Rows</p>
          <p className="text-xl font-bold text-slate-900">{displayRowCount}</p>
        </div>
        {totalValue != null && (
          <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
            <p className="text-xs font-medium text-slate-500 mb-1">Total</p>
            <p className="text-xl font-bold text-slate-900">{formatCurrency(totalValue)}</p>
          </div>
        )}
        {totalCount != null && valueFormat === "number" && (
          <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
            <p className="text-xs font-medium text-slate-500 mb-1">Sum</p>
            <p className="text-xl font-bold text-slate-900">{totalCount.toLocaleString()}</p>
          </div>
        )}
      </div>

      {isNetSalesDrill && netSalesKpis && (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
              <p className="text-xs font-medium text-slate-500 mb-1">Latest MoM Growth</p>
              <p className={`text-xl font-bold ${netSalesKpis.latestGrowthPct != null && netSalesKpis.latestGrowthPct < 0 ? "text-red-600" : "text-emerald-600"}`}>
                {netSalesKpis.latestGrowthPct != null ? `${netSalesKpis.latestGrowthPct > 0 ? "+" : ""}${netSalesKpis.latestGrowthPct}%` : "-"}
              </p>
            </div>
            <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
              <p className="text-xs font-medium text-slate-500 mb-1">Top 3 States Share</p>
              <p className="text-xl font-bold text-slate-900">
                {netSalesKpis.top3StatesShare != null ? `${netSalesKpis.top3StatesShare}%` : "-"}
              </p>
            </div>
            <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
              <p className="text-xs font-medium text-slate-500 mb-1">Top 10 Customers Share</p>
              <p className="text-xl font-bold text-slate-900">
                {netSalesKpis.top10CustomersShare != null ? `${netSalesKpis.top10CustomersShare}%` : "-"}
              </p>
            </div>
            <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
              <p className="text-xs font-medium text-slate-500 mb-1">Avg Transaction Value</p>
              <p className="text-xl font-bold text-slate-900">{formatCurrency(netSalesKpis.avgTransactionValue)}</p>
            </div>
            <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
              <p className="text-xs font-medium text-slate-500 mb-1">Revenue per Customer</p>
              <p className="text-xl font-bold text-slate-900">{formatCurrency(netSalesKpis.avgRevenuePerCustomer)}</p>
            </div>
            <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
              <p className="text-xs font-medium text-slate-500 mb-1">Net Sales (Overview)</p>
              <p className="text-xl font-bold text-slate-900">{formatCurrency(netSalesKpis.netSalesValue)}</p>
            </div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
              <p className="text-sm font-semibold text-slate-800 mb-3">Monthly Net Sales Trend</p>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={netSalesVisuals.trends}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                    <XAxis dataKey="month" tick={{ fontSize: 11 }} stroke="#94A3B8" />
                    <YAxis tick={{ fontSize: 11 }} stroke="#94A3B8" tickFormatter={(v) => `₹${(v / 10000000).toFixed(1)}Cr`} />
                    <Tooltip formatter={(v) => formatCurrency(Number(v || 0))} />
                    <Line type="monotone" dataKey="value" stroke="#D63384" strokeWidth={2.5} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
            <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
              <p className="text-sm font-semibold text-slate-800 mb-3">Top States by Net Sales</p>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={netSalesVisuals.topStates} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                    <XAxis type="number" tick={{ fontSize: 11 }} stroke="#94A3B8" tickFormatter={(v) => `₹${(v / 10000000).toFixed(1)}Cr`} />
                    <YAxis type="category" dataKey="name" width={90} tick={{ fontSize: 11 }} stroke="#94A3B8" />
                    <Tooltip formatter={(v) => formatCurrency(Number(v || 0))} />
                    <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                      {netSalesVisuals.topStates.map((_, idx) => (
                        <Cell key={`state-bar-${idx}`} fill={COLORS[idx % COLORS.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        </>
      )}

      {/* Group by */}
      {groupByOptions.length > 0 && !isDetailTable && !isMultiDetail && (
        <div className="flex items-center gap-2">
          <span className="text-sm text-slate-600">Group by:</span>
          <Select value={groupBy} onValueChange={setGroupBy}>
            <SelectTrigger className="w-[160px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {groupByOptions.map((opt) => (
                <SelectItem key={opt.value} value={opt.value}>
                  {opt.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {/* Table */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden">
        {loading ? (
          <div className="flex justify-center py-16">
            <Loader2 className="w-8 h-8 animate-spin text-[#D63384]" />
          </div>
        ) : isMultiDetail ? (
          <div className="divide-y divide-slate-100">
            {detailTables.map((tbl, ti) => (
              <div key={ti} className="px-0 py-2">
                <h3 className="text-sm font-semibold text-slate-800 px-4 pt-2 pb-3 font-['Manrope']">
                  {tbl.title || `Section ${ti + 1}`}
                </h3>
                {!tbl.rows || tbl.rows.length === 0 ? (
                  <p className="text-sm text-slate-500 py-6 text-center">No rows in this section.</p>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        {tbl.columns.map((col) => (
                          <TableHead
                            key={col.key}
                            className={
                              col.align === "right"
                                ? "text-right"
                                : col.align === "center"
                                  ? "text-center"
                                  : ""
                            }
                          >
                            {col.label}
                          </TableHead>
                        ))}
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {tbl.rows.map((row, ri) => (
                        <TableRow key={ri}>
                          {tbl.columns.map((col) => (
                            <TableCell
                              key={col.key}
                              className={
                                col.align === "right"
                                  ? "text-right tabular-nums"
                                  : col.align === "center"
                                    ? "text-center"
                                    : col.type && col.type !== "text"
                                      ? "tabular-nums"
                                      : ""
                              }
                            >
                              {formatDetailCell(col, row[col.key])}
                            </TableCell>
                          ))}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </div>
            ))}
          </div>
        ) : isDetailTable ? (
          <Table>
            <TableHeader>
              <TableRow>
                {detailColumns.map((col) => (
                  <TableHead
                    key={col.key}
                    className={
                      col.align === "right" ? "text-right" : col.align === "center" ? "text-center" : ""
                    }
                  >
                    {col.label}
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {detailRows.map((row, i) => (
                <TableRow key={i}>
                  {detailColumns.map((col) => (
                    <TableCell
                      key={col.key}
                      className={
                        col.align === "right"
                          ? "text-right tabular-nums"
                          : col.align === "center"
                            ? "text-center"
                            : col.type && col.type !== "text"
                              ? "tabular-nums"
                              : ""
                      }
                    >
                      {formatDetailCell(col, row[col.key])}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{dimensionLabel}</TableHead>
                <TableHead className="text-right">Value</TableHead>
                {showPctColumn && <TableHead className="text-right">%</TableHead>}
              </TableRow>
            </TableHeader>
            <TableBody>
              {(rows || []).map((row, i) => (
                <TableRow key={row.dimension ?? row.name ?? i}>
                  <TableCell className="font-medium">
                    {row.dimension ?? row.name ?? "-"}
                  </TableCell>
                  <TableCell className="text-right">
                    {formatRowValue(row, valueFormat)}
                  </TableCell>
                  {showPctColumn && (
                    <TableCell className="text-right">
                      {row.pct != null ? `${Number(row.pct).toFixed(2)}%` : "-"}
                    </TableCell>
                  )}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
        {!loading &&
          (isMultiDetail
            ? multiDetailRowCount === 0
            : isDetailTable
              ? detailRows.length === 0
              : !rows || rows.length === 0) && (
            <p className="text-sm text-slate-500 py-8 text-center">No data available.</p>
          )}
      </div>
    </div>
  );
}
