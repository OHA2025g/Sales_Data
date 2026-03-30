import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { 
  ShieldAlert, 
  AlertTriangle, 
  TrendingDown,
  Users,
  MapPin,
  Package,
  CheckCircle,
  XCircle,
  AlertCircle
} from "lucide-react";
import { KPICard, KPICardSkeleton } from "@/components/KPICard";
import { Progress } from "@/components/ui/progress";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ReferenceLine,
  Legend
} from "recharts";

import { API } from "@/apiConfig";

const COLORS = ["#D63384", "#0F172A", "#10B981", "#F59E0B", "#3B82F6", "#8B5CF6"];

const ANOMALY_SUMMARY_CARD_CLASS =
  "bg-white rounded-xl border border-slate-100 shadow-sm p-4 cursor-pointer transition hover:border-[#D63384]/40 hover:shadow-md focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#D63384] focus-visible:ring-offset-2";

const formatCurrency = (value) => {
  if (value >= 10000000) return `₹${(value / 10000000).toFixed(2)}Cr`;
  if (value >= 100000) return `₹${(value / 100000).toFixed(2)}L`;
  if (value >= 1000) return `₹${(value / 1000).toFixed(1)}K`;
  return `₹${value.toFixed(0)}`;
};

const getStatusColor = (status) => {
  switch (status) {
    case "healthy": return "text-emerald-500";
    case "warning": return "text-amber-500";
    case "danger": return "text-red-500";
    default: return "text-slate-500";
  }
};

const getStatusBg = (status) => {
  switch (status) {
    case "healthy": return "bg-emerald-50 border-emerald-200";
    case "warning": return "bg-amber-50 border-amber-200";
    case "danger": return "bg-red-50 border-red-200";
    default: return "bg-slate-50 border-slate-200";
  }
};

const getStatusIcon = (status) => {
  switch (status) {
    case "healthy": return <CheckCircle className="w-5 h-5 text-emerald-500" />;
    case "warning": return <AlertCircle className="w-5 h-5 text-amber-500" />;
    case "danger": return <XCircle className="w-5 h-5 text-red-500" />;
    default: return <AlertCircle className="w-5 h-5 text-slate-500" />;
  }
};

export default function RiskGovernance() {
  const [loading, setLoading] = useState(true);
  const [indicators, setIndicators] = useState([]);
  const [returnsTrend, setReturnsTrend] = useState([]);
  const [anomalies, setAnomalies] = useState(null);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [indicatorsRes, returnsRes, anomaliesRes] = await Promise.all([
        axios.get(`${API}/risk/indicators`),
        axios.get(`${API}/risk/returns-trend`),
        axios.get(`${API}/risk/anomalies`),
      ]);
      setIndicators(indicatorsRes.data);
      setReturnsTrend(returnsRes.data);
      setAnomalies(anomaliesRes.data);
    } catch (err) {
      console.error("Error fetching risk data:", err);
    } finally {
      setLoading(false);
    }
  };

  const healthyCount = indicators.filter(i => i.status === "healthy").length;
  const warningCount = indicators.filter(i => i.status === "warning").length;
  const dangerCount = indicators.filter(i => i.status === "danger").length;

  const navigate = useNavigate();

  const openDrill = (config) => {
    navigate("/drill", { state: { ...config, parentPath: "/risk", parentLabel: "Risk & Governance" } });
  };

  const openAnomalyDrill = (kind) => {
    const a = anomalies || {};
    const base = { type: "static" };
    const R = (c) => ({ ...c, align: "right" });
    if (kind === "zone_spikes") {
      openDrill({
        ...base,
        title: "Zone spikes (MoM) — full detail",
        detailColumns: [
          { key: "zone", label: "Zone", type: "text" },
          { key: "month", label: "Month", type: "text" },
          R({ key: "value", label: "Last month sales", type: "currency" }),
          R({ key: "prev_value", label: "Prior month sales", type: "currency" }),
          R({ key: "growth_pct", label: "MoM growth", type: "percent" }),
        ],
        detailRows: (a.zone_spikes || []).map((r) => ({
          zone: r.zone ?? "—",
          month: r.month ?? "—",
          value: r.value,
          prev_value: r.prev_value,
          growth_pct: r.growth_pct,
        })),
      });
      return;
    }
    if (kind === "product_surges") {
      openDrill({
        ...base,
        title: "Product surges — full detail",
        detailColumns: [
          { key: "product", label: "Product", type: "text" },
          R({ key: "recent_avg", label: "Recent period avg (per line)", type: "currency" }),
          R({ key: "history_avg", label: "History avg (per line)", type: "currency" }),
          R({ key: "lift_pct", label: "Lift vs history", type: "percent" }),
        ],
        detailRows: (a.product_surges || []).map((r) => ({
          product: r.product ?? "—",
          recent_avg: r.recent_avg,
          history_avg: r.history_avg,
          lift_pct: r.lift_pct,
        })),
      });
      return;
    }
    if (kind === "promo_trend") {
      openDrill({
        ...base,
        title: "Promoted vs non-promoted — monthly detail",
        detailColumns: [
          { key: "month", label: "Month", type: "text" },
          R({ key: "promoted", label: "Promoted sales", type: "currency" }),
          R({ key: "non_promoted", label: "Non-promoted sales", type: "currency" }),
        ],
        detailRows: (a.promo_trend || []).map((r) => ({
          month: r.month ?? "—",
          promoted: r.promoted,
          non_promoted: r.non_promoted,
        })),
      });
      return;
    }
    if (kind === "price_outliers") {
      openDrill({
        ...base,
        title: "Price variance outliers — full detail",
        detailColumns: [
          { key: "product", label: "Product", type: "text" },
          { key: "zone", label: "Zone", type: "text" },
          R({ key: "avg_ppu", label: "Avg PPU", type: "number" }),
          R({ key: "min_ppu", label: "Min PPU", type: "number" }),
          R({ key: "max_ppu", label: "Max PPU", type: "number" }),
          R({ key: "variance_pct", label: "Min–max vs avg", type: "percent" }),
          R({ key: "lines", label: "Line count", type: "number" }),
        ],
        detailRows: (a.price_outliers || []).map((r) => ({
          product: r.product ?? "—",
          zone: r.zone ?? "—",
          avg_ppu: r.avg_ppu,
          min_ppu: r.min_ppu,
          max_ppu: r.max_ppu,
          variance_pct: r.variance_pct,
          lines: r.lines,
        })),
      });
      return;
    }
    if (kind === "division_zone_outliers") {
      openDrill({
        ...base,
        title: "Division–zone outliers — full detail",
        detailColumns: [
          { key: "division", label: "Division", type: "text" },
          { key: "zone", label: "Zone", type: "text" },
          { key: "month", label: "Month", type: "text" },
          R({ key: "value", label: "Last month sales", type: "currency" }),
          R({ key: "prior_avg", label: "Prior 6m avg (per line)", type: "currency" }),
          R({ key: "lift_pct", label: "Lift vs prior avg", type: "percent" }),
        ],
        detailRows: (a.division_zone_outliers || []).map((r) => ({
          division: r.division ?? "—",
          zone: r.zone ?? "—",
          month: r.month ?? "—",
          value: r.value,
          prior_avg: r.prior_avg,
          lift_pct: r.lift_pct,
        })),
      });
    }
  };

  const anomalyCardKeyHandler = (e, kind) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      openAnomalyDrill(kind);
    }
  };

  const overallScore = indicators.length > 0 
    ? Math.round((healthyCount * 100 + warningCount * 50 + dangerCount * 0) / indicators.length)
    : 0;

  const fmtPct = (v) => (v == null ? "-" : `${Number(v).toFixed(2)}%`);

  const zoneSpikeChartData = (anomalies?.zone_spikes || []).map((r) => ({
    zone: String(r.zone ?? "Unknown"),
    month: r.month ?? "",
    sales: Number(r.value) || 0,
    mom_pct: Number(r.growth_pct) || 0,
  }));

  const productSurgeChartData = (anomalies?.product_surges || []).slice(0, 12).map((r) => ({
    product: String(r.product ?? "Unknown"),
    recent_avg: Number(r.recent_avg) || 0,
    history_avg: Number(r.history_avg) || 0,
    lift_pct: Number(r.lift_pct) || 0,
  }));

  const priceVarianceChartData = (anomalies?.price_outliers || [])
    .map((r) => {
      const product = String(r.product ?? "Unknown");
      const zone = String(r.zone ?? "Unknown");
      const shortProd = product.length > 14 ? `${product.slice(0, 14)}…` : product;
      const shortZone = zone.length > 10 ? `${zone.slice(0, 10)}…` : zone;
      return {
        label: `${shortProd} / ${shortZone}`,
        product,
        zone,
        variance_pct: Number(r.variance_pct) || 0,
        avg_ppu: Number(r.avg_ppu) || 0,
        min_ppu: Number(r.min_ppu) || 0,
        max_ppu: Number(r.max_ppu) || 0,
      };
    })
    .sort((a, b) => b.variance_pct - a.variance_pct)
    .slice(0, 15);

  const divisionZoneChartData = (anomalies?.division_zone_outliers || [])
    .map((r) => {
      const division = String(r.division ?? "Unknown");
      const zone = String(r.zone ?? "Unknown");
      const shortDiv = division.length > 12 ? `${division.slice(0, 12)}…` : division;
      const shortZ = zone.length > 10 ? `${zone.slice(0, 10)}…` : zone;
      return {
        label: `${shortDiv} · ${shortZ}`,
        division,
        zone,
        month: r.month ?? "",
        sales: Number(r.value) || 0,
        lift_pct: Number(r.lift_pct) || 0,
      };
    })
    .sort((a, b) => b.lift_pct - a.lift_pct)
    .slice(0, 15);

  return (
    <div className="space-y-6 animate-fade-in" data-testid="risk-governance">
      {/* Overall Risk Score */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="risk-score-card">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-2xl font-bold text-slate-900 font-['Manrope']">Risk Governance Dashboard</h2>
            <p className="text-slate-500">Monitor key risk indicators and take action</p>
          </div>
          <div className="text-center">
            <div className="text-4xl font-bold text-[#D63384] font-['Manrope']">{overallScore}</div>
            <p className="text-xs text-slate-500">Health Score</p>
          </div>
        </div>
        <Progress value={overallScore} className="h-3" />
        <div className="flex justify-between mt-3 text-sm">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-emerald-500" />
            <span className="text-slate-600">{healthyCount} Healthy</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-amber-500" />
            <span className="text-slate-600">{warningCount} Warning</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-red-500" />
            <span className="text-slate-600">{dangerCount} Critical</span>
          </div>
        </div>
      </div>

      <Tabs defaultValue="overview" className="w-full">
        <TabsList className="bg-slate-50 border border-slate-200">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="anomalies">Anomalies & Outliers</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-6 space-y-6">
          {/* KPI Summary */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            {loading ? (
              <>
                <KPICardSkeleton />
                <KPICardSkeleton />
                <KPICardSkeleton />
                <KPICardSkeleton />
              </>
            ) : (
              <>
                <KPICard
                  title="Risk Indicators"
                  value={indicators.length}
                  icon={ShieldAlert}
                  testId="kpi-indicators"
                  onClick={() => openDrill({ type: "static", title: "All Risk Indicators", staticRows: indicators.map((i) => ({ name: i.metric, value: i.value })), valueFormat: "number" })}
                />
                <KPICard
                  title="Healthy Metrics"
                  value={healthyCount}
                  icon={CheckCircle}
                  testId="kpi-healthy"
                  onClick={() => openDrill({ type: "static", title: "Healthy Metrics", staticRows: indicators.filter((i) => i.status === "healthy").map((i) => ({ name: i.metric, value: i.value })), valueFormat: "number" })}
                />
                <KPICard
                  title="Warnings"
                  value={warningCount}
                  icon={AlertTriangle}
                  testId="kpi-warnings"
                  onClick={() => openDrill({ type: "static", title: "Warning Indicators", staticRows: indicators.filter((i) => i.status === "warning").map((i) => ({ name: i.metric, value: i.value })), valueFormat: "number" })}
                />
                <KPICard
                  title="Critical Issues"
                  value={dangerCount}
                  icon={XCircle}
                  testId="kpi-critical"
                  onClick={() => openDrill({ type: "static", title: "Critical Indicators", staticRows: indicators.filter((i) => i.status === "danger").map((i) => ({ name: i.metric, value: i.value })), valueFormat: "number" })}
                />
              </>
            )}
          </div>

          {/* Risk Indicators Grid */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-6" data-testid="risk-indicators-grid">
            {indicators.map((indicator, idx) => (
              <div
                key={indicator.metric}
                className={`rounded-xl border p-6 ${getStatusBg(indicator.status)}`}
                data-testid={`risk-indicator-${idx}`}
              >
                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h4 className="text-sm font-semibold text-slate-900">{indicator.metric}</h4>
                    <p className="mt-1 text-[11px] text-slate-500">{indicator.description}</p>
                  </div>
                  {getStatusIcon(indicator.status)}
                </div>
                <div className="flex items-end justify-between">
                  <div>
                    <p className={`text-2xl font-bold font-['Manrope'] ${getStatusColor(indicator.status)}`}>
                      {typeof indicator.value === "number" && indicator.metric.includes("%")
                        ? `${indicator.value}%`
                        : indicator.value}
                    </p>
                    <p className="text-[11px] text-slate-500">
                      Threshold: {indicator.threshold}
                      {indicator.metric.includes("%") || indicator.metric.includes("Rate") ? "%" : ""}
                    </p>
                  </div>
                  <div className="w-20">
                    <Progress
                      value={Math.min((indicator.value / indicator.threshold) * 100, 150)}
                      className={`h-2 ${indicator.status === "danger" ? "[&>div]:bg-red-500" : indicator.status === "warning" ? "[&>div]:bg-amber-500" : "[&>div]:bg-emerald-500"}`}
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* Returns Trend Chart */}
          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="returns-trend-chart">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Returns Trend Analysis</h3>
            {loading ? (
              <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
            ) : (
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={returnsTrend}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="month" tick={{ fontSize: 12 }} stroke="#94A3B8" />
                  <YAxis yAxisId="left" tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
                  <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => `${v}%`} />
                  <Legend
                    verticalAlign="top"
                    align="right"
                    iconType="circle"
                    wrapperStyle={{ paddingBottom: 8 }}
                  />
                  <ReferenceLine yAxisId="right" y={2} stroke="#EF4444" strokeDasharray="3 3" label={{ value: "2% Threshold", position: "right", fill: "#EF4444", fontSize: 10 }} />
                  <Tooltip
                    content={({ active, payload, label }) => {
                      if (active && payload && payload.length) {
                        return (
                          <div className="custom-tooltip">
                            <p className="font-semibold mb-1">{label}</p>
                            <p className="text-sm">Returns Value: {formatCurrency(payload[0]?.value || 0)}</p>
                            <p className="text-sm">Returns Rate: {payload[1]?.value?.toFixed(2)}%</p>
                          </div>
                        );
                      }
                      return null;
                    }}
                  />
                  <Line yAxisId="left" type="monotone" dataKey="returns_value" stroke="#D63384" strokeWidth={2} dot={{ fill: "#D63384" }} name="Returns Value" />
                  <Line yAxisId="right" type="monotone" dataKey="returns_rate" stroke="#F59E0B" strokeWidth={2} dot={{ fill: "#F59E0B" }} name="Returns Rate %" />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>

          {/* Monthly Returns Table */}
          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="returns-table">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Monthly Returns Summary</h3>
            <div className="overflow-x-auto">
              <table className="w-full data-table">
                <thead>
                  <tr>
                    <th className="text-left">Month</th>
                    <th className="text-right">Returns Value</th>
                    <th className="text-right">Returns Quantity</th>
                    <th className="text-right">Returns Rate</th>
                    <th className="text-center">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {returnsTrend.map((row, idx) => (
                    <tr key={row.month} data-testid={`returns-row-${idx}`}>
                      <td className="font-medium">{row.month}</td>
                      <td className="text-right">{formatCurrency(row.returns_value)}</td>
                      <td className="text-right">{row.returns_qty?.toLocaleString()}</td>
                      <td className="text-right">
                        <span className={row.returns_rate > 2 ? "text-red-500" : row.returns_rate > 1.5 ? "text-amber-500" : "text-emerald-500"}>
                          {row.returns_rate}%
                        </span>
                      </td>
                      <td className="text-center">
                        {row.returns_rate <= 1.5 ? (
                          <CheckCircle className="w-4 h-4 text-emerald-500 inline" />
                        ) : row.returns_rate <= 2 ? (
                          <AlertCircle className="w-4 h-4 text-amber-500 inline" />
                        ) : (
                          <XCircle className="w-4 h-4 text-red-500 inline" />
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Risk Actions */}
          <div className="bg-gradient-to-r from-red-50 to-amber-50 border border-red-100 rounded-xl p-6" data-testid="risk-actions">
            <h3 className="font-semibold text-slate-900 mb-3 font-['Manrope'] flex items-center gap-2">
              <AlertTriangle className="w-5 h-5 text-amber-500" />
              Recommended Actions
            </h3>
            <ul className="space-y-3 text-sm text-slate-700">
              {dangerCount > 0 && (
                <li className="flex items-start gap-2">
                  <XCircle className="w-4 h-4 text-red-500 mt-0.5 flex-shrink-0" />
                  <span>
                    <strong>Critical:</strong> Address {dangerCount} critical risk indicator(s) immediately to mitigate business impact.
                  </span>
                </li>
              )}
              {warningCount > 0 && (
                <li className="flex items-start gap-2">
                  <AlertCircle className="w-4 h-4 text-amber-500 mt-0.5 flex-shrink-0" />
                  <span>
                    <strong>Warning:</strong> Monitor {warningCount} warning indicator(s) closely and develop contingency plans.
                  </span>
                </li>
              )}
              <li className="flex items-start gap-2">
                <MapPin className="w-4 h-4 text-blue-500 mt-0.5 flex-shrink-0" />
                <span>Consider geographic diversification to reduce zone concentration risk.</span>
              </li>
              <li className="flex items-start gap-2">
                <Users className="w-4 h-4 text-purple-500 mt-0.5 flex-shrink-0" />
                <span>Implement key account management program for top customers to reduce dependency risk.</span>
              </li>
              <li className="flex items-start gap-2">
                <TrendingDown className="w-4 h-4 text-emerald-500 mt-0.5 flex-shrink-0" />
                <span>Investigate returns by product and geography to identify quality or fulfillment issues.</span>
              </li>
            </ul>
          </div>
        </TabsContent>

        <TabsContent value="anomalies" className="mt-6 space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
            <div
              role="button"
              tabIndex={0}
              className={ANOMALY_SUMMARY_CARD_CLASS}
              onClick={() => openAnomalyDrill("zone_spikes")}
              onKeyDown={(e) => anomalyCardKeyHandler(e, "zone_spikes")}
              aria-label="Open full detail for zone spikes"
            >
              <p className="text-xs font-medium text-slate-500 mb-1">Zone spikes (MoM)</p>
              <p className="text-2xl font-bold text-slate-900">{anomalies?.zone_spikes?.length ?? 0}</p>
              <p className="text-xs text-slate-400">Top zones with highest growth · click for rows</p>
            </div>
            <div
              role="button"
              tabIndex={0}
              className={ANOMALY_SUMMARY_CARD_CLASS}
              onClick={() => openAnomalyDrill("product_surges")}
              onKeyDown={(e) => anomalyCardKeyHandler(e, "product_surges")}
              aria-label="Open full detail for product surges"
            >
              <p className="text-xs font-medium text-slate-500 mb-1">Product surges</p>
              <p className="text-2xl font-bold text-slate-900">{anomalies?.product_surges?.length ?? 0}</p>
              <p className="text-xs text-slate-400">Recent avg vs history avg · click for rows</p>
            </div>
            <div
              role="button"
              tabIndex={0}
              className={ANOMALY_SUMMARY_CARD_CLASS}
              onClick={() => openAnomalyDrill("promo_trend")}
              onKeyDown={(e) => anomalyCardKeyHandler(e, "promo_trend")}
              aria-label="Open full detail for promo trend by month"
            >
              <p className="text-xs font-medium text-slate-500 mb-1">Promo months tracked</p>
              <p className="text-2xl font-bold text-slate-900">{anomalies?.promo_trend?.length ?? 0}</p>
              <p className="text-xs text-slate-400">Promoted vs Non-promoted · click for rows</p>
            </div>
            <div
              role="button"
              tabIndex={0}
              className={ANOMALY_SUMMARY_CARD_CLASS}
              onClick={() => openAnomalyDrill("price_outliers")}
              onKeyDown={(e) => anomalyCardKeyHandler(e, "price_outliers")}
              aria-label="Open full detail for price variance outliers"
            >
              <p className="text-xs font-medium text-slate-500 mb-1">Price variance outliers</p>
              <p className="text-2xl font-bold text-slate-900">{anomalies?.price_outliers?.length ?? 0}</p>
              <p className="text-xs text-slate-400">Product+zone PPU variance · click for rows</p>
            </div>
            <div
              role="button"
              tabIndex={0}
              className={ANOMALY_SUMMARY_CARD_CLASS}
              onClick={() => openAnomalyDrill("division_zone_outliers")}
              onKeyDown={(e) => anomalyCardKeyHandler(e, "division_zone_outliers")}
              aria-label="Open full detail for division-zone outliers"
            >
              <p className="text-xs font-medium text-slate-500 mb-1">Division-zone outliers</p>
              <p className="text-2xl font-bold text-slate-900">{anomalies?.division_zone_outliers?.length ?? 0}</p>
              <p className="text-xs text-slate-400">Last month vs prior avg · click for rows</p>
            </div>
          </div>

          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Promoted vs Non-promoted Sales Trend</h3>
            {loading ? (
              <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
            ) : (
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={anomalies?.promo_trend || []}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="month" tick={{ fontSize: 12 }} stroke="#94A3B8" />
                  <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
                  <Tooltip />
                  <Line type="monotone" dataKey="promoted" stroke="#D63384" strokeWidth={2} dot={false} name="Promoted" />
                  <Line type="monotone" dataKey="non_promoted" stroke="#0F172A" strokeWidth={2} dot={false} name="Non-promoted" />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="zone-spike-chart">
              <h3 className="font-semibold text-slate-900 mb-1 font-['Manrope']">Zones with sudden MoM increase</h3>
              <p className="text-xs text-slate-500 mb-4">MoM % by zone (green = up, red = down; hover for sales and month).</p>
              {loading ? (
                <div className="h-80 bg-slate-100 animate-pulse rounded-lg" />
              ) : zoneSpikeChartData.length === 0 ? (
                <p className="text-center text-slate-500 py-16">No zone spike data detected.</p>
              ) : (
                <ResponsiveContainer width="100%" height={320}>
                  <BarChart layout="vertical" data={zoneSpikeChartData} margin={{ left: 8, right: 16, top: 8, bottom: 8 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                    <XAxis type="number" tick={{ fontSize: 11 }} stroke="#94A3B8" tickFormatter={(v) => `${v}%`} />
                    <YAxis type="category" dataKey="zone" width={88} tick={{ fontSize: 11 }} stroke="#94A3B8" />
                    <ReferenceLine x={0} stroke="#64748B" strokeDasharray="3 3" />
                    <Tooltip
                      content={({ active, payload }) => {
                        if (!active || !payload?.length) return null;
                        const d = payload[0].payload;
                        return (
                          <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 shadow-md text-sm">
                            <p className="font-semibold text-slate-900">{d.zone}</p>
                            <p className="text-slate-600">Month: {d.month || "—"}</p>
                            <p className="text-slate-600">Sales: {formatCurrency(d.sales)}</p>
                            <p className="text-slate-900">MoM: {fmtPct(d.mom_pct)}</p>
                          </div>
                        );
                      }}
                    />
                    <Bar dataKey="mom_pct" name="MoM %" radius={[0, 4, 4, 0]} maxBarSize={28}>
                      {zoneSpikeChartData.map((entry, i) => (
                        <Cell
                          key={`zone-mom-${i}`}
                          fill={entry.mom_pct >= 0 ? "#10B981" : "#EF4444"}
                        />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>

            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="product-surge-chart">
              <h3 className="font-semibold text-slate-900 mb-1 font-['Manrope']">Products with high recent sales vs history</h3>
              <p className="text-xs text-slate-500 mb-4">Recent vs history average revenue (top {productSurgeChartData.length}; hover for lift %).</p>
              {loading ? (
                <div className="h-80 bg-slate-100 animate-pulse rounded-lg" />
              ) : productSurgeChartData.length === 0 ? (
                <p className="text-center text-slate-500 py-16">No product surge data detected.</p>
              ) : (
                <ResponsiveContainer width="100%" height={320}>
                  <BarChart data={productSurgeChartData} margin={{ left: 4, right: 8, top: 8, bottom: 56 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                    <XAxis
                      dataKey="product"
                      tick={{ fontSize: 10 }}
                      stroke="#94A3B8"
                      angle={-32}
                      textAnchor="end"
                      interval={0}
                      height={52}
                    />
                    <YAxis tick={{ fontSize: 11 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} width={72} />
                    <Tooltip
                      content={({ active, payload }) => {
                        if (!active || !payload?.length) return null;
                        const d = payload[0].payload;
                        return (
                          <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 shadow-md text-sm">
                            <p className="font-semibold text-slate-900">{d.product}</p>
                            <p className="text-[#D63384]">Recent avg: {formatCurrency(d.recent_avg)}</p>
                            <p className="text-slate-600">History avg: {formatCurrency(d.history_avg)}</p>
                            <p className="text-slate-900 font-medium">Lift: {fmtPct(d.lift_pct)}</p>
                          </div>
                        );
                      }}
                    />
                    <Legend verticalAlign="top" align="right" wrapperStyle={{ paddingBottom: 8 }} />
                    <Bar dataKey="recent_avg" name="Recent avg" fill="#D63384" radius={[4, 4, 0, 0]} />
                    <Bar dataKey="history_avg" name="History avg" fill="#94A3B8" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="price-variance-chart">
              <h3 className="font-semibold text-slate-900 mb-1 font-['Manrope']">Price variance outliers (same product & zone)</h3>
              <p className="text-xs text-slate-500 mb-4">
                PPU spread % (top {priceVarianceChartData.length} by variance; hover for min/avg/max).
              </p>
              {loading ? (
                <div className="h-80 bg-slate-100 animate-pulse rounded-lg" />
              ) : priceVarianceChartData.length === 0 ? (
                <p className="text-center text-slate-500 py-16">No price outliers detected.</p>
              ) : (
                <ResponsiveContainer width="100%" height={320}>
                  <BarChart
                    layout="vertical"
                    data={priceVarianceChartData}
                    margin={{ left: 8, right: 16, top: 8, bottom: 8 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                    <XAxis type="number" tick={{ fontSize: 11 }} stroke="#94A3B8" tickFormatter={(v) => `${v}%`} />
                    <YAxis type="category" dataKey="label" width={130} tick={{ fontSize: 10 }} stroke="#94A3B8" />
                    <ReferenceLine x={0} stroke="#64748B" strokeDasharray="3 3" />
                    <Tooltip
                      content={({ active, payload }) => {
                        if (!active || !payload?.length) return null;
                        const d = payload[0].payload;
                        return (
                          <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 shadow-md text-sm">
                            <p className="font-semibold text-slate-900">{d.product}</p>
                            <p className="text-slate-600">Zone: {d.zone}</p>
                            <p className="text-[#D63384] font-medium">Variance: {fmtPct(d.variance_pct)}</p>
                            <p className="text-slate-600">Avg PPU: ₹{d.avg_ppu.toFixed(2)}</p>
                            <p className="text-slate-600">Min: ₹{d.min_ppu.toFixed(2)} · Max: ₹{d.max_ppu.toFixed(2)}</p>
                          </div>
                        );
                      }}
                    />
                    <Bar dataKey="variance_pct" name="Variance %" fill="#F59E0B" radius={[0, 4, 4, 0]} maxBarSize={26} />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>

            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="division-zone-chart">
              <h3 className="font-semibold text-slate-900 mb-1 font-['Manrope']">Division code × Zone sales outliers</h3>
              <p className="text-xs text-slate-500 mb-4">
                Lift % vs prior average (top {divisionZoneChartData.length}; hover for sales and month).
              </p>
              {loading ? (
                <div className="h-80 bg-slate-100 animate-pulse rounded-lg" />
              ) : divisionZoneChartData.length === 0 ? (
                <p className="text-center text-slate-500 py-16">No division-zone outliers detected.</p>
              ) : (
                <ResponsiveContainer width="100%" height={320}>
                  <BarChart
                    layout="vertical"
                    data={divisionZoneChartData}
                    margin={{ left: 8, right: 16, top: 8, bottom: 8 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                    <XAxis type="number" tick={{ fontSize: 11 }} stroke="#94A3B8" tickFormatter={(v) => `${v}%`} />
                    <YAxis type="category" dataKey="label" width={130} tick={{ fontSize: 10 }} stroke="#94A3B8" />
                    <ReferenceLine x={0} stroke="#64748B" strokeDasharray="3 3" />
                    <Tooltip
                      content={({ active, payload }) => {
                        if (!active || !payload?.length) return null;
                        const d = payload[0].payload;
                        return (
                          <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 shadow-md text-sm">
                            <p className="font-semibold text-slate-900">{d.division}</p>
                            <p className="text-slate-600">Zone: {d.zone}</p>
                            <p className="text-slate-600">Month: {d.month || "—"}</p>
                            <p className="text-slate-600">Sales: {formatCurrency(d.sales)}</p>
                            <p className="text-[#D63384] font-medium">Lift: {fmtPct(d.lift_pct)}</p>
                          </div>
                        );
                      }}
                    />
                    <Bar dataKey="lift_pct" name="Lift %" radius={[0, 4, 4, 0]} maxBarSize={26}>
                      {divisionZoneChartData.map((entry, i) => (
                        <Cell
                          key={`div-zone-${i}`}
                          fill={entry.lift_pct >= 0 ? "#10B981" : "#EF4444"}
                        />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
