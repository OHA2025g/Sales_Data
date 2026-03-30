import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { toast } from "sonner";
import {
  DollarSign,
  TrendingUp,
  ArrowUpRight,
  BarChart3,
  Target,
  RefreshCw,
  ChevronRight,
  Loader2,
  X,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
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
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  LineChart,
  Line,
  Cell,
} from "recharts";

import { API } from "@/apiConfig";

const KPI_CONFIG = [
  {
    id: "net_sales_value",
    name: "Net Sales Value",
    formula: "SUM(NET_SALES_VALUE)",
    granularity: ["month", "zone", "state", "product", "customer"],
    frequency: "Monthly",
    owner: "Sales Head",
    objective: "Revenue Growth",
    icon: DollarSign,
    format: "currency",
  },
  {
    id: "gross_sales_value",
    name: "Gross Sales Value",
    formula: "SUM(NET_SALES_VALUE where >0)",
    granularity: ["month", "zone", "state", "product", "customer"],
    frequency: "Monthly",
    owner: "Finance",
    objective: "Billing Strength",
    icon: DollarSign,
    format: "currency",
  },
  {
    id: "returns_value",
    name: "Returns Value",
    formula: "ABS(SUM(NET_SALES_VALUE where <0))",
    granularity: ["month", "product", "state"],
    frequency: "Monthly",
    owner: "Operations",
    objective: "Quality Control",
    icon: ArrowUpRight,
    format: "currency",
  },
  {
    id: "returns_rate_pct",
    name: "Returns Rate %",
    formula: "Returns Value / Gross Sales",
    granularity: ["month", "product"],
    frequency: "Monthly",
    owner: "Ops + Sales",
    objective: "Operational Efficiency",
    icon: BarChart3,
    format: "percent",
  },
  {
    id: "mom_growth_pct",
    name: "MoM Growth %",
    formula: "(Current Month - Previous Month) / Previous Month",
    granularity: ["month"],
    frequency: "Monthly",
    owner: "Sales Head",
    objective: "Growth Momentum",
    icon: TrendingUp,
    format: "growth",
  },
  {
    id: "revenue_concentration_pct",
    name: "Revenue Concentration %",
    formula: "Top 3 States Revenue / Total Revenue",
    granularity: ["state"],
    frequency: "Quarterly",
    owner: "Strategy",
    objective: "Risk Diversification",
    icon: Target,
    format: "percent",
  },
];

const formatCurrency = (value) => {
  if (value == null) return "-";
  const v = Number(value);
  if (v >= 10000000) return `₹${(v / 10000000).toFixed(2)}Cr`;
  if (v >= 100000) return `₹${(v / 100000).toFixed(2)}L`;
  if (v >= 1000) return `₹${(v / 1000).toFixed(1)}K`;
  return `₹${v.toFixed(0)}`;
};

const formatValue = (kpi, row) => {
  if (kpi.format === "currency" || kpi.id === "revenue_concentration_pct") return formatCurrency(row.value);
  if (kpi.format === "percent") return `${Number(row.value).toFixed(2)}%`;
  if (kpi.format === "growth") return formatCurrency(row.value);
  if (row.pct != null) return `${Number(row.pct).toFixed(2)}%`;
  return String(row.value);
};

const DIMENSION_LABELS = {
  month: "Month",
  zone: "Zone",
  state: "State",
  product: "Product",
  customer: "Customer",
};

export default function RevenueGrowthKPIs() {
  const navigate = useNavigate();
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [drillKpi, setDrillKpi] = useState(null);
  const [drillOpen, setDrillOpen] = useState(false);
  const [drillGroupBy, setDrillGroupBy] = useState("month");
  const [drillData, setDrillData] = useState([]);
  const [drillLoading, setDrillLoading] = useState(false);
  const [filters, setFilters] = useState({});
  const [visuals, setVisuals] = useState(null);

  useEffect(() => {
    fetchSummary();
  }, []);

  const fetchSummary = async () => {
    setLoading(true);
    const emptySummary = {
      net_sales_value: 0,
      gross_sales_value: 0,
      returns_value: 0,
      returns_rate_pct: 0,
      mom_growth_pct: null,
      revenue_concentration_pct: 0,
      data_loaded: false,
    };
    const emptyVisuals = {
      kpis: {
        cagr_3m_pct: 0,
        run_rate: 0,
        run_rate_target: 0,
        run_rate_vs_target_pct: 0,
        recovery_growth_return_adjusted_pct: 0,
      },
      run_rate_vs_target: [],
      growth_contribution: { zone: [], state: [], product: [] },
      new_vs_existing_growth: [],
      recovery_breakdown: [],
    };
    try {
      // Use same endpoints as Executive Dashboard so data loads when dashboard works
      const [overviewRes, trendsRes, concentrationRes, visualsRes] = await Promise.all([
        axios.get(`${API}/dashboard/overview`),
        axios.get(`${API}/dashboard/trends`),
        axios.get(`${API}/dashboard/concentration`),
        axios.get(`${API}/revenue-growth/visuals`),
      ]);
      const overview = overviewRes.data;
      const trends = trendsRes.data || [];
      const concentration = concentrationRes.data || {};
      const lastGrowth = trends.length >= 1 && trends[trends.length - 1].growth_pct != null
        ? trends[trends.length - 1].growth_pct
        : null;
      setSummary({
        net_sales_value: overview.net_sales_value,
        gross_sales_value: overview.gross_sales_value,
        returns_value: overview.returns_value,
        returns_rate_pct: overview.returns_rate,
        mom_growth_pct: lastGrowth,
        revenue_concentration_pct: concentration.top_3_states_pct ?? 0,
        data_loaded: true,
      });
      setVisuals(visualsRes.data || emptyVisuals);
    } catch (err) {
      console.error(err);
      setSummary(emptySummary);
      setVisuals(emptyVisuals);
      if (err.response?.status !== 404) {
        toast.error(err.code === "ERR_NETWORK"
          ? "Cannot reach server. Is the backend running on port 10000?"
          : err.response?.data?.detail || "Failed to load KPIs.");
      }
    } finally {
      setLoading(false);
    }
  };

  const loadData = async () => {
    try {
      const res = await axios.post(`${API}/data/load`);
      if (res.data?.status === "started" || res.data?.status === "running") {
        toast.success("Data load started. KPIs will refresh shortly.");
      } else {
        toast.success(`Data loaded: ${res.data?.records_loaded ?? 0} records`);
      }
      await fetchSummary();
    } catch (err) {
      toast.error(err.response?.data?.detail || "Failed to load data. Ensure backend and MongoDB are running.");
    }
  };

  const fetchDrill = async () => {
    if (!drillKpi) return;
    setDrillLoading(true);
    try {
      const params = new URLSearchParams({ kpi: drillKpi.id, group_by: drillGroupBy });
      if (filters.month) params.set("month", filters.month);
      if (filters.zone) params.set("zone", filters.zone);
      if (filters.state) params.set("state", filters.state);
      if (filters.product) params.set("product", filters.product);
      if (filters.customer) params.set("customer", filters.customer);
      const res = await axios.get(`${API}/revenue-kpi/drill?${params.toString()}`);
      setDrillData(res.data || []);
    } catch (err) {
      toast.error(err.response?.data?.detail || "Failed to load drill-down");
      setDrillData([]);
    } finally {
      setDrillLoading(false);
    }
  };

  useEffect(() => {
    if (drillOpen && drillKpi) fetchDrill();
  }, [drillOpen, drillKpi, drillGroupBy, filters]);

  const openDrill = (kpi) => {
    if (kpi.id === "net_sales_value") {
      navigate("/drill", {
        state: {
          type: "revenue-kpi",
          title: "Net Sales Value",
          kpi: "net_sales_value",
          kpiKey: "net_sales_value",
          valueFormat: "currency",
          groupByOptions: (kpi.granularity || []).map((g) => ({
            value: g,
            label: DIMENSION_LABELS[g] || g,
          })),
          parentPath: "/revenue-growth",
          parentLabel: "Revenue & Growth KPIs",
        },
      });
      return;
    }
    setDrillKpi(kpi);
    setFilters({});
    setDrillGroupBy(kpi.granularity[0]);
    setDrillOpen(true);
  };

  const drillInto = (dimensionValue) => {
    if (!drillKpi) return;
    const nextLevels = drillKpi.granularity;
    const currentIndex = nextLevels.indexOf(drillGroupBy);
    setFilters((prev) => ({ ...prev, [drillGroupBy]: dimensionValue }));
    if (currentIndex < nextLevels.length - 1) {
      setDrillGroupBy(nextLevels[currentIndex + 1]);
    }
  };

  const clearFilter = (key) => {
    setFilters((prev) => {
      const next = { ...prev };
      delete next[key];
      return next;
    });
    const idx = drillKpi?.granularity.indexOf(key) ?? -1;
    if (idx >= 0 && drillKpi?.granularity[idx - 1]) {
      setDrillGroupBy(drillKpi.granularity[idx - 1]);
    }
  };

  const summaryValue = (id) => {
    if (!summary) return "-";
    const v = summary[id];
    if (v == null) return "-";
    if (id === "net_sales_value" || id === "gross_sales_value" || id === "returns_value")
      return formatCurrency(v);
    if (id === "returns_rate_pct" || id === "revenue_concentration_pct") return `${Number(v).toFixed(2)}%`;
    if (id === "mom_growth_pct") return v != null ? `${v > 0 ? "+" : ""}${v}%` : "-";
    return String(v);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[50vh]">
        <Loader2 className="w-8 h-8 animate-spin text-[#D63384]" />
      </div>
    );
  }

  const noData = summary && summary.data_loaded === false;
  const growthContribChart =
    (visuals?.growth_contribution?.product && visuals.growth_contribution.product.length > 0
      ? visuals.growth_contribution.product
      : visuals?.growth_contribution?.state && visuals.growth_contribution.state.length > 0
        ? visuals.growth_contribution.state
        : visuals?.growth_contribution?.zone || []
    ).slice(0, 8);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-lg font-semibold text-slate-800 mb-1">Revenue & Growth KPIs</h2>
        <p className="text-sm text-slate-500">Drill down by month, zone, state, product, or customer per KPI.</p>
      </div>

      {noData && (
        <div className="flex items-center justify-between gap-4 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-amber-800">
          <p className="text-sm font-medium">No sales data loaded. Load data from Excel to view KPIs and drill-downs.</p>
          <Button onClick={loadData} className="bg-amber-600 hover:bg-amber-700 text-white shrink-0">
            <RefreshCw className="w-4 h-4 mr-2" />
            Load Data
          </Button>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {KPI_CONFIG.map((kpi) => {
          const Icon = kpi.icon;
          return (
            <div
              key={kpi.id}
              onClick={() => openDrill(kpi)}
              className="bg-white rounded-lg border border-slate-100 shadow-sm p-4 cursor-pointer hover:border-[#D63384]/30 hover:shadow-md transition-all flex flex-col gap-2"
              data-testid={`revenue-kpi-${kpi.id}`}
            >
              <div className="flex items-center gap-2">
                <div className="p-1.5 rounded-full bg-[#D63384]/10 text-[#D63384]">
                  <Icon className="w-4 h-4" />
                </div>
                <span className="text-sm font-medium text-slate-700">{kpi.name}</span>
              </div>
              <p className="text-2xl font-bold text-slate-900 font-['Manrope']">{summaryValue(kpi.id)}</p>
              <p className="text-xs text-slate-400">{kpi.formula}</p>
              <div className="flex flex-wrap gap-x-3 gap-y-0 text-[11px] text-slate-500 mt-auto">
                <span>Owner: {kpi.owner}</span>
                <span>Objective: {kpi.objective}</span>
              </div>
            </div>
          );
        })}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
        <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
          <p className="text-xs text-slate-500">3M CAGR</p>
          <p className="text-2xl font-bold font-['Manrope']">{Number(visuals?.kpis?.cagr_3m_pct || 0).toFixed(2)}%</p>
        </div>
        <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
          <p className="text-xs text-slate-500">Run-rate</p>
          <p className="text-2xl font-bold font-['Manrope']">{formatCurrency(visuals?.kpis?.run_rate || 0)}</p>
        </div>
        <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
          <p className="text-xs text-slate-500">Target</p>
          <p className="text-2xl font-bold font-['Manrope']">{formatCurrency(visuals?.kpis?.run_rate_target || 0)}</p>
        </div>
        <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
          <p className="text-xs text-slate-500">Run-rate vs Target</p>
          <p className="text-2xl font-bold font-['Manrope']">{Number(visuals?.kpis?.run_rate_vs_target_pct || 0).toFixed(2)}%</p>
        </div>
        <div className="bg-white rounded-lg border border-slate-100 shadow-sm p-4">
          <p className="text-xs text-slate-500">Recovery Growth (Return-adjusted)</p>
          <p className="text-2xl font-bold font-['Manrope']">{Number(visuals?.kpis?.recovery_growth_return_adjusted_pct || 0).toFixed(2)}%</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Run-rate vs Target</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={visuals?.run_rate_vs_target || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="name" tick={{ fontSize: 12 }} stroke="#94A3B8" />
              <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
              <Tooltip formatter={(value) => formatCurrency(Number(value || 0))} />
              <Bar dataKey="value" radius={[4, 4, 0, 0]}>
                {(visuals?.run_rate_vs_target || []).map((entry, idx) => (
                  <Cell key={`run-rate-${idx}`} fill={entry.name === "Run-rate" ? "#D63384" : "#0F172A"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Recovery Growth Breakdown</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={visuals?.recovery_breakdown || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="name" tick={{ fontSize: 11 }} stroke="#94A3B8" />
              <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" />
              <Tooltip formatter={(value) => `${Number(value || 0).toFixed(2)}%`} />
              <Bar dataKey="value" fill="#F59E0B" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Growth Contribution (Top)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={growthContribChart} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis type="number" tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
              <YAxis type="category" dataKey="dimension" tick={{ fontSize: 11 }} stroke="#94A3B8" width={110} />
              <Tooltip formatter={(value) => formatCurrency(Number(value || 0))} />
              <Bar dataKey="delta_value" fill="#10B981" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">New vs Existing Customer Growth</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={visuals?.new_vs_existing_growth || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="segment" tick={{ fontSize: 12 }} stroke="#94A3B8" />
              <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
              <Tooltip formatter={(value, name) => (name === "growth_pct" ? `${Number(value || 0).toFixed(2)}%` : formatCurrency(Number(value || 0)))} />
              <Legend />
              <Line type="monotone" dataKey="prev_value" stroke="#0F172A" strokeWidth={2} name="Previous" />
              <Line type="monotone" dataKey="latest_value" stroke="#D63384" strokeWidth={2} name="Latest" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <Sheet open={drillOpen} onOpenChange={setDrillOpen}>
        <SheetContent className="sm:max-w-xl overflow-y-auto">
          <SheetHeader>
            <SheetTitle className="font-['Manrope']">
              {drillKpi?.name ?? "Drill-down"}
            </SheetTitle>
          </SheetHeader>
          {drillKpi && (
            <div className="mt-4 space-y-4">
              {/* Breadcrumb / filters */}
              <div className="flex flex-wrap items-center gap-2 text-sm">
                <span className="text-slate-500">Filters:</span>
                {Object.entries(filters).map(([key, val]) => (
                  <span
                    key={key}
                    className="inline-flex items-center gap-1 rounded-md bg-slate-100 px-2 py-0.5 text-slate-700"
                  >
                    {DIMENSION_LABELS[key]}: {val}
                    <button
                      type="button"
                      onClick={() => clearFilter(key)}
                      className="ml-0.5 hover:text-red-600"
                      aria-label={`Clear ${key}`}
                    >
                      <X className="w-3 h-3" />
                    </button>
                  </span>
                ))}
                {Object.keys(filters).length > 0 && (
                  <Button variant="ghost" size="sm" onClick={() => setFilters({})}>
                    Clear all
                  </Button>
                )}
              </div>

              {/* Group by selector */}
              <div className="flex items-center gap-2">
                <span className="text-sm text-slate-600">Group by:</span>
                <Select value={drillGroupBy} onValueChange={setDrillGroupBy}>
                  <SelectTrigger className="w-[140px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {drillKpi.granularity.map((g) => (
                      <SelectItem key={g} value={g}>
                        {DIMENSION_LABELS[g]}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Table */}
              {drillLoading ? (
                <div className="flex justify-center py-8">
                  <Loader2 className="w-6 h-6 animate-spin text-[#D63384]" />
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>{DIMENSION_LABELS[drillGroupBy]}</TableHead>
                      <TableHead className="text-right">Value</TableHead>
                      {(drillKpi.id === "mom_growth_pct" || drillKpi.id === "revenue_concentration_pct") && (
                        <TableHead className="text-right">
                          {drillKpi.id === "mom_growth_pct" ? "MoM %" : "%"}
                        </TableHead>
                      )}
                      {drillKpi.granularity.indexOf(drillGroupBy) < drillKpi.granularity.length - 1 && (
                        <TableHead className="w-10" />
                      )}
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {drillData.map((row, i) => (
                      <TableRow
                        key={row.dimension ?? i}
                        className={drillKpi.granularity.indexOf(drillGroupBy) < drillKpi.granularity.length - 1 ? "cursor-pointer hover:bg-slate-50" : ""}
                        onClick={() => {
                          if (drillKpi.granularity.indexOf(drillGroupBy) < drillKpi.granularity.length - 1) {
                            drillInto(row.dimension);
                          }
                        }}
                      >
                        <TableCell className="font-medium">{row.dimension ?? "-"}</TableCell>
                        <TableCell className="text-right">{formatValue(drillKpi, row)}</TableCell>
                        {(drillKpi.id === "mom_growth_pct" || drillKpi.id === "revenue_concentration_pct") && (
                          <TableCell className="text-right">
                            {drillKpi.id === "mom_growth_pct"
                              ? row.growth_pct != null ? `${row.growth_pct > 0 ? "+" : ""}${row.growth_pct}%` : "-"
                              : row.pct != null ? `${row.pct}%` : "-"}
                          </TableCell>
                        )}
                        {drillKpi.granularity.indexOf(drillGroupBy) < drillKpi.granularity.length - 1 && (
                          <TableCell>
                            <ChevronRight className="w-4 h-4 text-slate-400" />
                          </TableCell>
                        )}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
              {!drillLoading && drillData.length === 0 && (
                <p className="text-sm text-slate-500 py-4">No data for the selected filters.</p>
              )}
            </div>
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}
