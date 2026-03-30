import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { toast } from "sonner";
import { 
  DollarSign, 
  TrendingUp, 
  Users, 
  Package, 
  BarChart3, 
  ShoppingCart,
  ArrowUpRight,
  RefreshCw,
  AlertTriangle,
  Loader2
} from "lucide-react";
import { KPICard, KPICardSkeleton } from "@/components/KPICard";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from "recharts";

import { API } from "@/apiConfig";

const COLORS = ["#D63384", "#0F172A", "#10B981", "#F59E0B", "#3B82F6", "#8B5CF6"];

const formatCurrency = (value) => {
  if (value >= 10000000) return `₹${(value / 10000000).toFixed(2)}Cr`;
  if (value >= 100000) return `₹${(value / 100000).toFixed(2)}L`;
  if (value >= 1000) return `₹${(value / 1000).toFixed(1)}K`;
  return `₹${value.toFixed(0)}`;
};

const formatNumber = (value) => {
  if (value >= 10000000) return `${(value / 10000000).toFixed(2)}Cr`;
  if (value >= 100000) return `${(value / 100000).toFixed(2)}L`;
  if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
  return value.toLocaleString();
};

const CustomTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    return (
      <div className="custom-tooltip">
        <p className="font-semibold text-slate-900 mb-1">{label}</p>
        {payload.map((entry, index) => (
          <p key={index} className="text-sm" style={{ color: entry.color }}>
            {entry.name}: {typeof entry.value === 'number' && entry.name.toLowerCase().includes('value') 
              ? formatCurrency(entry.value) 
              : formatNumber(entry.value)}
          </p>
        ))}
      </div>
    );
  }
  return null;
};

const DRILL_GROUP_OPTIONS = [
  { value: "month", label: "Month" },
  { value: "zone", label: "Zone" },
  { value: "state", label: "State" },
  { value: "product", label: "Product" },
  { value: "customer", label: "Customer" },
];
const DRILL_GROUP_OPTIONS_SHORT = [
  { value: "month", label: "Month" },
  { value: "zone", label: "Zone" },
  { value: "state", label: "State" },
];

export default function ExecutiveDashboard() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [dataLoading, setDataLoading] = useState(false);
  const [overview, setOverview] = useState(null);
  const [incentivesOverview, setIncentivesOverview] = useState(null);
  const [trends, setTrends] = useState([]);
  const [concentration, setConcentration] = useState(null);
  const [growthVisuals, setGrowthVisuals] = useState(null);
  const [riskAnomalies, setRiskAnomalies] = useState(null);
  const [customerRisk, setCustomerRisk] = useState(null);
  const [pricing, setPricing] = useState([]);
  const [dataLoaded, setDataLoaded] = useState(false);

  const openDrill = (config) => {
    navigate("/drill", { state: { ...config, parentPath: "/", parentLabel: "Executive Summary" } });
  };

  const loadData = async () => {
    setDataLoading(true);
    try {
      const response = await axios.post(`${API}/data/load`);
      if (response.data?.status === "started" || response.data?.status === "running") {
        toast.success("Data load started. It will populate dashboards shortly.");
      } else {
        toast.success(`Data loaded successfully: ${response.data.records_loaded} records`);
      }
      setDataLoaded(true);
      fetchDashboardData();
    } catch (err) {
      const msg = err.code === "ERR_NETWORK" || !err.response
        ? "Cannot reach the backend. Start it with: cd backend && source venv/bin/activate && uvicorn server:app --port 10000"
        : err.response?.data?.detail || "Failed to load data.";
      toast.error(msg);
      console.error(err);
    } finally {
      setDataLoading(false);
    }
  };

  const fetchDashboardData = async () => {
    setLoading(true);
    try {
      const [overviewRes, incOverviewRes, trendsRes, concentrationRes, growthRes, anomaliesRes, customerRiskRes, pricingRes] = await Promise.all([
        axios.get(`${API}/dashboard/overview`),
        axios.get(`${API}/incentives/overview`),
        axios.get(`${API}/dashboard/trends`),
        axios.get(`${API}/dashboard/concentration`),
        axios.get(`${API}/revenue-growth/visuals`),
        axios.get(`${API}/risk/anomalies`),
        axios.get(`${API}/customers/risk`),
        axios.get(`${API}/pricing/analysis`),
      ]);
      
      setOverview(overviewRes.data);
      setIncentivesOverview(incOverviewRes.data || null);
      setTrends(trendsRes.data);
      setConcentration(concentrationRes.data);
      setGrowthVisuals(growthRes.data);
      setRiskAnomalies(anomaliesRes.data);
      setCustomerRisk(customerRiskRes.data);
      setPricing(pricingRes.data || []);
      setDataLoaded(true);
    } catch (err) {
      console.error("Error fetching dashboard data:", err);
      setDataLoaded(false);
      const msg = err.code === "ERR_NETWORK" || !err.response
        ? "Cannot reach the server. Is the backend running on port 10000?"
        : err.response?.data?.detail || "Failed to load dashboard data.";
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDashboardData();
  }, []);

  if (!dataLoaded && !loading) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh]" data-testid="data-load-prompt">
        <AlertTriangle className="w-16 h-16 text-amber-500 mb-4" />
        <h2 className="text-2xl font-bold text-slate-900 mb-2 font-['Manrope']">No Data Found</h2>
        <p className="text-slate-500 mb-6 text-center max-w-md">
          Please load the sales data to view the dashboard analytics.
        </p>
        <Button 
          onClick={loadData} 
          disabled={dataLoading}
          className="bg-[#D63384] hover:bg-[#C2185B] gap-2"
          data-testid="load-data-btn"
        >
          {dataLoading ? (
            <>
              <Loader2 className="w-4 h-4 animate-spin" />
              Loading Data...
            </>
          ) : (
            <>
              <RefreshCw className="w-4 h-4" />
              Load Sales Data
            </>
          )}
        </Button>
      </div>
    );
  }

  const avgPriceRealization = pricing.length > 0
    ? pricing.reduce((sum, p) => sum + Number(p.price_realization || 0), 0) / pricing.length
    : 0;
  const topZoneShare = Number(concentration?.zones?.[0]?.pct || 0);
  const zoneSpikeCount = (riskAnomalies?.zone_spikes || []).length;
  const productSurgeCount = (riskAnomalies?.product_surges || []).length;
  const priceOutlierCount = (riskAnomalies?.price_outliers || []).length;
  const divisionZoneOutlierCount = (riskAnomalies?.division_zone_outliers || []).length;
  const stopBusinessPct = Number(customerRisk?.stop_business_pct || 0);
  const newExisting = growthVisuals?.new_vs_existing_growth || [];
  const newSeg = newExisting.find((x) => x.segment === "New");
  const existingSeg = newExisting.find((x) => x.segment === "Existing");
  const latestCombined = Number(newSeg?.latest_value || 0) + Number(existingSeg?.latest_value || 0);
  const newCustomerShare = latestCombined > 0 ? (Number(newSeg?.latest_value || 0) / latestCombined) * 100 : 0;
  const strategicCards = [
    { title: "3M CAGR", value: `${Number(growthVisuals?.kpis?.cagr_3m_pct || 0).toFixed(2)}%`, subtitle: "Net sales annualized trend" },
    { title: "Run-rate vs Target", value: `${Number(growthVisuals?.kpis?.run_rate_vs_target_pct || 0).toFixed(2)}%`, subtitle: "Last 3M vs previous 3M" },
    { title: "Recovery Growth (Adj.)", value: `${Number(growthVisuals?.kpis?.recovery_growth_return_adjusted_pct || 0).toFixed(2)}%`, subtitle: "Return-adjusted growth" },
    { title: "Top Zone Share", value: `${topZoneShare.toFixed(2)}%`, subtitle: "Revenue concentration" },
    { title: "New Customer Revenue Share", value: `${newCustomerShare.toFixed(2)}%`, subtitle: "Latest month mix" },
    { title: "Stop Business %", value: `${stopBusinessPct.toFixed(2)}%`, subtitle: "Revenue at risk" },
    { title: "Price Realization", value: `${(avgPriceRealization * 100).toFixed(2)}%`, subtitle: "Avg across products" },
    { title: "Zone Spike Alerts", value: zoneSpikeCount, subtitle: "MoM surge flags" },
    { title: "Product Surge Alerts", value: productSurgeCount, subtitle: "Recent vs history flags" },
    { title: "Price/Division Outliers", value: `${priceOutlierCount + divisionZoneOutlierCount}`, subtitle: "Pricing + division-zone" },
  ];
  const anomalyChartData = [
    { name: "Zone spikes", value: zoneSpikeCount },
    { name: "Product surges", value: productSurgeCount },
    { name: "Price outliers", value: priceOutlierCount },
    { name: "Div×Zone outliers", value: divisionZoneOutlierCount },
  ];
  const pricingTop = (pricing || [])
    .slice(0, 8)
    .map((p) => ({ product: p.product, realization_pct: Number(p.price_realization || 0) * 100 }));

  return (
    <div className="space-y-8 animate-fade-in" data-testid="executive-dashboard">
      {/* Load Data Button (when data exists) */}
      <div className="flex justify-end">
        <Button 
          onClick={loadData} 
          disabled={dataLoading}
          variant="outline"
          size="sm"
          className="gap-2"
          data-testid="refresh-data-btn"
        >
          {dataLoading ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <RefreshCw className="w-4 h-4" />
          )}
          Refresh Data
        </Button>
      </div>

      <Tabs defaultValue="overview" className="w-full">
        <TabsList className="bg-slate-50 border border-slate-200">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="strategic">Strategic KPIs</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-6 space-y-8">
      {/* KPI Cards Row */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-1.5">
        {loading ? (
          <>
            <KPICardSkeleton />
            <KPICardSkeleton />
            <KPICardSkeleton />
            <KPICardSkeleton />
          </>
        ) : overview ? (
          <>
            <KPICard
              title="Net Sales Value"
              value={formatCurrency(overview.net_sales_value)}
              icon={DollarSign}
              progress={100}
              testId="kpi-net-sales"
              onClick={() => openDrill({ type: "revenue-kpi", title: "Net Sales Value", kpi: "net_sales_value", kpiKey: "net_sales_value", valueFormat: "currency", groupByOptions: DRILL_GROUP_OPTIONS })}
            />
            <KPICard
              title="Total Transactions"
              value={formatNumber(overview.total_transactions)}
              subtitle={`Avg: ${formatCurrency(overview.avg_transaction_value)}`}
              icon={ShoppingCart}
              progress={85}
              testId="kpi-transactions"
              onClick={() => openDrill({ type: "dashboard", title: "Total Transactions", metric: "transactions", valueFormat: "number", groupByOptions: DRILL_GROUP_OPTIONS_SHORT })}
            />
            <KPICard
              title="Active Customers"
              value={overview.total_customers}
              subtitle={`Avg Revenue: ${formatCurrency(overview.avg_revenue_per_customer)}`}
              icon={Users}
              progress={75}
              testId="kpi-customers"
              onClick={() => openDrill({ type: "dashboard", title: "Active Customers", metric: "customers", valueFormat: "number", groupByOptions: DRILL_GROUP_OPTIONS_SHORT })}
            />
            <KPICard
              title="Returns Rate"
              value={`${overview.returns_rate.toFixed(2)}%`}
              subtitle={`Value: ${formatCurrency(overview.returns_value)}`}
              icon={ArrowUpRight}
              trend={overview.returns_rate > 2 ? 1 : -1}
              progress={overview.returns_rate * 50}
              testId="kpi-returns"
              onClick={() => openDrill({ type: "revenue-kpi", title: "Returns Rate", kpi: "returns_rate_pct", valueFormat: "percent", groupByOptions: [{ value: "month", label: "Month" }, { value: "product", label: "Product" }] })}
            />
          </>
        ) : null}
      </div>

      {/* Second Row KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-1.5">
        {loading ? (
          <>
            <KPICardSkeleton />
            <KPICardSkeleton />
            <KPICardSkeleton />
            <KPICardSkeleton />
          </>
        ) : overview && concentration ? (
          <>
            <KPICard
              title="Gross Sales"
              value={formatCurrency(overview.gross_sales_value)}
              icon={TrendingUp}
              testId="kpi-gross-sales"
              onClick={() => openDrill({ type: "revenue-kpi", title: "Gross Sales", kpi: "gross_sales_value", valueFormat: "currency", groupByOptions: DRILL_GROUP_OPTIONS })}
            />
            <KPICard
              title="Total Products"
              value={overview.total_products}
              icon={Package}
              testId="kpi-products"
              onClick={() => openDrill({ type: "dashboard", title: "Total Products", metric: "products", valueFormat: "number", groupByOptions: DRILL_GROUP_OPTIONS_SHORT })}
            />
            <KPICard
              title="Top 3 States Share"
              value={`${concentration.top_3_states_pct}%`}
              subtitle="Revenue concentration"
              icon={BarChart3}
              testId="kpi-state-concentration"
              onClick={() => openDrill({ type: "static", title: "Top 3 States Share", staticRows: concentration.top_states || [], valueFormat: "currency", showPctColumn: true })}
            />
            <KPICard
              title="Top 10 Customers Share"
              value={`${concentration.top_10_customers_pct}%`}
              subtitle="Customer concentration"
              icon={Users}
              testId="kpi-customer-concentration"
              onClick={() => openDrill({ type: "static", title: "Top 10 Customers Share", staticRows: concentration.top_customers || [], valueFormat: "currency", showPctColumn: true })}
            />
          </>
        ) : null}
      </div>

      {/* Executive Incentive KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-1.5">
        {loading ? (
          <>
            <KPICardSkeleton />
            <KPICardSkeleton />
            <KPICardSkeleton />
            <KPICardSkeleton />
          </>
        ) : incentivesOverview ? (
          <>
            <KPICard
              title="Total Incentive Paid"
              value={formatCurrency(incentivesOverview.total_incentive_paid)}
              subtitle={`Potential: ${formatCurrency(incentivesOverview.total_potential_incentive)}`}
              icon={DollarSign}
              testId="kpi-exec-incentive-paid"
              onClick={() => navigate("/incentives")}
            />
            <KPICard
              title="Achievement % (Budget)"
              value={`${Number(incentivesOverview.achievement_pct || 0).toFixed(2)}%`}
              subtitle={`Actual: ${formatCurrency(incentivesOverview.total_actual_sales)} · Budget: ${formatCurrency(incentivesOverview.total_budget)}`}
              icon={TrendingUp}
              testId="kpi-exec-incentive-ach"
              onClick={() => navigate("/incentives")}
            />
            <KPICard
              title="Incentive Cost %"
              value={`${Number(incentivesOverview.incentive_cost_pct || 0).toFixed(2)}%`}
              subtitle={`Revenue per ₹: ${Number(incentivesOverview.revenue_per_incentive || 0).toFixed(1)}`}
              icon={BarChart3}
              testId="kpi-exec-incentive-cost"
              onClick={() => navigate("/incentives")}
            />
            <KPICard
              title="Eligible Employees"
              value={`${Number(incentivesOverview.employees_eligible_pct || 0).toFixed(2)}%`}
              subtitle={`${incentivesOverview.employees_eligible} / ${incentivesOverview.employees_total}`}
              icon={Users}
              testId="kpi-exec-incentive-eligible"
              onClick={() => navigate("/incentives")}
            />
          </>
        ) : null}
      </div>

      {/* Monthly Trend Chart - full width */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="monthly-trend-chart">
        <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Monthly Sales Trend</h3>
        {loading ? (
          <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
        ) : (
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={trends}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} stroke="#94A3B8" />
              <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => `₹${(v/10000000).toFixed(1)}Cr`} />
              <Tooltip content={<CustomTooltip />} />
              <Line 
                type="monotone" 
                dataKey="value" 
                stroke="#D63384" 
                strokeWidth={3}
                dot={{ fill: "#D63384", strokeWidth: 2 }}
                name="Sales Value"
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Zone Distribution (left) + Top States (right) */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="zone-distribution-chart">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Zone Distribution</h3>
          {loading ? (
            <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
          ) : concentration ? (
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie
                  data={concentration.zones}
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={100}
                  paddingAngle={2}
                  dataKey="value"
                  nameKey="name"
                  label={({ name, pct }) => `${name}: ${pct}%`}
                >
                  {concentration.zones.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip content={<CustomTooltip />} />
              </PieChart>
            </ResponsiveContainer>
          ) : null}
        </div>

        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="state-performance-chart">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Top States by Revenue</h3>
          {loading ? (
            <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
          ) : concentration ? (
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={concentration.top_states} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                <XAxis type="number" tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => `₹${(v/10000000).toFixed(1)}Cr`} />
                <YAxis type="category" dataKey="name" tick={{ fontSize: 12 }} stroke="#94A3B8" width={100} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="value" fill="#D63384" radius={[0, 4, 4, 0]} name="Sales Value">
                  {concentration.top_states.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : null}
        </div>
      </div>

      {/* Monthly Growth Table */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="monthly-growth-table">
        <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Monthly Performance Summary</h3>
        <div className="overflow-x-auto">
          <table className="w-full data-table">
            <thead>
              <tr>
                <th className="text-left">Month</th>
                <th className="text-right">Sales Value</th>
                <th className="text-right">Quantity</th>
                <th className="text-right">Transactions</th>
                <th className="text-right">MoM Growth</th>
              </tr>
            </thead>
            <tbody>
              {trends.map((row, idx) => (
                <tr key={row.month} data-testid={`trend-row-${idx}`}>
                  <td className="font-medium">{row.month}</td>
                  <td className="text-right">{formatCurrency(row.value)}</td>
                  <td className="text-right">{formatNumber(row.quantity)}</td>
                  <td className="text-right">{row.transactions.toLocaleString()}</td>
                  <td className="text-right">
                    {row.growth_pct !== null ? (
                      <span className={row.growth_pct >= 0 ? "text-emerald-500" : "text-red-500"}>
                        {row.growth_pct >= 0 ? '+' : ''}{row.growth_pct}%
                      </span>
                    ) : '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
        </TabsContent>

        <TabsContent value="strategic" className="mt-6 space-y-6">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
            {loading ? (
              <>
                <KPICardSkeleton /><KPICardSkeleton /><KPICardSkeleton /><KPICardSkeleton /><KPICardSkeleton />
              </>
            ) : (
              strategicCards.map((k, idx) => (
                <div key={k.title} className="bg-white rounded-lg border border-slate-100 shadow-sm p-4" data-testid={`strategic-kpi-${idx}`}>
                  <p className="text-xs text-slate-500">{k.title}</p>
                  <p className="text-2xl font-bold text-slate-900 font-['Manrope'] mt-1">{k.value}</p>
                  <p className="text-[11px] text-slate-400 mt-1">{k.subtitle}</p>
                </div>
              ))
            )}
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
              <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Run-rate vs Target</h3>
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={growthVisuals?.run_rate_vs_target || []}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="name" tick={{ fontSize: 12 }} stroke="#94A3B8" />
                  <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="value" fill="#D63384" radius={[4, 4, 0, 0]} name="Sales Value" />
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
              <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Growth Contribution (Top Products)</h3>
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={(growthVisuals?.growth_contribution?.product || []).slice(0, 8)} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis type="number" tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
                  <YAxis type="category" dataKey="dimension" tick={{ fontSize: 11 }} stroke="#94A3B8" width={100} />
                  <Tooltip formatter={(v) => formatCurrency(Number(v || 0))} />
                  <Bar dataKey="delta_value" fill="#10B981" radius={[0, 4, 4, 0]} name="Sales Value" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6 lg:col-span-1">
              <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">New vs Existing Revenue</h3>
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={newExisting}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="segment" tick={{ fontSize: 12 }} stroke="#94A3B8" />
                  <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
                  <Tooltip content={<CustomTooltip />} />
                  <Legend />
                  <Bar dataKey="prev_value" fill="#0F172A" name="Previous Value" radius={[4, 4, 0, 0]} />
                  <Bar dataKey="latest_value" fill="#D63384" name="Latest Value" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6 lg:col-span-1">
              <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Anomaly Alerts Overview</h3>
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={anomalyChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="name" tick={{ fontSize: 11 }} stroke="#94A3B8" />
                  <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" />
                  <Tooltip />
                  <Bar dataKey="value" fill="#F59E0B" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6 lg:col-span-1">
              <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Top Product Price Realization</h3>
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={pricingTop}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="product" tick={{ fontSize: 10 }} stroke="#94A3B8" interval={0} angle={-30} textAnchor="end" height={70} />
                  <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => `${Number(v).toFixed(0)}%`} />
                  <Tooltip formatter={(v) => `${Number(v).toFixed(2)}%`} />
                  <Line type="monotone" dataKey="realization_pct" stroke="#3B82F6" strokeWidth={2.5} name="Realization %" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
