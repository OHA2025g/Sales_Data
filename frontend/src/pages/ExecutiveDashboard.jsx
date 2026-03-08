import { useState, useEffect } from "react";
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

const API = `${process.env.REACT_APP_BACKEND_URL}/api`;

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

export default function ExecutiveDashboard() {
  const [loading, setLoading] = useState(true);
  const [dataLoading, setDataLoading] = useState(false);
  const [overview, setOverview] = useState(null);
  const [trends, setTrends] = useState([]);
  const [concentration, setConcentration] = useState(null);
  const [dataLoaded, setDataLoaded] = useState(false);

  const loadData = async () => {
    setDataLoading(true);
    try {
      const response = await axios.post(`${API}/data/load`);
      toast.success(`Data loaded successfully: ${response.data.records_loaded} records`);
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
      const [overviewRes, trendsRes, concentrationRes] = await Promise.all([
        axios.get(`${API}/dashboard/overview`),
        axios.get(`${API}/dashboard/trends`),
        axios.get(`${API}/dashboard/concentration`)
      ]);
      
      setOverview(overviewRes.data);
      setTrends(trendsRes.data);
      setConcentration(concentrationRes.data);
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
            />
            <KPICard
              title="Total Transactions"
              value={formatNumber(overview.total_transactions)}
              subtitle={`Avg: ${formatCurrency(overview.avg_transaction_value)}`}
              icon={ShoppingCart}
              progress={85}
              testId="kpi-transactions"
            />
            <KPICard
              title="Active Customers"
              value={overview.total_customers}
              subtitle={`Avg Revenue: ${formatCurrency(overview.avg_revenue_per_customer)}`}
              icon={Users}
              progress={75}
              testId="kpi-customers"
            />
            <KPICard
              title="Returns Rate"
              value={`${overview.returns_rate.toFixed(2)}%`}
              subtitle={`Value: ${formatCurrency(overview.returns_value)}`}
              icon={ArrowUpRight}
              trend={overview.returns_rate > 2 ? 1 : -1}
              progress={overview.returns_rate * 50}
              testId="kpi-returns"
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
            />
            <KPICard
              title="Total Products"
              value={overview.total_products}
              icon={Package}
              testId="kpi-products"
            />
            <KPICard
              title="Top 3 States Share"
              value={`${concentration.top_3_states_pct}%`}
              subtitle="Revenue concentration"
              icon={BarChart3}
              testId="kpi-state-concentration"
            />
            <KPICard
              title="Top 10 Customers Share"
              value={`${concentration.top_10_customers_pct}%`}
              subtitle="Customer concentration"
              icon={Users}
              testId="kpi-customer-concentration"
            />
          </>
        ) : null}
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Monthly Trend Chart */}
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

        {/* Zone Distribution Chart */}
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
      </div>

      {/* State Performance Bar Chart */}
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
    </div>
  );
}
