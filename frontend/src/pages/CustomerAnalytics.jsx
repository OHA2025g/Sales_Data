import { useState, useEffect } from "react";
import axios from "axios";
import { 
  Users, 
  TrendingUp, 
  AlertTriangle,
  UserX,
  BarChart3,
  Target
} from "lucide-react";
import { KPICard, KPICardSkeleton } from "@/components/KPICard";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer
} from "recharts";

const API = `${process.env.REACT_APP_BACKEND_URL}/api`;

const COLORS = ["#D63384", "#0F172A", "#10B981", "#F59E0B", "#3B82F6", "#8B5CF6"];

const formatCurrency = (value) => {
  if (value >= 10000000) return `₹${(value / 10000000).toFixed(2)}Cr`;
  if (value >= 100000) return `₹${(value / 100000).toFixed(2)}L`;
  if (value >= 1000) return `₹${(value / 1000).toFixed(1)}K`;
  return `₹${value.toFixed(0)}`;
};

const CustomTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    return (
      <div className="custom-tooltip">
        <p className="font-semibold text-slate-900 mb-1">{label}</p>
        {payload.map((entry, index) => (
          <p key={index} className="text-sm" style={{ color: entry.color }}>
            {entry.name}: {typeof entry.value === 'number' ? formatCurrency(entry.value) : entry.value}
          </p>
        ))}
      </div>
    );
  }
  return null;
};

export default function CustomerAnalytics() {
  const [loading, setLoading] = useState(true);
  const [customers, setCustomers] = useState([]);
  const [concentration, setConcentration] = useState(null);
  const [risk, setRisk] = useState(null);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [custRes, concRes, riskRes] = await Promise.all([
        axios.get(`${API}/customers/performance?limit=50`),
        axios.get(`${API}/customers/concentration`),
        axios.get(`${API}/customers/risk`)
      ]);
      setCustomers(custRes.data);
      setConcentration(concRes.data);
      setRisk(riskRes.data);
    } catch (err) {
      console.error("Error fetching customer data:", err);
    } finally {
      setLoading(false);
    }
  };

  const totalValue = customers.reduce((sum, c) => sum + c.sales_value, 0);

  return (
    <div className="space-y-6 animate-fade-in" data-testid="customer-analytics">
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
              title="Total Customers"
              value={concentration?.total_customers || 0}
              icon={Users}
              testId="kpi-total-customers"
            />
            <KPICard
              title="Top 10 Share"
              value={`${concentration?.top_10_customers_pct || 0}%`}
              subtitle="Revenue concentration"
              icon={Target}
              testId="kpi-top10-share"
            />
            <KPICard
              title="Top 20 Share"
              value={`${concentration?.top_20_customers_pct || 0}%`}
              subtitle="Revenue concentration"
              icon={BarChart3}
              testId="kpi-top20-share"
            />
            <KPICard
              title="Stop Business"
              value={risk?.stop_business_count || 0}
              subtitle={`Value: ${formatCurrency(risk?.stop_business_value || 0)}`}
              icon={UserX}
              testId="kpi-stop-business"
            />
          </>
        )}
      </div>

      {/* Tabs for different views */}
      <Tabs defaultValue="performance" className="w-full">
        <TabsList className="mb-6 bg-slate-100">
          <TabsTrigger value="performance" data-testid="tab-performance">Performance</TabsTrigger>
          <TabsTrigger value="concentration" data-testid="tab-concentration">Concentration</TabsTrigger>
          <TabsTrigger value="risk" data-testid="tab-risk">Risk Analysis</TabsTrigger>
        </TabsList>

        {/* Performance Tab */}
        <TabsContent value="performance" className="space-y-6">
          {/* Top Customers Bar Chart */}
          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="top-customers-chart">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Top 10 Customers by Revenue</h3>
            {loading ? (
              <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
            ) : (
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={customers.slice(0, 10)} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis type="number" tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
                  <YAxis type="category" dataKey="customer_code" tick={{ fontSize: 11 }} stroke="#94A3B8" width={100} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="sales_value" fill="#D63384" radius={[0, 4, 4, 0]} name="Sales Value" />
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>

          {/* Customer Table */}
          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="customers-table">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Customer Performance Details</h3>
            <div className="overflow-x-auto">
              <table className="w-full data-table">
                <thead>
                  <tr>
                    <th className="text-left">Customer Code</th>
                    <th className="text-left">Type</th>
                    <th className="text-left">Location</th>
                    <th className="text-right">Sales Value</th>
                    <th className="text-right">Transactions</th>
                    <th className="text-right">Avg Transaction</th>
                    <th className="text-center">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {customers.map((cust, idx) => (
                    <tr key={cust.customer_code} data-testid={`customer-row-${idx}`}>
                      <td className="font-medium">{cust.customer_code}</td>
                      <td>
                        <Badge variant={cust.customer_type === "Trade" ? "default" : "secondary"}>
                          {cust.customer_type}
                        </Badge>
                      </td>
                      <td className="text-sm text-slate-600">{cust.city}, {cust.state}</td>
                      <td className="text-right font-medium">{formatCurrency(cust.sales_value)}</td>
                      <td className="text-right">{cust.transaction_count}</td>
                      <td className="text-right">{formatCurrency(cust.avg_transaction)}</td>
                      <td className="text-center">
                        {cust.stop_business === "Y" ? (
                          <Badge variant="destructive">Stopped</Badge>
                        ) : (
                          <Badge variant="outline" className="text-emerald-600 border-emerald-200">Active</Badge>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </TabsContent>

        {/* Concentration Tab */}
        <TabsContent value="concentration" className="space-y-6">
          {/* Pareto Chart */}
          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="pareto-chart">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Customer Concentration (Pareto Analysis)</h3>
            {loading ? (
              <div className="h-80 bg-slate-100 animate-pulse rounded-lg" />
            ) : concentration ? (
              <ResponsiveContainer width="100%" height={350}>
                <AreaChart data={concentration.pareto_data}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="rank" tick={{ fontSize: 12 }} stroke="#94A3B8" label={{ value: 'Customer Rank', position: 'bottom' }} />
                  <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" domain={[0, 100]} tickFormatter={(v) => `${v}%`} />
                  <Tooltip content={({ active, payload }) => {
                    if (active && payload && payload.length) {
                      const data = payload[0].payload;
                      return (
                        <div className="custom-tooltip">
                          <p className="font-semibold">Rank #{data.rank}</p>
                          <p className="text-sm">Customer: {data.customer}</p>
                          <p className="text-sm">Value: {formatCurrency(data.value)}</p>
                          <p className="text-sm text-[#D63384]">Cumulative: {data.cumulative_pct}%</p>
                        </div>
                      );
                    }
                    return null;
                  }} />
                  <Area 
                    type="monotone" 
                    dataKey="cumulative_pct" 
                    stroke="#D63384" 
                    fill="#D63384" 
                    fillOpacity={0.2}
                    name="Cumulative %"
                  />
                </AreaChart>
              </ResponsiveContainer>
            ) : null}
          </div>

          {/* Concentration Insights */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6 text-center">
              <p className="text-sm text-slate-500 mb-2">Top 10 Customers</p>
              <p className="text-3xl font-bold text-[#D63384] font-['Manrope']">{concentration?.top_10_customers_pct}%</p>
              <p className="text-xs text-slate-400 mt-1">of total revenue</p>
            </div>
            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6 text-center">
              <p className="text-sm text-slate-500 mb-2">Top 20 Customers</p>
              <p className="text-3xl font-bold text-[#0F172A] font-['Manrope']">{concentration?.top_20_customers_pct}%</p>
              <p className="text-xs text-slate-400 mt-1">of total revenue</p>
            </div>
            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6 text-center">
              <p className="text-sm text-slate-500 mb-2">Total Customers</p>
              <p className="text-3xl font-bold text-emerald-500 font-['Manrope']">{concentration?.total_customers}</p>
              <p className="text-xs text-slate-400 mt-1">in database</p>
            </div>
          </div>
        </TabsContent>

        {/* Risk Tab */}
        <TabsContent value="risk" className="space-y-6">
          {/* Risk Summary */}
          <div className="bg-amber-50 border border-amber-200 rounded-xl p-6" data-testid="risk-summary">
            <div className="flex items-start gap-4">
              <AlertTriangle className="w-8 h-8 text-amber-500 flex-shrink-0" />
              <div>
                <h3 className="font-semibold text-amber-800 mb-2">Customer Risk Overview</h3>
                <p className="text-sm text-amber-700">
                  {risk?.stop_business_count || 0} customers are marked as "Stop Business", 
                  representing {risk?.stop_business_pct || 0}% of total revenue (
                  {formatCurrency(risk?.stop_business_value || 0)}).
                </p>
              </div>
            </div>
          </div>

          {/* Stop Business Customers Table */}
          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="stop-business-table">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Stop Business Customers</h3>
            {risk?.stop_business_customers?.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full data-table">
                  <thead>
                    <tr>
                      <th className="text-left">Customer Code</th>
                      <th className="text-left">City</th>
                      <th className="text-left">State</th>
                      <th className="text-right">Sales Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {risk.stop_business_customers.map((cust, idx) => (
                      <tr key={cust.customer} data-testid={`stop-business-row-${idx}`}>
                        <td className="font-medium">{cust.customer}</td>
                        <td>{cust.city}</td>
                        <td>{cust.state}</td>
                        <td className="text-right font-medium">{formatCurrency(cust.value)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="text-slate-500 text-center py-8">No stop business customers found.</p>
            )}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
