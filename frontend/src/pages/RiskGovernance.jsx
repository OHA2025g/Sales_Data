import { useState, useEffect } from "react";
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
  ReferenceLine
} from "recharts";

const API = `${process.env.REACT_APP_BACKEND_URL}/api`;

const COLORS = ["#D63384", "#0F172A", "#10B981", "#F59E0B", "#3B82F6", "#8B5CF6"];

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

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [indicatorsRes, returnsRes] = await Promise.all([
        axios.get(`${API}/risk/indicators`),
        axios.get(`${API}/risk/returns-trend`)
      ]);
      setIndicators(indicatorsRes.data);
      setReturnsTrend(returnsRes.data);
    } catch (err) {
      console.error("Error fetching risk data:", err);
    } finally {
      setLoading(false);
    }
  };

  const healthyCount = indicators.filter(i => i.status === "healthy").length;
  const warningCount = indicators.filter(i => i.status === "warning").length;
  const dangerCount = indicators.filter(i => i.status === "danger").length;

  const overallScore = indicators.length > 0 
    ? Math.round((healthyCount * 100 + warningCount * 50 + dangerCount * 0) / indicators.length)
    : 0;

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
            />
            <KPICard
              title="Healthy Metrics"
              value={healthyCount}
              icon={CheckCircle}
              testId="kpi-healthy"
            />
            <KPICard
              title="Warnings"
              value={warningCount}
              icon={AlertTriangle}
              testId="kpi-warnings"
            />
            <KPICard
              title="Critical Issues"
              value={dangerCount}
              icon={XCircle}
              testId="kpi-critical"
            />
          </>
        )}
      </div>

      {/* Risk Indicators Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6" data-testid="risk-indicators-grid">
        {indicators.map((indicator, idx) => (
          <div 
            key={indicator.metric}
            className={`rounded-xl border p-6 ${getStatusBg(indicator.status)}`}
            data-testid={`risk-indicator-${idx}`}
          >
            <div className="flex items-start justify-between mb-4">
              <div>
                <h4 className="font-semibold text-slate-900">{indicator.metric}</h4>
                <p className="text-xs text-slate-500 mt-1">{indicator.description}</p>
              </div>
              {getStatusIcon(indicator.status)}
            </div>
            <div className="flex items-end justify-between">
              <div>
                <p className={`text-3xl font-bold font-['Manrope'] ${getStatusColor(indicator.status)}`}>
                  {typeof indicator.value === 'number' && indicator.metric.includes('%') 
                    ? `${indicator.value}%` 
                    : indicator.value}
                </p>
                <p className="text-xs text-slate-500">Threshold: {indicator.threshold}{indicator.metric.includes('%') || indicator.metric.includes('Rate') ? '%' : ''}</p>
              </div>
              <div className="w-20">
                <Progress 
                  value={Math.min((indicator.value / indicator.threshold) * 100, 150)} 
                  className={`h-2 ${indicator.status === 'danger' ? '[&>div]:bg-red-500' : indicator.status === 'warning' ? '[&>div]:bg-amber-500' : '[&>div]:bg-emerald-500'}`}
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
              <YAxis 
                yAxisId="left"
                tick={{ fontSize: 12 }} 
                stroke="#94A3B8" 
                tickFormatter={(v) => formatCurrency(v)}
              />
              <YAxis 
                yAxisId="right"
                orientation="right"
                tick={{ fontSize: 12 }} 
                stroke="#94A3B8" 
                tickFormatter={(v) => `${v}%`}
              />
              <ReferenceLine yAxisId="right" y={2} stroke="#EF4444" strokeDasharray="3 3" label={{ value: "2% Threshold", position: "right", fill: "#EF4444", fontSize: 10 }} />
              <Tooltip content={({ active, payload, label }) => {
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
              }} />
              <Line 
                yAxisId="left"
                type="monotone" 
                dataKey="returns_value" 
                stroke="#D63384" 
                strokeWidth={2}
                dot={{ fill: "#D63384" }}
                name="Returns Value"
              />
              <Line 
                yAxisId="right"
                type="monotone" 
                dataKey="returns_rate" 
                stroke="#F59E0B" 
                strokeWidth={2}
                dot={{ fill: "#F59E0B" }}
                name="Returns Rate %"
              />
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
              <span><strong>Critical:</strong> Address {dangerCount} critical risk indicator(s) immediately to mitigate business impact.</span>
            </li>
          )}
          {warningCount > 0 && (
            <li className="flex items-start gap-2">
              <AlertCircle className="w-4 h-4 text-amber-500 mt-0.5 flex-shrink-0" />
              <span><strong>Warning:</strong> Monitor {warningCount} warning indicator(s) closely and develop contingency plans.</span>
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
    </div>
  );
}
