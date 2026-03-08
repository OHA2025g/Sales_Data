import { useState, useEffect } from "react";
import axios from "axios";
import { 
  MapPin, 
  Users, 
  TrendingUp,
  ArrowLeft,
  ChevronRight,
  Building2
} from "lucide-react";
import { KPICard, KPICardSkeleton } from "@/components/KPICard";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  Treemap,
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
        <p className="font-semibold text-slate-900 mb-1">{label || payload[0]?.payload?.name}</p>
        {payload.map((entry, index) => (
          <p key={index} className="text-sm" style={{ color: entry.color }}>
            {entry.name}: {typeof entry.value === 'number' && entry.name.toLowerCase().includes('value') 
              ? formatCurrency(entry.value) 
              : entry.value?.toLocaleString()}
          </p>
        ))}
      </div>
    );
  }
  return null;
};

export default function GeographyIntelligence() {
  const [loading, setLoading] = useState(true);
  const [zones, setZones] = useState([]);
  const [drillLevel, setDrillLevel] = useState("zone"); // zone, state, city
  const [selectedZone, setSelectedZone] = useState(null);
  const [selectedState, setSelectedState] = useState(null);
  const [states, setStates] = useState([]);
  const [cities, setCities] = useState([]);

  useEffect(() => {
    fetchZones();
  }, []);

  const fetchZones = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API}/geography/zones`);
      setZones(response.data);
    } catch (err) {
      console.error("Error fetching zones:", err);
    } finally {
      setLoading(false);
    }
  };

  const fetchStates = async (zoneName) => {
    setLoading(true);
    try {
      const response = await axios.get(`${API}/geography/zones/${encodeURIComponent(zoneName)}/states`);
      setStates(response.data);
      setSelectedZone(zoneName);
      setDrillLevel("state");
    } catch (err) {
      console.error("Error fetching states:", err);
    } finally {
      setLoading(false);
    }
  };

  const fetchCities = async (stateName) => {
    setLoading(true);
    try {
      const response = await axios.get(`${API}/geography/states/${encodeURIComponent(stateName)}/cities`);
      setCities(response.data);
      setSelectedState(stateName);
      setDrillLevel("city");
    } catch (err) {
      console.error("Error fetching cities:", err);
    } finally {
      setLoading(false);
    }
  };

  const goBack = () => {
    if (drillLevel === "city") {
      setDrillLevel("state");
      setCities([]);
      setSelectedState(null);
    } else if (drillLevel === "state") {
      setDrillLevel("zone");
      setStates([]);
      setSelectedZone(null);
    }
  };

  const totalValue = zones.reduce((sum, z) => sum + z.sales_value, 0);
  const totalCustomers = zones.reduce((sum, z) => sum + z.customer_count, 0);

  const renderBreadcrumb = () => (
    <div className="flex items-center gap-2 text-sm text-slate-500 mb-4">
      <button 
        onClick={() => { setDrillLevel("zone"); setSelectedZone(null); setSelectedState(null); }}
        className={`hover:text-[#D63384] ${drillLevel === "zone" ? "text-[#D63384] font-medium" : ""}`}
      >
        All Zones
      </button>
      {selectedZone && (
        <>
          <ChevronRight className="w-4 h-4" />
          <button 
            onClick={() => { setDrillLevel("state"); setSelectedState(null); }}
            className={`hover:text-[#D63384] ${drillLevel === "state" ? "text-[#D63384] font-medium" : ""}`}
          >
            {selectedZone}
          </button>
        </>
      )}
      {selectedState && (
        <>
          <ChevronRight className="w-4 h-4" />
          <span className="text-[#D63384] font-medium">{selectedState}</span>
        </>
      )}
    </div>
  );

  const getCurrentData = () => {
    if (drillLevel === "city") return cities;
    if (drillLevel === "state") return states;
    return zones;
  };

  const currentData = getCurrentData();
  const currentTotal = currentData.reduce((sum, d) => sum + d.sales_value, 0);

  return (
    <div className="space-y-6 animate-fade-in" data-testid="geography-intelligence">
      {/* Breadcrumb & Back Button */}
      <div className="flex items-center justify-between">
        {renderBreadcrumb()}
        {drillLevel !== "zone" && (
          <Button variant="ghost" onClick={goBack} className="gap-2" data-testid="back-btn">
            <ArrowLeft className="w-4 h-4" />
            Back
          </Button>
        )}
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
              title={drillLevel === "zone" ? "Total Zones" : drillLevel === "state" ? "States in Zone" : "Cities in State"}
              value={currentData.length}
              icon={MapPin}
              testId="kpi-geo-count"
            />
            <KPICard
              title="Total Revenue"
              value={formatCurrency(currentTotal)}
              icon={TrendingUp}
              testId="kpi-geo-revenue"
            />
            <KPICard
              title="Total Customers"
              value={currentData.reduce((sum, d) => sum + d.customer_count, 0)}
              icon={Users}
              testId="kpi-geo-customers"
            />
            <KPICard
              title="Avg Revenue/Customer"
              value={formatCurrency(currentTotal / Math.max(currentData.reduce((sum, d) => sum + d.customer_count, 0), 1))}
              icon={Building2}
              testId="kpi-geo-avg"
            />
          </>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Distribution Chart */}
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="geo-distribution-chart">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">
            {drillLevel === "zone" ? "Zone" : drillLevel === "state" ? "State" : "City"} Distribution
          </h3>
          {loading ? (
            <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
          ) : (
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie
                  data={currentData.slice(0, 8)}
                  cx="50%"
                  cy="50%"
                  innerRadius={50}
                  outerRadius={90}
                  paddingAngle={2}
                  dataKey="sales_value"
                  nameKey="name"
                  label={({ name, contribution_pct }) => `${name?.substring(0, 10)}${name?.length > 10 ? '...' : ''}: ${contribution_pct}%`}
                >
                  {currentData.slice(0, 8).map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip content={<CustomTooltip />} />
              </PieChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Bar Chart */}
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="geo-bar-chart">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Revenue Comparison</h3>
          {loading ? (
            <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
          ) : (
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={currentData.slice(0, 10)} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                <XAxis type="number" tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
                <YAxis type="category" dataKey="name" tick={{ fontSize: 11 }} stroke="#94A3B8" width={100} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="sales_value" fill="#D63384" radius={[0, 4, 4, 0]} name="Sales Value">
                  {currentData.slice(0, 10).map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Data Table with Drill-Down */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="geo-table">
        <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">
          {drillLevel === "zone" ? "Zone" : drillLevel === "state" ? "State" : "City"} Performance
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full data-table">
            <thead>
              <tr>
                <th className="text-left">{drillLevel === "zone" ? "Zone" : drillLevel === "state" ? "State" : "City"}</th>
                <th className="text-right">Sales Value</th>
                <th className="text-right">Contribution %</th>
                <th className="text-right">Customers</th>
                <th className="text-right">Transactions</th>
                <th className="text-right">Avg/Customer</th>
                {drillLevel !== "city" && <th className="text-center">Drill Down</th>}
              </tr>
            </thead>
            <tbody>
              {currentData.map((item, idx) => (
                <tr key={item.name} data-testid={`geo-row-${idx}`}>
                  <td className="font-medium">
                    <div className="flex items-center gap-2">
                      <div 
                        className="w-3 h-3 rounded-full"
                        style={{ backgroundColor: COLORS[idx % COLORS.length] }}
                      />
                      {item.name}
                    </div>
                  </td>
                  <td className="text-right font-medium">{formatCurrency(item.sales_value)}</td>
                  <td className="text-right">
                    <div className="flex items-center justify-end gap-2">
                      <div className="w-16 h-2 bg-slate-100 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-[#D63384] rounded-full"
                          style={{ width: `${Math.min(item.contribution_pct, 100)}%` }}
                        />
                      </div>
                      <span className="text-sm">{item.contribution_pct}%</span>
                    </div>
                  </td>
                  <td className="text-right">{item.customer_count}</td>
                  <td className="text-right">{item.transaction_count.toLocaleString()}</td>
                  <td className="text-right">{formatCurrency(item.avg_per_customer)}</td>
                  {drillLevel !== "city" && (
                    <td className="text-center">
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => drillLevel === "zone" ? fetchStates(item.name) : fetchCities(item.name)}
                        data-testid={`drill-${item.name}`}
                      >
                        <ChevronRight className="w-4 h-4" />
                      </Button>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
