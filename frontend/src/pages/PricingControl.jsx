import { useState, useEffect } from "react";
import axios from "axios";
import { 
  DollarSign, 
  TrendingUp, 
  Percent,
  AlertCircle,
  CheckCircle
} from "lucide-react";
import { KPICard, KPICardSkeleton } from "@/components/KPICard";
import { Badge } from "@/components/ui/badge";
import {
  BarChart,
  Bar,
  ScatterChart,
  Scatter,
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

const CustomTooltip = ({ active, payload }) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
    return (
      <div className="custom-tooltip">
        <p className="font-semibold text-slate-900 mb-1">{data.product}</p>
        <p className="text-sm">Avg PPU: ₹{data.avg_ppu?.toFixed(2)}</p>
        <p className="text-sm">Avg PTR: ₹{data.avg_ptr?.toFixed(2)}</p>
        <p className="text-sm">Price Realization: {(data.price_realization * 100)?.toFixed(1)}%</p>
        <p className="text-sm">Discount: {data.avg_discount?.toFixed(1)}%</p>
        <p className="text-sm text-[#D63384]">Revenue: {formatCurrency(data.total_value)}</p>
      </div>
    );
  }
  return null;
};

export default function PricingControl() {
  const [loading, setLoading] = useState(true);
  const [pricing, setPricing] = useState([]);
  const [discountDist, setDiscountDist] = useState([]);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [pricingRes, discountRes] = await Promise.all([
        axios.get(`${API}/pricing/analysis`),
        axios.get(`${API}/pricing/discount-distribution`)
      ]);
      setPricing(pricingRes.data);
      setDiscountDist(discountRes.data);
    } catch (err) {
      console.error("Error fetching pricing data:", err);
    } finally {
      setLoading(false);
    }
  };

  const avgPriceRealization = pricing.length > 0 
    ? pricing.reduce((sum, p) => sum + p.price_realization, 0) / pricing.length 
    : 0;
  
  const avgDiscount = pricing.length > 0 
    ? pricing.reduce((sum, p) => sum + p.avg_discount, 0) / pricing.length 
    : 0;

  const belowRealization = pricing.filter(p => p.price_realization < 1).length;
  const aboveRealization = pricing.filter(p => p.price_realization >= 1).length;

  return (
    <div className="space-y-6 animate-fade-in" data-testid="pricing-control">
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
              title="Avg Price Realization"
              value={`${(avgPriceRealization * 100).toFixed(1)}%`}
              subtitle="PPU / List Price"
              icon={DollarSign}
              testId="kpi-price-realization"
            />
            <KPICard
              title="Avg Discount"
              value={`${avgDiscount.toFixed(2)}%`}
              icon={Percent}
              testId="kpi-avg-discount"
            />
            <KPICard
              title="At/Above List Price"
              value={aboveRealization}
              subtitle="Products"
              icon={CheckCircle}
              testId="kpi-above-price"
            />
            <KPICard
              title="Below List Price"
              value={belowRealization}
              subtitle="Products"
              icon={AlertCircle}
              testId="kpi-below-price"
            />
          </>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Price Realization by Product */}
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="price-realization-chart">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Price Realization by Product</h3>
          {loading ? (
            <div className="h-80 bg-slate-100 animate-pulse rounded-lg" />
          ) : (
            <ResponsiveContainer width="100%" height={350}>
              <BarChart data={pricing.slice(0, 15)}>
                <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                <XAxis dataKey="product" tick={{ fontSize: 10 }} stroke="#94A3B8" interval={0} angle={-45} textAnchor="end" height={80} />
                <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" domain={[0.9, 1.1]} tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
                <ReferenceLine y={1} stroke="#10B981" strokeDasharray="3 3" label={{ value: "100%", position: "right", fill: "#10B981" }} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="price_realization" name="Price Realization" radius={[4, 4, 0, 0]}>
                  {pricing.slice(0, 15).map((entry, index) => (
                    <Cell 
                      key={`cell-${index}`} 
                      fill={entry.price_realization >= 1 ? "#10B981" : "#EF4444"} 
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Discount Distribution */}
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="discount-dist-chart">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Discount Distribution</h3>
          {loading ? (
            <div className="h-80 bg-slate-100 animate-pulse rounded-lg" />
          ) : (
            <ResponsiveContainer width="100%" height={350}>
              <BarChart data={discountDist}>
                <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                <XAxis dataKey="range" tick={{ fontSize: 12 }} stroke="#94A3B8" />
                <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" />
                <Tooltip content={({ active, payload }) => {
                  if (active && payload && payload.length) {
                    const data = payload[0].payload;
                    return (
                      <div className="custom-tooltip">
                        <p className="font-semibold">{data.range}</p>
                        <p className="text-sm">Count: {data.count.toLocaleString()}</p>
                        <p className="text-sm">Value: {formatCurrency(data.value)}</p>
                      </div>
                    );
                  }
                  return null;
                }} />
                <Bar dataKey="count" fill="#D63384" name="Line Items" radius={[4, 4, 0, 0]}>
                  {discountDist.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Pricing Details Table */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="pricing-table">
        <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Product Pricing Analysis</h3>
        <div className="overflow-x-auto">
          <table className="w-full data-table">
            <thead>
              <tr>
                <th className="text-left">Product</th>
                <th className="text-right">Avg PPU</th>
                <th className="text-right">Avg PTR</th>
                <th className="text-right">Avg MRP</th>
                <th className="text-right">Price Realization</th>
                <th className="text-right">Avg Discount</th>
                <th className="text-right">Total Revenue</th>
                <th className="text-center">Status</th>
              </tr>
            </thead>
            <tbody>
              {pricing.map((item, idx) => (
                <tr key={item.product} data-testid={`pricing-row-${idx}`}>
                  <td className="font-medium">{item.product}</td>
                  <td className="text-right">₹{item.avg_ppu.toFixed(2)}</td>
                  <td className="text-right">₹{item.avg_ptr.toFixed(2)}</td>
                  <td className="text-right">₹{item.avg_mrp.toFixed(2)}</td>
                  <td className="text-right">
                    <span className={item.price_realization >= 1 ? "text-emerald-500" : "text-red-500"}>
                      {(item.price_realization * 100).toFixed(1)}%
                    </span>
                  </td>
                  <td className="text-right">{item.avg_discount.toFixed(2)}%</td>
                  <td className="text-right font-medium">{formatCurrency(item.total_value)}</td>
                  <td className="text-center">
                    {item.price_realization >= 1 ? (
                      <Badge className="bg-emerald-100 text-emerald-700 hover:bg-emerald-100">At/Above</Badge>
                    ) : (
                      <Badge variant="destructive">Below</Badge>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Insights Card */}
      <div className="bg-gradient-to-r from-blue-50 to-purple-50 border border-blue-100 rounded-xl p-6" data-testid="pricing-insights">
        <h3 className="font-semibold text-slate-900 mb-3 font-['Manrope']">Pricing Insights</h3>
        <ul className="space-y-2 text-sm text-slate-700">
          <li className="flex items-start gap-2">
            <CheckCircle className="w-4 h-4 text-emerald-500 mt-0.5 flex-shrink-0" />
            <span>Majority of products ({aboveRealization}/{pricing.length}) are selling at or above list price.</span>
          </li>
          <li className="flex items-start gap-2">
            <TrendingUp className="w-4 h-4 text-blue-500 mt-0.5 flex-shrink-0" />
            <span>Average price realization is {(avgPriceRealization * 100).toFixed(1)}% - {avgPriceRealization >= 1 ? "healthy pricing discipline" : "potential margin leakage"}.</span>
          </li>
          <li className="flex items-start gap-2">
            <Percent className="w-4 h-4 text-amber-500 mt-0.5 flex-shrink-0" />
            <span>Average discount rate is {avgDiscount.toFixed(2)}% across all products.</span>
          </li>
        </ul>
      </div>
    </div>
  );
}
