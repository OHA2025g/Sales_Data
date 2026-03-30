import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { 
  Package, 
  TrendingUp, 
  Users, 
  ArrowLeft,
  ChevronRight,
  BarChart3
} from "lucide-react";
import { KPICard, KPICardSkeleton } from "@/components/KPICard";
import { Button } from "@/components/ui/button";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer
} from "recharts";

import { API } from "@/apiConfig";

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

const DRILL_GROUP_OPTIONS = [
  { value: "month", label: "Month" },
  { value: "zone", label: "Zone" },
  { value: "state", label: "State" },
  { value: "product", label: "Product" },
];

export default function ProductIntelligence() {
  const [loading, setLoading] = useState(true);
  const [products, setProducts] = useState([]);
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [productDetails, setProductDetails] = useState(null);
  const [detailsLoading, setDetailsLoading] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    fetchProducts();
  }, []);

  const fetchProducts = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API}/products/performance`);
      setProducts(response.data);
    } catch (err) {
      console.error("Error fetching products:", err);
    } finally {
      setLoading(false);
    }
  };

  const fetchProductDetails = async (productName) => {
    setDetailsLoading(true);
    try {
      const response = await axios.get(`${API}/products/${encodeURIComponent(productName)}/details`);
      setProductDetails(response.data);
      setSelectedProduct(productName);
    } catch (err) {
      console.error("Error fetching product details:", err);
    } finally {
      setDetailsLoading(false);
    }
  };

  const openDrill = (config) => {
    navigate("/drill", { state: { ...config, parentPath: "/products", parentLabel: "Product Intelligence" } });
  };

  const topProducts = products.slice(0, 10);
  const totalValue = products.reduce((sum, p) => sum + p.sales_value, 0);
  const totalQty = products.reduce((sum, p) => sum + p.sales_qty, 0);
  const avgContribution = products.length > 0 ? 100 / products.length : 0;

  // Product detail view
  if (selectedProduct && productDetails) {
    return (
      <div className="space-y-6 animate-fade-in" data-testid="product-detail-view">
        <Button 
          variant="ghost" 
          onClick={() => { setSelectedProduct(null); setProductDetails(null); }}
          className="gap-2 mb-4"
          data-testid="back-to-products-btn"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Products
        </Button>

        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
          <h2 className="text-2xl font-bold text-slate-900 font-['Manrope'] mb-2">{selectedProduct}</h2>
          <p className="text-slate-500">Detailed performance analysis</p>
        </div>

        {/* Monthly Trend */}
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="product-trend-chart">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Monthly Sales Trend</h3>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={productDetails.monthly_trend}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} stroke="#94A3B8" />
              <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
              <Tooltip content={<CustomTooltip />} />
              <Line type="monotone" dataKey="value" stroke="#D63384" strokeWidth={3} name="Sales Value" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* By Zone */}
          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="product-zone-chart">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Sales by Zone</h3>
            <ResponsiveContainer width="100%" height={250}>
              <PieChart>
                <Pie
                  data={productDetails.by_zone}
                  cx="50%"
                  cy="50%"
                  innerRadius={50}
                  outerRadius={80}
                  paddingAngle={2}
                  dataKey="value"
                  nameKey="zone"
                  label={({ zone }) => zone}
                >
                  {productDetails.by_zone.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip content={<CustomTooltip />} />
              </PieChart>
            </ResponsiveContainer>
          </div>

          {/* By State */}
          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="product-state-chart">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Top States</h3>
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={productDetails.by_state.slice(0, 5)} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                <XAxis type="number" tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
                <YAxis type="category" dataKey="state" tick={{ fontSize: 11 }} stroke="#94A3B8" width={80} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="value" fill="#D63384" radius={[0, 4, 4, 0]} name="Sales Value" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Top Customers for Product */}
        <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="product-customers-table">
          <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Top Customers</h3>
          <div className="overflow-x-auto">
            <table className="w-full data-table">
              <thead>
                <tr>
                  <th className="text-left">Customer</th>
                  <th className="text-right">Sales Value</th>
                  <th className="text-right">Quantity</th>
                </tr>
              </thead>
              <tbody>
                {productDetails.top_customers.map((cust, idx) => (
                  <tr key={cust.customer} data-testid={`product-customer-row-${idx}`}>
                    <td className="font-medium">{cust.customer}</td>
                    <td className="text-right">{formatCurrency(cust.value)}</td>
                    <td className="text-right">{cust.qty.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    );
  }

  // Main products list view
  return (
    <div className="space-y-6 animate-fade-in" data-testid="product-intelligence">
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
              title="Total Products"
              value={products.length}
              icon={Package}
              testId="kpi-total-products"
              onClick={() => openDrill({ type: "dashboard", title: "Total Products", metric: "products", valueFormat: "number", groupByOptions: DRILL_GROUP_OPTIONS })}
            />
            <KPICard
              title="Total Revenue"
              value={formatCurrency(totalValue)}
              icon={TrendingUp}
              testId="kpi-total-revenue"
              onClick={() => openDrill({ type: "revenue-kpi", title: "Total Revenue", kpi: "net_sales_value", valueFormat: "currency", groupByOptions: DRILL_GROUP_OPTIONS })}
            />
            <KPICard
              title="Total Quantity"
              value={totalQty.toLocaleString()}
              icon={BarChart3}
              testId="kpi-total-qty"
              onClick={() => openDrill({ type: "static", title: "Total Quantity by Product", staticRows: products.map((p) => ({ name: p.product, value: p.sales_qty })), valueFormat: "number" })}
            />
            <KPICard
              title="Total Customers"
              value={products.reduce((sum, p) => sum + p.customer_count, 0)}
              icon={Users}
              testId="kpi-product-customers"
              onClick={() => openDrill({ type: "static", title: "Customers by Product", staticRows: products.map((p) => ({ name: p.product, value: p.customer_count })), valueFormat: "number" })}
            />
          </>
        )}
      </div>

      {/* Top Products Chart */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="top-products-chart">
        <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Top 10 Products by Revenue</h3>
        {loading ? (
          <div className="h-80 bg-slate-100 animate-pulse rounded-lg" />
        ) : (
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={topProducts}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="product" tick={{ fontSize: 10 }} stroke="#94A3B8" interval={0} angle={-45} textAnchor="end" height={80} />
              <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => formatCurrency(v)} />
              <Tooltip content={<CustomTooltip />} />
              <Bar dataKey="sales_value" fill="#D63384" radius={[4, 4, 0, 0]} name="Sales Value">
                {topProducts.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Products Table */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6" data-testid="products-table">
        <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Product Performance Details</h3>
        <div className="overflow-x-auto">
          <table className="w-full data-table">
            <thead>
              <tr>
                <th className="text-left">Product</th>
                <th className="text-left">Brand</th>
                <th className="text-left">Division</th>
                <th className="text-right">Sales Value</th>
                <th className="text-right">Contribution %</th>
                <th className="text-right">Returns Rate</th>
                <th className="text-right">Customers</th>
                <th className="text-center">Action</th>
              </tr>
            </thead>
            <tbody>
              {products.map((product, idx) => (
                <tr key={product.product} data-testid={`product-row-${idx}`}>
                  <td className="font-medium">{product.product}</td>
                  <td>
                    <span className="px-2 py-1 text-xs rounded-full bg-slate-100 text-slate-700">
                      {product.brand}
                    </span>
                  </td>
                  <td className="text-sm text-slate-600">{product.division}</td>
                  <td className="text-right font-medium">{formatCurrency(product.sales_value)}</td>
                  <td className="text-right">
                    <div className="flex items-center justify-end gap-2">
                      <div className="w-16 h-2 bg-slate-100 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-[#D63384] rounded-full"
                          style={{ width: `${Math.min(product.contribution_pct * 5, 100)}%` }}
                        />
                      </div>
                      <span className="text-sm">{product.contribution_pct}%</span>
                    </div>
                  </td>
                  <td className="text-right">
                    <span className={product.returns_rate > 2 ? "text-red-500" : "text-emerald-500"}>
                      {product.returns_rate}%
                    </span>
                  </td>
                  <td className="text-right">{product.customer_count}</td>
                  <td className="text-center">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => fetchProductDetails(product.product)}
                      disabled={detailsLoading}
                      data-testid={`drill-down-${product.product}`}
                    >
                      <ChevronRight className="w-4 h-4" />
                    </Button>
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
