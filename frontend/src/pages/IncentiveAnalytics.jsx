import { useEffect, useMemo, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { toast } from "sonner";
import { DollarSign, Target, BadgePercent, Users, Loader2 } from "lucide-react";
import { KPICard, KPICardSkeleton } from "@/components/KPICard";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
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
  Legend,
  ScatterChart,
  Scatter,
  ZAxis,
} from "recharts";
import { API } from "@/apiConfig";

const COLORS = ["#D63384", "#0F172A", "#10B981", "#F59E0B", "#3B82F6", "#8B5CF6"];

const formatCurrency = (value) => {
  if (value == null) return "-";
  const v = Number(value) || 0;
  if (v >= 10000000) return `₹${(v / 10000000).toFixed(2)}Cr`;
  if (v >= 100000) return `₹${(v / 100000).toFixed(2)}L`;
  if (v >= 1000) return `₹${(v / 1000).toFixed(1)}K`;
  return `₹${v.toFixed(0)}`;
};

const fmtPct = (v) => (v == null ? "-" : `${Number(v).toFixed(2)}%`);

function buildIncentiveEmployeeDrillState(data) {
  const m = data?.meta || {};
  const R = (c) => ({ ...c, align: "right" });
  const subt = [m.zone, m.division, m.role, m.hq]
    .filter((x) => x != null && String(x).trim() !== "")
    .join(" · ");
  return {
    type: "static",
    title: `Employee ${m.emp_id} — incentive drill-down`,
    subtitle: subt || undefined,
    parentPath: "/incentives",
    parentLabel: "Incentive Analytics",
    detailTables: [
      {
        title: "Cycle-wise breakdown (FY × cycle)",
        columns: [
          { key: "fy", label: "FY", type: "text" },
          { key: "cycle", label: "Cycle", type: "text" },
          R({ key: "budget", label: "Budget", type: "currency" }),
          R({ key: "actual", label: "Actual (sales)", type: "currency" }),
          R({ key: "incentive", label: "Final incentive", type: "currency" }),
          R({ key: "potential", label: "Potential", type: "currency" }),
          R({ key: "achievement_pct", label: "Ach %", type: "percent" }),
          R({ key: "incentive_cost_pct", label: "Cost %", type: "percent" }),
          R({ key: "payout_ratio_pct", label: "Payout %", type: "percent" }),
          R({ key: "lines", label: "Data rows", type: "number" }),
        ],
        rows: data?.by_cycle || [],
      },
      {
        title: "Product × division mix",
        columns: [
          { key: "product", label: "Product", type: "text" },
          { key: "division", label: "Division", type: "text" },
          R({ key: "actual", label: "Actual (sales)", type: "currency" }),
          R({ key: "incentive", label: "Incentive", type: "currency" }),
          R({ key: "incentive_cost_pct", label: "Cost %", type: "percent" }),
          R({ key: "lines", label: "Data rows", type: "number" }),
        ],
        rows: data?.by_product_division || [],
      },
    ],
  };
}

export default function IncentiveAnalytics() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [overview, setOverview] = useState(null);
  const [trend, setTrend] = useState([]);
  const [distribution, setDistribution] = useState([]);
  const [zoneDivision, setZoneDivision] = useState([]);
  const [employeeScatter, setEmployeeScatter] = useState([]);
  const [anomalies, setAnomalies] = useState([]);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [ovRes, trRes, distRes, zdRes, scatterRes, anomRes] = await Promise.all([
        axios.get(`${API}/incentives/overview`),
        axios.get(`${API}/incentives/trend`),
        axios.get(`${API}/incentives/distribution`, { params: { buckets: 12 } }),
        axios.get(`${API}/incentives/zone-division`),
        axios.get(`${API}/incentives/employee-scatter`, { params: { limit: 450 } }),
        axios.get(`${API}/incentives/anomalies`, { params: { limit: 30 } }),
      ]);
      setOverview(ovRes.data || null);
      setTrend(trRes.data || []);
      setDistribution(distRes.data || []);
      setZoneDivision(zdRes.data || []);
      setEmployeeScatter(scatterRes.data || []);
      setAnomalies(anomRes.data || []);
    } catch (err) {
      console.error("Error fetching incentive analytics:", err);
      toast.error(err.response?.data?.detail || "Failed to load incentive analytics.");
      setOverview(null);
      setTrend([]);
      setDistribution([]);
      setZoneDivision([]);
      setEmployeeScatter([]);
      setAnomalies([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const openEmployeeDrill = useCallback(
    async (empId) => {
      const id = Number(empId);
      if (!Number.isFinite(id)) return;
      try {
        const { data } = await axios.get(`${API}/incentives/employee-drill`, { params: { emp_id: id } });
        navigate("/drill", { state: buildIncentiveEmployeeDrillState(data) });
      } catch (err) {
        toast.error(err.response?.data?.detail || "Could not load employee drill-down.");
      }
    },
    [navigate]
  );

  const trendChartData = useMemo(() => {
    return (trend || []).map((r) => ({
      key: `${r.fy || ""}-${r.cycle || ""}`.replace(/^-/g, ""),
      fy: r.fy,
      cycle: r.cycle,
      label: `${r.cycle || ""}`,
      actual: Number(r.actual || 0),
      budget: Number(r.budget || 0),
      incentive: Number(r.incentive || 0),
      achievement_pct: Number(r.achievement_pct || 0),
      incentive_cost_pct: Number(r.incentive_cost_pct || 0),
    }));
  }, [trend]);

  const heatmap = useMemo(() => {
    const rows = zoneDivision || [];
    const zones = Array.from(new Set(rows.map((r) => String(r.zone || "").trim()).filter(Boolean))).sort();
    const divisions = Array.from(new Set(rows.map((r) => String(r.division || "").trim()).filter(Boolean))).sort();
    const byKey = new Map();
    for (const r of rows) {
      const z = String(r.zone || "").trim();
      const d = String(r.division || "").trim();
      if (!z || !d) continue;
      const inc = Number(r.incentive || 0);
      const act = Number(r.actual || 0);
      const costPct = act > 0 ? (inc / act) * 100 : 0;
      byKey.set(`${z}||${d}`, { incentive: inc, actual: act, costPct });
    }
    const maxCost = Math.max(
      0,
      ...Array.from(byKey.values()).map((v) => Number(v.costPct || 0))
    );
    return { zones, divisions, byKey, maxCost: maxCost || 1 };
  }, [zoneDivision]);

  const scatterData = useMemo(() => {
    return (employeeScatter || [])
      .map((r) => ({
        emp_id: r.emp_id,
        zone: r.zone || "—",
        division: r.division || "—",
        role: r.role || "—",
        hq: r.hq || "—",
        actual: Number(r.actual || 0),
        incentive: Number(r.incentive || 0),
        achievement_pct: Number(r.achievement_pct || 0),
        incentive_cost_pct: Number(r.incentive_cost_pct || 0),
        payout_ratio_pct: Number(r.payout_ratio_pct || 0),
      }))
      .filter((r) => r.actual > 0 || r.incentive > 0);
  }, [employeeScatter]);

  const zones = useMemo(() => {
    return Array.from(new Set(scatterData.map((d) => d.zone))).sort();
  }, [scatterData]);

  const zoneColor = (z) => {
    const idx = zones.indexOf(z);
    return COLORS[(idx >= 0 ? idx : 0) % COLORS.length];
  };

  return (
    <div className="space-y-8 animate-fade-in" data-testid="incentive-analytics">
      <Tabs defaultValue="overview" className="w-full">
        <TabsList className="bg-slate-50 border border-slate-200">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="analysis">Analysis</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-6 space-y-8">
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
                  title="Total Incentive Paid"
                  value={formatCurrency(overview.total_incentive_paid)}
                  subtitle={`Potential: ${formatCurrency(overview.total_potential_incentive)}`}
                  icon={DollarSign}
                  progress={Math.min(100, (Number(overview.payout_ratio_pct || 0)))}
                  testId="kpi-inc-total"
                />
                <KPICard
                  title="Achievement %"
                  value={fmtPct(overview.achievement_pct)}
                  subtitle={`Actual: ${formatCurrency(overview.total_actual_sales)} · Budget: ${formatCurrency(overview.total_budget)}`}
                  icon={Target}
                  progress={Math.min(100, Number(overview.achievement_pct || 0))}
                  testId="kpi-inc-ach"
                />
                <KPICard
                  title="Incentive Cost %"
                  value={fmtPct(overview.incentive_cost_pct)}
                  subtitle={`Revenue per ₹ incentive: ${Number(overview.revenue_per_incentive || 0).toFixed(1)}`}
                  icon={BadgePercent}
                  progress={Math.min(100, Number(overview.incentive_cost_pct || 0) * 20)}
                  testId="kpi-inc-cost"
                />
                <KPICard
                  title="Eligible Employees"
                  value={fmtPct(overview.employees_eligible_pct)}
                  subtitle={`${overview.employees_eligible} / ${overview.employees_total}`}
                  icon={Users}
                  progress={Math.min(100, Number(overview.employees_eligible_pct || 0))}
                  testId="kpi-inc-eligible"
                />
              </>
            ) : null}
          </div>

          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
            <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Cycle-wise trend (Sales vs Budget vs Incentive)</h3>
            {loading ? (
              <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
            ) : trendChartData.length === 0 ? (
              <p className="text-center text-slate-500 py-16">No incentive trend data available.</p>
            ) : (
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={trendChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="label" tick={{ fontSize: 12 }} stroke="#94A3B8" />
                  <YAxis tick={{ fontSize: 12 }} stroke="#94A3B8" tickFormatter={(v) => `₹${(v / 10000000).toFixed(1)}Cr`} />
                  <Tooltip
                    formatter={(v, name) => {
                      if (name === "Achievement %") return [fmtPct(v), name];
                      if (name === "Incentive Cost %") return [fmtPct(v), name];
                      return [formatCurrency(v), name];
                    }}
                    labelFormatter={(l) => `Cycle: ${l}`}
                  />
                  <Legend />
                  <Line type="monotone" dataKey="actual" name="Actual Sales" stroke="#D63384" strokeWidth={2.5} dot={false} />
                  <Line type="monotone" dataKey="budget" name="Budget" stroke="#0F172A" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="incentive" name="Incentive Paid" stroke="#10B981" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </TabsContent>

        <TabsContent value="analysis" className="mt-6 space-y-8">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
              <h3 className="font-semibold text-slate-900 mb-4 font-['Manrope']">Incentive distribution (histogram)</h3>
              {loading ? (
                <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
              ) : distribution.length === 0 ? (
                <p className="text-center text-slate-500 py-16">No distribution data available.</p>
              ) : (
                <ResponsiveContainer width="100%" height={320}>
                  <BarChart data={distribution}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                    <XAxis dataKey="label" tick={{ fontSize: 11 }} stroke="#94A3B8" interval={1} angle={-20} textAnchor="end" height={60} />
                    <YAxis tick={{ fontSize: 11 }} stroke="#94A3B8" />
                    <Tooltip />
                    <Bar dataKey="count" name="Employees" radius={[4, 4, 0, 0]}>
                      {distribution.map((_, i) => (
                        <Cell key={`bar-${i}`} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>

            <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
              <h3 className="font-semibold text-slate-900 mb-2 font-['Manrope']">Zone × Division incentive cost heatmap</h3>
              <p className="text-xs text-slate-500 mb-4">Cell value = Incentive Cost % (Incentive / Actual Sales).</p>
              {loading ? (
                <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
              ) : heatmap.zones.length === 0 || heatmap.divisions.length === 0 ? (
                <p className="text-center text-slate-500 py-16">No zone/division data available.</p>
              ) : (
                <div className="overflow-auto border border-slate-100 rounded-lg">
                  <table className="min-w-full text-sm">
                    <thead className="sticky top-0 bg-white z-10">
                      <tr>
                        <th className="text-left p-2 border-b border-slate-100">Zone</th>
                        {heatmap.divisions.map((d) => (
                          <th key={d} className="text-right p-2 border-b border-slate-100 whitespace-nowrap">
                            {d}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {heatmap.zones.map((z) => (
                        <tr key={z} className="hover:bg-slate-50">
                          <td className="p-2 border-b border-slate-100 font-medium whitespace-nowrap">{z}</td>
                          {heatmap.divisions.map((d) => {
                            const v = heatmap.byKey.get(`${z}||${d}`);
                            const pct = v ? Number(v.costPct || 0) : 0;
                            const intensity = Math.min(1, pct / heatmap.maxCost);
                            const bg = `rgba(214, 51, 132, ${0.06 + intensity * 0.35})`;
                            return (
                              <td
                                key={`${z}-${d}`}
                                className="p-2 border-b border-slate-100 text-right tabular-nums"
                                title={
                                  v
                                    ? `Incentive: ${formatCurrency(v.incentive)} | Actual: ${formatCurrency(v.actual)}`
                                    : "No data"
                                }
                                style={{ backgroundColor: bg }}
                              >
                                {v ? fmtPct(pct) : "-"}
                              </td>
                            );
                          })}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>

          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
            <h3 className="font-semibold text-slate-900 mb-2 font-['Manrope']">Sales vs Incentive (employee-level)</h3>
            <p className="text-xs text-slate-500 mb-4">
              Each point is an employee. Click a point to open cycle-wise breakdown and product×division mix. Use this view to spot misaligned payouts vs sales.
            </p>
            {loading ? (
              <div className="h-80 bg-slate-100 animate-pulse rounded-lg" />
            ) : scatterData.length === 0 ? (
              <p className="text-center text-slate-500 py-16">No scatter data available.</p>
            ) : (
              <ResponsiveContainer width="100%" height={360}>
                <ScatterChart margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis
                    dataKey="actual"
                    name="Actual Sales"
                    tick={{ fontSize: 11 }}
                    stroke="#94A3B8"
                    tickFormatter={(v) => `₹${(Number(v || 0) / 10000000).toFixed(1)}Cr`}
                  />
                  <YAxis
                    dataKey="incentive"
                    name="Incentive Paid"
                    tick={{ fontSize: 11 }}
                    stroke="#94A3B8"
                    tickFormatter={(v) => `₹${(Number(v || 0) / 100000).toFixed(1)}L`}
                  />
                  <ZAxis dataKey="achievement_pct" range={[40, 220]} name="Achievement %" />
                  <Tooltip
                    cursor={{ strokeDasharray: "3 3" }}
                    content={({ active, payload }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0].payload;
                      return (
                        <div className="rounded-lg border border-slate-200 bg-white px-3 py-2 shadow-md text-sm">
                          <p className="font-semibold text-slate-900">Emp {d.emp_id}</p>
                          <p className="text-slate-600">{d.zone} · {d.division}</p>
                          <p className="text-slate-600">Role: {d.role}</p>
                          <p className="text-slate-600">Actual: {formatCurrency(d.actual)}</p>
                          <p className="text-slate-600">Incentive: {formatCurrency(d.incentive)}</p>
                          <p className="text-slate-900">Ach: {fmtPct(d.achievement_pct)} · Cost: {fmtPct(d.incentive_cost_pct)} · Payout: {fmtPct(d.payout_ratio_pct)}</p>
                          <p className="text-xs text-[#D63384] mt-1 font-medium">Click point for drill-down</p>
                        </div>
                      );
                    }}
                  />
                  {zones.map((z) => (
                    <Scatter
                      key={z}
                      name={z}
                      data={scatterData.filter((d) => d.zone === z)}
                      fill={zoneColor(z)}
                      cursor="pointer"
                      onClick={(dot) => {
                        const p = dot?.payload ?? dot;
                        const eid = p?.emp_id ?? dot?.emp_id;
                        if (eid != null) openEmployeeDrill(eid);
                      }}
                    />
                  ))}
                  <Legend />
                </ScatterChart>
              </ResponsiveContainer>
            )}
          </div>

          <div className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
            <h3 className="font-semibold text-slate-900 mb-2 font-['Manrope']">Incentive anomalies</h3>
            <p className="text-xs text-slate-500 mb-4">
              Flagged employees with high incentive cost vs sales, or low achievement despite incentive payout. Click a row for the same drill-down as the scatter chart.
            </p>
            {loading ? (
              <div className="h-64 bg-slate-100 animate-pulse rounded-lg" />
            ) : anomalies.length === 0 ? (
              <p className="text-center text-slate-500 py-16">No anomalies detected.</p>
            ) : (
              <div className="overflow-auto border border-slate-100 rounded-lg">
                <table className="min-w-full text-sm">
                  <thead className="sticky top-0 bg-white z-10">
                    <tr>
                      <th className="text-left p-2 border-b border-slate-100">Emp</th>
                      <th className="text-left p-2 border-b border-slate-100">Zone</th>
                      <th className="text-left p-2 border-b border-slate-100">Division</th>
                      <th className="text-left p-2 border-b border-slate-100">Reason</th>
                      <th className="text-right p-2 border-b border-slate-100">Actual</th>
                      <th className="text-right p-2 border-b border-slate-100">Incentive</th>
                      <th className="text-right p-2 border-b border-slate-100">Ach %</th>
                      <th className="text-right p-2 border-b border-slate-100">Cost %</th>
                      <th className="text-right p-2 border-b border-slate-100">Payout %</th>
                    </tr>
                  </thead>
                  <tbody>
                    {anomalies.map((r, i) => (
                      <tr
                        key={`${r.emp_id}-${i}`}
                        role="button"
                        tabIndex={0}
                        className="hover:bg-slate-50 cursor-pointer focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#D63384] focus-visible:ring-offset-2"
                        onClick={() => openEmployeeDrill(r.emp_id)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault();
                            openEmployeeDrill(r.emp_id);
                          }
                        }}
                      >
                        <td className="p-2 border-b border-slate-100 font-medium">Emp {r.emp_id}</td>
                        <td className="p-2 border-b border-slate-100">{r.zone || "—"}</td>
                        <td className="p-2 border-b border-slate-100">{r.division || "—"}</td>
                        <td className="p-2 border-b border-slate-100">{r.reason || "—"}</td>
                        <td className="p-2 border-b border-slate-100 text-right tabular-nums">{formatCurrency(r.actual)}</td>
                        <td className="p-2 border-b border-slate-100 text-right tabular-nums">{formatCurrency(r.incentive)}</td>
                        <td className="p-2 border-b border-slate-100 text-right tabular-nums">{fmtPct(r.achievement_pct)}</td>
                        <td className="p-2 border-b border-slate-100 text-right tabular-nums">{fmtPct(r.incentive_cost_pct)}</td>
                        <td className="p-2 border-b border-slate-100 text-right tabular-nums">{fmtPct(r.payout_ratio_pct)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <div className="flex justify-end">
            <button
              className="inline-flex items-center gap-2 text-sm text-slate-600 hover:text-slate-900"
              onClick={fetchData}
              disabled={loading}
            >
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
              Refresh Incentive Data
            </button>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}

