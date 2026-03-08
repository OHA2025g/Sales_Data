import { useState, useEffect } from "react";
import axios from "axios";
import { Lightbulb, Target, CheckCircle2, Loader2, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Accordion, AccordionItem, AccordionTrigger, AccordionContent } from "@/components/ui/accordion";

const API = `${process.env.REACT_APP_BACKEND_URL}/api`;

export default function AIInsightsTabContent({ currentPage }) {
  const [insights, setInsights] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchInsights = async () => {
    setLoading(true);
    setError(null);
    const opts = { timeout: 60000 };
    const isOldFallback = (data) =>
      data?.insights?.[0] === "Unable to generate AI insights at this time.";

    const dashboard = currentPage || "Executive Summary";
    try {
      let response;
      try {
        response = await axios.get(`${API}/insights/generate`, { ...opts, params: { dashboard } });
      } catch (getErr) {
        response = null;
      }
      if (!response?.data || isOldFallback(response.data)) {
        try {
          response = await axios.post(
            `${API}/insights/generate`,
            { context: `${dashboard} - analysis`, dashboard, data_summary: {} },
            opts
          );
        } catch (postErr) {
          response = null;
        }
      }
      if (response?.data && !isOldFallback(response.data)) {
        setInsights(response.data);
        setError(null);
      } else {
        throw new Error("No insights");
      }
    } catch (err) {
      try {
        const getRes = await axios.get(`${API}/insights/generate`, { ...opts, params: { dashboard: currentPage || "Executive Summary" } });
        setInsights(getRes.data);
        setError(null);
      } catch (getErr) {
        setError(getErr.response?.data?.detail || "Failed to load insights. Ensure backend is running and REACT_APP_BACKEND_URL points to the API.");
        setInsights(null);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchInsights();
  }, [currentPage]);

  if (loading && !insights) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-slate-500">
        <Loader2 className="w-10 h-10 animate-spin text-[#D63384] mb-4" />
        <p className="text-sm">Analyzing data...</p>
      </div>
    );
  }

  if (error && !insights) {
    return (
      <div className="text-center py-12">
        <p className="text-red-500 text-sm mb-4">{error}</p>
        <Button onClick={fetchInsights} size="sm" variant="outline">
          Try Again
        </Button>
      </div>
    );
  }

  if (!insights) return null;

  const pageName = currentPage || "Executive Summary";

  return (
    <div className="max-w-4xl mx-auto">
      {/* Page header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4 mb-8 pb-6 border-b border-slate-200">
        <div>
          <h2 className="text-lg font-semibold text-slate-900 font-['Manrope']">
            Insights for {pageName}
          </h2>
          <p className="text-sm text-slate-500 mt-1">
            Based on real dashboard data. Switch dashboards in the sidebar to see insights for that view.
          </p>
        </div>
        <Button
          onClick={fetchInsights}
          variant="outline"
          size="sm"
          className="gap-2 shrink-0 border-slate-300 hover:bg-slate-50"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh insights
        </Button>
      </div>

      {/* Accordion: Key Insights, Recommendations, Action Items */}
      <Accordion
        type="single"
        collapsible
        defaultValue="key-insights"
        className="rounded-xl border border-slate-200 bg-white shadow-sm overflow-hidden divide-y divide-slate-200"
      >
        <AccordionItem value="key-insights" className="border-b-0 px-5">
          <AccordionTrigger className="py-4 hover:no-underline hover:bg-amber-50/50 [&[data-state=open]]:bg-amber-50/80">
            <span className="flex items-center gap-2">
              <Lightbulb className="w-5 h-5 text-amber-600 shrink-0" />
              <span className="font-semibold text-slate-900 font-['Manrope']">Key Insights</span>
            </span>
          </AccordionTrigger>
          <AccordionContent className="pb-4 pt-0">
            <ul className="space-y-3">
              {(insights.insights && insights.insights.length > 0)
                ? insights.insights.map((insight, idx) => (
                    <li
                      key={idx}
                      className="text-sm text-slate-700 pl-4 border-l-2 border-amber-300 leading-relaxed"
                    >
                      {insight}
                    </li>
                  ))
                : <li className="text-sm text-slate-500 italic">No insights yet. Ensure the backend is connected to MongoDB with sales data and try Refresh insights.</li>}
            </ul>
          </AccordionContent>
        </AccordionItem>

        <AccordionItem value="recommendations" className="border-b-0 px-5">
          <AccordionTrigger className="py-4 hover:no-underline hover:bg-blue-50/50 [&[data-state=open]]:bg-blue-50/80">
            <span className="flex items-center gap-2">
              <Target className="w-5 h-5 text-blue-600 shrink-0" />
              <span className="font-semibold text-slate-900 font-['Manrope']">Recommendations</span>
            </span>
          </AccordionTrigger>
          <AccordionContent className="pb-4 pt-0">
            <ul className="space-y-3">
              {(insights.recommendations && insights.recommendations.length > 0)
                ? insights.recommendations.map((rec, idx) => (
                    <li
                      key={idx}
                      className="text-sm text-slate-700 pl-4 border-l-2 border-blue-300 leading-relaxed"
                    >
                      {rec}
                    </li>
                  ))
                : <li className="text-sm text-slate-500 italic">No recommendations yet. Ensure the backend is connected to MongoDB with sales data and try Refresh insights.</li>}
            </ul>
          </AccordionContent>
        </AccordionItem>

        <AccordionItem value="action-items" className="border-b-0 px-5">
          <AccordionTrigger className="py-4 hover:no-underline hover:bg-emerald-50/50 [&[data-state=open]]:bg-emerald-50/80">
            <span className="flex items-center gap-2">
              <CheckCircle2 className="w-5 h-5 text-emerald-600 shrink-0" />
              <span className="font-semibold text-slate-900 font-['Manrope']">Action Items</span>
            </span>
          </AccordionTrigger>
          <AccordionContent className="pb-4 pt-0">
            <ul className="space-y-3">
              {(insights.action_items && insights.action_items.length > 0)
                ? insights.action_items.map((action, idx) => (
                    <li
                      key={idx}
                      className="text-sm text-slate-700 pl-4 border-l-2 border-emerald-300 leading-relaxed"
                    >
                      {action}
                    </li>
                  ))
                : <li className="text-sm text-slate-500 italic">No action items yet. Ensure the backend is connected to MongoDB with sales data and try Refresh insights.</li>}
            </ul>
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </div>
  );
}
