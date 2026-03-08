import { useState, useEffect } from "react";
import { X, Sparkles, Lightbulb, Target, CheckCircle2, Loader2, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import axios from "axios";

const API = `${process.env.REACT_APP_BACKEND_URL}/api`;

export default function AIInsightsPanel({ isOpen, onClose, currentPage, contextData }) {
  const [insights, setInsights] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const generateInsights = async () => {
    setLoading(true);
    setError(null);
    const opts = { timeout: 60000 };
    const isOldFallback = (data) =>
      data?.insights?.[0] === "Unable to generate AI insights at this time.";

    const dashboard = currentPage || "Executive Summary";
    try {
      let response;
      try {
        response = await axios.post(
          `${API}/insights/generate`,
          { context: `${dashboard} - analysis`, dashboard, data_summary: contextData ?? {} },
          opts
        );
      } catch (postErr) {
        response = null;
        throw postErr;
      }
      if (response?.data && isOldFallback(response.data)) {
        const getRes = await axios.get(`${API}/insights/generate`, { ...opts, params: { dashboard } });
        setInsights(getRes.data);
      } else {
        setInsights(response?.data ?? null);
      }
      setError(null);
    } catch (err) {
      const dashboard = currentPage || "Executive Summary";
      try {
        const getRes = await axios.get(`${API}/insights/generate`, { ...opts, params: { dashboard } });
        setInsights(getRes.data);
        setError(null);
      } catch (getErr) {
        console.error("Error generating insights:", getErr);
        setError(getErr.response?.data?.detail || "Failed to generate insights. Ensure backend is running and data is loaded.");
        setInsights(null);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (isOpen) {
      setError(null);
      if (!insights) generateInsights();
    }
  }, [isOpen]);

  if (!isOpen) return null;

  return (
    <div 
      className="fixed right-6 bottom-6 w-96 bg-white/95 backdrop-blur-xl border border-[#D63384]/20 shadow-2xl rounded-2xl z-50 overflow-hidden flex flex-col max-h-[600px] animate-fade-in"
      data-testid="ai-insights-panel"
    >
      {/* Header */}
      <div className="ai-panel-header p-4 text-white flex justify-between items-center">
        <div className="flex items-center gap-2">
          <Sparkles className="w-5 h-5" />
          <span className="font-semibold font-['Manrope']">AI Insights</span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={generateInsights}
            disabled={loading}
            className="p-1.5 hover:bg-white/20 rounded-lg transition-colors"
            data-testid="refresh-insights-btn"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
          <button
            onClick={onClose}
            className="p-1.5 hover:bg-white/20 rounded-lg transition-colors"
            data-testid="close-insights-btn"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Content */}
      <ScrollArea className="flex-1 p-4">
        {loading && !insights ? (
          <div className="flex flex-col items-center justify-center py-12 text-slate-500">
            <Loader2 className="w-8 h-8 animate-spin text-[#D63384] mb-3" />
            <p className="text-sm">Analyzing data...</p>
          </div>
        ) : error && !insights ? (
          <div className="text-center py-8">
            <p className="text-red-500 text-sm mb-4">{error}</p>
            <Button onClick={generateInsights} size="sm" variant="outline">
              Try Again
            </Button>
          </div>
        ) : insights ? (
          <div className="space-y-6">
            {/* Insights Section */}
            <div>
              <div className="flex items-center gap-2 mb-3">
                <Lightbulb className="w-4 h-4 text-amber-500" />
                <h3 className="font-semibold text-sm text-slate-900">Key Insights</h3>
              </div>
              <ul className="space-y-2">
                {insights.insights?.map((insight, idx) => (
                  <li 
                    key={idx} 
                    className="text-sm text-slate-600 pl-4 border-l-2 border-amber-200 py-1"
                    data-testid={`insight-${idx}`}
                  >
                    {insight}
                  </li>
                ))}
              </ul>
            </div>

            {/* Recommendations Section */}
            <div>
              <div className="flex items-center gap-2 mb-3">
                <Target className="w-4 h-4 text-blue-500" />
                <h3 className="font-semibold text-sm text-slate-900">Recommendations</h3>
              </div>
              <ul className="space-y-2">
                {insights.recommendations?.map((rec, idx) => (
                  <li 
                    key={idx} 
                    className="text-sm text-slate-600 pl-4 border-l-2 border-blue-200 py-1"
                    data-testid={`recommendation-${idx}`}
                  >
                    {rec}
                  </li>
                ))}
              </ul>
            </div>

            {/* Action Items Section */}
            <div>
              <div className="flex items-center gap-2 mb-3">
                <CheckCircle2 className="w-4 h-4 text-emerald-500" />
                <h3 className="font-semibold text-sm text-slate-900">Action Items</h3>
              </div>
              <ul className="space-y-2">
                {insights.action_items?.map((action, idx) => (
                  <li 
                    key={idx} 
                    className="text-sm text-slate-600 pl-4 border-l-2 border-emerald-200 py-1"
                    data-testid={`action-item-${idx}`}
                  >
                    {action}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        ) : null}
      </ScrollArea>

      {/* Footer */}
      <div className="p-3 border-t border-slate-100 bg-slate-50">
        <p className="text-xs text-slate-400 text-center">
          Based on real dashboard data • Refresh to recalculate
        </p>
      </div>
    </div>
  );
}
