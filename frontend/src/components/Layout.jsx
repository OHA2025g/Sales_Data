import { useState } from "react";
import { Outlet, NavLink, useLocation } from "react-router-dom";
import { 
  LayoutDashboard, 
  Package, 
  MapPin, 
  Users, 
  DollarSign, 
  BadgePercent,
  ShieldAlert,
  ChevronLeft,
  ChevronRight,
  TrendingUp,
  Sparkles,
  BarChart2
} from "lucide-react";
import AIInsightsPanel from "@/components/AIInsightsPanel";
import AIInsightsTabContent from "@/components/AIInsightsTabContent";
import DataChatBot from "@/components/DataChatBot";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";

const navItems = [
  { path: "/", label: "Executive Summary", icon: LayoutDashboard },
  { path: "/revenue-growth", label: "Revenue & Growth KPIs", icon: BarChart2 },
  { path: "/products", label: "Product Intelligence", icon: Package },
  { path: "/geography", label: "Geography Intelligence", icon: MapPin },
  { path: "/customers", label: "Customer Analytics", icon: Users },
  { path: "/pricing", label: "Pricing & Discount", icon: DollarSign },
  { path: "/incentives", label: "Incentive Analytics", icon: BadgePercent },
  { path: "/risk", label: "Risk & Governance", icon: ShieldAlert },
];

export default function Layout() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [aiPanelOpen, setAiPanelOpen] = useState(false);
  const [activeTab, setActiveTab] = useState("ai-insights");
  const location = useLocation();
  const tabValue = location.pathname === "/drill" ? "dashboards" : activeTab;

  const getCurrentPageName = () => {
    if (location.pathname === "/drill") return "KPI Drill-down";
    const item = navItems.find(item => item.path === location.pathname);
    return item ? item.label : "Dashboard";
  };

  return (
    <div className="flex min-h-screen bg-[#F8F9FA]" data-testid="main-layout">
      {/* Sidebar */}
      <aside 
        className={`fixed left-0 top-0 h-screen bg-slate-900 text-white z-50 transition-all duration-300 ${
          sidebarCollapsed ? 'w-20' : 'w-64'
        }`}
        data-testid="sidebar"
      >
        {/* Logo */}
        <div className="h-16 flex items-center justify-between px-4 border-b border-slate-800">
          {!sidebarCollapsed && (
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-[#D63384] to-purple-600 flex items-center justify-center">
                <TrendingUp className="w-5 h-5 text-white" />
              </div>
              <span className="font-bold text-lg font-['Manrope']">SalesIQ</span>
            </div>
          )}
          {sidebarCollapsed && (
            <div className="w-8 h-8 mx-auto rounded-lg bg-gradient-to-br from-[#D63384] to-purple-600 flex items-center justify-center">
              <TrendingUp className="w-5 h-5 text-white" />
            </div>
          )}
        </div>

        {/* Navigation */}
        <nav className="p-4 space-y-2">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = location.pathname === item.path;
            return (
              <NavLink
                key={item.path}
                to={item.path}
                data-testid={`nav-${item.path.replace('/', '') || 'home'}`}
                className={`flex items-center gap-3 px-4 py-3 rounded-lg transition-all duration-200 ${
                  isActive 
                    ? 'sidebar-item-active text-white' 
                    : 'text-slate-400 hover:text-white hover:bg-white/10'
                }`}
              >
                <Icon className="w-5 h-5 flex-shrink-0" />
                {!sidebarCollapsed && (
                  <span className="text-sm font-medium">{item.label}</span>
                )}
              </NavLink>
            );
          })}
        </nav>

        {/* Collapse Toggle */}
        <button
          onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
          className="absolute -right-3 top-20 w-6 h-6 bg-slate-800 border border-slate-700 rounded-full flex items-center justify-center text-slate-400 hover:text-white hover:bg-slate-700 transition-colors"
          data-testid="sidebar-toggle"
        >
          {sidebarCollapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
        </button>
      </aside>

      {/* Main Content */}
      <main 
        className={`flex-1 transition-all duration-300 ${
          sidebarCollapsed ? 'ml-20' : 'ml-64'
        }`}
      >
        {/* Header */}
        <header className="h-16 bg-white border-b border-slate-200 flex items-center justify-between px-8 sticky top-0 z-40">
          <div>
            <h1 className="text-xl font-bold text-slate-900 font-['Manrope']" data-testid="page-title">
              {getCurrentPageName()}
            </h1>
            <p className="text-xs text-slate-500">Real-time analytics and insights</p>
          </div>
          <div className="flex items-center gap-4">
            <Button
              onClick={() => setAiPanelOpen(!aiPanelOpen)}
              className="bg-gradient-to-r from-[#D63384] to-purple-600 hover:from-[#C2185B] hover:to-purple-700 text-white gap-2"
              data-testid="ai-insights-btn"
            >
              <Sparkles className="w-4 h-4" />
              AI Insights
            </Button>
          </div>
        </header>

        {/* Tabs: AI Insights | Dashboards */}
        <div className="px-8 pt-4">
          <Tabs value={tabValue} onValueChange={setActiveTab} className="w-full">
            <TabsList className="bg-slate-100 p-1 rounded-lg">
              <TabsTrigger value="ai-insights" className="data-[state=active]:bg-white data-[state=active]:shadow-sm gap-2">
                <Sparkles className="w-4 h-4" />
                AI Insights
              </TabsTrigger>
              <TabsTrigger value="dashboards" className="data-[state=active]:bg-white data-[state=active]:shadow-sm gap-2">
                <LayoutDashboard className="w-4 h-4" />
                Dashboards
              </TabsTrigger>
            </TabsList>
            <TabsContent value="ai-insights" className="p-8 pt-6 mt-0">
              <AIInsightsTabContent currentPage={getCurrentPageName()} />
            </TabsContent>
            <TabsContent value="dashboards" className="p-8 pt-6 mt-0">
              <Outlet context={{ setAiPanelOpen }} />
            </TabsContent>
          </Tabs>
        </div>
      </main>

      {/* AI Insights Panel */}
      <AIInsightsPanel 
        isOpen={aiPanelOpen} 
        onClose={() => setAiPanelOpen(false)}
        currentPage={getCurrentPageName()}
      />

      {/* Data & Insights Chatbot */}
      <DataChatBot />
    </div>
  );
}
