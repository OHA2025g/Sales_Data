import "@/App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Toaster } from "@/components/ui/sonner";
import Layout from "@/components/Layout";
import ExecutiveDashboard from "@/pages/ExecutiveDashboard";
import RevenueGrowthKPIs from "@/pages/RevenueGrowthKPIs";
import ProductIntelligence from "@/pages/ProductIntelligence";
import GeographyIntelligence from "@/pages/GeographyIntelligence";
import CustomerAnalytics from "@/pages/CustomerAnalytics";
import PricingControl from "@/pages/PricingControl";
import RiskGovernance from "@/pages/RiskGovernance";

function App() {
  return (
    <div className="App">
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<ExecutiveDashboard />} />
            <Route path="revenue-growth" element={<RevenueGrowthKPIs />} />
            <Route path="products" element={<ProductIntelligence />} />
            <Route path="geography" element={<GeographyIntelligence />} />
            <Route path="customers" element={<CustomerAnalytics />} />
            <Route path="pricing" element={<PricingControl />} />
            <Route path="risk" element={<RiskGovernance />} />
          </Route>
        </Routes>
      </BrowserRouter>
      <Toaster position="top-right" richColors />
    </div>
  );
}

export default App;
