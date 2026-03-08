import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import { cn } from "@/lib/utils";

export function KPICard({ 
  title, 
  value, 
  subtitle, 
  icon: Icon, 
  trend, 
  trendLabel,
  progress,
  className,
  onClick,
  testId
}) {
  const getTrendIcon = () => {
    if (!trend) return null;
    if (trend > 0) return <TrendingUp className="w-2.5 h-2.5" />;
    if (trend < 0) return <TrendingDown className="w-2.5 h-2.5" />;
    return <Minus className="w-2.5 h-2.5" />;
  };

  const getTrendColor = () => {
    if (!trend) return "text-slate-500";
    if (trend > 0) return "text-emerald-500";
    if (trend < 0) return "text-red-500";
    return "text-slate-500";
  };

  return (
    <div 
      className={cn(
        "kpi-card bg-white rounded-lg shadow-[0_2px_8px_rgba(0,0,0,0.03)] border border-slate-100 p-4 flex flex-col justify-between relative overflow-hidden h-[8rem] min-h-[8rem] min-w-0",
        onClick && "cursor-pointer",
        className
      )}
      onClick={onClick}
      data-testid={testId}
    >
      {/* Top: icon + header (title) on one row, then value */}
      <div className="flex flex-col gap-0.5 min-w-0 flex-shrink-0">
        <div className="flex items-center gap-2 min-w-0">
          {Icon && (
            <div className="p-1.5 rounded-full bg-[#D63384]/10 text-[#D63384] w-fit flex-shrink-0">
              <Icon className="w-4 h-4" />
            </div>
          )}
          <p className="text-xs font-medium text-slate-500 truncate" title={title}>
            {title}
          </p>
        </div>
        <p className="text-2xl font-bold text-slate-900 font-['Manrope'] tracking-tight truncate" title={String(value)}>
          {value}
        </p>
      </div>

      {/* Bottom: subtitle, trend, progress */}
      <div className="flex flex-col gap-1 mt-auto flex-shrink-0 min-w-0 pb-0.5">
        {subtitle && (
          <p className="text-[11px] text-slate-400 truncate" title={subtitle}>
            {subtitle}
          </p>
        )}
        {trend !== undefined && (
          <div className={cn("flex items-center gap-0.5 text-[11px] font-medium leading-tight", getTrendColor())}>
            {getTrendIcon()}
            <span className="leading-tight">{trend > 0 ? '+' : ''}{trend.toFixed(1)}%</span>
            {trendLabel && <span className="text-slate-400 ml-0.5 truncate">{trendLabel}</span>}
          </div>
        )}
        {progress !== undefined && (
          <div className="h-0.5 bg-slate-100 rounded-full overflow-hidden mt-0.5 flex-shrink-0">
            <div 
              className="h-full bg-gradient-to-r from-[#D63384] to-purple-500 rounded-full progress-bar-animated"
              style={{ width: `${Math.min(progress, 100)}%` }}
            />
          </div>
        )}
      </div>

      {/* Decorative element */}
      <div className="absolute -right-3 -bottom-3 w-16 h-16 bg-[#D63384]/5 rounded-full pointer-events-none" aria-hidden />
    </div>
  );
}

export function KPICardSkeleton() {
  return (
    <div className="bg-white rounded-lg shadow-sm border border-slate-100 p-4 animate-pulse h-[8rem] min-h-[8rem] flex flex-col justify-between">
      <div className="flex items-center gap-2 mb-1">
        <div className="w-7 h-7 bg-slate-200 rounded-full flex-shrink-0" />
        <div className="h-3 bg-slate-200 rounded w-24" />
      </div>
      <div className="h-6 bg-slate-200 rounded w-28 mb-1.5" />
      <div className="h-2.5 bg-slate-200 rounded w-16 mt-auto" />
    </div>
  );
}
