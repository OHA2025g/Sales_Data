import { Loader2 } from "lucide-react";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const formatCurrency = (value) => {
  if (value == null) return "-";
  const v = Number(value);
  if (v >= 10000000) return `₹${(v / 10000000).toFixed(2)}Cr`;
  if (v >= 100000) return `₹${(v / 100000).toFixed(2)}L`;
  if (v >= 1000) return `₹${(v / 1000).toFixed(1)}K`;
  return `₹${v.toFixed(0)}`;
};

const formatRowValue = (row, valueFormat) => {
  const v = row.value;
  if (valueFormat === "currency") return formatCurrency(v);
  if (valueFormat === "percent") return row.pct != null ? `${Number(row.pct).toFixed(2)}%` : (v != null ? `${Number(v).toFixed(2)}%` : "-");
  if (valueFormat === "number") return v != null ? Number(v).toLocaleString() : "-";
  return v != null ? String(v) : "-";
};

const DIMENSION_LABELS = {
  month: "Month",
  zone: "Zone",
  state: "State",
  product: "Product",
  customer: "Customer",
};

/**
 * Reusable drill-down sheet: table of dimension + value (optional pct).
 * - rows: [{ dimension or name, value, pct? }]
 * - valueFormat: 'currency' | 'number' | 'percent'
 * - Optional group-by: groupBy, groupByOptions [{ value, label }], onGroupByChange
 */
export function DrillDownSheet({
  open,
  onOpenChange,
  title = "Drill-down",
  rows = [],
  valueFormat = "number",
  dimensionLabel,
  loading = false,
  groupBy,
  groupByOptions = [],
  onGroupByChange,
  showPctColumn = false,
}) {
  const label = dimensionLabel || (groupBy && DIMENSION_LABELS[groupBy]) || "Dimension";

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="sm:max-w-xl overflow-y-auto">
        <SheetHeader>
          <SheetTitle className="font-['Manrope']">{title}</SheetTitle>
        </SheetHeader>
        <div className="mt-4 space-y-4">
          {groupByOptions.length > 0 && onGroupByChange && (
            <div className="flex items-center gap-2">
              <span className="text-sm text-slate-600">Group by:</span>
              <Select value={groupBy || ""} onValueChange={onGroupByChange}>
                <SelectTrigger className="w-[140px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {groupByOptions.map((opt) => (
                    <SelectItem key={opt.value} value={opt.value}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          {loading ? (
            <div className="flex justify-center py-8">
              <Loader2 className="w-6 h-6 animate-spin text-[#D63384]" />
            </div>
          ) : (
            <>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>{label}</TableHead>
                    <TableHead className="text-right">Value</TableHead>
                    {showPctColumn && <TableHead className="text-right">%</TableHead>}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(rows || []).map((row, i) => (
                    <TableRow key={row.dimension ?? row.name ?? i}>
                      <TableCell className="font-medium">
                        {row.dimension ?? row.name ?? "-"}
                      </TableCell>
                      <TableCell className="text-right">
                        {formatRowValue(row, valueFormat)}
                      </TableCell>
                      {showPctColumn && (
                        <TableCell className="text-right">
                          {row.pct != null ? `${Number(row.pct).toFixed(2)}%` : "-"}
                        </TableCell>
                      )}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              {!loading && (!rows || rows.length === 0) && (
                <p className="text-sm text-slate-500 py-4">No data available.</p>
              )}
            </>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
