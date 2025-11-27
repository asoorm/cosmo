import {
  AreaChart,
  Area,
  Legend,
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
} from "recharts";
import { useChartData, createRangeFromDateRange } from "@/lib/insights-helpers";
import { formatDateTime } from "@/lib/format-date";
import { formatMetric } from "@/lib/format-metric";
import { useId } from "react";
import type { Range, DateRange } from "../date-picker-with-range";

export const RequestsChart = ({
  series,
  syncId,
  dateRange,
  range,
}: {
  series: { timestamp: string; requests: number; errors: number }[];
  syncId: string;
  dateRange?: DateRange;
  range?: Range;
}) => {
  const chartData = series.map(({ timestamp, ...rest }) => {
    const isoTimestamp = timestamp.replace(" ", "T") + "Z";
    const timestampMs = new Date(isoTimestamp).getTime();

    return {
      ...rest,
      timestamp: timestampMs,
    };
  });

  const { data, ticks, domain, timeFormatter } = useChartData(
    createRangeFromDateRange(dateRange, range),
    chartData,
  );
  const id = useId();

  return (
    <ResponsiveContainer width="99%" height="100%">
      <AreaChart
        data={data}
        margin={{ top: 8, right: 8, bottom: 8, left: 0 }}
        syncId={syncId}
      >
        <defs>
          <linearGradient id={`${id}-gradient`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={"hsl(var(--muted-foreground))"} />
            <stop offset="95%" stopColor={"hsl(var(--muted))"} />
          </linearGradient>
        </defs>
        <Area
          name="Requests"
          type="monotone"
          dataKey="requests"
          stroke="hsl(var(--muted-foreground))"
          fill={`url(#${id}-gradient)`}
          dot={false}
          strokeWidth={1.5}
          opacity="0.4"
        />
        <Area
          name="Errors"
          type="monotone"
          dataKey="errors"
          animationDuration={300}
          stroke="hsl(var(--destructive))"
          fill="none"
          fillOpacity="1"
          dot={false}
          strokeWidth={1.5}
        />
        <XAxis
          dataKey="timestamp"
          type="number"
          domain={domain}
          ticks={ticks}
          tick={{ fill: "hsl(var(--muted-foreground))", fontSize: "13px" }}
          tickFormatter={timeFormatter}
          axisLine={false}
          tickCount={5}
        />
        <YAxis
          tick={{ fill: "hsl(var(--muted-foreground))", fontSize: "13px" }}
          tickFormatter={(value) => formatMetric(value)}
          axisLine={false}
          tickLine={false}
        />
        <Legend
          verticalAlign="top"
          align="right"
          wrapperStyle={{ fontSize: "13px", marginTop: "-10px" }}
        />
        <Tooltip
          wrapperClassName="rounded-md border !border-popover !bg-popover/60 p-2 text-sm shadow-md outline-0 backdrop-blur-lg"
          labelFormatter={(label) => formatDateTime(parseInt(label as string))}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
};
