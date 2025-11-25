import {
  ResponsiveContainer,
  Legend,
  XAxis,
  YAxis,
  Tooltip,
  LineChart,
  Line,
} from "recharts";
import { formatDateTime } from "@/lib/format-date";
import { formatMetric } from "@/lib/format-metric";
import { useChartData, createRangeFromDateRange } from "@/lib/insights-helpers";
import type { Range, DateRange } from "../date-picker-with-range";

export const LatencyChart = ({
  series,
  syncId,
  dateRange,
  range,
}: {
  series: {
    timestamp: string;
    minDuration: number;
    maxDuration: number;
  }[];
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

  return (
    <ResponsiveContainer width="99%" height="100%">
      <LineChart
        data={data}
        margin={{ top: 8, right: 8, bottom: 8, left: 0 }}
        syncId={syncId}
      >
        <Line
          name="Max. Duration"
          type="monotone"
          dataKey="maxDuration"
          animationDuration={300}
          dot={false}
          stroke="hsl(var(--warning))"
          strokeWidth={1.5}
        />
        <Line
          name="Min. Duration"
          type="monotone"
          dataKey="minDuration"
          animationDuration={300}
          dot={false}
          stroke="hsl(var(--chart-primary))"
          strokeWidth={1.5}
        />
        <XAxis
          dataKey="timestamp"
          type="number"
          domain={domain}
          tick={{ fill: "hsl(var(--muted-foreground))", fontSize: "13px" }}
          tickFormatter={timeFormatter}
          ticks={ticks}
          axisLine={false}
          tickCount={5}
        />
        <YAxis
          tick={{ fill: "hsl(var(--muted-foreground))", fontSize: "13px" }}
          tickFormatter={formatMetric}
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
          formatter={(value: number) => `${value} ms`}
        />
      </LineChart>
    </ResponsiveContainer>
  );
};
