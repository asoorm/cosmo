import {
  AreaChart,
  Area,
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
} from "recharts";
import { formatDateTime } from "@/lib/format-date";
import { useId } from "react";

const tickFormatter = (tick: number) =>
  tick === 0 || tick % 1 != 0 ? "" : `${tick}`;

export const RequestsChart = ({
  data,
  syncId,
}: {
  data: { timestamp: string; requests: number; errors: number }[];
  syncId: string;
}) => {
  const chartData = data.map(({ timestamp, ...rest }) => {
    const isoTimestamp = timestamp.replace(" ", "T") + "Z";
    const timestampMs = new Date(isoTimestamp).getTime();

    return {
      ...rest,
      timestamp: timestampMs,
    };
  });
  const timestamps = chartData.map((d) => d.timestamp);
  const minTimestamp = Math.min(...timestamps);
  const maxTimestamp = Math.max(...timestamps);
  const id = useId();

  return (
    <ResponsiveContainer width="99%" height="100%">
      <AreaChart
        data={chartData}
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
          domain={[minTimestamp, maxTimestamp]}
          tick={{ fill: "hsl(var(--muted-foreground))", fontSize: "13px" }}
          tickFormatter={(value) => formatDateTime(value)}
          axisLine={false}
          tickCount={5}
        />
        <YAxis
          tick={{ fill: "hsl(var(--muted-foreground))", fontSize: "13px" }}
          tickFormatter={tickFormatter}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          wrapperClassName="rounded-md border !border-popover !bg-popover/60 p-2 text-sm shadow-md outline-0 backdrop-blur-lg"
          labelFormatter={(label) => formatDateTime(parseInt(label as string))}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
};
