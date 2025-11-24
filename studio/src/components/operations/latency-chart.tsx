import {
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
  LineChart,
  Line,
} from "recharts";
import { formatDateTime } from "@/lib/format-date";

const tickFormatter = (tick: number) =>
  tick === 0 || tick % 1 != 0 ? "" : `${tick}`;

export const LatencyChart = ({
  data,
  syncId,
}: {
  data: {
    timestamp: string;
    minDuration: number;
    maxDuration: number;
  }[];
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

  return (
    <ResponsiveContainer width="99%" height="100%">
      <LineChart
        data={chartData}
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
      </LineChart>
    </ResponsiveContainer>
  );
};
