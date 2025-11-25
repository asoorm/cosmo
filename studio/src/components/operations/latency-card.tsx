import type {
  OperationDetailLatencyMetrics,
  OperationDetailRequestMetrics,
} from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import type { Range, DateRange } from "../date-picker-with-range";
import { InfoTooltip } from "../info-tooltip";
import { Card, CardHeader, CardContent, CardTitle } from "../ui/card";
import { formatRequestMetricsTooltip } from "./utils";
import { LatencyChart } from "./latency-chart";

export const LatencyCard = ({
  latencyMetrics,
  requestMetrics,
  range,
  dateRange,
  syncId,
}: {
  latencyMetrics?: OperationDetailLatencyMetrics;
  requestMetrics?: OperationDetailRequestMetrics;
  range?: Range;
  dateRange?: DateRange;
  syncId: string;
}) => (
  <Card className="bg-transparent">
    <CardHeader>
      <div className="flex space-x-2">
        <CardTitle>Latency over time</CardTitle>
        <InfoTooltip>
          {formatRequestMetricsTooltip({
            sum: requestMetrics?.totalRequestCount,
            range,
          })}
        </InfoTooltip>
      </div>
    </CardHeader>
    <CardContent className="h-48 border-b pb-2">
      <LatencyChart
        series={latencyMetrics?.requests || []}
        syncId={syncId}
        dateRange={dateRange}
        range={range}
      />
    </CardContent>
  </Card>
);
