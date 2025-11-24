import type { OperationDetailRequestMetrics } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { getInfoTip } from "../analytics/metrics";
import type { Range } from "../date-picker-with-range";
import { InfoTooltip } from "../info-tooltip";
import { Card, CardContent, CardHeader, CardTitle } from "../ui/card";
import { RequestsChart } from "./requests-chart";
import { formatRequestMetricsTooltip } from "./utils";

export const RequestsCard = ({
  requestMetrics,
  range,
  syncId,
}: {
  requestMetrics?: OperationDetailRequestMetrics;
  range?: Range;
  syncId: string;
}) => (
  <Card className="bg-transparent">
    <CardHeader>
      <div className="flex space-x-2">
        <CardTitle>Requests over time</CardTitle>
        <InfoTooltip>
          {formatRequestMetricsTooltip({
            sum: requestMetrics?.totalRequestCount,
            range,
          })}
        </InfoTooltip>
      </div>
    </CardHeader>
    <CardContent className="h-48 border-b pb-2">
      <RequestsChart data={requestMetrics?.requests || []} syncId={syncId} />
    </CardContent>
  </Card>
);
