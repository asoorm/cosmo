import { getInfoTip } from "../analytics/metrics";
import type { Range } from "../date-picker-with-range";

export const formatRequestMetricsTooltip = ({
  sum,
  range,
}: {
  sum?: number;
  range?: Range;
}) => {
  if (sum === undefined) {
    return `No requests in ${getInfoTip(range)}`;
  }

  return `${sum} ${sum > 1 ? "requests" : "request"} in ${getInfoTip(range)}`;
};

