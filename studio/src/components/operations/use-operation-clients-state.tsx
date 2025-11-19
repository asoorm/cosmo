import { useMemo } from "react";
import type { Range } from "@/components/date-picker-with-range";
import { useAnalyticsQueryState } from "@/components/analytics/useAnalyticsQueryState";

export const useOperationClientsState = (customDefaultRange?: Range) => {
  const { range, dateRange } = useAnalyticsQueryState(customDefaultRange);

  return useMemo(() => ({
    range: range || 720,
    dateRange,
  }), [dateRange, range]);
}
