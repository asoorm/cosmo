import { useMemo } from "react";
import { useRouter } from "next/router";
import type { Range } from "@/components/date-picker-with-range";
import { useAnalyticsQueryState } from "@/components/analytics/useAnalyticsQueryState";

/**
 * Manages state for date range picker
 */
export const useDateRangeState = (customDefaultRange?: Range) => {
  const { range, dateRange } = useAnalyticsQueryState(customDefaultRange);
  const router = useRouter();

  return useMemo(() => ({
    range: router.query.dateRange ? undefined : (range || 720),
    dateRange,
  }), [dateRange, range, router.query.dateRange]);
}
