import { DateRange } from '../../../types/index.js';
import { isoDateRangeToTimestamps, getDateRange } from '../analytics/util.js';

export const normalizeDateRange = (
  dateRange?: DateRange<string>,
  range?: number,
): {
  start: number;
  end: number;
} => {
  if (dateRange && dateRange.start > dateRange.end) {
    const tmp = dateRange.start;
    dateRange.start = dateRange.end;
    dateRange.end = tmp;
  }

  const parsedDateRange = isoDateRangeToTimestamps(dateRange, range);
  const [start, end] = getDateRange(parsedDateRange);

  return { start, end };
};
