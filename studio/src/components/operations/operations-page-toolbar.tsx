import { useFeatureLimit } from "@/hooks/use-feature-limit";
import { formatISO } from "date-fns";
import { DatePickerWithRange } from "../date-picker-with-range";
import type {
  Range,
  DateRange,
  DateRangePickerChangeHandler,
} from "../date-picker-with-range";
import { useApplyParams } from "../analytics/use-apply-params";

export const OperationsPageToolbar = ({
  range,
  dateRange,
}: {
  range?: Range;
  dateRange: DateRange;
}) => {
  const tracingRetention = useFeatureLimit("tracing-retention", 7);
  const applyNewParams = useApplyParams();

  const onDateRangeChange: DateRangePickerChangeHandler = ({
    range,
    dateRange,
  }) => {
    if (range) {
      applyNewParams({
        dateRange: null,
        range: range.toString(),
      });
    } else if (dateRange) {
      const stringifiedDateRange = JSON.stringify({
        start: formatISO(dateRange.start as Date),
        end: formatISO((dateRange.end as Date) ?? (dateRange.start as Date)),
      });
      applyNewParams({
        dateRange: stringifiedDateRange,
        range: null,
      });
    }
  };

  return (
    <div className="flex gap-2">
      <DatePickerWithRange
        range={range}
        dateRange={dateRange}
        onChange={onDateRangeChange}
        calendarDaysLimit={tracingRetention}
      />
    </div>
  );
};
