import { useFeatureLimit } from "@/hooks/use-feature-limit";
import { formatISO } from "date-fns";
import { DatePickerWithRange } from "../date-picker-with-range";
import type {
  Range,
  DateRange,
  DateRangePickerChangeHandler,
} from "../date-picker-with-range";
import { useApplyParams } from "../analytics/use-apply-params";
import { AnalyticsFilters, AnalyticsSelectedFilters, AnalyticsFilter } from "../analytics/filters";
import { ColumnFiltersState } from "@tanstack/react-table";

export const OperationsPageToolbar = ({
  range,
  dateRange,
  filters = [],
  selectedFilters = [],
  onResetFilters,
}: {
  range?: Range;
  dateRange: DateRange;
  filters?: AnalyticsFilter[];
  selectedFilters?: ColumnFiltersState;
  onResetFilters?: () => void;
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
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap gap-2">
        <DatePickerWithRange
          range={range}
          dateRange={dateRange}
          onChange={onDateRangeChange}
          calendarDaysLimit={tracingRetention}
        />
        <AnalyticsFilters filters={filters} />
      </div>
      <AnalyticsSelectedFilters
        filters={filters}
        selectedFilters={selectedFilters}
        onReset={onResetFilters}
      />
    </div>
  );
};
