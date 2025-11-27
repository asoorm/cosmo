import { AnalyticsViewFilterOperator } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { useMemo } from "react";
import { AnalyticsFilter } from "@/components/analytics/filters";
import { useFilterState, useFilterCallbacks } from "./use-filter-state";

/**
 * Manage filters for operations page - reads state from URL, builds UI filters
 */
export const useOperationsFilters = (
  allOperationNames: string[],
  allOperationTypes: string[],
) => {
  const columnFilters = useFilterState();
  const { findById, onSelect, buildOption, resetFilters } =
    useFilterCallbacks(columnFilters);

  const filters = useMemo<AnalyticsFilter[]>(() => {
    const result: AnalyticsFilter[] = [];

    if (allOperationNames.length > 0) {
      result.push({
        id: "operationName",
        title: "Operation Name",
        selectedOptions: findById("operationName"),
        onSelect: (value) => onSelect(value ?? [], "operationName"),
        options: allOperationNames.map((name) => buildOption(name)),
      });
    }

    if (allOperationTypes.length > 0) {
      result.push({
        id: "operationType",
        title: "Operation Type",
        selectedOptions: findById("operationType"),
        onSelect: (value) => onSelect(value ?? [], "operationType"),
        options: allOperationTypes.map((type) => buildOption(type)),
      });
    }

    result.push({
      id: "hasErrors",
      title: "Status",
      selectedOptions: findById("hasErrors"),
      onSelect: (value) => onSelect(value ?? [], "hasErrors"),
      options: [
        {
          label: "Error",
          value: JSON.stringify({
            label: "Error",
            operator: AnalyticsViewFilterOperator.EQUALS,
            value: "true",
          }),
        },
      ],
    });

    return result;
  }, [allOperationNames, allOperationTypes, buildOption, findById, onSelect]);

  return {
    filters,
    columnFilters,
    resetFilters,
  };
};
