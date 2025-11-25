import {
  AnalyticsViewFilterOperator,
  AnalyticsFilter as AnalyticsFilterProto,
} from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { useCallback, useMemo } from "react";
import { ColumnFiltersState } from "@tanstack/react-table";
import { useRouter } from "next/router";
import { useApplyParams } from "@/components/analytics/use-apply-params";
import { isArray, isString } from "lodash";

/**
 * URL normalization
 */
export const useFilterState = (): ColumnFiltersState => {
  const router = useRouter();

  return useMemo<ColumnFiltersState>(() => {
    if (!router.isReady || !router.query.filterState) return [];

    return JSON.parse(decodeURIComponent(router.query.filterState as string));
  }, [router.isReady, router.query.filterState]);
};

/**
 * API normalization
 */
export const transformFiltersForAPI = (
  columnFilters: ColumnFiltersState,
): AnalyticsFilterProto[] => {
  const apiFilters: AnalyticsFilterProto[] = [];

  for (const filter of columnFilters) {
    const { value } = filter;
    if (!isArray(value)) {
      continue;
    }

    for (const item of value) {
      if (isString(item)) {
        const parsed = JSON.parse(item);
        apiFilters.push(
          new AnalyticsFilterProto({
            field: filter.id,
            value: parsed.value,
            operator: parsed.operator,
          }),
        );
      }
    }
  }

  return apiFilters;
};

/**
 * Manage filter state via callbacks
 */
export const useFilterCallbacks = (columnFilters: ColumnFiltersState) => {
  const applyNewParams = useApplyParams();

  const findById = useCallback(
    (id: string) =>
      (columnFilters.find((f) => f.id === id)?.value as string[]) ?? [],
    [columnFilters],
  );

  const onSelect = useCallback(
    (value: string[], id: string) => {
      const newFilters = columnFilters.filter((f) => f.id !== id);
      if (value && value.length > 0) {
        newFilters.push({ id, value });
      }

      const stringifiedFilters =
        newFilters.length > 0 ? JSON.stringify(newFilters) : null;
      applyNewParams({
        filterState: stringifiedFilters,
      });
    },
    [columnFilters, applyNewParams],
  );

  const buildOption = useCallback(
    (value: string) => ({
      label: value || "-",
      value: JSON.stringify({
        label: value || "-",
        operator: AnalyticsViewFilterOperator.EQUALS,
        value: value || "",
      }),
    }),
    [],
  );

  const resetFilters = useCallback(() => {
    applyNewParams({
      filterState: null,
    });
  }, [applyNewParams]);

  return {
    findById,
    onSelect,
    buildOption,
    resetFilters,
  };
};
