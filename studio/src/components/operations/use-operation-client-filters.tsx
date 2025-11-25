import {
  AnalyticsViewFilterOperator,
  AnalyticsFilter as AnalyticsFilterProto,
} from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import type { OperationClient } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { useCallback, useMemo } from "react";
import { ColumnFiltersState } from "@tanstack/react-table";
import { AnalyticsFilter } from "@/components/analytics/filters";
import { useRouter } from "next/router";
import { useApplyParams } from "@/components/analytics/use-apply-params";
import { isArray, isString } from "lodash";

const getUniqueClientValues = (
  clients: OperationClient[],
): {
  names: Set<string>;
  versions: Set<string>;
} => {
  const names = new Set<string>();
  const versions = new Set<string>();

  clients.forEach((client) => {
    names.add(client.name);
    versions.add(client.version);
  });

  return { names, versions };
};

/**
 * Parse column filters from URL
 */
export const useOperationFilterState = (): ColumnFiltersState => {
  const router = useRouter();

  return useMemo<ColumnFiltersState>(() => {
    if (!router.isReady || !router.query.filterState) return [];

    return JSON.parse(decodeURIComponent(router.query.filterState as string));
  }, [router.isReady, router.query.filterState]);
};

/**
 * Normalize column filters for API
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
 * Manage filters for operations pages - reads state from URL, builds UI filters from allClients
 */
export const useOperationClientFilters = (allClients: OperationClient[]) => {
  const applyNewParams = useApplyParams();
  const columnFilters = useOperationFilterState();

  const findById = useCallback(
    (id: OperationClient["name"] | OperationClient["version"]) =>
      (columnFilters.find((f) => f.id === id)?.value as string[]) ?? [],
    [columnFilters],
  );

  const onSelect = useCallback(
    (
      value: string[],
      id: OperationClient["name"] | OperationClient["version"],
    ) => {
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
    (value: OperationClient["name"] | OperationClient["version"]) => ({
      label: value || "-",
      value: JSON.stringify({
        label: value || "-",
        operator: AnalyticsViewFilterOperator.EQUALS,
        value: value || "",
      }),
    }),
    [],
  );

  const filters = useMemo<AnalyticsFilter[]>(() => {
    const { names: clientNames, versions: clientVersions } =
      getUniqueClientValues(allClients);

    const result: AnalyticsFilter[] = [];

    if (clientNames.size > 0) {
      result.push({
        id: "clientName",
        title: "Client Name",
        selectedOptions: findById("clientName"),
        onSelect: (value) => onSelect(value ?? [], "clientName"),
        options: Array.from(clientNames).map((name) => buildOption(name)),
      });
    }

    if (clientVersions.size > 0) {
      result.push({
        id: "clientVersion",
        title: "Client Version",
        selectedOptions: findById("clientVersion"),
        onSelect: (value) => onSelect(value ?? [], "clientVersion"),
        options: Array.from(clientVersions).map((version) => buildOption(version)),
      });
    }

    return result;
  }, [allClients, buildOption, findById, onSelect]);

  const resetFilters = useCallback(() => {
    applyNewParams({
      filterState: null,
    });
  }, [applyNewParams]);

  return {
    filters,
    columnFilters,
    resetFilters,
  };
};
