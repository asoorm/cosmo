import type { OperationClient } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { useMemo } from "react";
import { AnalyticsFilter } from "@/components/analytics/filters";
import {
  useFilterState,
  useFilterCallbacks,
  transformFiltersForAPI,
} from "./use-filter-state";

export { transformFiltersForAPI };

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
 * Manage filters for operations pages - reads state from URL, builds UI filters from allClients
 */
export const useOperationClientFilters = (allClients: OperationClient[]) => {
  const columnFilters = useFilterState();
  const { findById, onSelect, buildOption, resetFilters } =
    useFilterCallbacks(columnFilters);

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

  return {
    filters,
    columnFilters,
    resetFilters,
  };
};
