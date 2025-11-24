import { useEffect, useMemo, useState } from "react";
import { ColumnFiltersState } from "@tanstack/react-table";
import { AnalyticsFilter } from "@/components/analytics/filters";
import { AnalyticsViewFilterOperator } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import type { OperationClient } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { useRouter } from "next/router";
import { useApplyParams } from "@/components/analytics/use-apply-params";

export const useOperationClientFilters = (
  allClients: OperationClient[],
) => {
  const router = useRouter();
  const applyNewParams = useApplyParams();
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);

  // Sync filters from URL on mount
  useEffect(() => {
    if (router.isReady && router.query.filterState) {
      try {
        const filterStateFromUrl = JSON.parse(
          decodeURIComponent(router.query.filterState as string)
        );
        setColumnFilters(filterStateFromUrl);
      } catch (e) {
        console.error("Failed to parse filterState from URL", e);
      }
    }
  }, [router.isReady, router.query.filterState]);

  const filters = useMemo<AnalyticsFilter[]>(() => {
    const clientNames = new Set<string>();
    const clientVersions = new Set<string>();

    allClients.forEach((client) => {
      clientNames.add(client.name);
      clientVersions.add(client.version);
    });

    const result: AnalyticsFilter[] = [];

    if (clientNames.size > 0) {
      result.push({
        id: "clientName",
        title: "Client Name",
        selectedOptions:
          (columnFilters.find((f) => f.id === "clientName")?.value as string[]) ||
          [],
        onSelect: (value) => {
          const newFilters = columnFilters.filter((f) => f.id !== "clientName");
          if (value && value.length > 0) {
            newFilters.push({ id: "clientName", value });
          }
          setColumnFilters(newFilters);

          // Sync to URL
          const stringifiedFilters = newFilters.length > 0
            ? JSON.stringify(newFilters)
            : null;
          applyNewParams({
            filterState: stringifiedFilters,
          });
        },
        options: Array.from(clientNames).map((name) => ({
          label: name || "-",
          value: JSON.stringify({
            label: name || "-",
            operator: AnalyticsViewFilterOperator.EQUALS,
            value: name || "",
          }),
        })),
      });
    }

    if (clientVersions.size > 0) {
      result.push({
        id: "clientVersion",
        title: "Client Version",
        selectedOptions:
          (columnFilters.find((f) => f.id === "clientVersion")?.value as
            | string[]
            | undefined) || [],
        onSelect: (value) => {
          const newFilters = columnFilters.filter((f) => f.id !== "clientVersion");
          if (value && value.length > 0) {
            newFilters.push({ id: "clientVersion", value });
          }
          setColumnFilters(newFilters);

          // Sync to URL
          const stringifiedFilters = newFilters.length > 0
            ? JSON.stringify(newFilters)
            : null;
          applyNewParams({
            filterState: stringifiedFilters,
          });
        },
        options: Array.from(clientVersions).map((version) => ({
          label: version || "-",
          value: JSON.stringify({
            label: version || "-",
            operator: AnalyticsViewFilterOperator.EQUALS,
            value: version || "",
          }),
        })),
      });
    }

    return result;
  }, [allClients, columnFilters, applyNewParams]);

  const resetFilters = () => {
    setColumnFilters([]);
    applyNewParams({
      filterState: null,
    });
  };

  return {
    filters,
    columnFilters,
    setColumnFilters,
    resetFilters,
  };
};
