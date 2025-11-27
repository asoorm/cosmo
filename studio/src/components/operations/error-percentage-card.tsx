import type { OperationDetailTopClientItem } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import type { OperationDetailRequestMetrics } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { ChevronRightIcon } from "@heroicons/react/24/outline";
import { formatPercentMetric } from "@/lib/format-metric";
import Link from "next/link";
import { useRouter } from "next/router";
import { createFilterState } from "../analytics/constructAnalyticsTableQueryState";
import { createSortingState } from "./use-sorting-state";
import type { Range } from "../date-picker-with-range";
import { ClientsChart } from "./clients-chart";

export const ErrorPercentageCard = ({
  operationName,
  operationHash,
  organizationSlug,
  namespace,
  range,
  topClients,
  topErrorClients,
  requestMetrics,
}: {
  operationName: string;
  operationHash: string;
  organizationSlug?: string;
  namespace: string;
  range?: Range;
  topClients: OperationDetailTopClientItem[];
  topErrorClients: OperationDetailTopClientItem[];
  requestMetrics: OperationDetailRequestMetrics | undefined;
}) => {
  const router = useRouter();
  return (
    <Card className="bg-transparent">
      <CardContent className="border-b py-4">
        <Tooltip delayDuration={200}>
          <TooltipTrigger asChild>
            <h4 className="group text-sm font-medium">
              <Link
                href={{
                  pathname:
                    "/[organizationSlug]/[namespace]/graph/[slug]/analytics/traces",
                  query: {
                    organizationSlug,
                    namespace,
                    slug: router.query.slug,
                    range,
                    dateRange: router.query.dateRange ?? undefined,
                    filterState: createFilterState({
                      operationName,
                      operationHash,
                    }),
                    ...router.query,
                  },
                }}
                className="inline-flex rounded-md px-2 py-1 hover:bg-muted"
              >
                Error percentage
                <ChevronRightIcon className="h4 ml-1 w-4 transition-all group-hover:ml-2" />
              </Link>
            </h4>
          </TooltipTrigger>
          <TooltipContent>
            View traces for {requestMetrics?.totalErrorCount.toString()} errors
          </TooltipContent>
        </Tooltip>
        <p className="px-2 pb-2 text-xl font-semibold">
          {formatPercentMetric(requestMetrics?.errorPercentage || 0)}
        </p>
      </CardContent>
      <CardContent className="min-h-48 border-b py-4">
        <Tooltip delayDuration={200}>
          <TooltipTrigger asChild>
            <h4 className="group pb-2 text-sm font-medium">
              <Link
                href={{
                  pathname: `${router.pathname}/clients`,
                  query: {
                    organizationSlug,
                    namespace,
                    slug: router.query.slug,
                    range,
                    dateRange: router.query.dateRange ?? undefined,
                    ...createSortingState([{ id: "totalRequests", desc: true }]),
                    ...router.query,
                  },
                }}
                className="inline-flex rounded-md px-2 py-1 hover:bg-muted"
              >
                Top {topClients.length}{" "}
                {topClients.length === 1 ? "Client" : "Clients"}
                <ChevronRightIcon className="h4 ml-1 w-4 transition-all group-hover:ml-2" />
              </Link>
            </h4>
          </TooltipTrigger>
          <TooltipContent>View all clients</TooltipContent>
        </Tooltip>
        <ClientsChart data={topClients || []} />
      </CardContent>
      <CardContent className="min-h-48 py-4">
        <Tooltip delayDuration={200}>
          <TooltipTrigger asChild>
            <h4 className="group pb-2 text-sm font-medium">
              <Link
                href={{
                  pathname: `${router.pathname}/clients`,
                  query: {
                    organizationSlug,
                    namespace,
                    slug: router.query.slug,
                    range,
                    dateRange: router.query.dateRange ?? undefined,
                    ...createSortingState([{ id: "totalErrors", desc: true }]),
                    ...router.query,
                  },
                }}
                className="inline-flex rounded-md px-2 py-1 hover:bg-muted"
              >
                Top {topErrorClients.length} Error{" "}
                {topErrorClients.length === 1 ? "Client" : "Clients"}
                <ChevronRightIcon className="h4 ml-1 w-4 transition-all group-hover:ml-2" />
              </Link>
            </h4>
          </TooltipTrigger>
          <TooltipContent>View all clients</TooltipContent>
        </Tooltip>
        <ClientsChart data={topErrorClients || []} type="errors" />
      </CardContent>
    </Card>
  );
};
