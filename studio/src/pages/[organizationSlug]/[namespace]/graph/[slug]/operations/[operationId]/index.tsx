import { getOperationDetailMetricsPage } from "@wundergraph/cosmo-connect/dist/platform/v1/platform-PlatformService_connectquery";
import { useQuery } from "@connectrpc/connect-query";
import { EnumStatusCode } from "@wundergraph/cosmo-connect/dist/common/common_pb";
import { EmptyState } from "@/components/empty-state";
import {
  GraphPageLayout,
  getGraphLayout,
  GraphContext,
} from "@/components/layout/graph-layout";
import { NextPageWithLayout } from "@/lib/page";
import { useCurrentOrganization } from "@/hooks/use-current-organization";
import { useWorkspace } from "@/hooks/use-workspace";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { getInfoTip } from "@/components/analytics/metrics";
import type { Range } from "@/components/date-picker-with-range";
import { InfoTooltip } from "@/components/info-tooltip";
import { createFilterState } from "@/components/analytics/constructAnalyticsTableQueryState";
import { OperationsToolbar } from "@/components/operations/operations-toolbar";
import { OperationDetailToolbar } from "@/components/operations/operation-detail-toolbar";
import { useOperationClientsState } from "@/components/operations/use-operation-clients-state";
import { ClientsChart } from "@/components/operations/clients-chart";
import { RequestsChart } from "@/components/operations/requests-chart";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Loader } from "@/components/ui/loader";
import { formatPercentMetric } from "@/lib/format-metric";
import {
  ChevronRightIcon,
  ExclamationTriangleIcon,
} from "@heroicons/react/24/outline";
import { useRouter } from "next/router";
import { formatISO } from "date-fns";
import Link from "next/link";
import { useContext, useId } from "react";
import { CodeViewer } from "@/components/code-viewer";

const formatRequestMetricsTooltip = ({
  sum,
  range,
}: {
  sum?: bigint;
  range?: Range;
}) => {
  if (sum === undefined) {
    return `No requests in ${getInfoTip(range)}`;
  }

  return `${sum.toString()} ${sum > 1 ? "requests" : "request"} in ${getInfoTip(
    range,
  )}`;
};

const OperationDetailsPage: NextPageWithLayout = () => {
  const router = useRouter();
  const [type, name, hash] = decodeURIComponent(
    router.query.operationId as string,
  ).split("-");
  const organizationSlug = useCurrentOrganization()?.slug;
  const slug = router.query.slug as string;
  const {
    namespace: { name: namespace },
  } = useWorkspace();
  const { range, dateRange } = useOperationClientsState();
  const syncId = useId();

  const graphContext = useContext(GraphContext);
  const { data, isLoading, error, refetch } = useQuery(
    getOperationDetailMetricsPage,
    {
      namespace: graphContext?.graph?.namespace,
      federatedGraphName: graphContext?.graph?.name,
      operationHash: hash,
      operationName: name,
      operationType: type,
      range,
      dateRange: range
        ? undefined
        : {
          start: formatISO(dateRange.start),
          end: formatISO(dateRange.end),
        },
    },
    {
      placeholderData: (prev) => prev,
    },
  );

  if (isLoading) return <Loader fullscreen />;

  if (!isLoading && (error || data?.response?.code !== EnumStatusCode.OK)) {
    return (
      <div className="my-auto">
        <EmptyState
          icon={<ExclamationTriangleIcon />}
          title="Could not operation metrics"
          description={
            data?.response?.details || error?.message || "Please try again"
          }
          actions={<Button onClick={() => refetch()}>Retry</Button>}
        />
      </div>
    );
  }

  if (!data || !data.metadata || !data.topClients) {
    return (
      <EmptyState
        icon={<ExclamationTriangleIcon />}
        title="Failed to load operation metrics"
        description={data?.response?.details}
        actions={<Button onClick={() => undefined}>Retry</Button>}
      />
    );
  }

  return (
    <GraphPageLayout
      title={name}
      subtitle="Metrics related to a specific operation"
      breadcrumbs={[
        <Link
          key={0}
          href={`/${organizationSlug}/${namespace}/graph/${slug}/operations`}
        >
          Operations
        </Link>,
      ]}
      toolbar={<OperationsToolbar tab="metrics" />}
    >
      <div className="w-full space-y-4">
        <OperationDetailToolbar range={range} dateRange={dateRange} />
        <div className="flex min-h-0 flex-1 grid-cols-2 flex-col gap-4 lg:grid">
          <div className="col-span-1 flex flex-col rounded-md border">
            <div className="px-4 py-4">
              <dl className="space-y-4">
                <div className="flex flex-col">
                  <dt className="text-sm text-muted-foreground">Name</dt>
                  <dd>{data?.metadata?.name}</dd>
                </div>
                <div className="flex flex-col">
                  <dt className="text-sm text-muted-foreground">Type</dt>
                  <dd>{data?.metadata?.type}</dd>
                </div>
                <div className="flex flex-col">
                  <dt className="text-sm text-muted-foreground">Content</dt>
                  <dd className="mt-2">
                    <div className="rounded border">
                      <CodeViewer
                        code={data?.metadata?.content || ""}
                        language="graphql"
                        disableLinking
                        className="scrollbar-custom max-h-96 overflow-auto"
                      />
                    </div>
                  </dd>
                </div>
              </dl>
            </div>
          </div>
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
                            operationName: name,
                            operationHash: hash,
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
                  View traces for{" "}
                  {data.requestMetrics?.totalErrorCount.toString()} errors
                </TooltipContent>
              </Tooltip>
              <p className="px-2 pb-4 text-xl font-semibold">
                {formatPercentMetric(data.requestMetrics?.errorPercentage || 0)}
              </p>
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
                          ...router.query,
                        },
                      }}
                      className="inline-flex rounded-md px-2 py-1 hover:bg-muted"
                    >
                      Top {data.topClients.length}{" "}
                      {data.topClients.length === 1 ? "Client" : "Clients"}
                      <ChevronRightIcon className="h4 ml-1 w-4 transition-all group-hover:ml-2" />
                    </Link>
                  </h4>
                </TooltipTrigger>
                <TooltipContent>View all clients</TooltipContent>
              </Tooltip>
              <ClientsChart data={data?.topClients || []} />
            </CardContent>
          </Card>
        </div>

        <Card className="bg-transparent">
          <CardHeader>
            <div className="flex space-x-2">
              <CardTitle>Requests over time</CardTitle>
              <InfoTooltip>
                {formatRequestMetricsTooltip({
                  sum: data.requestMetrics?.totalRequestCount,
                  range,
                })}
              </InfoTooltip>
            </div>
          </CardHeader>
          <CardContent className="h-48 border-b pb-2">
            <RequestsChart
              data={data.requestMetrics?.requests || []}
              syncId={syncId}
            />
          </CardContent>
        </Card>
      </div>
    </GraphPageLayout>
  );
};

OperationDetailsPage.getLayout = (page) =>
  getGraphLayout(page, {
    title: "Operation Metrics",
  });

export default OperationDetailsPage;
