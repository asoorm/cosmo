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
import { OperationsToolbar } from "@/components/operations/operations-toolbar";
import { FiltersToolbar } from "@/components/operations/filters-toolbar";
import { useOperationClientsState } from "@/components/operations/use-operation-clients-state";
import { Button } from "@/components/ui/button";
import { Loader } from "@/components/ui/loader";
import { ExclamationTriangleIcon } from "@heroicons/react/24/outline";
import { useRouter } from "next/router";
import { formatISO } from "date-fns";
import Link from "next/link";
import { useContext, useId } from "react";
import { DetailCard } from "@/components/operations/detail-card";
import { LatencyCard } from "@/components/operations/latency-card";
import { RequestsCard } from "@/components/operations/requests-card";
import { ErrorPercentageCard } from "@/components/operations/error-percentage-card";
import {
  useOperationClientFilters,
  transformFiltersForAPI,
} from "@/components/operations/use-operation-client-filters";
import { useFilterState } from "@/components/operations/use-filter-state";

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
  const columnFilters = useFilterState();

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
      filters: transformFiltersForAPI(columnFilters),
    },
    {
      placeholderData: (prev) => prev,
      enabled: !!graphContext?.graph,
    },
  );

  const { filters, resetFilters } = useOperationClientFilters(
    data?.allClients || [],
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
        <FiltersToolbar
          range={range}
          dateRange={dateRange}
          filters={filters}
          selectedFilters={columnFilters}
          onResetFilters={resetFilters}
        />
        <div className="flex min-h-0 flex-1 grid-cols-2 flex-col gap-4 lg:grid">
          <DetailCard metadata={data.metadata} />
          <ErrorPercentageCard
            operationName={name}
            operationHash={hash}
            organizationSlug={organizationSlug}
            namespace={namespace}
            range={range}
            topClients={data.topClients}
            topErrorClients={data.topErrorClients}
            requestMetrics={data.requestMetrics}
          />
        </div>
        <RequestsCard
          requestMetrics={data.requestMetrics}
          range={range}
          dateRange={dateRange}
          syncId={syncId}
        />
        <LatencyCard
          latencyMetrics={data.latencyMetrics}
          requestMetrics={data.requestMetrics}
          range={range}
          dateRange={dateRange}
          syncId={syncId}
        />
      </div>
    </GraphPageLayout>
  );
};

OperationDetailsPage.getLayout = (page) =>
  getGraphLayout(page, {
    title: "Operation Metrics",
  });

export default OperationDetailsPage;
