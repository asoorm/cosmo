import { EmptyState } from "@/components/empty-state";
import {
  GraphPageLayout,
  getGraphLayout,
} from "@/components/layout/graph-layout";
import { EnumStatusCode } from "@wundergraph/cosmo-connect/dist/common/common_pb";
import { getOperationsPage } from "@wundergraph/cosmo-connect/dist/platform/v1/platform-PlatformService_connectquery";
import { Button } from "@/components/ui/button";
import { Loader } from "@/components/ui/loader";
import { Pagination } from "@/components/ui/pagination";
import { GraphContext } from "@/components/layout/graph-layout";
import { OperationsTable } from "@/components/operations/operations-table";
import { OperationsPageToolbar } from "@/components/operations/operations-page-toolbar";
import { useOperationClientsState } from "@/components/operations/use-operation-clients-state";
import { useSortingState } from "@/components/operations/use-sorting-state";
import { NextPageWithLayout } from "@/lib/page";
import { useQuery } from "@connectrpc/connect-query";
import { ExclamationTriangleIcon } from "@heroicons/react/24/outline";
import { formatISO } from "date-fns";
import { useRouter } from "next/router";
import { useContext } from "react";

const DEFAULT_OPERATIONS_TABLE_SORT = [{ id: "timestamp", desc: true }];

const OperationsPage: NextPageWithLayout = () => {
  const router = useRouter();
  const graphContext = useContext(GraphContext);
  const { sorting, setSorting } = useSortingState(DEFAULT_OPERATIONS_TABLE_SORT);
  const { range, dateRange } = useOperationClientsState();
  const pageNumber = router.query.page
    ? parseInt(router.query.page as string)
    : 1;
  const limit = Number.parseInt((router.query.pageSize as string) || "10");

  const { data, isLoading, error, refetch } = useQuery(
    getOperationsPage,
    {
      namespace: graphContext?.graph?.namespace,
      federatedGraphName: graphContext?.graph?.name,
      limit: limit > 50 ? 50 : limit,
      offset: (pageNumber - 1) * limit,
      sorting,
      range: router.query.dateRange ? undefined : range,
      dateRange: router.query.dateRange
        ? {
            start: formatISO(dateRange.start),
            end: formatISO(dateRange.end),
          }
        : undefined,
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
          title="Could not retrieve operations data"
          description={
            data?.response?.details || error?.message || "Please try again"
          }
          actions={<Button onClick={() => refetch()}>Retry</Button>}
        />
      </div>
    );
  }

  if (!data || !data.operations) {
    return (
      <EmptyState
        icon={<ExclamationTriangleIcon />}
        title="Could not retrieve operations"
        description={data?.response?.details}
        actions={<Button onClick={() => undefined}>Retry</Button>}
      />
    );
  }

  if (data.operations.length === 0) {
    return (
      <EmptyState
        icon={<ExclamationTriangleIcon />}
        title="No operations found"
        description="No operations have been recorded for this graph."
      />
    );
  }

  const noOfPages = Math.ceil(data.count / limit);

  return (
    <div className="flex h-full flex-col gap-y-3">
      <OperationsPageToolbar range={range} dateRange={dateRange} />
      <OperationsTable
        operations={data.operations}
        sorting={sorting}
        setSorting={setSorting}
      />
      <Pagination limit={limit} noOfPages={noOfPages} pageNumber={pageNumber} />
    </div>
  );
};

OperationsPage.getLayout = (page) =>
  getGraphLayout(
    <GraphPageLayout
      title="Operations"
      subtitle="A list of recorded operations"
    >
      {page}
    </GraphPageLayout>,
    {
      title: "Operations",
    },
  );

export default OperationsPage;
