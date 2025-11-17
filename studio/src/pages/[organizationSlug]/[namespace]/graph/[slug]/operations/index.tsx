import { EmptyState } from "@/components/empty-state";
import {
  GraphPageLayout,
  getGraphLayout,
} from "@/components/layout/graph-layout";
import { OperationPageItem } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { EnumStatusCode } from "@wundergraph/cosmo-connect/dist/common/common_pb";
import { getOperationsPage } from "@wundergraph/cosmo-connect/dist/platform/v1/platform-PlatformService_connectquery";
import { formatDateTime } from "@/lib/format-date";
import { Button } from "@/components/ui/button";
import { Loader } from "@/components/ui/loader";
import { Pagination } from "@/components/ui/pagination";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  TableWrapper,
} from "@/components/ui/table";
import { GraphContext } from "@/components/layout/graph-layout";
import { NextPageWithLayout } from "@/lib/page";
import { cn } from "@/lib/utils";
import { useQuery } from "@connectrpc/connect-query";
import { ExclamationTriangleIcon } from "@heroicons/react/24/outline";
import { useRouter } from "next/router";
import { useContext } from "react";
import type { ReactNode } from "react";
import { HiOutlineCheck } from "react-icons/hi2";

const OperationsTableRow = ({
  children,
  hasError,
}: {
  children: ReactNode;
  hasError: boolean;
}) => {
  return (
    <TableRow className={cn("group cursor-pointer py-1 hover:bg-secondary/30", {
      'bg-destructive/10': hasError,
    })}>
      {children}
    </TableRow>
  );
};

const OperationsStatusTableCell = ({ hasError }: { hasError: boolean }) => {
  return (
    <div className="flex items-center space-x-2">
      {hasError ? (
        <ExclamationTriangleIcon className="h-5 w-5 mt-2 text-destructive" />
      ) : (
        <HiOutlineCheck className="h-5 w-5 mt-2" />
      )}
    </div>
  );
};

const OperationsPage: NextPageWithLayout = () => {
  const router = useRouter();
  const graphContext = useContext(GraphContext);
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
      <TableWrapper>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Type</TableHead>
              <TableHead>Last Called</TableHead>
              <TableHead>Requests</TableHead>
              <TableHead>Health</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.operations.map((operation: OperationPageItem) => {
              const hasError = operation.totalErrorCount > 0;

              return (
                <OperationsTableRow key={`${operation.type}-${operation.name}-${operation.hash}`} hasError={hasError}>
                  <TableCell>{operation.name}</TableCell>
                  <TableCell>{operation.type}</TableCell>
                  <TableCell>
                    {formatDateTime(new Date(operation.timestamp))}
                  </TableCell>
                  <TableCell>
                    {operation.totalRequestCount.toString()}
                  </TableCell>
                  <OperationsStatusTableCell hasError={hasError} />
                </OperationsTableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableWrapper>
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
