import { getOperationDetailClientPage } from "@wundergraph/cosmo-connect/dist/platform/v1/platform-PlatformService_connectquery";
import { EnumStatusCode } from "@wundergraph/cosmo-connect/dist/common/common_pb";
import { useQuery } from "@connectrpc/connect-query";
import {
  GraphPageLayout,
  getGraphLayout,
} from "@/components/layout/graph-layout";
import { NextPageWithLayout } from "@/lib/page";
import { useCurrentOrganization } from "@/hooks/use-current-organization";
import { useWorkspace } from "@/hooks/use-workspace";
import { EmptyState } from "@/components/empty-state";
import { ClientsTable } from "@/components/operations/clients-table";
import { OperationsToolbar } from "@/components/operations/operations-toolbar";
import { GraphContext } from "@/components/layout/graph-layout";
import { OperationDetailToolbar } from "@/components/operations/operation-detail-toolbar";
import { useOperationClientsState } from "@/components/operations/use-operation-clients-state";
import { Button } from "@/components/ui/button";
import { Loader } from "@/components/ui/loader";
import { ExclamationTriangleIcon } from "@heroicons/react/24/outline";
import { formatISO } from "date-fns";
import { useRouter } from "next/router";
import Link from "next/link";
import { useContext } from "react";

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

  const graphContext = useContext(GraphContext);
  const pageNumber = router.query.page
    ? parseInt(router.query.page as string)
    : 1;
  const limit = Number.parseInt((router.query.pageSize as string) || "10");

  const { data, isLoading, error, refetch } = useQuery(
    getOperationDetailClientPage,
    {
      namespace: graphContext?.graph?.namespace,
      federatedGraphName: graphContext?.graph?.name,
      limit: limit > 50 ? 50 : limit,
      offset: (pageNumber - 1) * limit,
      operationHash: hash,
      operationName: name,
      operationType: type,
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
          title="Could not retrieve operation list"
          description={
            data?.response?.details || error?.message || "Please try again"
          }
          actions={<Button onClick={() => refetch()}>Retry</Button>}
        />
      </div>
    );
  }

  if (!data || !data.clients) {
    return (
      <EmptyState
        icon={<ExclamationTriangleIcon />}
        title="Could not retrieve operation list"
        description={data?.response?.details}
        actions={<Button onClick={() => undefined}>Retry</Button>}
      />
    );
  }

  return (
    <GraphPageLayout
      title={name}
      subtitle="Operation clients"
      breadcrumbs={[
        <Link
          key={0}
          href={`/${organizationSlug}/${namespace}/graph/${slug}/operations`}
        >
          Operations
        </Link>,
      ]}
      toolbar={<OperationsToolbar tab="clients" />}
    >
      <div className="w-full space-y-4">
        <OperationDetailToolbar range={range} dateRange={dateRange} />
        <ClientsTable
          list={data.clients}
          noOfPages={Math.ceil(data.count / limit)}
          pageNumber={pageNumber}
          limit={limit}
        />
      </div>
    </GraphPageLayout>
  );
};

OperationDetailsPage.getLayout = (page) =>
  getGraphLayout(page, {
    title: "Operation Clients",
  });

export default OperationDetailsPage;
