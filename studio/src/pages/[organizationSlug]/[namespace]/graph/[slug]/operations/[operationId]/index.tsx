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
import { OperationDetailToolbar } from "@/components/operations/operation-detail-toolbar";
import { useOperationClientsState } from "@/components/operations/use-operation-clients-state";
import { Button } from "@/components/ui/button";
import { Loader } from "@/components/ui/loader";
import { ExclamationTriangleIcon } from "@heroicons/react/24/outline";
import { useRouter } from "next/router";
import Link from "next/link";
import { useContext } from "react";
import { CodeViewer } from "@/components/code-viewer";

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
  const { data, isLoading, error, refetch } = useQuery(
    getOperationDetailMetricsPage,
    {
      namespace: graphContext?.graph?.namespace,
      federatedGraphName: graphContext?.graph?.name,
      operationHash: hash,
      operationName: name,
      operationType: type,
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
            <h3 className="border-b px-4 py-2 font-semibold tracking-tight">
              Operation
            </h3>
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

          <div className="col-span-1 flex flex-col rounded-md border">
            <h3 className="border-b px-4 py-2 font-semibold tracking-tight">
              Top clients
            </h3>
            <div className="px-4 py-4">
              <ul>
                {data.topClients.map(({ name, version, count }, index) => (
                  <li key={`${name}-${version}`} className="mb-2">
                    <div className="font-medium">
                      {index + 1}. {name} {version} <em>{count}</em>
                    </div>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      </div>
    </GraphPageLayout>
  );
};

OperationDetailsPage.getLayout = (page) =>
  getGraphLayout(page, {
    title: "Operation Metrics",
  });

export default OperationDetailsPage;
