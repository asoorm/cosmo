import {
  GraphPageLayout,
  getGraphLayout,
} from "@/components/layout/graph-layout";
import { useRouter } from "next/router";
import { NextPageWithLayout } from "@/lib/page";
import { useCurrentOrganization } from "@/hooks/use-current-organization";
import { useWorkspace } from "@/hooks/use-workspace";
import Link from "next/link";
import { ClientToolbar } from "@/components/operations/client-toolbar";
import { useOperationClientsState } from "@/components/operations/use-operation-clients-state";

const OperationDetailsPage: NextPageWithLayout = () => {
  const router = useRouter();
  const [type, name, hash] = decodeURIComponent(router.query.operationId as string).split('-');
  const organizationSlug = useCurrentOrganization()?.slug;
  const slug = router.query.slug as string;
  const {
    namespace: { name: namespace },
  } = useWorkspace();

  const { range, dateRange } = useOperationClientsState();

  return (
    <GraphPageLayout
      title={name}
      subtitle="Detail related to a specific operation"
      breadcrumbs={[
        <Link
          key={0}
          href={`/${organizationSlug}/${namespace}/graph/${slug}/operations`}
        >
          Operations
        </Link>,
      ]}
    >
    <div className="w-full space-y-4">
      <ClientToolbar range={range} dateRange={dateRange} />
    </div>
    </GraphPageLayout>
  );
}

OperationDetailsPage.getLayout = (page) =>
  getGraphLayout(page, {
    title: "Operation Details",
  });

export default OperationDetailsPage;
