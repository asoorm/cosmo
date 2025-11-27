import type { OperationDetailMetadata } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { CodeViewer } from "../code-viewer";
import { Card, CardContent, CardHeader, CardTitle } from "../ui/card";

export const DetailCard = ({
  metadata,
}: {
  metadata?: OperationDetailMetadata;
}) => (
  <Card className="col-span-1 bg-transparent">
    <CardHeader>
      <CardTitle>Operation Details</CardTitle>
    </CardHeader>
    <CardContent className="flex flex-col gap-y-3 text-sm">
      <div className="flex gap-x-4">
        <span className="w-28 text-muted-foreground">Name</span>
        <span className="truncate">{metadata?.name}</span>
      </div>
      <div className="flex gap-x-4">
        <span className="w-28 text-muted-foreground">Type</span>
        <span>{metadata?.type}</span>
      </div>
      <div className="flex flex-col gap-y-2">
        <span className="text-muted-foreground">Content</span>
        <div className="rounded border">
          <CodeViewer
            code={metadata?.content || ""}
            language="graphql"
            disableLinking
            className="scrollbar-custom max-h-96 overflow-auto"
          />
        </div>
      </div>
    </CardContent>
  </Card>
);
