import { OperationPageItem } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  TableWrapper,
} from "@/components/ui/table";
import { formatDateTime } from "@/lib/format-date";
import { cn } from "@/lib/utils";
import { ExclamationTriangleIcon } from "@heroicons/react/24/outline";
import { useRouter } from "next/router";
import type { ReactNode } from "react";
import { HiOutlineCheck } from "react-icons/hi2";

const OperationsTableRow = ({
  children,
  hasError,
  operationHash,
  operationName,
  operationType,
}: {
  children: ReactNode;
  hasError: boolean;
  operationHash: string;
  operationName: string;
  operationType: string;
}) => {
  const router = useRouter();
  const id = encodeURIComponent(
    `${operationType}-${operationName}-${operationHash}`,
  );

  const handleRowClick = () => {
    const route = `${router.asPath.split("?")[0]}/${id}`;

    router.push(route);
  };

  return (
    <TableRow
      onClick={handleRowClick}
      className={cn("group cursor-pointer py-1 hover:bg-secondary/30", {
        "bg-destructive/10": hasError,
      })}
    >
      {children}
    </TableRow>
  );
};

const OperationsStatusTableCell = ({ hasError }: { hasError: boolean }) => {
  return (
    <TableCell className="flex items-center space-x-2">
      {hasError ? (
        <ExclamationTriangleIcon className="mt-2 h-5 w-5 text-destructive" />
      ) : (
        <HiOutlineCheck className="mt-2 h-5 w-5" />
      )}
    </TableCell>
  );
};

export const OperationsTable = ({
  operations,
}: {
  operations: OperationPageItem[];
}) => {
  return (
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
          {operations.map((operation) => (
            <OperationsTableRow
              key={`${operation.type}-${operation.name}-${operation.hash}`}
              operationType={operation.type}
              operationName={operation.name}
              operationHash={operation.hash}
              hasError={operation.hasErrors}
            >
              <TableCell>{operation.name}</TableCell>
              <TableCell>{operation.type}</TableCell>
              <TableCell>
                {formatDateTime(new Date(operation.timestamp))}
              </TableCell>
              <TableCell>{operation.totalRequestCount.toString()}</TableCell>
              <OperationsStatusTableCell hasError={operation.hasErrors} />
            </OperationsTableRow>
          ))}
        </TableBody>
      </Table>
    </TableWrapper>
  );
};
