import type { OperationDetailClientPageItem } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import { Pagination } from "../ui/pagination";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  TableWrapper,
} from "../ui/table";

export const ClientsTable = ({
  list,
  limit,
  noOfPages,
  pageNumber,
}: {
  list: OperationDetailClientPageItem[];
  limit: number;
  noOfPages: number;
  pageNumber: number;
}) => {
  return (
    <>
      <TableWrapper>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Client Name</TableHead>
              <TableHead>Client Version</TableHead>
              <TableHead>Requests</TableHead>
              <TableHead>Errors</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {list.map((operation) => (
              <TableRow
                key={`${operation.clientName}-${operation.clientVersion}`}
              >
                <TableCell>{operation.clientName}</TableCell>
                <TableCell>{operation.clientVersion}</TableCell>
                <TableCell>{operation.totalRequests.toString()}</TableCell>
                <TableCell>{operation.totalErrors.toString()}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableWrapper>
      <Pagination limit={limit} noOfPages={noOfPages} pageNumber={pageNumber} />
    </>
  );
};
