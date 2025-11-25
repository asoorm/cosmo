import type { OperationDetailClientPageItem } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import type { SortingState } from "@tanstack/react-table";
import { cn } from "@/lib/utils";
import { Pagination } from "../ui/pagination";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableRow,
  TableWrapper,
} from "../ui/table";
import { SortableTableHead, useSortableTableHeader } from "./sortable-table-head";

export const ClientsTable = ({
  list,
  limit,
  noOfPages,
  pageNumber,
  sorting,
  setSorting,
}: {
  list: OperationDetailClientPageItem[];
  limit: number;
  noOfPages: number;
  pageNumber: number;
  sorting: SortingState;
  setSorting: (sort: SortingState) => void;
}) => {
  const { handleHeaderClick } = useSortableTableHeader(sorting, setSorting);

  return (
  <>
    <TableWrapper>
      <Table>
        <TableHeader>
          <TableRow>
            <SortableTableHead id="clientName" label="Client Name" sorting={sorting} onClick={handleHeaderClick} />
            <SortableTableHead id="clientVersion" label="Client Version" sorting={sorting} onClick={handleHeaderClick} />
            <SortableTableHead id="totalRequests" label="Requests" sorting={sorting} onClick={handleHeaderClick} />
            <SortableTableHead id="totalErrors" label="Errors" sorting={sorting} onClick={handleHeaderClick} />
          </TableRow>
        </TableHeader>
        <TableBody>
          {list.map((operation) => (
            <TableRow
              key={`${operation.clientName}-${operation.clientVersion}`}
              className={cn({"bg-destructive/10": operation.totalErrors > 0})}
            >
              <TableCell>{operation.clientName}</TableCell>
              <TableCell>{operation.clientVersion}</TableCell>
              <TableCell>{operation.totalRequests}</TableCell>
              <TableCell>{operation.totalErrors}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableWrapper>
    <Pagination limit={limit} noOfPages={noOfPages} pageNumber={pageNumber} />
  </>
);
};
