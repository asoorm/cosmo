import type { OperationDetailClientPageItem } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import type { SortingState } from "@tanstack/react-table";
import type { Dispatch, SetStateAction, SyntheticEvent } from "react";
import { useCallback } from "react";
import { ChevronDownIcon, ChevronUpIcon } from "@heroicons/react/24/outline";
import { cn } from "@/lib/utils";
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

const SortableTableHead = ({
  id,
  label,
  sorting,
  onClick,
}: {
  id: string;
  label: string;
  sorting: SortingState;
  onClick: (event: SyntheticEvent<HTMLTableCellElement>) => void;
}) => {
  const currentSort = sorting.find(s => s.id === id);

  return (
    <TableHead
      id={id}
      onClick={onClick}
      className="select-none cursor-pointer hover:text-foreground"
    >
      <div className="inline-flex items-center space-x-1">
        <span>{label}</span>
        <span className="inline-block w-3">
          {currentSort ? (
            currentSort.desc ? (
              <ChevronDownIcon className="h-3 w-3" />
            ) : (
              <ChevronUpIcon className="h-3 w-3" />
            )
          ) : null}
        </span>
      </div>
    </TableHead>
  );
};

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
  const handleHeaderClick = useCallback((event: SyntheticEvent<HTMLTableCellElement>) => {
    const headerId = event.currentTarget.id;
    const sortIndex = sorting.findIndex((s) => s.id === headerId);

    const currentSortColumn = sorting[sortIndex]

    if (!currentSortColumn) {
      setSorting([{ id: headerId, desc: true }])
      return;
    }

    setSorting([{
      ...currentSortColumn,
      desc: !currentSortColumn.desc,
    }])
}, [sorting, setSorting])

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
