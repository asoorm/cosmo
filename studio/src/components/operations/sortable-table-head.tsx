import type { SortingState } from "@tanstack/react-table";
import type { SyntheticEvent } from "react";
import { useCallback } from "react";
import { ChevronDownIcon, ChevronUpIcon } from "@heroicons/react/24/outline";
import { TableHead } from "../ui/table";

export const SortableTableHead = ({
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
  const sortAvailable = !!currentSort;

  return (
    <TableHead
      id={id}
      onClick={onClick}
      className="select-none cursor-pointer hover:text-foreground"
    >
      <div className="inline-flex items-center space-x-1">
        <span>{label}</span>
        <span className="inline-block w-3">
          {sortAvailable && currentSort.desc && (
            <ChevronDownIcon className="h-3 w-3" />
          )}
          {sortAvailable && !currentSort.desc && (
            <ChevronUpIcon className="h-3 w-3" />
          )}
        </span>
      </div>
    </TableHead>
  );
};

export const useSortableTableHeader = (
  sorting: SortingState,
  setSorting: (sort: SortingState) => void
) => {
  const handleHeaderClick = useCallback(
    (event: SyntheticEvent<HTMLTableCellElement>) => {
      const headerId = event.currentTarget.id;
      const sortIndex = sorting.findIndex((s) => s.id === headerId);

      const currentSortColumn = sorting[sortIndex];

      if (!currentSortColumn) {
        setSorting([{ id: headerId, desc: true }]);
        return;
      }

      setSorting([{
        ...currentSortColumn,
        desc: !currentSortColumn.desc,
      }]);
    },
    [sorting, setSorting]
  );

  return { handleHeaderClick };
};
