import type { SortingState } from "@tanstack/react-table";
import { useRouter } from "next/router";
import { useCallback, useEffect, useState } from "react";

/**
 * Convert sorting state to URL query params
 */
export const createSortingState = (sorting: SortingState): { sort?: string; sortDir?: string } => {
  if (sorting.length === 0) {
    return {};
  }

  return {
    sort: sorting[0].id,
    sortDir: sorting[0].desc ? "desc" : "asc",
  };
};

/**
 * Manage & sync URL state for simple sorting
 */
export const useSortingState = (defaultSort: SortingState) => {
  const router = useRouter();
  const [sorting, setSorting] = useState<SortingState>(defaultSort);

  useEffect(() => {
    if (router.isReady) {
      if (router.query.sort) {
        setSorting([
          {
            id: router.query.sort.toString(),
            desc: router.query.sortDir?.toString() !== "asc",
          },
        ]);
      } else {
        setSorting(defaultSort);
      }
    }
  }, [router.isReady, router.query.sort, router.query.sortDir, defaultSort]);

  const updateSorting = useCallback(
    (newSorting: SortingState) => {
      setSorting(newSorting);

      const query = { ...router.query };

      if (newSorting.length > 0) {
        query.sort = newSorting[0].id;
        query.sortDir = newSorting[0].desc ? "desc" : "asc";
      } else {
        delete query.sort;
        delete query.sortDir;
      }

      router.push({ query }, undefined, { shallow: true });
    },
    [router]
  );

  return {
    sorting,
    setSorting: updateSorting,
  };
};
