import { OperationDetailTopClientItem } from "@wundergraph/cosmo-connect/dist/platform/v1/platform_pb";
import BarList from "@/components/analytics/barlist";
import { useCallback } from "react";
import { cn } from "@/lib/utils";

export const ClientsChart = ({
  data,
  type = "requests",
}: {
  data: OperationDetailTopClientItem[];
  type?: "requests" | "errors";
}) => {
  const valueFormatter = useCallback((number: number) => {
    if (number > Number.MAX_SAFE_INTEGER) {
      return `${Number.MAX_SAFE_INTEGER.toLocaleString()}+`;
    }

    return number.toString();
  }, []);

  const renderName = useCallback((name: string) => {
    if (name.trim() === "") {
      return "-";
    }

    const boundedName = name.slice(0, 14);

    if (boundedName.length < name.length) {
      return `${boundedName}…`;
    }

    return boundedName;
  }, []);

  return (
    <BarList
      data={data.map((row) => ({
        key: row.name + row.version,
        value: row.count,
        name: (
          <div className="flex items-center">
            <span className="flex w-32 shrink-0 truncate">
              {renderName(row.name)}
            </span>
            <span className="truncate">
              {row.version.slice(0, 15) || "-------"}
            </span>
          </div>
        ),
      }))}
      valueFormatter={valueFormatter}
      rowHeight={4}
      rowClassName={cn("bg-muted text-muted-foreground hover:text-foreground", {
        "bg-destructive/20": type === "errors",
      })}
    />
  );
};
