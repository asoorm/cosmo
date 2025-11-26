import {
  AnalyticsFilter,
  AnalyticsViewFilterOperator,
  Sort,
} from '@wundergraph/cosmo-connect/dist/platform/v1/platform_pb';
import { DateRange } from '../../../types/index.js';
import { ClickHouseClient } from '../../clickhouse/index.js';
import { normalizeDateRange } from './util.js';

export class OperationsViewRepository {
  constructor(private client: ClickHouseClient) {}

  /**
   * Get operations page data
   */
  public async getOperations({
    organizationId,
    graphId,
    limit,
    offset,
    sorting = [],
    range,
    dateRange,
    filters = [],
  }: {
    organizationId: string;
    graphId: string;
    limit: number;
    offset: number;
    sorting?: Sort[];
    range?: number;
    dateRange?: DateRange<string>;
    filters?: AnalyticsFilter[];
  }) {
    const { start, end } = normalizeDateRange(dateRange, range);
    const filterClause = OperationsViewRepository.buildOperationsFilterClauses(filters);

    const query = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate
      SELECT
        "OperationHash" AS hash,
        "OperationName" AS name,
        "OperationType" AS type,
        max("Timestamp") AS timestamp,
        toInt32(sum("TotalRequests")) as totalRequestCount,
        IF(sum("TotalErrors") > 0, true, false) as hasErrors,
        count(DISTINCT "OperationHash") OVER () AS count
      FROM
        ${this.client.database}.operation_request_metrics_5_30
      WHERE
        "Timestamp" >= startDate AND "Timestamp" <= endDate
        AND "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
        ${filterClause}
      GROUP BY
        "OperationHash",
        "OperationName",
        "OperationType"
      ORDER BY
        ${OperationsViewRepository.buildOperationsOrderByClause(sorting)}
      LIMIT ${limit} OFFSET ${offset}
    `;

    const result = await this.client.queryPromise<{
      hash: string;
      name: string;
      type: string;
      timestamp: string;
      totalRequestCount: number;
      hasErrors: boolean;
      count: number;
    }>(query);
    const operations = result.map((row) => ({
      ...row,
      count: Number(row.count),
    }));

    return {
      count: operations[0]?.count ?? 0,
      operations,
    };
  }

  /**
   * Get all operation names and types for filter options
   */
  public async getAllOperationNamesAndTypes({ organizationId, graphId }: { organizationId: string; graphId: string }) {
    const namesQuery = `
      SELECT DISTINCT "OperationName" AS name
      FROM ${this.client.database}.operation_request_metrics_5_30
      WHERE "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
      ORDER BY name ASC
      LIMIT 1000
    `;

    const typesQuery = `
      SELECT DISTINCT "OperationType" AS type
      FROM ${this.client.database}.operation_request_metrics_5_30
      WHERE "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
      ORDER BY type ASC
    `;

    const [nameRows, typeRows] = await Promise.all([
      this.client.queryPromise<{ name: string }>(namesQuery),
      this.client.queryPromise<{ type: string }>(typesQuery),
    ]);

    return {
      allOperationNames: nameRows.map((row) => row.name),
      allOperationTypes: typeRows.map((row) => row.type),
    };
  }

  private static buildOperationsFilterClauses(filters: AnalyticsFilter[]): string {
    const clauses: string[] = [];

    const operationNameFilters = filters.filter(
      (f) => f.field === 'operationName' && f.operator === AnalyticsViewFilterOperator.EQUALS,
    );
    const operationTypeFilters = filters.filter(
      (f) => f.field === 'operationType' && f.operator === AnalyticsViewFilterOperator.EQUALS,
    );
    const hasErrorsFilters = filters.filter(
      (f) => f.field === 'hasErrors' && f.operator === AnalyticsViewFilterOperator.EQUALS,
    );

    if (operationNameFilters.length > 0) {
      const values = operationNameFilters.map((f) => `'${f.value.replace(/'/g, "\\'")}'`).join(', ');
      clauses.push(`"OperationName" IN (${values})`);
    }

    if (operationTypeFilters.length > 0) {
      const values = operationTypeFilters.map((f) => `'${f.value.replace(/'/g, "\\'")}'`).join(', ');
      clauses.push(`"OperationType" IN (${values})`);
    }

    if (hasErrorsFilters.length > 0 && hasErrorsFilters[0].value === 'true') {
      clauses.push(`"TotalErrors" > 0`);
    }

    return clauses.length > 0 ? `AND ${clauses.join(' AND ')}` : '';
  }

  private static buildOperationsOrderByClause(sorting: Sort[], defaultSort = 'timestamp DESC'): string {
    const columnMap: Record<string, string> = {
      name: 'name',
      type: 'type',
      timestamp: 'timestamp',
      totalRequestCount: 'totalRequestCount',
      hasErrors: 'hasErrors',
    };

    let orderBy: string;
    if (sorting.length === 0) {
      orderBy = defaultSort;
    } else {
      orderBy = sorting
        .map((s) => {
          const column = columnMap[s.id] || s.id;
          return `${column} ${s.desc ? 'DESC' : 'ASC'}`;
        })
        .join(', ');
    }

    return `${orderBy}, hash ASC`;
  }
}
