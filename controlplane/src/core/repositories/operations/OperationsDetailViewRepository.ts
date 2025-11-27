import {
  AnalyticsFilter,
  AnalyticsViewFilterOperator,
  Sort,
} from '@wundergraph/cosmo-connect/dist/platform/v1/platform_pb';
import { DateRange } from '../../../types/index.js';
import { ClickHouseClient } from '../../clickhouse/index.js';
import { normalizeDateRange } from './util.js';

export class OperationsDetailViewRepository {
  constructor(private client: ClickHouseClient) {}

  /**
   * Obtains basic metadata information regarding given operation
   * identified by its hash.
   */
  public async getOperationMetadataByHash({
    organizationId,
    graphId,
    operationHash,
  }: {
    organizationId: string;
    graphId: string;
    operationHash: string;
  }) {
    const query = `
      SELECT
        "OperationType" as type,
        "OperationContent" as content,
        "OperationName" as name
      FROM
        ${this.client.database}.gql_metrics_operations
      WHERE
        "OperationHash" = '${operationHash}'
        AND "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
    `;

    const result = await this.client.queryPromise<{
      type: string;
      name: string;
      content: string;
    }>(query);

    return {
      metadata: result?.[0] || null,
    };
  }

  /**
   * Obtains top clients by request and error count for given operation
   * identified by its hash.
   */
  public async getTopClientsForOperationByHash({
    organizationId,
    graphId,
    operationHash,
    range,
    dateRange,
    filters = [],
  }: {
    organizationId: string;
    graphId: string;
    operationHash: string;
    range?: number;
    dateRange?: DateRange<string>;
    filters?: AnalyticsFilter[];
  }) {
    const { start, end } = normalizeDateRange(dateRange, range);
    const filterClause = OperationsDetailViewRepository.buildClientFilterClauses(filters);

    const subQuery = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate,
        clients AS (
          SELECT
            oprm."ClientName" as name,
            oprm."ClientVersion" as version,
            toInt32(sum(oprm."TotalRequests")) as totalRequestCount,
            toInt32(sum(oprm."TotalErrors")) as totalErrorCount
          FROM
            ${this.client.database}.operation_request_metrics_5_30 as oprm
          WHERE
            oprm."Timestamp" >= startDate AND oprm."Timestamp" <= endDate
            AND oprm."OperationHash" = '${operationHash}'
            AND oprm."OrganizationID" = '${organizationId}'
            AND oprm."FederatedGraphID" = '${graphId}'
            ${filterClause}
          GROUP BY
            oprm."ClientName",
            oprm."ClientVersion"
        )
    `;

    const topClientQuery = `
      ${subQuery}
      SELECT clients.name, clients.version, clients.totalRequestCount as count
      FROM clients
      ORDER BY
        clients.totalRequestCount DESC
      LIMIT
        5
    `;

    const topErrorClientQuery = `
      ${subQuery}
      SELECT clients.name, clients.version, clients.totalErrorCount as count
      FROM clients
      WHERE clients.totalErrorCount > 0
      ORDER BY
        clients.totalErrorCount DESC
      LIMIT
        5
    `;

    const [topClientResult, topErrorClientResult] = await Promise.all([
      this.client.queryPromise<{
        name: string;
        version: string;
        count: number;
      }>(topClientQuery),
      this.client.queryPromise<{
        name: string;
        version: string;
        count: number;
      }>(topErrorClientQuery),
    ]);

    const anyErrors = topErrorClientResult.some((client) => client.count > 0);

    return {
      topClients: topClientResult,
      topErrorClients: anyErrors ? topErrorClientResult : [],
    };
  }

  /**
   * Obtains all clients with their versions for given operation
   * identified by its hash.
   */
  public async getAllClientsWithVersionsForOperationByHash({
    organizationId,
    graphId,
    operationHash,
  }: {
    organizationId: string;
    graphId: string;
    operationHash: string;
  }) {
    const query = `
      SELECT DISTINCT
        "ClientName" AS name,
        "ClientVersion" AS version
      FROM
        ${this.client.database}.operation_request_metrics_5_30
      WHERE
        "OperationHash" = '${operationHash}'
        AND "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
      ORDER BY
        name ASC, version ASC
      LIMIT 100
    `;

    const result = await this.client.queryPromise<{
      name: string;
      version: string;
    }>(query);

    return {
      allClients: result,
    };
  }

  /**
   * Obtains paginated list of clients for given operation
   * identified by its hash.
   */
  public async getOperationClientListByHash({
    organizationId,
    graphId,
    operationHash,
    limit,
    offset,
    range,
    dateRange,
    filters = [],
    sorting = [],
  }: {
    organizationId: string;
    graphId: string;
    operationHash: string;
    limit: number;
    offset: number;
    range?: number;
    dateRange?: DateRange<string>;
    filters?: AnalyticsFilter[];
    sorting?: Sort[];
  }) {
    const { start, end } = normalizeDateRange(dateRange, range);
    const filterClause = OperationsDetailViewRepository.buildClientFilterClauses(filters);
    const orderByClause = OperationsDetailViewRepository.buildOrderByClause(sorting);

    const query = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate
      SELECT
        toInt32(sum("TotalRequests")) AS totalRequests,
        toInt32(sum("TotalErrors")) AS totalErrors,
        "ClientName" AS clientName,
        "ClientVersion" AS clientVersion,
        COUNT(*) OVER() as count
      FROM
        ${this.client.database}.operation_request_metrics_5_30
      WHERE
        "Timestamp" >= startDate AND "Timestamp" <= endDate
        AND "OperationHash" = '${operationHash}'
        AND "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
        ${filterClause}
      GROUP BY
        "ClientName",
        "ClientVersion"
      ORDER BY
        ${orderByClause}
      LIMIT ${limit} OFFSET ${offset}
    `;

    const result = await this.client.queryPromise<{
      totalRequests: number;
      totalErrors: number;
      clientName: string;
      clientVersion: string;
      count: number;
    }>(query);

    const clients = result.map((row) => ({
      ...row,
      count: Number(row.count),
    }));

    return {
      count: clients[0]?.count ?? 0,
      clients,
    };
  }

  /**
   * Filtered total request / error metrics for given operation
   * identified by its hash.
   */
  public async getRequestsForOperationByHash({
    organizationId,
    graphId,
    operationHash,
    range,
    dateRange,
    filters = [],
  }: {
    organizationId: string;
    graphId: string;
    operationHash: string;
    range?: number;
    dateRange?: DateRange<string>;
    filters?: AnalyticsFilter[];
  }) {
    const { start, end } = normalizeDateRange(dateRange, range);
    const filterClause = OperationsDetailViewRepository.buildClientFilterClauses(filters, 'oprm');

    const metricsQuery = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate
      SELECT
        toStartOfInterval(startDate + toIntervalMinute(n.number * 5), INTERVAL 5 MINUTE) as timestamp,
        toInt32(sum(oprm."TotalRequests")) as requests,
        toInt32(sum(oprm."TotalErrors")) as errors
      FROM
        numbers(toUInt32((toUnixTimestamp(endDate) - toUnixTimestamp(startDate)) / 300)) AS n
      LEFT JOIN ${this.client.database}.operation_request_metrics_5_30 AS oprm
        ON toStartOfInterval(oprm."Timestamp", INTERVAL 5 MINUTE) = timestamp
        AND oprm."Timestamp" >= startDate
        AND oprm."Timestamp" <= endDate
        AND oprm."OperationHash" = '${operationHash}'
        AND oprm."OrganizationID" = '${organizationId}'
        AND oprm."FederatedGraphID" = '${graphId}'
        ${filterClause}
      GROUP BY timestamp
      ORDER BY timestamp ASC
    `;

    const sumQuery = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate
      SELECT
        toInt32(sum("TotalRequests")) as totalRequestCount,
        toInt32(sum("TotalErrors")) as totalErrorCount,
        if(totalErrorCount > 0, round(totalErrorCount / totalRequestCount * 100, 2), 0) errorPercentage
      FROM
        ${this.client.database}.operation_request_metrics_5_30
      WHERE
        "Timestamp" >= startDate
        AND "Timestamp" <= endDate
        AND "OperationHash" = '${operationHash}'
        AND "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
    `;

    const metricsResultQueryPromise = this.client.queryPromise<{
      timestamp: string;
      requests: number;
      errors: number;
    }>(metricsQuery);
    const sumResultQueryPromise = this.client.queryPromise<{
      totalRequestCount: number;
      totalErrorCount: number;
      errorPercentage: string;
    }>(sumQuery);

    const [result, sumResult] = await Promise.all([metricsResultQueryPromise, sumResultQueryPromise]);

    const totalRequestCount = sumResult[0]?.totalRequestCount ?? 0;
    const totalErrorCount = sumResult[0]?.totalErrorCount ?? 0;
    const errorPercentage = sumResult[0]?.errorPercentage ? Number(sumResult[0]?.errorPercentage) : 0;

    return {
      requestMetrics: {
        requests: result,
        totalRequestCount,
        totalErrorCount,
        errorPercentage,
      },
    };
  }

  /**
   * Filtered latency metrics for given operation
   * identified by its hash.
   */
  public async getLatencyForOperationByHash({
    organizationId,
    graphId,
    operationHash,
    range,
    dateRange,
    filters = [],
  }: {
    organizationId: string;
    graphId: string;
    operationHash: string;
    range?: number;
    dateRange?: DateRange<string>;
    filters?: AnalyticsFilter[];
  }) {
    const { start, end } = normalizeDateRange(dateRange, range);
    const filterClause = OperationsDetailViewRepository.buildClientFilterClauses(filters, 'oplm');

    const query = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate
      SELECT
        toStartOfInterval(startDate + toIntervalMinute(n.number * 5), INTERVAL 5 MINUTE) as timestamp,
        round(min(oplm."MinDuration"), 2) as minDuration,
        round(max(oplm."MaxDuration"), 2) as maxDuration
      FROM
        numbers(toUInt32((toUnixTimestamp(endDate) - toUnixTimestamp(startDate)) / 300)) AS n
      LEFT JOIN ${this.client.database}.operation_latency_metrics_5_30 AS oplm
        ON toStartOfInterval(oplm."Timestamp", INTERVAL 5 MINUTE) = timestamp
        AND oplm."Timestamp" >= startDate
        AND oplm."Timestamp" <= endDate
        AND oplm."OperationHash" = '${operationHash}'
        AND oplm."OrganizationID" = '${organizationId}'
        AND oplm."FederatedGraphID" = '${graphId}'
        ${filterClause}
      GROUP BY timestamp
      ORDER BY timestamp ASC
    `;

    const result = await this.client.queryPromise<{
      timestamp: string;
      minDuration: number;
      maxDuration: number;
    }>(query);

    return {
      latencyMetrics: {
        requests: result,
      },
    };
  }

  private static buildClientFilterClauses(filters: AnalyticsFilter[], tableAlias?: string): string {
    const clauses: string[] = [];
    const prefix = tableAlias ? `${tableAlias}.` : '';

    const clientNameFilters = filters.filter(
      (f) => f.field === 'clientName' && f.operator === AnalyticsViewFilterOperator.EQUALS,
    );
    const clientVersionFilters = filters.filter(
      (f) => f.field === 'clientVersion' && f.operator === AnalyticsViewFilterOperator.EQUALS,
    );

    if (clientNameFilters.length > 0) {
      const values = clientNameFilters.map((f) => `'${f.value.replace(/'/g, "\\'")}'`).join(', ');
      clauses.push(`${prefix}"ClientName" IN (${values})`);
    }

    if (clientVersionFilters.length > 0) {
      const values = clientVersionFilters.map((f) => `'${f.value.replace(/'/g, "\\'")}'`).join(', ');
      clauses.push(`${prefix}"ClientVersion" IN (${values})`);
    }

    return clauses.length > 0 ? `AND ${clauses.join(' AND ')}` : '';
  }

  private static buildOrderByClause(sorting: Sort[], defaultSort = 'totalRequests DESC'): string {
    if (sorting.length === 0) {
      return defaultSort;
    }

    const columnMap: Record<string, string> = {
      clientName: 'clientName',
      clientVersion: 'clientVersion',
      totalRequests: 'totalRequests',
      totalErrors: 'totalErrors',
    };

    return sorting
      .map((s) => {
        const column = columnMap[s.id] || s.id;
        return `${column} ${s.desc ? 'DESC' : 'ASC'}`;
      })
      .join(', ');
  }
}
