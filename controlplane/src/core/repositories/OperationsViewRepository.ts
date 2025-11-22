import { DateRange } from '../../types/index.js';
import { ClickHouseClient } from '../clickhouse/index.js';
import { isoDateRangeToTimestamps, getDateRange } from './analytics/util.js';

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
  }: {
    organizationId: string;
    graphId: string;
    limit: number;
    offset: number;
  }) {
    const query = `
      SELECT
        "OperationHash" AS hash,
        "OperationName" AS name,
        "OperationType" AS type,
        max("Timestamp") AS timestamp,
        sum("TotalRequests") as totalRequestCount,
        IF(sum("TotalErrors") > 0, true, false) as hasErrors,
        count("OperationHash") OVER () AS count 
      FROM
        ${this.client.database}.operation_request_metrics_5_30
      WHERE
        "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
      GROUP BY
        "OperationHash",
        "OperationName",
        "OperationType"
      ORDER BY
        timestamp DESC
      LIMIT ${limit} OFFSET ${offset}
    `;

    const result = await this.client.queryPromise<{
      hash: string;
      name: string;
      type: string;
      timestamp: string;
      totalRequestCount: string;
      hasErrors: boolean;
      count: number;
    }>(query, { organizationId, graphId, limit, offset });

    return {
      count: Number(result?.[0]?.count ?? 0),
      operations: result.map(({ count, totalRequestCount, ...row }) => ({
        ...row,
        totalRequestCount: BigInt(totalRequestCount),
      })),
    };
  }

  public async getOperationMetadataByNameHashType({
    organizationId,
    graphId,
    operationName,
    operationHash,
    operationType,
  }: {
    organizationId: string;
    graphId: string;
    operationName: string;
    operationHash: string;
    operationType: string;
  }) {
    const query = `
      SELECT
        "OperationType" as type,
        "OperationContent" as content,
        "OperationName" as name
      FROM
        ${this.client.database}.gql_metrics_operations
      WHERE
        "OperationName" = '${operationName}'
        AND "OperationType" = '${operationType}'
        AND "OperationHash" = '${operationHash}'
        AND "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
    `;

    const result = await this.client.queryPromise<{
      type: string;
      name: string;
      content: string;
    }>(query, {
      organizationId,
      graphId,
      operationName,
      operationHash,
      operationType,
    });

    return {
      metadata: result?.[0] || null,
    };
  }

  public async getTopClientsForOperationByNameHashType({
    organizationId,
    graphId,
    operationName,
    operationHash,
    operationType,
    range,
    dateRange,
  }: {
    organizationId: string;
    graphId: string;
    operationName: string;
    operationHash: string;
    operationType: string;
    range?: number;
    dateRange?: DateRange<string>;
  }) {
    const { start, end } = OperationsViewRepository.normalizeDateRange(dateRange, range);

    const query = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate
      SELECT
        "ClientName" as name,
        "ClientVersion" as version,
        SUM("TotalRequests") as count
      FROM
        ${this.client.database}.operation_request_metrics_5_30
      WHERE
        "Timestamp" >= startDate AND "Timestamp" <= endDate
        AND "OperationName" = '${operationName}'
        AND "OperationType" = '${operationType}'
        AND "OperationHash" = '${operationHash}'
        AND "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
      GROUP BY
        "ClientName",
        "ClientVersion"
      ORDER BY
        count DESC
      LIMIT
        5
    `;

    const result = await this.client.queryPromise<{
      name: string;
      version: string;
      count: string;
    }>(query, {
      organizationId,
      graphId,
      operationName,
      operationHash,
      operationType,
    });

    return {
      topClients: result.map(({ count, ...row }) => ({
        ...row,
        count: BigInt(count),
      })),
    };
  }

  public async getOperationClientListByNameHashType({
    organizationId,
    graphId,
    operationName,
    operationHash,
    operationType,
    limit,
    offset,
    range,
    dateRange,
  }: {
    organizationId: string;
    graphId: string;
    operationName: string;
    operationHash: string;
    operationType: string;
    limit: number;
    offset: number;
    range?: number;
    dateRange?: DateRange<string>;
  }) {
    const { start, end } = OperationsViewRepository.normalizeDateRange(dateRange, range);

    const query = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate
      SELECT
        "Timestamp" AS timestamp,
        "OperationHash" AS hash,
        "TotalRequests" AS totalRequests,
        "TotalErrors" AS totalErrors,
        "ClientName" AS clientName,
        "ClientVersion" AS clientVersion,
        COUNT(*) OVER() as count
      FROM
        ${this.client.database}.operation_request_metrics_5_30
      WHERE
        timestamp >= startDate AND "Timestamp" <= endDate
        AND "OperationName" = '${operationName}'
        AND "OperationType" = '${operationType}'
        AND "OperationHash" = '${operationHash}'
        AND "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
      ORDER BY
        timestamp DESC
      LIMIT ${limit} OFFSET ${offset}
    `;

    const result = await this.client.queryPromise<{
      timestamp: string;
      hash: string;
      totalRequests: string;
      totalErrors: string;
      clientName: string;
      clientVersion: string;
      count: number;
    }>(query, { organizationId, graphId, limit, offset });

    return {
      count: Number(result?.[0]?.count ?? 0),
      clients: result.map(({ count, totalRequests, totalErrors, ...row }) => ({
        ...row,
        totalRequests: BigInt(totalRequests),
        totalErrors: BigInt(totalErrors),
      })),
    };
  }

  public async getRequestsForOperationByNameHashType({
    organizationId,
    graphId,
    operationName,
    operationHash,
    operationType,
    range,
    dateRange,
  }: {
    organizationId: string;
    graphId: string;
    operationName: string;
    operationHash: string;
    operationType: string;
    range?: number;
    dateRange?: DateRange<string>;
  }) {
    const { start, end } = OperationsViewRepository.normalizeDateRange(dateRange, range);

    const metricsQuery = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate
      SELECT
        toStartOfInterval(startDate + toIntervalMinute(n.number * 5), INTERVAL 5 MINUTE) as timestamp,
        sum(oprm."TotalRequests") as requests,
        sum(oprm."TotalErrors") as errors
      FROM
        numbers(toUInt32((toUnixTimestamp(endDate) - toUnixTimestamp(startDate)) / 300)) AS n
      LEFT JOIN ${this.client.database}.operation_request_metrics_5_30 AS oprm
        ON toStartOfInterval(oprm."Timestamp", INTERVAL 5 MINUTE) = timestamp
        AND oprm."Timestamp" >= startDate
        AND oprm."Timestamp" <= endDate
        AND oprm."OperationName" = '${operationName}'
        AND oprm."OperationType" = '${operationType}'
        AND oprm."OperationHash" = '${operationHash}'
        AND oprm."OrganizationID" = '${organizationId}'
        AND oprm."FederatedGraphID" = '${graphId}'
      GROUP BY timestamp
      ORDER BY timestamp ASC
    `;

    const sumQuery = `
      WITH
        toDateTime('${start}') AS startDate,
        toDateTime('${end}') AS endDate
      SELECT
        sum("TotalRequests") as totalRequestCount,
        sum("TotalErrors") as totalErrorCount,
        if(totalErrorCount > 0, round(totalErrorCount / totalRequestCount * 100, 2), 0) errorPercentage
      FROM
        ${this.client.database}.operation_request_metrics_5_30
      WHERE
        "Timestamp" >= startDate
        AND "Timestamp" <= endDate
        AND "OperationName" = '${operationName}'
        AND "OperationType" = '${operationType}'
        AND "OperationHash" = '${operationHash}'
        AND "OrganizationID" = '${organizationId}'
        AND "FederatedGraphID" = '${graphId}'
      GROUP BY
        "OperationName", "OperationHash", "OperationType"
    `;

    const metricsResultQueryPromise = this.client.queryPromise<{
      timestamp: string;
      requests: string;
      errors: string;
    }>(metricsQuery);
    const sumResultQueryPromise = this.client.queryPromise<{
      totalRequestCount: string;
      totalErrorCount: string;
      errorPercentage: string;
    }>(sumQuery);

    const [result, sumResult] = await Promise.all([metricsResultQueryPromise, sumResultQueryPromise]);

    const requests = result.map(({ timestamp, requests, errors }) => ({
      timestamp,
      requests: BigInt(requests),
      errors: BigInt(errors),
    }));

    const totalRequestCount = sumResult[0]?.totalRequestCount ? BigInt(sumResult[0]?.totalRequestCount) : 0n;
    const totalErrorCount = sumResult[0]?.totalErrorCount ? BigInt(sumResult[0]?.totalErrorCount) : 0n;
    const errorPercentage = sumResult[0]?.errorPercentage ? Number(sumResult[0]?.errorPercentage) : 0;

    return {
      requestMetrics: {
        requests,
        totalRequestCount,
        totalErrorCount,
        errorPercentage,
      },
    };
  }

  private static normalizeDateRange(
    dateRange?: DateRange<string>,
    range?: number,
  ): {
    start: number;
    end: number;
  } {
    if (dateRange && dateRange.start > dateRange.end) {
      const tmp = dateRange.start;
      dateRange.start = dateRange.end;
      dateRange.end = tmp;
    }

    const parsedDateRange = isoDateRangeToTimestamps(dateRange, range);
    const [start, end] = getDateRange(parsedDateRange);

    return { start, end };
  }
}
