import { ClickHouseClient } from '../clickhouse/index.js';

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
      count: Number(result?.[0].count),
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
  }: {
    organizationId: string;
    graphId: string;
    operationName: string;
    operationHash: string;
    operationType: string;
  }) {
    const query = `
      SELECT
        "ClientName" as name,
        "ClientVersion" as version,
        SUM("TotalRequests") as count
      FROM
        ${this.client.database}.operation_request_metrics_5_30
      WHERE
        "OperationName" = '${operationName}'
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
  }: {
    organizationId: string;
    graphId: string;
    operationName: string;
    operationHash: string;
    operationType: string;
    limit: number;
    offset: number;
  }) {
    const query = `
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
        "OperationName" = '${operationName}'
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
      count: Number(result?.[0].count),
      clients: result.map(({ count, totalRequests, totalErrors, ...row }) => ({
        ...row,
        totalRequests: BigInt(totalRequests),
        totalErrors: BigInt(totalErrors),
      })),
    };
  }
}
