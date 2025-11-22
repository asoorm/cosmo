/* eslint-disable camelcase */
import { PlainMessage } from '@bufbuild/protobuf';
import { HandlerContext } from '@connectrpc/connect';
import { EnumStatusCode } from '@wundergraph/cosmo-connect/dist/common/common_pb';
import {
  GetOperationDetailMetricsPageRequest,
  GetOperationDetailMetricsPageResponse,
} from '@wundergraph/cosmo-connect/dist/platform/v1/platform_pb';
import type { RouterOptions } from '../../routes.js';
import { OrganizationRepository } from '../../repositories/OrganizationRepository.js';
import { FederatedGraphRepository } from '../../repositories/FederatedGraphRepository.js';
import { OperationsViewRepository } from '../../repositories/OperationsViewRepository.js';
import { enrichLogger, getLogger, handleError, validateDateRanges } from '../../util.js';

export function getOperationDetailMetricsPage(
  opts: RouterOptions,
  req: GetOperationDetailMetricsPageRequest,
  ctx: HandlerContext,
): Promise<PlainMessage<GetOperationDetailMetricsPageResponse>> {
  let logger = getLogger(ctx, opts.logger);

  return handleError<PlainMessage<GetOperationDetailMetricsPageResponse>>(ctx, logger, async () => {
    if (!opts.chClient) {
      return {
        response: {
          code: EnumStatusCode.ERR_ANALYTICS_DISABLED,
        },
        topClients: [],
        requestMetrics: {
          requests: [],
          totalRequestCount: 0n,
          totalErrorCount: 0n,
          errorPercentage: 0,
        },
      };
    }

    const authContext = await opts.authenticator.authenticate(ctx.requestHeader);
    logger = enrichLogger(ctx, logger, authContext);

    const fedGraphRepo = new FederatedGraphRepository(logger, opts.db, authContext.organizationId);
    const orgRepo = new OrganizationRepository(logger, opts.db, opts.billingDefaultPlanId);
    const graph = await fedGraphRepo.byName(req.federatedGraphName, req.namespace);
    if (!graph) {
      return {
        response: {
          code: EnumStatusCode.ERR_NOT_FOUND,
          details: `Federated graph '${req.federatedGraphName}' not found`,
        },
        topClients: [],
        requestMetrics: {
          requests: [],
          totalRequestCount: 0n,
          totalErrorCount: 0n,
          errorPercentage: 0,
        },
      };
    }

    const analyticsRetention = await orgRepo.getFeature({
      organizationId: authContext.organizationId,
      featureId: 'analytics-retention',
    });

    const { range, dateRange } = validateDateRanges({
      limit: analyticsRetention?.limit ?? 7,
      range: req.range,
      dateRange: req.dateRange,
    });

    const repo = new OperationsViewRepository(opts.chClient);
    const metadata = await repo.getOperationMetadataByNameHashType({
      organizationId: authContext.organizationId,
      graphId: graph.id,
      operationName: req.operationName,
      operationHash: req.operationHash,
      operationType: req.operationType,
    });
    const topClients = await repo.getTopClientsForOperationByNameHashType({
      organizationId: authContext.organizationId,
      graphId: graph.id,
      operationName: req.operationName,
      operationHash: req.operationHash,
      operationType: req.operationType,
      range,
      dateRange,
    });
    const requests = await repo.getRequestsForOperationByNameHashType({
      organizationId: authContext.organizationId,
      graphId: graph.id,
      operationName: req.operationName,
      operationHash: req.operationHash,
      operationType: req.operationType,
      range,
      dateRange,
    });

    return {
      response: {
        code: EnumStatusCode.OK,
      },
      ...metadata,
      ...topClients,
      ...requests,
    };
  });
}
