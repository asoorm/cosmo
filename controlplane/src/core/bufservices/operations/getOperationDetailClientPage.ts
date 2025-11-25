/* eslint-disable camelcase */
import { PlainMessage } from '@bufbuild/protobuf';
import { HandlerContext } from '@connectrpc/connect';
import { EnumStatusCode } from '@wundergraph/cosmo-connect/dist/common/common_pb';
import {
  GetOperationDetailClientPageRequest,
  GetOperationDetailClientPageResponse,
} from '@wundergraph/cosmo-connect/dist/platform/v1/platform_pb';
import type { RouterOptions } from '../../routes.js';
import { FederatedGraphRepository } from '../../repositories/FederatedGraphRepository.js';
import { OperationsViewRepository } from '../../repositories/OperationsViewRepository.js';
import { OrganizationRepository } from '../../repositories/OrganizationRepository.js';
import { enrichLogger, getLogger, handleError, validateDateRanges } from '../../util.js';

export function getOperationDetailClientPage(
  opts: RouterOptions,
  req: GetOperationDetailClientPageRequest,
  ctx: HandlerContext,
): Promise<PlainMessage<GetOperationDetailClientPageResponse>> {
  let logger = getLogger(ctx, opts.logger);

  return handleError<PlainMessage<GetOperationDetailClientPageResponse>>(ctx, logger, async () => {
    if (!opts.chClient) {
      return {
        response: {
          code: EnumStatusCode.ERR_ANALYTICS_DISABLED,
        },
        clients: [],
        allClients: [],
        count: 0,
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
        clients: [],
        allClients: [],
        count: 0,
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

    console.log(`range: ${range}, dateRange: ${JSON.stringify(dateRange)}`);

    const repo = new OperationsViewRepository(opts.chClient);
    const [view, allClients] = await Promise.all([
      repo.getOperationClientListByNameHashType({
        organizationId: authContext.organizationId,
        graphId: graph.id,
        operationName: req.operationName,
        operationHash: req.operationHash,
        operationType: req.operationType,
        limit: req.limit,
        offset: req.offset,
        range,
        dateRange,
        filters: req.filters,
      }),
      repo.getAllClientsWithVersionsForOperationByNameHashType({
        organizationId: authContext.organizationId,
        graphId: graph.id,
        operationName: req.operationName,
        operationHash: req.operationHash,
        operationType: req.operationType,
      }),
    ]);

    return {
      response: {
        code: EnumStatusCode.OK,
      },
      ...view,
      ...allClients,
    };
  });
}
