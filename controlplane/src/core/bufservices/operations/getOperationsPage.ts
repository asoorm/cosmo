/* eslint-disable camelcase */
import { PlainMessage } from '@bufbuild/protobuf';
import { HandlerContext } from '@connectrpc/connect';
import { EnumStatusCode } from '@wundergraph/cosmo-connect/dist/common/common_pb';
import {
  GetOperationsPageRequest,
  GetOperationsPageResponse,
} from '@wundergraph/cosmo-connect/dist/platform/v1/platform_pb';
import type { RouterOptions } from '../../routes.js';
import { FederatedGraphRepository } from '../../repositories/FederatedGraphRepository.js';
import { OrganizationRepository } from '../../repositories/OrganizationRepository.js';
import { OperationsViewRepository } from '../../repositories/operations/OperationsViewRepository.js';
import { enrichLogger, getLogger, handleError, validateDateRanges } from '../../util.js';

export function getOperationsPage(
  opts: RouterOptions,
  req: GetOperationsPageRequest,
  ctx: HandlerContext,
): Promise<PlainMessage<GetOperationsPageResponse>> {
  let logger = getLogger(ctx, opts.logger);

  return handleError<PlainMessage<GetOperationsPageResponse>>(ctx, logger, async () => {
    if (!opts.chClient) {
      return {
        response: {
          code: EnumStatusCode.ERR_ANALYTICS_DISABLED,
        },
        operations: [],
        count: 0,
        allOperationNames: [],
        allOperationTypes: [],
      };
    }

    const authContext = await opts.authenticator.authenticate(ctx.requestHeader);
    logger = enrichLogger(ctx, logger, authContext);

    const fedGraphRepo = new FederatedGraphRepository(logger, opts.db, authContext.organizationId);
    const graph = await fedGraphRepo.byName(req.federatedGraphName, req.namespace);
    if (!graph) {
      return {
        response: {
          code: EnumStatusCode.ERR_NOT_FOUND,
          details: `Federated graph '${req.federatedGraphName}' not found`,
        },
        operations: [],
        count: 0,
        allOperationNames: [],
        allOperationTypes: [],
      };
    }

    const orgRepo = new OrganizationRepository(logger, opts.db, opts.billingDefaultPlanId);
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

    const [view, filterOptions] = await Promise.all([
      repo.getOperations({
        organizationId: authContext.organizationId,
        graphId: graph.id,
        limit: req.limit,
        offset: req.offset,
        sorting: req.sorting,
        range,
        dateRange,
        filters: req.filters,
      }),
      repo.getAllOperationNamesAndTypes({
        organizationId: authContext.organizationId,
        graphId: graph.id,
      }),
    ]);

    return {
      response: {
        code: EnumStatusCode.OK,
      },
      ...view,
      ...filterOptions,
    };
  });
}
