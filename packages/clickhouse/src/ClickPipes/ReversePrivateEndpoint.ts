import * as ClickPipes from "@distilled.cloud/clickhouse/click_pipes";
import * as Retry from "@distilled.cloud/clickhouse/Retry";
import { OwnedBySomeoneElse, Unowned } from "alchemy/AdoptPolicy";
import { isResolved } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { Providers } from "../Providers.ts";
import { markedDescription, waitForObserved } from "./internal.ts";

export type ReversePrivateEndpointTarget =
  | { type: "VPC_ENDPOINT_SERVICE"; vpcEndpointServiceName: string }
  | { type: "VPC_RESOURCE"; vpcResourceConfigurationId: string; vpcResourceShareArn: string }
  | { type: "MSK_MULTI_VPC"; mskClusterArn: string; mskAuthentication: "SASL_IAM" | "SASL_SCRAM" }
  | { type: "GCP_PSC_SERVICE_ATTACHMENT"; gcpServiceAttachment: string };

export interface ReversePrivateEndpointProps {
  /** Organization containing the service. */
  organizationId: string;
  /** Service containing the endpoint. */
  serviceId: string;
  /** Existing endpoint UUID for explicit adoption. */
  reversePrivateEndpointId?: string;
  /** Immutable description, at most 180 characters; Alchemy reserves 75 for its ownership marker. */
  description: string;
  /** Immutable destination and authentication. */
  target: ReversePrivateEndpointTarget;
  /** Complete desired DNS mapping set. Omission preserves existing mappings. */
  customPrivateDnsMappings?: ClickPipes.CustomPrivateDnsMapping[];
}

export interface ReversePrivateEndpointAttributes {
  /** Organization containing the service. */
  organizationId: string;
  /** Service containing the endpoint. */
  serviceId: string;
  /** ClickHouse endpoint UUID. */
  reversePrivateEndpointId: string;
  /** Network-provider endpoint ID, when available. */
  endpointId: string | undefined;
  /** Current endpoint status. PendingAcceptance needs action in the source cloud. */
  status: ClickPipes.ReversePrivateEndpointStatus | undefined;
  /** Internal DNS names currently reported. */
  dnsNames: string[];
  /** DNS targets currently reported; targets can appear after Ready. */
  dnsTargets: ClickPipes.ReversePrivateEndpointDnsTarget[];
  /** Custom DNS mappings, or undefined when the API omits them. */
  customPrivateDnsMappings: ClickPipes.CustomPrivateDnsMapping[] | undefined;
}

export interface ReversePrivateEndpoint extends Resource<
  "ClickHouse.ClickPipes.ReversePrivateEndpoint",
  ReversePrivateEndpointProps,
  ReversePrivateEndpointAttributes,
  never,
  Providers
> {}

/**
 * A private connection from ClickPipes to a source network. The source cloud
 * may require the endpoint owner to accept it. `PendingAcceptance` is returned
 * for external approval; `Ready` does not guarantee every DNS target has appeared.
 *
 * ### Connect to an AWS endpoint service
 * **Example:** Create a private source connection
 * ```typescript
 * import * as Alchemy from "alchemy";
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 * import * as Effect from "effect/Effect";
 *
 * export default Alchemy.Stack("Ingestion", {
 *   providers: ClickHouse.providers(), state: Alchemy.localState(),
 * }, Effect.gen(function* () {
 *   return yield* ClickHouse.ClickPipes.ReversePrivateEndpoint("SourceNetwork", {
 *     organizationId: "00000000-0000-4000-8000-000000000001",
 *     serviceId: "00000000-0000-4000-8000-000000000002",
 *     description: "Source network",
 *     target: { type: "VPC_ENDPOINT_SERVICE", vpcEndpointServiceName: "com.amazonaws.vpce.us-east-1.vpce-svc-example" },
 *   });
 * }));
 * ```
 *
 * Changing the target or description replaces the endpoint, deleting the old
 * endpoint first. Omitted DNS mappings are unmanaged; an empty list clears them.
 * Existing endpoints require their UUID and `Alchemy.AdoptPolicy.adopt(true)`. Failed creates
 * are not retried automatically. An `[alchemy:<sha256>]` description suffix
 * binds the full stack/stage/FQN, generation and immutable configuration to
 * recover interrupted creates. Matching a target or owning its service does
 * not prove ownership. Older unmarked endpoints require explicit UUID adoption.
 * Deletion does not remove the source service.
 *
 * @resource
 * @product ClickPipes
 */
export const ReversePrivateEndpoint = Resource<ReversePrivateEndpoint>(
  "ClickHouse.ClickPipes.ReversePrivateEndpoint",
);

export class ReversePrivateEndpointSafetyError extends Data.TaggedError(
  "ReversePrivateEndpointSafetyError",
)<{ message: string }> {}

type Scope = { organizationId: string; serviceId: string };
const list = Effect.fn(function* (scope: Scope) {
  const { result } = yield* ClickPipes.clickPipeReversePrivateEndpointGetList({
    organizationId: scope.organizationId,
    serviceId: scope.serviceId,
  }).pipe(Effect.catchTag("ClickPipesServiceNotFound", () => Effect.succeed({ result: [] })));
  if (!result || result.some((endpoint) => !endpoint.id))
    return yield* new ReversePrivateEndpointSafetyError({
      message: "Endpoint listing did not include complete identities.",
    });
  return result;
});

const targetMatches = (
  endpoint: ClickPipes.ReversePrivateEndpoint,
  target: ReversePrivateEndpointTarget,
) => {
  switch (target.type) {
    case "VPC_ENDPOINT_SERVICE":
      return (
        endpoint.type === target.type &&
        endpoint.vpcEndpointServiceName === target.vpcEndpointServiceName
      );
    case "VPC_RESOURCE":
      return (
        endpoint.type === target.type &&
        endpoint.vpcResourceConfigurationId === target.vpcResourceConfigurationId &&
        endpoint.vpcResourceShareArn === target.vpcResourceShareArn
      );
    case "MSK_MULTI_VPC":
      return (
        endpoint.type === target.type &&
        endpoint.mskClusterArn === target.mskClusterArn &&
        endpoint.mskAuthentication === target.mskAuthentication
      );
    case "GCP_PSC_SERVICE_ATTACHMENT":
      return (
        endpoint.type === target.type &&
        endpoint.gcpServiceAttachment === target.gcpServiceAttachment
      );
  }
};
const completeTarget = (endpoint: ClickPipes.ReversePrivateEndpoint) => {
  switch (endpoint.type) {
    case "VPC_ENDPOINT_SERVICE":
      return endpoint.vpcEndpointServiceName != null;
    case "VPC_RESOURCE":
      return endpoint.vpcResourceConfigurationId != null && endpoint.vpcResourceShareArn != null;
    case "MSK_MULTI_VPC":
      return endpoint.mskClusterArn != null && endpoint.mskAuthentication != null;
    case "GCP_PSC_SERVICE_ATTACHMENT":
      return endpoint.gcpServiceAttachment != null;
    default:
      return false;
  }
};
const mappingKeys = (mappings: ClickPipes.CustomPrivateDnsMapping[]) =>
  mappings
    .map((m) => JSON.stringify([m.privateDnsName ?? null, m.targetId ?? null]))
    .sort()
    .join("|");
const endpointDescription = (props: ReversePrivateEndpointProps) =>
  markedDescription(props.description, {
    organizationId: props.organizationId,
    serviceId: props.serviceId,
    description: props.description,
    target: props.target,
  });
const attributes = Effect.fn(function* (scope: Scope, endpoint: ClickPipes.ReversePrivateEndpoint) {
  if (!endpoint.id)
    return yield* new ReversePrivateEndpointSafetyError({
      message: "Endpoint response omitted its ID.",
    });
  return {
    ...scope,
    reversePrivateEndpointId: endpoint.id,
    endpointId: endpoint.endpointId,
    status: endpoint.status,
    dnsNames: endpoint.dnsNames ?? [],
    dnsTargets: endpoint.dnsTargets ?? [],
    customPrivateDnsMappings: endpoint.customPrivateDnsMappings,
  };
});
const waitForEndpoint = Effect.fn(function* (scope: Scope, id: string) {
  return yield* waitForObserved({
    description: `Reverse private endpoint ${id}`,
    observe: list(scope).pipe(Effect.map((endpoints) => endpoints.find((item) => item.id === id))),
    ready: (endpoint) => endpoint?.status === "Ready" || endpoint?.status === "PendingAcceptance",
    failed: (endpoint) =>
      endpoint?.status === "Failed" ||
      endpoint?.status === "Rejected" ||
      endpoint?.status === "Expired",
    slow: true,
  });
});

export const ReversePrivateEndpointProvider = () =>
  Provider.succeed(ReversePrivateEndpoint, {
    nuke: { skip: true },
    stables: ["organizationId", "serviceId", "reversePrivateEndpointId"],
    diff: Effect.fn(function* ({ news, olds, output }) {
      if (!isResolved(news) || !output) return;
      const current = (yield* list(output)).find(
        (endpoint) => endpoint.id === output.reversePrivateEndpointId,
      );
      if (!current) return { action: "update", stables: ["organizationId", "serviceId"] };
      if (current.description === undefined || !completeTarget(current))
        return yield* new ReversePrivateEndpointSafetyError({
          message: "The managed endpoint or its immutable configuration is missing.",
        });
      if (
        news.organizationId !== output.organizationId ||
        news.serviceId !== output.serviceId ||
        (news.reversePrivateEndpointId !== undefined &&
          news.reversePrivateEndpointId !== output.reversePrivateEndpointId) ||
        news.description !== olds.description ||
        !targetMatches(current, news.target)
      ) {
        if (news.reversePrivateEndpointId === output.reversePrivateEndpointId)
          return yield* new ReversePrivateEndpointSafetyError({
            message:
              "A replacement cannot reuse the same endpoint UUID. Omit reversePrivateEndpointId first.",
          });
        return { action: "replace", deleteFirst: true } as const;
      }
      if (news.customPrivateDnsMappings !== undefined) {
        if (current.customPrivateDnsMappings === undefined)
          return yield* new ReversePrivateEndpointSafetyError({
            message: "The API omitted DNS mappings; refusing to guess the current set.",
          });
        if (
          mappingKeys(current.customPrivateDnsMappings) !==
          mappingKeys(news.customPrivateDnsMappings)
        )
          return { action: "update" } as const;
      }
    }),
    read: Effect.fn(function* ({ olds, output, previous }) {
      const scope = {
        organizationId: output?.organizationId ?? olds.organizationId,
        serviceId: output?.serviceId ?? olds.serviceId,
      };
      const endpointId = output?.reversePrivateEndpointId ?? olds.reversePrivateEndpointId;
      const endpoints = yield* list(scope);
      const description = yield* endpointDescription(olds);
      const owned = endpoints.filter((endpoint) => endpoint.description === description);
      const matches = endpointId
        ? endpoints.filter((endpoint) => endpoint.id === endpointId)
        : owned.length
          ? owned
          : endpoints.filter((endpoint) => targetMatches(endpoint, olds.target));
      if (matches.length > 1)
        return yield* new ReversePrivateEndpointSafetyError({
          message: "Multiple endpoints match; select a UUID for adoption.",
        });
      if (!matches[0]) return;
      const attrs = yield* attributes(scope, matches[0]);
      if (
        output ||
        matches[0].description === (yield* endpointDescription(olds)) ||
        (previous?.organizationId === scope.organizationId &&
          previous.serviceId === scope.serviceId &&
          previous.reversePrivateEndpointId === attrs.reversePrivateEndpointId)
      )
        return attrs;
      if (!endpointId)
        return yield* new OwnedBySomeoneElse({
          message: "Select the exact endpoint UUID and opt into adoption.",
          resourceType: ReversePrivateEndpoint.Type,
          physicalName: attrs.reversePrivateEndpointId,
        });
      return Unowned(attrs);
    }),
    reconcile: Effect.fn(function* ({ news, output }) {
      if (
        output &&
        (news.organizationId !== output.organizationId ||
          news.serviceId !== output.serviceId ||
          (news.reversePrivateEndpointId !== undefined &&
            news.reversePrivateEndpointId !== output.reversePrivateEndpointId))
      )
        return yield* new ReversePrivateEndpointSafetyError({
          message: "Endpoint identity changed without replacement.",
        });
      const scope = { organizationId: news.organizationId, serviceId: news.serviceId };
      const description = yield* endpointDescription(news);
      const endpointId = output?.reversePrivateEndpointId ?? news.reversePrivateEndpointId;
      const endpoints = yield* list(scope);
      const owned = endpoints.filter((endpoint) => endpoint.description === description);
      const matches = endpointId
        ? endpoints.filter((endpoint) => endpoint.id === endpointId)
        : owned.length
          ? owned
          : endpoints.filter((endpoint) => targetMatches(endpoint, news.target));
      if (matches.length > 1)
        return yield* new ReversePrivateEndpointSafetyError({
          message: "Multiple endpoints match; select a UUID for adoption.",
        });
      let endpoint: ClickPipes.ReversePrivateEndpoint | undefined = matches[0];
      if (endpoint && !output && endpoint.description !== description) {
        const attrs = yield* attributes(scope, endpoint);
        return yield* new OwnedBySomeoneElse({
          message: "Select the exact endpoint UUID and opt into adoption.",
          resourceType: ReversePrivateEndpoint.Type,
          physicalName: attrs.reversePrivateEndpointId,
        });
      }
      if (!endpoint) {
        if (description.length > 255)
          return yield* new ReversePrivateEndpointSafetyError({
            message:
              "Endpoint description plus Alchemy's 75-character ownership suffix exceeds 255 characters.",
          });
        if (news.reversePrivateEndpointId)
          return yield* new ReversePrivateEndpointSafetyError({
            message:
              "The explicitly selected endpoint is missing; remove reversePrivateEndpointId to create one.",
          });
        endpoint = (yield* ClickPipes.createClickPipeReversePrivateEndpoint({
          ...scope,
          description,
          ...news.target,
          customPrivateDnsMappings: news.customPrivateDnsMappings,
        }).pipe(Retry.none)).result;
        if (!endpoint)
          return yield* new ReversePrivateEndpointSafetyError({
            message: "Create returned no endpoint; inspect the service before retrying.",
          });
      }
      const identity = yield* attributes(scope, endpoint);
      endpoint = yield* waitForEndpoint(scope, identity.reversePrivateEndpointId);
      if (
        !endpoint ||
        !targetMatches(endpoint, news.target) ||
        (endpoint.description !== news.description && endpoint.description !== description)
      )
        return yield* new ReversePrivateEndpointSafetyError({
          message: "Endpoint's immutable configuration differs from the desired configuration.",
        });
      const attrs = yield* attributes(scope, endpoint);
      if (
        news.customPrivateDnsMappings !== undefined &&
        attrs.customPrivateDnsMappings === undefined
      )
        return yield* new ReversePrivateEndpointSafetyError({
          message: "The API omitted DNS mappings; refusing to guess the current set.",
        });
      if (
        news.customPrivateDnsMappings !== undefined &&
        attrs.customPrivateDnsMappings !== undefined &&
        mappingKeys(attrs.customPrivateDnsMappings) !== mappingKeys(news.customPrivateDnsMappings)
      ) {
        yield* ClickPipes.updateClickPipeReversePrivateEndpoint({
          ...scope,
          reversePrivateEndpointId: attrs.reversePrivateEndpointId,
          customPrivateDnsMappings: news.customPrivateDnsMappings,
        });
        const expected = mappingKeys(news.customPrivateDnsMappings);
        const observed = yield* waitForObserved({
          description: `Reverse private endpoint ${attrs.reversePrivateEndpointId} DNS mappings`,
          observe: list(scope).pipe(
            Effect.map((endpoints) =>
              endpoints.find((item) => item.id === attrs.reversePrivateEndpointId),
            ),
          ),
          ready: (item) =>
            item?.customPrivateDnsMappings !== undefined &&
            mappingKeys(item.customPrivateDnsMappings) === expected,
        });
        if (!observed)
          return yield* new ReversePrivateEndpointSafetyError({
            message: "Endpoint disappeared during DNS mapping update.",
          });
        return yield* attributes(scope, observed);
      }
      return attrs;
    }),
    delete: Effect.fn(function* ({ output }) {
      const scope = { organizationId: output.organizationId, serviceId: output.serviceId };
      const current = (yield* list(scope)).find(
        (endpoint) => endpoint.id === output.reversePrivateEndpointId,
      );
      if (!current) return;
      if (current.status !== "Deleting")
        yield* ClickPipes.deleteClickPipeReversePrivateEndpoint({
          ...scope,
          reversePrivateEndpointId: output.reversePrivateEndpointId,
        }).pipe(
          Retry.none,
          Effect.catchTag("ClickPipeReversePrivateEndpointNotFound", () => Effect.void),
        );
      yield* waitForObserved({
        description: `Reverse private endpoint ${output.reversePrivateEndpointId} deletion`,
        observe: list(scope),
        ready: (endpoints) =>
          !endpoints.some((endpoint) => endpoint.id === output.reversePrivateEndpointId),
        slow: true,
      });
    }),
  });
