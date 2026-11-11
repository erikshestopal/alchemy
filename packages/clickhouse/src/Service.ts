import * as Organizations from "@distilled.cloud/clickhouse/organization";
import * as Retry from "@distilled.cloud/clickhouse/Retry";
import * as Services from "@distilled.cloud/clickhouse/service";
import { OwnedBySomeoneElse, Unowned } from "alchemy/AdoptPolicy";
import { isResolved } from "alchemy/Diff";
import { createPhysicalName } from "alchemy/PhysicalName";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import { createInternalTags, diffTags } from "alchemy/Tags";
import { unwrapRedacted } from "alchemy/Util/data";
import { sha256, sha256Object } from "alchemy/Util/sha256";
import { stableStringify } from "alchemy/Util/stable";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import type { Providers } from "./Providers.ts";

export interface ServiceProps {
  /** Organization that owns the service. Changing it replaces the service. */
  organizationId: string;
  /** Exact existing service UUID to adopt. Changing it replaces the service. */
  serviceId?: string;
  /** Cloud provider. Changing it replaces the service. */
  provider: Services.CreateInstanceRequestProvider;
  /** Cloud region. Changing it replaces the service. */
  region: string;
  /** Display name. Omission preserves the observed name or generates one on creation. */
  name?: string;
  /** Deprecated legacy tier; current organizations use replica sizing instead. */
  tier?: Services.CreateInstanceRequestTier;
  /** Deprecated total memory minimum for legacy services. Do not combine with replica sizing. */
  minTotalMemoryGb?: number;
  /** Deprecated total memory maximum for legacy services. Do not combine with replica sizing. */
  maxTotalMemoryGb?: number;
  /** Autoscaling mode. Horizontal scaling requires organization entitlement. */
  autoscalingMode?: Services.CreateInstanceRequestAutoscalingMode;
  /** Minimum memory in GiB per replica. */
  minReplicaMemoryGb?: number;
  /** Maximum memory in GiB per replica. */
  maxReplicaMemoryGb?: number;
  /** Fixed replica count. Mutually exclusive with a replica count range. */
  numReplicas?: number;
  /** Minimum replica count. Supply together with maxReplicas. */
  minReplicas?: number;
  /** Maximum replica count. Supply together with minReplicas. */
  maxReplicas?: number;
  /** Allow idle scaling. Omission preserves it; ClickHouse defaults to true on creation. */
  idleScaling?: boolean;
  /** Idle timeout in minutes. Omission preserves it. */
  idleTimeoutMinutes?: number;
  /** Existing warehouse UUID. Changing it replaces the service. */
  dataWarehouseId?: string;
  /** Create a read-only secondary in an existing warehouse. Changing it replaces the service. */
  isReadonly?: boolean;
  /** Backup UUID to restore on creation. Changing it replaces the service. */
  backupId?: string;
  /** Encryption configuration copied verbatim from the exported backup. */
  backupEncryptionConfig?: Services.BackupEncryptionConfig;
  /** Instance profile. Switching between Gen 1 and Gen 2 replaces the service. */
  profile?: string;
  /** Enable transparent data encryption at creation. Changing it replaces the service. */
  hasTransparentDataEncryption?: boolean;
  /** Create-only regulatory compliance mode. Changing it replaces the service. */
  complianceType?: Services.CreateInstanceRequestComplianceType;
  /** Accept private-preview terms when creating the organization's first service. */
  privatePreviewTermsChecked?: boolean;
  /** Complete IP access list. Omission preserves it; an empty list clears it. */
  ipAccessList?: Services.IpAccessListEntry[];
  /** Complete service associations to existing private endpoint IDs. Omission preserves them. */
  privateEndpointIds?: string[];
  /** Whether the MySQL endpoint is enabled. Omission preserves it. */
  mysqlEndpoint?: boolean;
  /** Warehouse-wide release channel. Omission preserves it. */
  releaseChannel?: Services.CreateInstanceRequestReleaseChannel;
  /** Core-dump setting, subject to the organization's policy. Omission preserves it. */
  enableCoreDumps?: boolean;
  /** Complete user tags; up to 45 in addition to Alchemy's five reserved tags. Undefined values create valueless tags. */
  tags?: Record<string, string | undefined>;
  /** Desired running or stopped state. Omission preserves the current state. */
  state?: "running" | "stopped";
}

export interface ServiceAttributes {
  /** Organization that owns the service. */
  organizationId: string;
  /** Stable service UUID. */
  serviceId: string;
  /** Latest service details, excluding its customer-provided encryption key. */
  service: Omit<Services.Service, "encryptionKey">;
  /** Password revealed at creation, retained in state as a secret; unavailable on adoption. */
  password: Redacted.Redacted<string> | undefined;
  /** Acknowledged creation intent; adoption records declared intent, not a verified restore origin. */
  creationIntent: ServiceCreationIntent | undefined;
}

/** Create-only settings acknowledged by the last successful reconcile. */
export interface ServiceCreationIntent {
  /** Organization in which the service was created or adopted. */
  organizationId: string;
  /** Cloud provider. */
  provider: ServiceProps["provider"];
  /** Cloud region. */
  region: string;
  /** Legacy tier, if declared. */
  tier: ServiceProps["tier"];
  /** Source warehouse, if declared. */
  dataWarehouseId: string | undefined;
  /** Read-only creation mode, if declared. */
  isReadonly: boolean | undefined;
  /** Restore source, if declared. */
  backupId: string | undefined;
  /** Hash of the restore encryption configuration, if declared. */
  backupEncryptionConfigHash: string | undefined;
  /** Transparent encryption setting, if declared. */
  hasTransparentDataEncryption: boolean | undefined;
  /** Regulatory compliance mode, if declared. */
  complianceType: ServiceProps["complianceType"];
}

export interface ServiceLookup {
  /** Organization containing the service. */
  organizationId: string;
  /** Exact service UUID. */
  serviceId: string;
}

export interface Service extends Resource<
  "ClickHouse.Service",
  ServiceProps,
  ServiceAttributes,
  never,
  Providers,
  ServiceLookup
> {}

/**
 * A ClickHouse Cloud service in a new or existing warehouse.
 *
 * ### Creating a service
 * **Example:** Create a service with one fixed replica
 * ```typescript
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 *
 * const db = yield* ClickHouse.Service("Analytics", {
 *   organizationId: "00000000-0000-4000-8000-000000000001",
 *   provider: "aws",
 *   region: "us-east-1",
 *   minReplicaMemoryGb: 8,
 *   maxReplicaMemoryGb: 8,
 *   numReplicas: 1,
 *   ipAccessList: [],
 * });
 * ```
 * The generated password is returned only once and stored as `Redacted` in
 * state. ClickHouse cannot reveal it again. Password reset is a separate action.
 *
 * ### Memory and replica sizing
 * **Example:** Bound memory while keeping two replicas
 * ```typescript
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 *
 * const db = yield* ClickHouse.Service("Analytics", {
 *   organizationId: "00000000-0000-4000-8000-000000000001",
 *   provider: "gcp",
 *   region: "us-east1",
 *   autoscalingMode: "vertical",
 *   minReplicaMemoryGb: 8,
 *   maxReplicaMemoryGb: 16,
 *   numReplicas: 2,
 * });
 * ```
 * Memory is per replica, not a warehouse total. A vertical replica range must
 * have equal minimum and maximum values. Do not mix `numReplicas` with a range
 * or legacy total-memory fields with per-replica settings.
 *
 * **Example:** Horizontal autoscaling on an entitled organization
 * ```typescript
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 *
 * const db = yield* ClickHouse.Service("Analytics", {
 *   organizationId: "00000000-0000-4000-8000-000000000001",
 *   provider: "aws",
 *   region: "us-east-1",
 *   autoscalingMode: "horizontal",
 *   minReplicaMemoryGb: 8,
 *   maxReplicaMemoryGb: 8,
 *   minReplicas: 2,
 *   maxReplicas: 4,
 * });
 * ```
 * Horizontal scaling needs equal memory bounds and the organization's
 * entitlement. This example does not enable the feature or change your plan.
 *
 * ### Idle scaling
 * **Example:** Suspend idle compute after ten minutes
 * ```typescript
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 *
 * const db = yield* ClickHouse.Service("Analytics", {
 *   organizationId: "00000000-0000-4000-8000-000000000001",
 *   provider: "azure",
 *   region: "eastus2",
 *   minReplicaMemoryGb: 8,
 *   maxReplicaMemoryGb: 8,
 *   numReplicas: 2,
 *   idleScaling: true,
 *   idleTimeoutMinutes: 10,
 * });
 * ```
 * Idle services wake for traffic. Explicit `state: "stopped"` is a separate
 * stop operation; set `state: "running"` to start one again. Shutdown can
 * outlast a bounded wait; keep state and retry destroy after it completes.
 *
 * ### Network access and tags
 * **Example:** Allow one application egress address
 * ```typescript
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 *
 * const db = yield* ClickHouse.Service("Analytics", {
 *   organizationId: "00000000-0000-4000-8000-000000000001",
 *   provider: "aws",
 *   region: "us-east-1",
 *   minReplicaMemoryGb: 8,
 *   maxReplicaMemoryGb: 8,
 *   numReplicas: 1,
 *   ipAccessList: [{ source: "192.0.2.10/32", description: "Replace with application egress IP" }],
 *   tags: { application: "analytics", environment: "production" },
 * });
 * ```
 * Replace the documentation address before connecting. An empty access list
 * permits no public SQL clients. Five reserved ownership tags count toward
 * ClickHouse's 50-tag limit. Runtime SQL access uses separately granted users;
 * see `SQL.Connect`, not the Cloud API key.
 *
 * ### Look up an existing service
 * **Example:** Reference without adoption or deletion
 * ```typescript
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 *
 * const db = ClickHouse.Service.lookup({
 *   organizationId: "00000000-0000-4000-8000-000000000001",
 *   serviceId: "00000000-0000-4000-8000-000000000002",
 * });
 * ```
 * Lookup reads attributes without managing the service. Use `SQL.Database`
 * with migrations to manage schemas inside it and `SQL.Connect` to connect
 * an application host with its own SQL identity.
 *
 * ### Ownership and recovery
 * Adopt an existing service only by its exact UUID with `Alchemy.AdoptPolicy.adopt(true)`.
 * A matching name is never ownership evidence. Declared create-only settings
 * must match the observed service; omit the backup source because ClickHouse
 * does not expose its restore origin. Adoption records the declared intent,
 * not a verified history. Interrupted creates recover by stack, stage, full
 * logical ID, generation and creation-intent tags, never by display name.
 * A lost creation response loses the one-time password; recovery never resets
 * it. Discard a captured plan after a failed apply.
 *
 * ### Updates and replacement
 * Omitted mutable settings remain unchanged. Empty managed IP, private-endpoint
 * and tag sets clear their respective sets. Private endpoint IDs refer to
 * customer network endpoints already available to the service; this resource
 * neither provisions those endpoints nor deletes shared registrations.
 * Changing organization, provider, region, warehouse, restore source,
 * encryption, compliance, or hardware generation replaces the service and
 * may destroy data. Name changes keep the UUID. Horizontal scaling,
 * Gen 2 and encryption require the corresponding ClickHouse entitlement.
 * BYOC services are outside this provider's scope and cannot be adopted.
 * Core-dump policy on the organization remains authoritative.
 * Replacements use the declared settings and Cloud defaults, rather than
 * copying omitted settings from the old service. They create first; a matching
 * name blocks creation without deleting the old service.
 * Back up data before replacing a service.
 *
 * @resource
 * @product Services
 */
export const Service = Resource<Service>("ClickHouse.Service");

export class ServiceSafetyError extends Data.TaggedError("ServiceSafetyError")<{
  message: string;
  reason:
    | "InvalidConfiguration"
    | "InvalidResponse"
    | "IdentityMismatch"
    | "NotReady"
    | "AmbiguousOwnership";
  serviceId?: string;
  state?: string;
}> {}

const fail = (message: string, reason: ServiceSafetyError["reason"] = "InvalidConfiguration") =>
  Effect.fail(new ServiceSafetyError({ message, reason }));

const observe = (organizationId: string, serviceId: string) =>
  Services.getInstance({ organizationId, serviceId }).pipe(
    Effect.flatMap(({ result }) =>
      result?.id === serviceId
        ? Effect.succeed(result)
        : fail("ClickHouse returned a service without the requested UUID.", "InvalidResponse"),
    ),
    Effect.catchTag("ServiceNotFound", () => Effect.undefined),
  );

const list = Effect.fn(function* (organizationId: string) {
  const { result } = yield* Services.instanceGetList({ organizationId });
  if (!result) return yield* fail("ClickHouse omitted the service list.", "InvalidResponse");
  return result;
});

const ownershipTags = Effect.fn(function* (
  fqn: string,
  instanceId: string,
  intent: ServiceCreationIntent,
) {
  const internal = yield* createInternalTags(fqn);
  const tags = Object.fromEntries(
    yield* Effect.forEach(Object.entries(internal), ([key, value]) =>
      Effect.map(
        sha256(JSON.stringify(value)),
        (hash) => [key.replaceAll("::", "."), hash] as const,
      ),
    ),
  );
  return {
    ...tags,
    "alchemy.instance": yield* sha256(instanceId),
    "alchemy.intent": yield* sha256Object(intent),
  };
});

const ownedService = Effect.fn(function* (organizationId: string, tags: Record<string, string>) {
  const matches = (yield* list(organizationId)).filter((service) => {
    const observed = Object.fromEntries((service.tags ?? []).map(({ key, value }) => [key, value]));
    return Object.entries(tags).every(([key, value]) => observed[key] === value);
  });
  if (matches.length > 1) {
    return yield* fail(
      "Multiple services carry this resource's ownership and generation tags.",
      "AmbiguousOwnership",
    );
  }
  return matches[0];
});

const attrs = Effect.fn(function* (
  organizationId: string,
  service: Services.Service,
  password?: Redacted.Redacted<string>,
  creationIntent?: ServiceCreationIntent,
) {
  if (!service.id) return yield* fail("ClickHouse omitted the service UUID.", "InvalidResponse");
  const { encryptionKey: _encryptionKey, ...safeService } = service;
  return {
    organizationId,
    serviceId: service.id,
    service: safeService,
    password,
    creationIntent,
  };
});

const creationIntent = Effect.fn(function* (props: ServiceProps) {
  return {
    organizationId: props.organizationId,
    provider: props.provider,
    region: props.region,
    tier: props.tier,
    dataWarehouseId: props.dataWarehouseId,
    isReadonly: props.isReadonly,
    backupId: props.backupId,
    backupEncryptionConfigHash: props.backupEncryptionConfig
      ? yield* sha256Object(unwrapRedacted(props.backupEncryptionConfig))
      : undefined,
    hasTransparentDataEncryption: props.hasTransparentDataEncryption,
    complianceType: props.complianceType,
  } satisfies ServiceCreationIntent;
});

const validate = Effect.fn(function* (props: ServiceProps) {
  if (props.isReadonly && !props.dataWarehouseId) {
    return yield* fail("A read-only service requires dataWarehouseId.");
  }
  if (props.backupEncryptionConfig && !props.backupId) {
    return yield* fail("backupEncryptionConfig requires backupId.");
  }
  if ((props.minReplicas === undefined) !== (props.maxReplicas === undefined)) {
    return yield* fail("minReplicas and maxReplicas must be supplied together.");
  }
  if (props.numReplicas !== undefined && props.minReplicas !== undefined) {
    return yield* fail("numReplicas cannot be combined with a replica count range.");
  }
  if (
    (props.minTotalMemoryGb !== undefined || props.maxTotalMemoryGb !== undefined) &&
    (props.minReplicaMemoryGb !== undefined ||
      props.maxReplicaMemoryGb !== undefined ||
      props.autoscalingMode !== undefined ||
      props.minReplicas !== undefined)
  ) {
    return yield* fail("Legacy total memory cannot be combined with replica scaling settings.");
  }
  if (props.autoscalingMode === "horizontal" && props.numReplicas !== undefined) {
    return yield* fail("Horizontal autoscaling cannot use numReplicas.");
  }
  if (
    props.autoscalingMode === "horizontal" &&
    props.minReplicaMemoryGb !== undefined &&
    props.maxReplicaMemoryGb !== undefined &&
    props.minReplicaMemoryGb !== props.maxReplicaMemoryGb
  ) {
    return yield* fail("Horizontal autoscaling requires equal replica memory bounds.");
  }
  if (
    props.autoscalingMode === "vertical" &&
    props.minReplicas !== undefined &&
    props.minReplicas !== props.maxReplicas
  ) {
    return yield* fail("Vertical autoscaling requires an equal replica count range.");
  }
});

const same = (left: unknown, right: unknown) => stableStringify(left) === stableStringify(right);
const keyOfIp = (entry: Services.IpAccessListEntry) =>
  JSON.stringify([entry.source, entry.description]);
const delta = <T>(actual: T[], desired: T[], key: (entry: T) => string) => {
  const old = new Set(actual.map(key));
  const next = new Set(desired.map(key));
  return {
    add: desired.filter((item) => !old.has(key(item))),
    remove: actual.filter((item) => !next.has(key(item))),
  };
};

const desiredTags = Effect.fn(function* (
  reserved: Record<string, string>,
  props: ServiceProps,
  observed?: Services.ResourceTagsV1[],
) {
  const old = Object.fromEntries(
    (observed ?? []).map(({ key, value }) => [key, JSON.stringify(value ?? null)]),
  );
  const user: Record<string, string | undefined> =
    props.tags === undefined
      ? Object.fromEntries(
          (observed ?? [])
            .filter(({ key }) => !(key in reserved))
            .map(({ key, value }) => [key, value]),
        )
      : props.tags;
  for (const [key, value] of Object.entries(user)) {
    if (
      !/^[a-zA-Z0-9._-]{1,128}$/.test(key) ||
      (value !== undefined && !/^[a-zA-Z0-9._-]{1,256}$/.test(value))
    ) {
      return yield* fail(
        "ClickHouse tag keys and values must use letters, digits, dots, underscores or hyphens.",
      );
    }
  }
  const desired = { ...user, ...reserved };
  if (Object.keys(desired).length > 50)
    return yield* fail("ClickHouse allows at most 50 tags, including Alchemy's reserved tags.");
  const next = Object.fromEntries(
    Object.entries(desired).map(([key, value]) => [key, JSON.stringify(value ?? null)]),
  );
  const changes = diffTags(old, next);
  const remove = [...new Set([...changes.removed, ...changes.updated.map(({ Key }) => Key)])].map(
    (key) => ({ key, value: observed?.find((tag) => tag.key === key)?.value }),
  );
  const add = changes.upsert.map(({ Key }) => ({ key: Key, value: desired[Key] }));
  return { desired, add, remove };
});

const changed = Effect.fn(function* (
  reserved: Record<string, string>,
  service: Services.Service,
  props: ServiceProps,
) {
  const tags = yield* desiredTags(reserved, props, service.tags);
  if (props.tags !== undefined && service.tags === undefined)
    return yield* fail("ClickHouse omitted the complete tag set.", "InvalidResponse");
  if (props.ipAccessList !== undefined && service.ipAccessList === undefined)
    return yield* fail("ClickHouse omitted the complete IP access list.", "InvalidResponse");
  if (props.privateEndpointIds !== undefined && service.privateEndpointIds === undefined)
    return yield* fail(
      "ClickHouse omitted the complete private endpoint associations.",
      "InvalidResponse",
    );
  const ip =
    props.ipAccessList === undefined
      ? undefined
      : delta(service.ipAccessList!, props.ipAccessList, keyOfIp);
  const endpoints =
    props.privateEndpointIds === undefined
      ? undefined
      : delta(service.privateEndpointIds!, props.privateEndpointIds, (x) => x);
  if (
    endpoints?.add.length &&
    (!service.availablePrivateEndpointIds ||
      endpoints.add.some((id) => !service.availablePrivateEndpointIds!.includes(id)))
  ) {
    return yield* fail(
      "A private endpoint is not listed as available; register it separately before association.",
    );
  }
  if (props.mysqlEndpoint !== undefined && service.endpoints === undefined)
    return yield* fail("ClickHouse omitted the service endpoints.", "InvalidResponse");
  const mysqlEnabled =
    service.endpoints?.some((endpoint) => endpoint.protocol === "mysql") ?? false;
  const basic: Services.UpdateInstanceRequest = {
    organizationId: props.organizationId,
    serviceId: service.id!,
    ...(props.name !== undefined && props.name !== service.name ? { name: props.name } : {}),
    ...(props.releaseChannel !== undefined && props.releaseChannel !== service.releaseChannel
      ? { releaseChannel: props.releaseChannel }
      : {}),
    ...(props.enableCoreDumps !== undefined && props.enableCoreDumps !== service.enableCoreDumps
      ? { enableCoreDumps: props.enableCoreDumps }
      : {}),
    ...(props.mysqlEndpoint !== undefined && props.mysqlEndpoint !== mysqlEnabled
      ? { endpoints: [{ protocol: "mysql", enabled: props.mysqlEndpoint }] }
      : {}),
    ...(ip && (ip.add.length || ip.remove.length) ? { ipAccessList: ip } : {}),
    ...(endpoints && (endpoints.add.length || endpoints.remove.length)
      ? { privateEndpointIds: endpoints }
      : {}),
    ...(service.tags !== undefined && (tags.add.length || tags.remove.length)
      ? { tags: { add: tags.add, remove: tags.remove } }
      : {}),
  };
  return basic;
});

const scaling = (service: Services.Service, props: ServiceProps) => {
  const bandChanged =
    props.minReplicas !== undefined &&
    (props.minReplicas !== (service.minReplicas ?? service.numReplicas) ||
      props.maxReplicas !== (service.maxReplicas ?? service.numReplicas));
  return {
    ...(props.autoscalingMode !== undefined && props.autoscalingMode !== service.autoscalingMode
      ? { autoscalingMode: props.autoscalingMode }
      : {}),
    ...(props.minReplicaMemoryGb !== undefined &&
    props.minReplicaMemoryGb !== service.minReplicaMemoryGb
      ? { minReplicaMemoryGb: props.minReplicaMemoryGb }
      : {}),
    ...(props.maxReplicaMemoryGb !== undefined &&
    props.maxReplicaMemoryGb !== service.maxReplicaMemoryGb
      ? { maxReplicaMemoryGb: props.maxReplicaMemoryGb }
      : {}),
    ...(props.numReplicas !== undefined && props.numReplicas !== service.numReplicas
      ? { numReplicas: props.numReplicas }
      : {}),
    ...(bandChanged ? { minReplicas: props.minReplicas, maxReplicas: props.maxReplicas } : {}),
    ...(props.idleScaling !== undefined && props.idleScaling !== service.idleScaling
      ? { idleScaling: props.idleScaling }
      : {}),
    ...(props.idleTimeoutMinutes !== undefined &&
    props.idleTimeoutMinutes !== service.idleTimeoutMinutes
      ? { idleTimeoutMinutes: props.idleTimeoutMinutes }
      : {}),
    ...(props.profile !== undefined && props.profile !== service.profile
      ? { profile: props.profile }
      : {}),
  };
};

const validateScaling = Effect.fn(function* (props: ServiceProps, observed?: Services.Service) {
  if (observed?.byocId) {
    return yield* fail("BYOC services are outside this provider's scope.");
  }
  const profile = props.profile ?? observed?.profile;
  const minMemory = props.minReplicaMemoryGb ?? observed?.minReplicaMemoryGb;
  const maxMemory = props.maxReplicaMemoryGb ?? observed?.maxReplicaMemoryGb;
  const mode = props.autoscalingMode ?? observed?.autoscalingMode;
  if (mode === "horizontal" && props.numReplicas !== undefined) {
    return yield* fail("Horizontal autoscaling cannot use numReplicas.");
  }
  if (
    mode !== "horizontal" &&
    props.minReplicas !== undefined &&
    props.minReplicas !== props.maxReplicas
  ) {
    return yield* fail("Vertical autoscaling requires an equal replica count range.");
  }
  if (
    mode === "horizontal" &&
    (props.minReplicaMemoryGb !== undefined ||
      props.maxReplicaMemoryGb !== undefined ||
      props.autoscalingMode === "horizontal") &&
    (minMemory === undefined || maxMemory === undefined || minMemory !== maxMemory)
  ) {
    return yield* fail("Horizontal autoscaling requires known, equal replica memory bounds.");
  }
  if (
    profile === "v2-default" &&
    (props.provider !== "aws" ||
      (props.idleScaling ?? observed?.idleScaling) !== false ||
      minMemory === undefined ||
      minMemory !== maxMemory ||
      (!observed &&
        props.numReplicas === undefined &&
        (props.minReplicas === undefined || props.minReplicas !== props.maxReplicas)))
  ) {
    return yield* fail(
      "Gen 2 requires AWS, idleScaling false, fixed replica memory and a fixed replica count.",
    );
  }
});

const waitReady = Effect.fn(function* (
  organizationId: string,
  serviceId: string,
  wanted?: "running" | "stopped",
) {
  const observed = yield* observe(organizationId, serviceId).pipe(
    Effect.repeat({
      schedule: Schedule.spaced("10 seconds"),
      times: 30,
      until: (service) =>
        !!service &&
        (wanted === "stopped"
          ? service.state === "stopped"
          : service.state === "running" || service.state === "idle"),
    }),
  );
  if (
    !observed ||
    (wanted === "stopped"
      ? observed.state !== "stopped"
      : observed.state !== "running" && observed.state !== "idle")
  ) {
    return yield* new ServiceSafetyError({
      reason: "NotReady",
      message: `Service ${serviceId} did not become ready within five minutes; latest state: ${observed?.state ?? "missing"}. A fresh deploy resumes this generation.`,
      serviceId,
      state: observed?.state,
    });
  }
  return observed;
});

export const ServiceProvider = () =>
  Provider.succeed(Service, {
    stables: ["organizationId", "serviceId", "password"],
    list: Effect.fn(function* () {
      const organizations = yield* Organizations.organizationGetList
        .items({})
        .pipe(Stream.runCollect);
      const services = yield* Effect.forEach(organizations, (organization) =>
        Effect.gen(function* () {
          if (!organization.id)
            return yield* fail("ClickHouse omitted an organization UUID.", "InvalidResponse");
          const observed = yield* list(organization.id);
          const owned = observed.filter((service) => {
            const tags = Object.fromEntries(
              (service.tags ?? []).map(({ key, value }) => [key, value]),
            );
            return [
              "alchemy.stack",
              "alchemy.stage",
              "alchemy.id",
              "alchemy.instance",
              "alchemy.intent",
            ].every((key) => /^[a-f0-9]{64}$/.test(tags[key] ?? ""));
          });
          return yield* Effect.forEach(owned, (service) => attrs(organization.id!, service));
        }),
      );
      return services.flat();
    }),
    diff: Effect.fn(function* ({ olds, news, output }) {
      if (!isResolved(news)) return;
      yield* validate(news);
      const prior = output?.creationIntent ?? (olds ? yield* creationIntent(olds) : undefined);
      const intent = yield* creationIntent(news);
      const currentProfile = output?.service.profile ?? olds?.profile;
      if (
        (news.serviceId !== undefined &&
          news.serviceId !== (output?.serviceId ?? olds?.serviceId)) ||
        (prior !== undefined && !same(intent, prior)) ||
        (news.profile !== undefined &&
          currentProfile !== undefined &&
          (news.profile === "v2-default") !== (currentProfile === "v2-default"))
      ) {
        if (news.serviceId !== undefined && news.serviceId === output?.serviceId) {
          return yield* fail(
            "A replacement cannot reuse the same service UUID. Omit serviceId to create a new service.",
            "IdentityMismatch",
          );
        }
        return { action: "replace" };
      }
    }),
    lookup: Effect.fn(function* ({ organizationId, serviceId }) {
      const service = yield* observe(organizationId, serviceId);
      return service ? yield* attrs(organizationId, service) : undefined;
    }),
    read: Effect.fn(function* ({ id, fqn, instanceId, olds, output, previous }) {
      const organizationId = output?.organizationId ?? olds.organizationId;
      const serviceId = output?.serviceId ?? olds.serviceId;
      const intent = yield* creationIntent(olds);
      const reserved = yield* ownershipTags(fqn, instanceId, intent);
      const service = serviceId
        ? yield* observe(organizationId, serviceId)
        : yield* ownedService(organizationId, reserved);
      if (!service && !serviceId) {
        const name = olds.name ?? (yield* createPhysicalName({ id, maxLength: 50 }));
        if ((yield* list(organizationId)).some((service) => service.name === name)) {
          return yield* new OwnedBySomeoneElse({
            message: "A matching name is not ownership evidence. Adopt by service UUID.",
            resourceType: Service.Type,
            logicalId: id,
            physicalName: name,
          });
        }
      }
      if (!service) return undefined;
      const acknowledged =
        output ??
        (previous?.organizationId === organizationId && previous.serviceId === serviceId
          ? previous
          : undefined);
      const result = yield* attrs(
        organizationId,
        service,
        acknowledged?.password,
        acknowledged?.creationIntent ?? (!serviceId ? intent : undefined),
      );
      const observed = Object.fromEntries(
        (service.tags ?? []).map(({ key, value }) => [key, value]),
      );
      const owned = Object.entries(reserved).every(([key, value]) => observed[key] === value);
      return acknowledged || owned ? result : Unowned(result);
    }),
    reconcile: Effect.fn(function* ({ id, fqn, instanceId, news, olds, output }) {
      yield* validate(news);
      if (
        output &&
        (output.organizationId !== news.organizationId ||
          (news.serviceId !== undefined && news.serviceId !== output.serviceId))
      )
        return yield* fail("Service identity changed without replacement.", "IdentityMismatch");
      const intent = yield* creationIntent(news);
      const reserved = yield* ownershipTags(fqn, instanceId, intent);
      if (output?.creationIntent && !same(intent, output.creationIntent)) {
        return yield* fail(
          "Service create-only settings changed without replacement.",
          "IdentityMismatch",
        );
      }
      const serviceId = output?.serviceId ?? news.serviceId;
      let service = serviceId
        ? yield* observe(news.organizationId, serviceId)
        : yield* ownedService(news.organizationId, reserved);
      let password = output?.password;
      if (service && (service.provider !== news.provider || service.region !== news.region)) {
        return yield* fail(
          "The selected service's provider or region does not match the requested identity.",
          "IdentityMismatch",
        );
      }
      if (
        service &&
        output &&
        !olds &&
        !output.creationIntent &&
        ((news.dataWarehouseId !== undefined && news.dataWarehouseId !== service.dataWarehouseId) ||
          (news.isReadonly !== undefined && news.isReadonly !== service.isReadonly) ||
          (news.tier !== undefined && news.tier !== service.tier) ||
          (news.hasTransparentDataEncryption !== undefined &&
            news.hasTransparentDataEncryption !== service.hasTransparentDataEncryption) ||
          (news.complianceType !== undefined && news.complianceType !== service.complianceType) ||
          news.backupId !== undefined ||
          news.backupEncryptionConfig !== undefined)
      ) {
        return yield* fail(
          "The selected service does not match the requested create-only settings, or its restore origin cannot be verified.",
        );
      }
      if (service && output && news.profile !== undefined) {
        const currentProfile = service.profile ?? output.service.profile;
        if (currentProfile === undefined) {
          return yield* fail(
            "ClickHouse omitted the current profile; its generation cannot be verified.",
            "InvalidResponse",
          );
        }
        if ((news.profile === "v2-default") !== (currentProfile === "v2-default")) {
          return yield* fail(
            "Service hardware generation changed without replacement.",
            "IdentityMismatch",
          );
        }
      }
      yield* validateScaling(news, service);
      if (!service) {
        if (news.serviceId)
          return yield* fail("The selected service no longer exists.", "IdentityMismatch");
        const name = news.name ?? (yield* createPhysicalName({ id, maxLength: 50 }));
        if ((yield* list(news.organizationId)).some((candidate) => candidate.name === name))
          return yield* new OwnedBySomeoneElse({
            message: "A matching name is not ownership evidence. Adopt by service UUID.",
            resourceType: Service.Type,
            logicalId: id,
            physicalName: name,
          });
        const tags = yield* desiredTags(reserved, news);
        const response = yield* Services.createInstance({
          organizationId: news.organizationId,
          provider: news.provider,
          region: news.region,
          name,
          ipAccessList: news.ipAccessList ?? [],
          tier: news.tier,
          minTotalMemoryGb: news.minTotalMemoryGb,
          maxTotalMemoryGb: news.maxTotalMemoryGb,
          autoscalingMode: news.autoscalingMode,
          minReplicaMemoryGb: news.minReplicaMemoryGb,
          maxReplicaMemoryGb: news.maxReplicaMemoryGb,
          numReplicas: news.numReplicas,
          minReplicas: news.minReplicas,
          maxReplicas: news.maxReplicas,
          idleScaling: news.idleScaling,
          idleTimeoutMinutes: news.idleTimeoutMinutes,
          dataWarehouseId: news.dataWarehouseId,
          isReadonly: news.isReadonly,
          backupId: news.backupId,
          backupEncryptionConfig: news.backupEncryptionConfig,
          profile: news.profile,
          hasTransparentDataEncryption: news.hasTransparentDataEncryption,
          complianceType: news.complianceType,
          privatePreviewTermsChecked: news.privatePreviewTermsChecked,
          releaseChannel: news.releaseChannel,
          enableCoreDumps: news.enableCoreDumps,
          endpoints:
            news.mysqlEndpoint === undefined
              ? undefined
              : [{ protocol: "mysql", enabled: news.mysqlEndpoint }],
          tags: Object.entries(tags.desired).map(([key, value]) => ({ key, value })),
        }).pipe(Retry.none);
        if (!response.result?.service?.id)
          return yield* fail(
            "Service creation returned no UUID. Inspect the organization before retrying.",
            "InvalidResponse",
          );
        const createdServiceId = response.result.service.id;
        service = response.result.service;
        const revealed = response.result.password;
        password =
          revealed === undefined
            ? undefined
            : Redacted.isRedacted(revealed)
              ? revealed
              : Redacted.make(revealed);
        service = yield* waitReady(news.organizationId, createdServiceId);
      }
      if (service.state !== "running" && service.state !== "idle" && service.state !== "stopped") {
        service = yield* waitReady(news.organizationId, service.id!, news.state);
      }
      const basic = yield* changed(reserved, service, news);
      if (Object.keys(basic).length > 2) {
        yield* Services.updateInstance(basic).pipe(Retry.none);
        service = yield* waitReady(
          news.organizationId,
          service.id!,
          service.state === "stopped" ? "stopped" : undefined,
        );
      }
      const legacy = news.minTotalMemoryGb !== undefined || news.maxTotalMemoryGb !== undefined;
      if (
        legacy &&
        ((news.minTotalMemoryGb !== undefined &&
          news.minTotalMemoryGb !== service.minTotalMemoryGb) ||
          (news.maxTotalMemoryGb !== undefined &&
            news.maxTotalMemoryGb !== service.maxTotalMemoryGb))
      ) {
        yield* Services.updateInstanceScaling({
          organizationId: news.organizationId,
          serviceId: service.id!,
          minTotalMemoryGb: news.minTotalMemoryGb,
          maxTotalMemoryGb: news.maxTotalMemoryGb,
        }).pipe(Retry.none);
        service = yield* waitReady(
          news.organizationId,
          service.id!,
          service.state === "stopped" ? "stopped" : undefined,
        );
      }
      let changes = scaling(service, news);
      if (
        changes.profile &&
        (changes.minReplicaMemoryGb !== undefined || changes.maxReplicaMemoryGb !== undefined)
      ) {
        yield* Services.updateInstanceReplicaScaling({
          organizationId: news.organizationId,
          serviceId: service.id!,
          profile: changes.profile,
        }).pipe(Retry.none);
        service = yield* waitReady(
          news.organizationId,
          service.id!,
          service.state === "stopped" ? "stopped" : undefined,
        );
        const { profile: _profile, ...remaining } = scaling(service, news);
        changes = remaining;
      }
      if (Object.keys(changes).length) {
        yield* Services.updateInstanceReplicaScaling({
          organizationId: news.organizationId,
          serviceId: service.id!,
          ...changes,
        }).pipe(Retry.none);
        service = yield* waitReady(
          news.organizationId,
          service.id!,
          service.state === "stopped" ? "stopped" : undefined,
        );
      }
      if (
        (news.state === "stopped" && service.state !== "stopped") ||
        (news.state === "running" && service.state === "stopped")
      ) {
        yield* Services.updateInstanceState({
          organizationId: news.organizationId,
          serviceId: service.id!,
          command: news.state === "stopped" ? "stop" : "start",
        }).pipe(Retry.none);
        service = yield* waitReady(news.organizationId, service.id!, news.state);
      }
      return yield* attrs(news.organizationId, service, password, intent);
    }),
    delete: Effect.fn(function* ({ output }) {
      const service = yield* observe(output.organizationId, output.serviceId);
      if (!service) return;
      if (service.state !== "stopped") {
        if (service.state !== "stopping") {
          yield* Services.updateInstanceState({
            organizationId: output.organizationId,
            serviceId: output.serviceId,
            command: "stop",
          }).pipe(Retry.none);
        }
        const stopped = yield* observe(output.organizationId, output.serviceId).pipe(
          Effect.repeat({
            schedule: Schedule.spaced("10 seconds"),
            times: 30,
            until: (current) => !current || current.state === "stopped",
          }),
        );
        if (!stopped) return;
        if (stopped.state !== "stopped")
          return yield* new Provider.DeleteInProgress({
            message: `Service ${output.serviceId} is still stopping. Retry destroy after shutdown completes.`,
          });
      }
      yield* Services.deleteInstance({
        organizationId: output.organizationId,
        serviceId: output.serviceId,
      }).pipe(Retry.none);
      const remaining = yield* observe(output.organizationId, output.serviceId).pipe(
        Effect.repeat({
          schedule: Schedule.spaced("10 seconds"),
          times: 30,
          until: (current) => !current,
        }),
      );
      if (remaining)
        return yield* new Provider.DeleteInProgress({
          message: `Service ${output.serviceId} is still being deleted. Retry destroy to confirm completion.`,
        });
    }),
  });
