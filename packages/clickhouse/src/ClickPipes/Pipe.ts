import * as ClickPipes from "@distilled.cloud/clickhouse/click_pipes";
import * as Retry from "@distilled.cloud/clickhouse/Retry";
import { OwnedBySomeoneElse, Unowned } from "alchemy/AdoptPolicy";
import { isResolved } from "alchemy/Diff";
import { createPhysicalName } from "alchemy/PhysicalName";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import { sha256 } from "alchemy/Util";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import type { Providers } from "../Providers.ts";
import { waitForObserved } from "./internal.ts";

type Secret = string | Redacted.Redacted<string>;
type PasswordAuth<Method extends "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"> = {
  authentication: Method;
  credentials: { username: string; password: Secret };
};
type MutualTls = {
  authentication: "MUTUAL_TLS";
  credentials: { certificate: string; privateKey: Secret };
};
type WorkloadIdentity = { authentication: "SERVICE_ACCOUNT_WORKLOAD_IDENTITY" };
type KafkaAuth =
  | ({ type: "kafka" } & (PasswordAuth<"PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"> | MutualTls))
  | ({ type: "msk" } & (
      | PasswordAuth<"SCRAM-SHA-512">
      | MutualTls
      | { authentication: "IAM_ROLE"; iamRole: string }
      | { authentication: "IAM_USER"; credentials: { accessKeyId: Secret; secretKey: Secret } }
    ))
  | ({ type: "gcmk" } & (PasswordAuth<"PLAIN"> | MutualTls | WorkloadIdentity))
  | ({ type: "confluent" } & (PasswordAuth<"PLAIN"> | MutualTls))
  | ({ type: "warpstream" } & PasswordAuth<"PLAIN">)
  | ({ type: "redpanda" } & (PasswordAuth<"SCRAM-SHA-256" | "SCRAM-SHA-512"> | MutualTls))
  | ({ type: "dokafka" } & (PasswordAuth<"SCRAM-SHA-256"> | MutualTls))
  | { type: "azureeventhub"; authentication: "PLAIN"; credentials: { connectionString: Secret } };
type StorageAuth =
  | { authentication?: undefined }
  | { authentication: "IAM_ROLE"; iamRole: string }
  | { authentication: "IAM_USER"; accessKey: { accessKeyId: Secret; secretKey: Secret } }
  | { authentication: "CONNECTION_STRING"; connectionString: Secret }
  | { authentication: "SERVICE_ACCOUNT"; serviceAccountKey: Secret }
  | { authentication: "SERVICE_ACCOUNT_WORKLOAD_IDENTITY" };
type DatabaseAuth =
  | { authentication: "basic"; credentials: { username: string; password: Secret } }
  | { authentication: "IAM_ROLE"; iamRole: string }
  | { authentication: "SERVICE_ACCOUNT_WORKLOAD_IDENTITY" };

/** A source family and its authentication configuration. */
export type PipeSource =
  | {
      kind: "kafka";
      config: Omit<
        ClickPipes.ClickPipePostKafkaSource,
        "type" | "authentication" | "credentials" | "iamRole"
      > &
        KafkaAuth;
    }
  | {
      kind: "objectStorage";
      config: Omit<
        ClickPipes.ClickPipePostObjectStorageSource,
        "authentication" | "iamRole" | "accessKey" | "connectionString" | "serviceAccountKey"
      > &
        StorageAuth;
    }
  | {
      kind: "kinesis";
      config: Omit<
        ClickPipes.ClickPipePostKinesisSource,
        "authentication" | "iamRole" | "accessKey"
      > &
        (
          | { authentication: "IAM_ROLE"; iamRole: string }
          | { authentication: "IAM_USER"; accessKey: { accessKeyId: Secret; secretKey: Secret } }
        );
    }
  | { kind: "pubsub"; config: ClickPipes.ClickPipePostPubSubSource }
  | {
      kind: "postgres";
      config: Omit<
        ClickPipes.ClickPipeMutatePostgresSource,
        "authentication" | "credentials" | "iamRole"
      > &
        DatabaseAuth;
    }
  | {
      kind: "mysql";
      config: Omit<
        ClickPipes.ClickPipeMutateMySQLSource,
        "authentication" | "credentials" | "iamRole"
      > &
        DatabaseAuth;
    }
  | { kind: "mongodb"; config: ClickPipes.ClickPipeMutateMongoDBSource }
  | {
      kind: "bigquery";
      config:
        | (Omit<ClickPipes.ClickPipePostBigQueryServiceAccountSource, "authentication"> & {
            authentication: "SERVICE_ACCOUNT";
          })
        | ClickPipes.ClickPipePostBigQueryWorkloadIdentitySource;
    };

export interface PipeProps {
  /** Organization that owns the service. */
  organizationId: string;
  /** Service that owns the pipe. */
  serviceId: string;
  /** Existing pipe UUID, for explicit adoption with Alchemy.adopt(true). */
  clickPipeId?: string;
  /** Pipe name. Omission generates a physical name on creation. */
  name?: string;
  /** Source family and authentication. */
  source: PipeSource;
  /** Destination configuration. Columns update the pipe, not the table schema. */
  destination: ClickPipes.ClickPipeMutateDestination;
  /** Field mappings. Omission preserves them. */
  fieldMappings?: ClickPipes.ClickPipeFieldMapping[];
  /** Per-pipe scaling. Omission leaves scaling unmanaged. */
  scaling?: ClickPipes.ClickPipeScaling;
  /** Per-pipe settings. Omission leaves settings unmanaged. */
  settings?: Omit<
    ClickPipes.UpdateClickPipeSettingsRequest,
    "organizationId" | "serviceId" | "clickPipeId"
  >;
  /** Desired running or stopped state. Omission leaves the state unmanaged. */
  state?: "running" | "stopped";
}

export interface PipeAttributes {
  /** Organization that owns the service. */
  organizationId: string;
  /** Service that owns the pipe. */
  serviceId: string;
  /** Stable pipe UUID. */
  clickPipeId: string;
  /** Current name. */
  name: string | undefined;
  /** Current lifecycle state. */
  state: ClickPipes.ClickPipeState | undefined;
  /** Current scaling. */
  scaling: ClickPipes.ClickPipeScaling | undefined;
  /** Current pipe settings. */
  settings: ClickPipes.ClickPipeSettings | undefined;
  /** Source family last acknowledged by this resource. */
  sourceKind?: PipeSource["kind"];
  /** Redacted fingerprint of the last acknowledged immutable configuration. */
  immutableIntent?: Redacted.Redacted<string>;
  /** Redacted fingerprint of the last acknowledged mutable streaming source configuration. */
  mutableSourceIntent?: Redacted.Redacted<string>;
  /** Fingerprint of the observed source identity, checked before updates and deletion. */
  observedSourceIdentity?: Redacted.Redacted<string>;
}

export interface Pipe extends Resource<
  "ClickHouse.ClickPipes.Pipe",
  PipeProps,
  PipeAttributes,
  never,
  Providers
> {}

/**
 * An ingestion pipeline owned by one ClickHouse Cloud service. Source and
 * authentication are selected together. Destination columns and field mappings
 * configure the pipe; they do not migrate a destination table.
 *
 * ### Ingest from object storage
 * **Example:** Create a batch pipe
 * ```typescript
 * import * as Alchemy from "alchemy";
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 * import * as Effect from "effect/Effect";
 *
 * export default Alchemy.Stack("Ingestion", {
 *   providers: ClickHouse.providers(), state: Alchemy.localState(),
 * }, Effect.gen(function* () {
 *   return yield* ClickHouse.ClickPipes.Pipe("Events", {
 *     organizationId: "00000000-0000-4000-8000-000000000001",
 *     serviceId: "00000000-0000-4000-8000-000000000002",
 *     source: { kind: "objectStorage", config: {
 *       type: "s3", format: "JSONEachRow", url: "https://example.com/events/*.json",
 *     } },
 *     destination: { database: "default", table: "events", managedTable: true,
 *       columns: [{ name: "event", type: "String" }] },
 *     fieldMappings: [{ sourceField: "event", destinationField: "event" }],
 *   });
 * }));
 * ```
 *
 * Immutable source and destination changes replace the pipe, deleting it first.
 * Deletion calls only the ClickPipe delete API, as the official Terraform
 * provider does; it never drops destination tables or changes source grants,
 * binlog retention, MongoDB configuration, or BigQuery datasets. Source-artifact
 * cleanup is delegated to ClickHouse. PostgreSQL's pipe-managed slots and
 * publications must not be shared with another consumer; their deletion is part
 * of PeerDB's source cleanup. The Cloud API exposes no source-retention option
 * or source-cleanup completion status. Caller-managed grants remain the caller's
 * responsibility. BigQuery uses a caller-owned staging bucket: dedicate its
 * prefix to this pipe. Initial-load staging objects are automatically cleaned
 * up by ClickHouse; the bucket and any retained objects remain caller-managed.
 * Database replacement may require new destination table names: existing data
 * is retained, and snapshot destinations must be empty. Resync is explicit.
 *
 * Pipes have no tags or description for an ownership marker. Names, including
 * generated names, never authorize recovery. After an ambiguous create, inspect
 * the service and adopt the exact UUID with `Alchemy.AdoptPolicy.adopt(true)`.
 * Adoption verifies visible immutable settings; secrets cannot be read back.
 * Mutable credentials supplied on adoption are applied to the adopted pipe.
 * Creates, source edits, state commands and deletes are not automatically retried.
 * Absolute scaling/settings writes retain bounded SDK retries. Readiness polls for up to two minutes;
 * Snapshot means initial loading is underway, not that it has finished.
 * Database edits pause a running pipe. Set `state: "running"` to resume it on
 * recovery if a pause or update reply is lost; omitted state remains unmanaged.
 *
 * @see https://clickhouse.com/docs/integrations/clickpipes/programmatic-access/openapi
 * @see https://github.com/ClickHouse/terraform-provider-clickhouse/blob/dc26b13d87ba0ef0b75205ddf9decc0a0e1e64e2/internal/service/clickhouse/resource/clickpipe.go#L6636-L6661
 *
 * @resource
 * @product ClickPipes
 */
export const Pipe = Resource<Pipe>("ClickHouse.ClickPipes.Pipe");

export class PipeSafetyError extends Data.TaggedError("PipeSafetyError")<{ message: string }> {}

type Scope = { organizationId: string; serviceId: string };

const normalize = (value: unknown): unknown => {
  if (Redacted.isRedacted(value)) {
    const unwrapped = Redacted.value(value);
    return normalize(unwrapped);
  }
  if (Array.isArray(value)) return value.map(normalize);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, v]) => v !== undefined)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([key, v]) => [key, normalize(v)]),
    );
  }
  return value;
};

const same = (left: unknown, right: unknown) =>
  JSON.stringify(normalize(left)) === JSON.stringify(normalize(right));

const matches = (desired: unknown, observed: unknown): boolean => {
  if (desired === undefined) return true;
  if (Array.isArray(desired))
    return (
      Array.isArray(observed) &&
      desired.length === observed.length &&
      desired.every((value, index) => matches(value, observed[index]))
    );
  if (Redacted.isRedacted(desired) || desired === null) return same(desired, observed);
  if (typeof desired !== "object") return same(desired, observed);
  if (observed === null || typeof observed !== "object") return false;
  return Object.entries(desired).every(([key, value]) =>
    matches(value, Reflect.get(observed, key)),
  );
};

const databaseKind = (kind: PipeSource["kind"]) =>
  kind === "postgres" || kind === "mysql" || kind === "mongodb" || kind === "bigquery";

const database = (source: PipeSource) => databaseKind(source.kind);

const omit = (value: object, keys: readonly string[]) =>
  Object.fromEntries(Object.entries(value).filter(([key]) => !keys.includes(key)));

const immutableSource = (source: PipeSource): unknown => {
  switch (source.kind) {
    case "kafka": {
      return omit(source.config, [
        "authentication",
        "credentials",
        "iamRole",
        "caCertificate",
        "reversePrivateEndpointIds",
      ]);
    }
    case "objectStorage": {
      return omit(source.config, [
        "authentication",
        "iamRole",
        "connectionString",
        "path",
        "azureContainerName",
        "accessKey",
        "serviceAccountKey",
        "skipInitialLoad",
        "startAfter",
      ]);
    }
    case "kinesis": {
      return omit(source.config, ["authentication", "iamRole", "accessKey"]);
    }
    case "pubsub": {
      return omit(source.config, ["authentication", "ackDeadline", "serviceAccountKey"]);
    }
    case "postgres": {
      return {
        ...omit(source.config, [
          "credentials",
          "tlsHost",
          "caCertificate",
          "disableTls",
          "skipCertVerification",
          "tableMappings",
          "settings",
        ]),
        settings: omit(source.config.settings ?? {}, ["syncIntervalSeconds", "pullBatchSize"]),
      };
    }
    case "mysql": {
      return {
        ...omit(source.config, [
          "credentials",
          "authentication",
          "iamRole",
          "tlsHost",
          "caCertificate",
          "disableTls",
          "skipCertVerification",
          "serverId",
          "tableMappings",
          "settings",
        ]),
        settings: omit(source.config.settings, [
          "syncIntervalSeconds",
          "pullBatchSize",
          "useCompression",
        ]),
      };
    }
    case "mongodb": {
      return {
        uri: mongoEndpoint(source.config.uri),
        ...omit(source.config, [
          "credentials",
          "uri",
          "readPreference",
          "tlsHost",
          "caCertificate",
          "disableTls",
          "skipCertVerification",
          "tableMappings",
          "settings",
        ]),
        settings: omit(source.config.settings, ["syncIntervalSeconds", "pullBatchSize"]),
      };
    }
    case "bigquery":
      return source.config;
  }
};

const mongoEndpoint = (uri: Secret) => {
  const value = Redacted.isRedacted(uri) ? Redacted.value(uri) : uri;
  return value.replace(/^(mongodb(?:\+srv)?:\/\/)[^/]*@/, "$1");
};

const fingerprint = Effect.fn(function* (value: unknown) {
  const serialized = yield* Effect.sync(() => JSON.stringify(normalize(value)));
  return Redacted.make(yield* sha256(serialized));
});

const immutableIntent = (props: PipeProps) =>
  fingerprint({
    kind: props.source.kind,
    source: immutableSource(props.source),
    destination: omit(props.destination, ["columns"]),
  });

const observedImmutableDiffers = (desired: unknown, observed: unknown): boolean => {
  if (desired === undefined || observed === undefined || Redacted.isRedacted(desired)) return false;
  if (Array.isArray(desired)) {
    if (!Array.isArray(observed) || desired.length !== observed.length) return true;
    return desired.some((value, index) => observedImmutableDiffers(value, observed[index]));
  }
  if (desired === null || typeof desired !== "object") return !same(desired, observed);
  if (observed === null || typeof observed !== "object") return true;
  return Object.entries(desired).some(
    ([key, value]) =>
      key !== "credentials" &&
      key !== "serviceAccountKey" &&
      observedImmutableDiffers(value, Reflect.get(observed, key)),
  );
};

const assertObservedSource = Effect.fn(function* (
  pipe: ClickPipes.ClickPipe,
  kind: PipeSource["kind"],
) {
  const source = pipe.source?.[kind];
  if (!source || Object.values(pipe.source ?? {}).filter(Boolean).length !== 1)
    return yield* new PipeSafetyError({
      message: "The API omitted the acknowledged source family; refusing to guess its identity.",
    });
  return source;
});

const assertObservedImmutable = Effect.fn(function* (pipe: ClickPipes.ClickPipe, props: PipeProps) {
  const source = yield* assertObservedSource(pipe, props.source.kind);
  const observed = pipe.source?.mongodb
    ? { ...source, uri: mongoEndpoint(pipe.source.mongodb.uri) }
    : source;
  if (
    observedImmutableDiffers(immutableSource(props.source), observed) ||
    observedImmutableDiffers(omit(props.destination, ["columns"]), pipe.destination)
  )
    return yield* new PipeSafetyError({
      message: "The observed immutable pipe configuration differs from the acknowledged intent.",
    });
});

const visibleDrift = (
  desired: object,
  observed: object | null | undefined,
  keys: readonly string[],
) => {
  if (!observed) return false;
  return Object.entries(desired).some(
    ([key, value]) =>
      keys.includes(key) &&
      value !== undefined &&
      Reflect.has(observed, key) &&
      !matches(value, Reflect.get(observed, key)),
  );
};

const sourceDrift = (source: PipeSource, observed: ClickPipes.ClickPipeSource | undefined) => {
  switch (source.kind) {
    case "kafka":
      return visibleDrift(source.config, observed?.kafka, [
        "authentication",
        "iamRole",
        "caCertificate",
        "reversePrivateEndpointIds",
      ]);
    case "objectStorage":
      return visibleDrift(source.config, observed?.objectStorage, [
        "authentication",
        "iamRole",
        "path",
        "azureContainerName",
        "skipInitialLoad",
        "startAfter",
      ]);
    case "kinesis":
      return visibleDrift(source.config, observed?.kinesis, ["authentication", "iamRole"]);
    case "pubsub":
      return visibleDrift(source.config, observed?.pubsub, ["authentication", "ackDeadline"]);
    case "postgres":
      return (
        visibleDrift(source.config, observed?.postgres, [
          "tlsHost",
          "caCertificate",
          "disableTls",
          "skipCertVerification",
          "tableMappings",
        ]) ||
        visibleDrift(source.config.settings ?? {}, observed?.postgres?.settings, [
          "syncIntervalSeconds",
          "pullBatchSize",
        ])
      );
    case "mysql":
      return (
        visibleDrift(source.config, observed?.mysql, [
          "host",
          "port",
          "authentication",
          "iamRole",
          "tlsHost",
          "caCertificate",
          "disableTls",
          "skipCertVerification",
          "serverId",
          "tableMappings",
        ]) ||
        visibleDrift(source.config.settings, observed?.mysql?.settings, [
          "syncIntervalSeconds",
          "pullBatchSize",
          "useCompression",
        ])
      );
    case "mongodb":
      return (
        visibleDrift(source.config, observed?.mongodb, [
          "readPreference",
          "tlsHost",
          "caCertificate",
          "disableTls",
          "skipCertVerification",
          "tableMappings",
        ]) ||
        visibleDrift(source.config.settings, observed?.mongodb?.settings, [
          "syncIntervalSeconds",
          "pullBatchSize",
        ])
      );
    case "bigquery":
      return false;
  }
};

const postSource = (source: PipeSource): ClickPipes.ClickPipePostSource => {
  switch (source.kind) {
    case "kafka":
      return { kafka: source.config };
    case "objectStorage":
      return { objectStorage: source.config };
    case "kinesis":
      return { kinesis: source.config };
    case "pubsub":
      return { pubsub: source.config };
    case "postgres":
      return { postgres: source.config };
    case "mysql":
      return { mysql: source.config };
    case "mongodb":
      return { mongodb: source.config };
    case "bigquery":
      return { bigquery: source.config };
  }
};

const mappingDelta = <T>(
  desired: T[] | undefined,
  observed: T[] | undefined,
  key: (item: T) => string,
): Effect.Effect<{ add: T[] | undefined; remove: T[] | undefined }, PipeSafetyError> => {
  if (desired === undefined) return Effect.succeed({ add: undefined, remove: undefined });
  if (observed === undefined)
    return Effect.fail(
      new PipeSafetyError({
        message: "The API omitted table mappings; refusing to guess the current set.",
      }),
    );
  const previous = new Map(observed.map((item) => [key(item), item]));
  const next = new Map(desired.map((item) => [key(item), item]));
  if (
    previous.size !== observed.length ||
    next.size !== desired.length ||
    previous.has("") ||
    next.has("")
  )
    return Effect.fail(
      new PipeSafetyError({ message: "Table mappings must have unique complete identities." }),
    );
  for (const [id, value] of next) {
    const current = previous.get(id);
    if (current && !matches(value, current))
      return Effect.fail(
        new PipeSafetyError({
          message: "Changing an existing table mapping requires a safe replacement.",
        }),
      );
  }
  return Effect.succeed({
    add: desired.filter((item) => !previous.has(key(item))),
    remove: observed.filter((item) => !next.has(key(item))),
  });
};

const modifiedMapping = <T>(
  desired: T[] | undefined,
  observed: T[] | undefined,
  key: (item: T) => string,
) =>
  desired?.some((item) => {
    const current = observed?.find((value) => key(item) === key(value));
    return current !== undefined && !matches(item, current);
  }) ?? false;

// The API rejects removing and re-adding the same source table in one PATCH.
const mappingReplacement = (
  source: PipeSource,
  observed: ClickPipes.ClickPipeSource | undefined,
) => {
  switch (source.kind) {
    case "postgres":
      return modifiedMapping(source.config.tableMappings, observed?.postgres?.tableMappings, (m) =>
        JSON.stringify([m.sourceSchemaName, m.sourceTable]),
      );
    case "mysql":
      return modifiedMapping(source.config.tableMappings, observed?.mysql?.tableMappings, (m) =>
        JSON.stringify([m.sourceSchemaName, m.sourceTable]),
      );
    case "mongodb":
      return modifiedMapping(source.config.tableMappings, observed?.mongodb?.tableMappings, (m) =>
        JSON.stringify([m.sourceDatabaseName, m.sourceCollection]),
      );
    default:
      return false;
  }
};

const sourcePatch = Effect.fn(function* (
  source: PipeSource,
  observed: ClickPipes.ClickPipeSource | undefined,
): Effect.fn.Return<ClickPipes.ClickPipePatchSource, PipeSafetyError> {
  switch (source.kind) {
    case "kafka": {
      const c: ClickPipes.ClickPipePostKafkaSource = source.config;
      return {
        kafka: {
          authentication: c.authentication,
          credentials: c.credentials,
          iamRole: c.iamRole,
          caCertificate: c.caCertificate,
          reversePrivateEndpointIds: c.reversePrivateEndpointIds,
        },
      };
    }
    case "objectStorage": {
      const c: ClickPipes.ClickPipePostObjectStorageSource = source.config;
      return {
        objectStorage: {
          authentication: c.authentication,
          iamRole: c.iamRole,
          connectionString: c.connectionString,
          path: c.path,
          azureContainerName: c.azureContainerName,
          accessKey: c.accessKey,
          serviceAccountKey: c.serviceAccountKey,
          skipInitialLoad: c.skipInitialLoad,
          startAfter: c.startAfter,
        },
      };
    }
    case "kinesis": {
      const c: ClickPipes.ClickPipePostKinesisSource = source.config;
      return {
        kinesis: { authentication: c.authentication, iamRole: c.iamRole, accessKey: c.accessKey },
      };
    }
    case "pubsub": {
      const c = source.config;
      return {
        pubsub: {
          authentication: c.authentication,
          ackDeadline: c.ackDeadline,
          serviceAccountKey: "serviceAccountKey" in c ? c.serviceAccountKey : undefined,
        },
      };
    }
    case "postgres": {
      const c = source.config;
      const delta = yield* mappingDelta(c.tableMappings, observed?.postgres?.tableMappings, (m) =>
        m.sourceSchemaName && m.sourceTable && m.targetTable
          ? JSON.stringify([m.sourceSchemaName, m.sourceTable])
          : "",
      );
      return {
        postgres: {
          credentials: "credentials" in c ? c.credentials : undefined,
          host: c.host,
          port: c.port,
          database: c.database,
          tlsHost: c.tlsHost,
          caCertificate: c.caCertificate,
          disableTls: c.disableTls,
          skipCertVerification: c.skipCertVerification,
          settings: c.settings && {
            syncIntervalSeconds: c.settings.syncIntervalSeconds,
            pullBatchSize: c.settings.pullBatchSize,
          },
          tableMappingsToAdd: delta.add,
          tableMappingsToRemove: delta.remove,
        },
      };
    }
    case "mysql": {
      const c: ClickPipes.ClickPipeMutateMySQLSource = source.config;
      const delta = yield* mappingDelta(c.tableMappings, observed?.mysql?.tableMappings, (m) =>
        m.sourceSchemaName && m.sourceTable && m.targetTable
          ? JSON.stringify([m.sourceSchemaName, m.sourceTable])
          : "",
      );
      return {
        mysql: {
          credentials: c.credentials,
          host: c.host,
          port: c.port,
          authentication: c.authentication,
          iamRole: c.iamRole,
          tlsHost: c.tlsHost,
          caCertificate: c.caCertificate,
          disableTls: c.disableTls,
          skipCertVerification: c.skipCertVerification,
          serverId: c.serverId,
          settings: {
            syncIntervalSeconds: c.settings.syncIntervalSeconds,
            pullBatchSize: c.settings.pullBatchSize,
            useCompression: c.settings.useCompression,
          },
          tableMappingsToAdd: delta.add,
          tableMappingsToRemove: delta.remove,
        },
      };
    }
    case "mongodb": {
      const c = source.config;
      const delta = yield* mappingDelta(c.tableMappings, observed?.mongodb?.tableMappings, (m) =>
        m.sourceDatabaseName && m.sourceCollection && m.targetTable
          ? JSON.stringify([m.sourceDatabaseName, m.sourceCollection])
          : "",
      );
      return {
        mongodb: {
          credentials: c.credentials,
          uri: c.uri,
          readPreference: c.readPreference,
          tlsHost: c.tlsHost,
          caCertificate: c.caCertificate,
          disableTls: c.disableTls,
          skipCertVerification: c.skipCertVerification,
          settings: {
            syncIntervalSeconds: c.settings.syncIntervalSeconds,
            pullBatchSize: c.settings.pullBatchSize,
          },
          tableMappingsToAdd: delta.add,
          tableMappingsToRemove: delta.remove,
        },
      };
    }
    case "bigquery":
      return yield* new PipeSafetyError({
        message: "BigQuery source updates require replacement.",
      });
  }
});

const list = Effect.fn(function* ({ organizationId, serviceId }: Scope) {
  const pages = yield* Stream.runCollect(
    ClickPipes.clickPipeGetList.pages({ organizationId, serviceId }),
  ).pipe(Effect.catchTag("ClickPipesServiceNotFound", () => Effect.succeed([])));
  const pipes: ClickPipes.ClickPipe[] = [];
  for (const page of pages) {
    if (!page.result)
      return yield* new PipeSafetyError({
        message: "Pipe listing omitted its results; absence is unverified.",
      });
    pipes.push(...page.result);
  }
  return pipes;
});
const find = Effect.fn(function* (scope: Scope, id: string | undefined, name: string) {
  const pipes = yield* list(scope);
  if (pipes.some((pipe) => !pipe.id))
    return yield* new PipeSafetyError({ message: "Pipe listing omitted an identity." });
  const matches = pipes.filter((pipe) => (id ? pipe.id === id : pipe.name === name));
  if (matches.length > 1)
    return yield* new PipeSafetyError({ message: "Multiple pipes match; select a UUID." });
  const match = matches[0];
  if (!match?.id) return;
  const response = yield* ClickPipes.getClickPipe({
    organizationId: scope.organizationId,
    serviceId: scope.serviceId,
    clickPipeId: match.id,
  }).pipe(Effect.catchTag("ClickPipeNotFound", () => Effect.succeed(undefined)));
  if (!response) return;
  const { result } = response;
  if (!result || result.id !== match.id)
    return yield* new PipeSafetyError({ message: "Pipe read omitted the expected identity." });
  return result;
});
const attrs = Effect.fn(function* (
  scope: Scope,
  pipe: ClickPipes.ClickPipe,
): Effect.fn.Return<PipeAttributes, PipeSafetyError> {
  if (!pipe.id) return yield* new PipeSafetyError({ message: "Pipe response omitted its UUID." });
  return {
    ...scope,
    clickPipeId: pipe.id,
    name: pipe.name,
    state: pipe.state,
    scaling: pipe.scaling,
    settings: pipe.settings,
  };
});

const waitFor = Effect.fn(function* (
  scope: Scope,
  id: string,
  predicate: (pipe: ClickPipes.ClickPipe | undefined) => boolean,
) {
  return yield* waitForObserved({
    description: `ClickPipe ${id}`,
    observe: find(scope, id, ""),
    ready: predicate,
    failed: (pipe) => pipe?.state === "Failed" || pipe?.state === "InternalError",
  });
});

const settled = (pipe: ClickPipes.ClickPipe | undefined) =>
  pipe?.state === "Running" ||
  pipe?.state === "Snapshot" ||
  pipe?.state === "Stopped" ||
  pipe?.state === "Paused" ||
  pipe?.state === "Completed";

/** Explicitly resync a pipe; source support and data effects are determined by ClickHouse. */
export const resyncPipe = (scope: Scope & { clickPipeId: string }) =>
  ClickPipes.updateClickPipeState({ ...scope, command: "resync" }).pipe(Retry.none);

const stopped = (pipe: ClickPipes.ClickPipe | undefined) =>
  pipe?.state === "Stopped" || pipe?.state === "Paused" || pipe?.state === "Completed";

const sourceIdentity = Effect.fn(function* (pipe: ClickPipes.ClickPipe, kind: PipeSource["kind"]) {
  yield* assertObservedSource(pipe, kind);
  switch (kind) {
    case "kafka": {
      const source = pipe.source?.kafka;
      return yield* fingerprint({
        kind,
        type: source?.type,
        brokers: source?.brokers,
        topics: source?.topics,
      });
    }
    case "objectStorage":
      return yield* fingerprint({
        kind,
        type: pipe.source?.objectStorage?.type,
        url: pipe.source?.objectStorage?.url,
      });
    case "kinesis":
      return yield* fingerprint({
        kind,
        streamName: pipe.source?.kinesis?.streamName,
        region: pipe.source?.kinesis?.region,
      });
    case "pubsub":
      return yield* fingerprint({
        kind,
        projectId: pipe.source?.pubsub?.projectId,
        topic: pipe.source?.pubsub?.topic,
      });
    case "postgres": {
      const source = pipe.source?.postgres;
      if (!source?.host || !source.port || !source.database)
        return yield* new PipeSafetyError({
          message:
            "PostgreSQL source identity was omitted; deletion and retargeting cannot be verified.",
        });
      return yield* fingerprint({
        kind,
        host: source.host,
        port: source.port,
        database: source.database,
        publicationName: source.settings?.publicationName,
        replicationSlotName: source.settings?.replicationSlotName,
      });
    }
    case "mysql":
      return yield* fingerprint({
        kind,
        host: pipe.source?.mysql?.host,
        port: pipe.source?.mysql?.port,
      });
    case "mongodb": {
      const source = pipe.source?.mongodb;
      if (!source?.uri)
        return yield* new PipeSafetyError({
          message:
            "MongoDB source identity was omitted; deletion and retargeting cannot be verified.",
        });
      return yield* fingerprint({ kind, uri: mongoEndpoint(source.uri) });
    }
    case "bigquery": {
      const source = pipe.source?.bigquery;
      return yield* fingerprint({
        kind,
        projectId: source?.projectId,
        snapshotStagingPath: source?.snapshotStagingPath,
      });
    }
  }
});

const assertUnchangedSource = Effect.fn(function* (
  pipe: ClickPipes.ClickPipe,
  output: PipeAttributes,
) {
  if (
    output.sourceKind &&
    output.observedSourceIdentity &&
    !same(yield* sourceIdentity(pipe, output.sourceKind), output.observedSourceIdentity)
  )
    return yield* new PipeSafetyError({
      message:
        "The source identity changed outside this stack; inspect it before updating or deleting the pipe.",
    });
});

export const PipeProvider = () =>
  Provider.succeed(Pipe, {
    nuke: { skip: true },
    stables: ["organizationId", "serviceId", "clickPipeId"],
    diff: Effect.fn(function* ({ news, olds, output }) {
      if (!isResolved(news) || !output) return;
      if (
        news.organizationId !== output.organizationId ||
        news.serviceId !== output.serviceId ||
        (news.clickPipeId !== undefined && news.clickPipeId !== output.clickPipeId) ||
        !same(
          yield* immutableIntent(news),
          output.immutableIntent ?? (yield* immutableIntent(olds)),
        )
      ) {
        if (news.clickPipeId === output.clickPipeId)
          return yield* new PipeSafetyError({
            message: "A replacement cannot reuse the same pipe UUID. Omit clickPipeId first.",
          });
        return { action: "replace", deleteFirst: true } as const;
      }
      const current = yield* find(output, output.clickPipeId, "");
      if (!current) return { action: "update", stables: ["organizationId", "serviceId"] };
      yield* assertUnchangedSource(current, output);
      yield* assertObservedImmutable(current, news);
      if (mappingReplacement(news.source, current.source)) {
        if (news.clickPipeId === output.clickPipeId)
          return yield* new PipeSafetyError({
            message: "A replacement cannot reuse the same pipe UUID. Omit clickPipeId first.",
          });
        return { action: "replace", deleteFirst: true } as const;
      }
      if (
        !same(news, olds) ||
        (news.name !== undefined && current.name !== news.name) ||
        sourceDrift(news.source, current.source) ||
        (news.destination.columns !== undefined &&
          !matches(news.destination.columns, current.destination?.columns)) ||
        (news.fieldMappings !== undefined && !matches(news.fieldMappings, current.fieldMappings)) ||
        (news.scaling !== undefined && !matches(news.scaling, current.scaling)) ||
        (news.settings !== undefined && !matches(news.settings, current.settings)) ||
        (news.state === "running" &&
          current.state !== "Running" &&
          current.state !== "Snapshot" &&
          current.state !== "Completed") ||
        (news.state === "stopped" && !stopped(current))
      )
        return { action: "update" } as const;
    }),
    read: Effect.fn(function* ({ id, olds, output, previous }) {
      const scope = {
        organizationId: output?.organizationId ?? olds.organizationId,
        serviceId: output?.serviceId ?? olds.serviceId,
      };
      const pipeId = output?.clickPipeId ?? olds.clickPipeId;
      const name = olds.name ?? (yield* createPhysicalName({ id, maxLength: 64 }));
      const pipe = yield* find(scope, pipeId, name);
      if (!pipe) return;
      if (output) return { ...output, ...(yield* attrs(scope, pipe)) };
      const result = {
        ...(yield* attrs(scope, pipe)),
        sourceKind: olds.source.kind,
        observedSourceIdentity: yield* sourceIdentity(pipe, olds.source.kind),
      };
      if (
        previous?.organizationId === scope.organizationId &&
        previous.serviceId === scope.serviceId &&
        previous.clickPipeId === result.clickPipeId
      )
        return result;
      if (!pipeId)
        return yield* new OwnedBySomeoneElse({
          message: "Select the pipe UUID and opt into adoption.",
          resourceType: Pipe.Type,
          logicalId: id,
          physicalName: result.clickPipeId,
        });
      return Unowned(result);
    }),
    reconcile: Effect.fn(function* ({ id, news, output }) {
      const intent = yield* immutableIntent(news);
      if (
        output &&
        (news.organizationId !== output.organizationId ||
          news.serviceId !== output.serviceId ||
          (news.clickPipeId !== undefined && news.clickPipeId !== output.clickPipeId) ||
          (output.immutableIntent && !same(intent, output.immutableIntent)))
      )
        return yield* new PipeSafetyError({
          message: "Pipe identity or immutable configuration changed without replacement.",
        });
      const scope = { organizationId: news.organizationId, serviceId: news.serviceId };
      const name = news.name ?? (yield* createPhysicalName({ id, maxLength: 64 }));
      const pipeId = output?.clickPipeId ?? news.clickPipeId;
      let pipe = yield* find(scope, pipeId, name);
      if (pipe && output) yield* assertUnchangedSource(pipe, output);
      if (pipe && !output)
        return yield* new OwnedBySomeoneElse({
          message: "Select the pipe UUID and opt into adoption.",
          resourceType: Pipe.Type,
          logicalId: id,
          physicalName: pipe.id,
        });
      if (!pipe) {
        if (news.clickPipeId)
          return yield* new PipeSafetyError({
            message:
              "The explicitly selected pipe does not exist; remove clickPipeId to create a new pipe.",
          });
        pipe = (yield* ClickPipes.createClickPipe({
          ...scope,
          name,
          source: postSource(news.source),
          destination: news.destination,
          fieldMappings: news.fieldMappings,
          scaling: news.scaling,
          settings: news.settings,
          startPaused: !database(news.source) && news.state === "stopped" ? true : undefined,
        }).pipe(Retry.none)).result;
        if (!pipe?.id)
          return yield* new PipeSafetyError({
            message: "Create returned no pipe UUID; inspect the service before retrying.",
          });
      }
      const identity = yield* attrs(scope, pipe);
      pipe = yield* waitFor(scope, identity.clickPipeId, settled);
      if (!pipe)
        return yield* new PipeSafetyError({ message: "Pipe disappeared during reconciliation." });
      yield* assertObservedImmutable(pipe, news);
      const sourceIntent = yield* fingerprint(news.source);
      let source: ClickPipes.ClickPipePatchSource | undefined;
      if (
        news.source.kind !== "bigquery" &&
        (sourceDrift(news.source, pipe.source) ||
          (output && !same(sourceIntent, output.mutableSourceIntent)))
      )
        source = yield* sourcePatch(news.source, pipe.source);
      const resumeAfterEdit = source !== undefined && database(news.source) && !stopped(pipe);
      if (resumeAfterEdit) {
        yield* ClickPipes.updateClickPipeState({
          ...scope,
          clickPipeId: identity.clickPipeId,
          command: "stop",
        }).pipe(Retry.none);
        pipe = yield* waitFor(scope, identity.clickPipeId, stopped);
      }
      if (!pipe)
        return yield* new PipeSafetyError({ message: "Pipe disappeared before its update." });
      const update = {
        ...(news.name !== undefined && news.name !== pipe.name ? { name: news.name } : {}),
        ...(source ? { source } : {}),
        ...(news.destination.columns !== undefined &&
        !matches(news.destination.columns, pipe.destination?.columns)
          ? { destination: { columns: news.destination.columns } }
          : {}),
        ...(news.fieldMappings !== undefined && !matches(news.fieldMappings, pipe.fieldMappings)
          ? { fieldMappings: news.fieldMappings }
          : {}),
      };
      if (Object.keys(update).length)
        yield* ClickPipes.updateClickPipe({
          ...scope,
          clickPipeId: identity.clickPipeId,
          ...update,
        }).pipe(Retry.none);
      if (news.settings !== undefined && !matches(news.settings, pipe.settings))
        yield* ClickPipes.updateClickPipeSettings({
          ...scope,
          clickPipeId: identity.clickPipeId,
          ...news.settings,
        });
      if (news.scaling !== undefined && !matches(news.scaling, pipe.scaling))
        yield* ClickPipes.updateClickPipeScaling({
          ...scope,
          clickPipeId: identity.clickPipeId,
          ...news.scaling,
        });
      pipe = yield* waitFor(scope, identity.clickPipeId, settled);
      const desiredState = news.state ?? (resumeAfterEdit ? "running" : undefined);
      if (
        (desiredState === "stopped" && !stopped(pipe)) ||
        (desiredState === "running" && stopped(pipe) && pipe?.state !== "Completed")
      ) {
        yield* ClickPipes.updateClickPipeState({
          ...scope,
          clickPipeId: identity.clickPipeId,
          command: desiredState === "running" ? "start" : "stop",
        }).pipe(Retry.none);
        pipe = yield* waitFor(scope, identity.clickPipeId, (value) =>
          desiredState === "running"
            ? value?.state === "Running" ||
              value?.state === "Snapshot" ||
              value?.state === "Completed"
            : stopped(value),
        );
      } else {
        pipe = yield* find(scope, identity.clickPipeId, "");
      }
      if (!pipe)
        return yield* new PipeSafetyError({ message: "Pipe disappeared after reconciliation." });
      pipe = yield* waitFor(
        scope,
        identity.clickPipeId,
        (value) =>
          value !== undefined &&
          (news.name === undefined || value.name === news.name) &&
          !sourceDrift(news.source, value.source) &&
          matches(news.destination.columns, value.destination?.columns) &&
          matches(news.fieldMappings, value.fieldMappings) &&
          matches(news.settings, value.settings) &&
          matches(news.scaling, value.scaling),
      );
      if (!pipe)
        return yield* new PipeSafetyError({
          message: "Pipe disappeared during configuration readback.",
        });
      return {
        ...(yield* attrs(scope, pipe)),
        sourceKind: news.source.kind,
        immutableIntent: intent,
        mutableSourceIntent: sourceIntent,
        observedSourceIdentity: yield* sourceIdentity(pipe, news.source.kind),
      };
    }),
    delete: Effect.fn(function* ({ output, olds }) {
      const scope = { organizationId: output.organizationId, serviceId: output.serviceId };
      const current = yield* find(scope, output.clickPipeId, "");
      if (!current) return;
      yield* assertObservedSource(current, output.sourceKind ?? olds.source.kind);
      yield* assertUnchangedSource(current, output);
      if (!output.observedSourceIdentity) yield* assertObservedImmutable(current, olds);
      // The vendor deletes directly, including Failed/Provisioning pipes; stop is not a prerequisite.
      yield* ClickPipes.deleteClickPipe({ ...scope, clickPipeId: output.clickPipeId }).pipe(
        Retry.none,
        Effect.catchTag("ClickPipeNotFound", () => Effect.void),
      );
      yield* waitForObserved({
        description: `ClickPipe ${output.clickPipeId} deletion`,
        observe: find(scope, output.clickPipeId, ""),
        ready: (pipe) => !pipe,
      });
    }),
  });
