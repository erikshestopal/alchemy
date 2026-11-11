import {
  diffMigrations,
  migrationAttributesOf,
  migrationsAttrs,
  migrationsInputOf,
  readMigrationRecords,
  resolveMigrations,
  runMigrations,
  stampedOf,
  type MigrationAttributes,
  type MigrationsInput,
} from "@alchemy.run/sql/Migrations";
import * as Services from "@distilled.cloud/clickhouse/service";
import * as Sql from "@distilled.cloud/clickhouse/SQL";
import { Unowned } from "alchemy/AdoptPolicy";
import { isResolved } from "alchemy/Diff";
import { createPhysicalName } from "alchemy/PhysicalName";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import { sha256 } from "alchemy/Util/sha256";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import type { Providers } from "../Providers.ts";
import { makeMigrationExecutor } from "./internal/Migrations.ts";

export interface DatabaseProps {
  /** Cloud organization owning the service. Changing it replaces the database. */
  organizationId: string;
  /** Existing Cloud service UUID. Changing it replaces the database. */
  serviceId: string;
  /** SQL identifier. Omission generates a name on creation and preserves an admitted name. */
  name?: string;
  /** Existing SQL user with database DDL and system.databases visibility, not a Cloud API key. */
  username: string;
  /** Password for the supplied SQL user. No users, grants or passwords are created or reset. */
  password: Redacted.Redacted<string>;
  /** SQL migration directory or Drizzle schema output. A custom table is one unqualified identifier. */
  migrations?: MigrationsInput;
  /** Optional migration-only SQL identity; does not replace the DDL username. Supply both migration credentials together. */
  migrationsUser?: string;
  /** Password for migrationsUser. Omission of both credentials uses the DDL identity for migrations. */
  migrationsPassword?: Redacted.Redacted<string>;
  /**
   * Permit deleting this database and all its tables after verifying its admitted UUID.
   * A failed update cannot enable deletion: the last acknowledged value is authoritative.
   * External DDL writers must serialize with deploy/destroy; DROP by name has no UUID CAS.
   * @default false
   */
  dropOnDelete?: boolean;
}

export interface DatabaseAttributes extends MigrationAttributes {
  /** Admitted organization, authoritative even after a failed update. */
  organizationId: string;
  /** Admitted service UUID, authoritative even after a failed update. */
  serviceId: string;
  /** Observed SQL database name. External renaming is not supported. */
  name: string;
  /** Exact physical database UUID; ownership is never inferred from its name. */
  uuid: string;
  /** Observed database engine, such as Shared or Atomic. */
  engine: string;
  /** Last acknowledged destructive-deletion permission. Defaults to false. */
  dropOnDelete: boolean;
}

export interface Database extends Resource<
  "ClickHouse.SQL.Database",
  DatabaseProps,
  DatabaseAttributes,
  never,
  Providers
> {}

/**
 * A SQL database on an existing ClickHouse Cloud service, retained on destroy by default.
 * Cloud credentials discover the HTTPS endpoint; separate, explicitly supplied SQL
 * credentials execute DDL through distilled. Keep the state store secure.
 *
 * ### Creating a database
 * **Example:** Retain analytics data when the stack is destroyed
 * ```typescript
 * import { Database } from "@alchemy.run/clickhouse/SQL/Database";
 *
 * const analytics = yield* Database("Analytics", {
 *   organizationId,
 *   serviceId,
 *   username: "database_admin",
 *   password: Config.Redacted("ANALYTICS_SQL_PASSWORD"),
 * });
 * ```
 *
 * ### Ownership and recovery <!-- api-prose -->
 * New databases receive a deterministic UUIDv8 derived from the resource FQN and
 * physical generation. An interrupted create is recovered only by that UUID.
 * A missing database is recreated with the same UUID only when its name is free.
 * Recreating a deleted database cannot restore its data. Replan after a failed
 * apply; writes are not automatically retried, including transport failures.
 * A database is ready only after its exact UUID is visible in system.databases.
 * Renamed or UUID-replaced managed databases fail closed.
 *
 * A pre-existing name is foreign, even when it matches the generated name. Explicit
 * adoption uses Alchemy's durable admission checkpoint to acknowledge its actual UUID.
 * Changing scope or an explicit name replaces the database; omitting name preserves it.
 * A replacement cannot reuse its predecessor's physical database.
 * System, default and information_schema databases cannot be managed by this resource.
 *
 * ### Deletion <!-- api-prose -->
 * `dropOnDelete: true` permits DROP of the acknowledged database and all its data.
 * Default retention leaves the database outside stack management after destroy;
 * redeclaring that name requires explicit adoption. Deletion checks the exact UUID
 * immediately before DROP, but ClickHouse DROP takes a name, not a compare-and-swap
 * UUID. External DDL writers MUST serialize with deployments and destroys.
 *
 * ### Migrations
 * **Example:** Apply forward-only SQL files over HTTPS
 * ```typescript
 * const analytics = yield* Database("Analytics", {
 *   organizationId,
 *   serviceId,
 *   username: "database_admin",
 *   password: Config.Redacted("ANALYTICS_SQL_PASSWORD"),
 *   migrations: "./migrations",
 * });
 * ```
 * ClickHouse does not transact DDL. Each statement is preceded by a durable
 * intent in `__alchemy_migrations__progress`; acknowledged statements are skipped.
 * Migration inserts use synchronous acknowledgment even when the SQL user has
 * asynchronous defaults. Statements cannot override these acknowledgment controls
 * or switch settings profiles; unrelated query and table settings remain supported.
 * An uncertain or failed statement requires inspection and progress repair before
 * retrying. Applied files cannot be edited, removed or renamed. Serialize all
 * migrations for a database; the API provides no lock or conditional write.
 * Custom history names are supported from the first deployment and then pinned:
 * moving history and its progress log cannot be atomic on ClickHouse.
 * Foreign history conversion requires explicit preparation outside deployment.
 *
 * @resource
 * @product SQL
 */
export const Database = Resource<Database>("ClickHouse.SQL.Database");

export class DatabaseSafetyError extends Data.TaggedError("ClickHouseSqlDatabaseSafetyError")<{
  readonly message: string;
}> {}

type SqlClient = ReturnType<typeof Sql.makeSqlClient>;
const rowSchema = Schema.Array(
  Schema.Struct({ name: Schema.String, uuid: Schema.String, engine: Schema.String }),
);
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const zeroUuid = "00000000-0000-0000-0000-000000000000";

const requireName = Effect.fn(function* (name: string) {
  if (
    !name.trim() ||
    ["system", "default", "information_schema", "_temporary_and_external_tables"].includes(
      name.toLowerCase(),
    )
  ) {
    return yield* new DatabaseSafetyError({
      message: "Refusing to manage a reserved database name.",
    });
  }
});

const generationUuid = Effect.fn(function* (fqn: string, instanceId: string) {
  const hash = yield* sha256(JSON.stringify([fqn, instanceId]));
  const variant = ((Number.parseInt(hash[16]!, 16) & 3) | 8).toString(16);
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-8${hash.slice(13, 16)}-${variant}${hash.slice(17, 20)}-${hash.slice(20, 32)}`;
});

const connect = Effect.fn(function* (
  scope: Pick<DatabaseProps, "organizationId" | "serviceId">,
  credentials: Pick<DatabaseProps, "username" | "password">,
  database?: string,
) {
  if (
    !scope.organizationId ||
    !scope.serviceId ||
    !credentials.username ||
    !Redacted.isRedacted(credentials.password)
  ) {
    return yield* new DatabaseSafetyError({
      message: "Database scope and explicit SQL credentials are required.",
    });
  }
  const { result } = yield* Services.getInstance({
    organizationId: scope.organizationId,
    serviceId: scope.serviceId,
  });
  const endpoint = result?.endpoints?.find((endpoint) => endpoint.protocol === "https");
  if (result?.id !== scope.serviceId || !endpoint?.host || !endpoint.port) {
    return yield* new DatabaseSafetyError({
      message: "The selected service has no verified HTTPS SQL endpoint.",
    });
  }
  return Sql.makeSqlClient({
    url: `https://${endpoint.host}:${endpoint.port}`,
    username: credentials.username,
    password: credentials.password,
    database,
  });
});

const observe = Effect.fn(function* (
  sql: SqlClient,
  identity: { uuid: string } | { name: string },
) {
  const rows = yield* sql
    .query(
      "uuid" in identity
        ? {
            sql: "SELECT db.name, toString(db.uuid) AS uuid, db.engine FROM system.databases AS db WHERE db.uuid = {uuid:UUID}",
            parameters: { uuid: identity.uuid },
          }
        : {
            sql: "SELECT name, toString(uuid) AS uuid, engine FROM system.databases WHERE name = {name:String}",
            parameters: { name: identity.name },
          },
    )
    .pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(rowSchema)),
      Effect.mapError((error) =>
        error._tag === "SchemaError"
          ? new DatabaseSafetyError({ message: "Could not decode the observed database identity." })
          : error,
      ),
    );
  const row = rows.at(0);
  if (
    rows.length > 1 ||
    (row &&
      (!uuidPattern.test(row.uuid) ||
        row.uuid === zeroUuid ||
        ("uuid" in identity ? row.uuid !== identity.uuid : row.name !== identity.name)))
  ) {
    return yield* new DatabaseSafetyError({
      message: "The observed database has no unambiguous, nonzero matching UUID.",
    });
  }
  return row;
});

const observeAcknowledged = Effect.fn(function* (sql: SqlClient, output: DatabaseAttributes) {
  yield* requireName(output.name);
  const current = yield* observe(sql, { uuid: output.uuid });
  const named = yield* observe(sql, { name: output.name });
  if ((current && current.name !== output.name) || (named && named.uuid !== output.uuid)) {
    return yield* new DatabaseSafetyError({
      message:
        "The acknowledged database identity was renamed or replaced; refusing to touch another database.",
    });
  }
  return current;
});

const attributes = (
  scope: Pick<DatabaseProps, "organizationId" | "serviceId">,
  row: { name: string; uuid: string; engine: string },
  dropOnDelete: boolean,
  prior?: Partial<MigrationAttributes>,
): DatabaseAttributes => ({
  organizationId: scope.organizationId,
  serviceId: scope.serviceId,
  ...row,
  dropOnDelete,
  ...migrationAttributesOf(prior),
});

const sameScope = (
  left: Pick<DatabaseProps, "organizationId" | "serviceId">,
  right: Pick<DatabaseProps, "organizationId" | "serviceId">,
) => left.organizationId === right.organizationId && left.serviceId === right.serviceId;

export const DatabaseProvider = () =>
  Provider.succeed(Database, {
    nuke: { skip: true },
    stables: ["organizationId", "serviceId", "name", "uuid"],
    diff: Effect.fn(function* ({ news, olds, output }) {
      if (!output) return { action: "update" };
      if (
        isResolved(news) &&
        (!sameScope(news, output) || (news.name !== undefined && news.name !== output.name))
      )
        return { action: "replace" };
      const sql = yield* connect(output, isResolved(news) && sameScope(news, output) ? news : olds);
      if (!(yield* observeAcknowledged(sql, output))) return { action: "update" };
      if (!isResolved(news)) return;
      const input = migrationsInputOf(news);
      if (
        input &&
        output.migrationsTable &&
        resolveMigrations({ input, stamped: stampedOf(output) }).table !== output.migrationsTable
      ) {
        return yield* new DatabaseSafetyError({
          message:
            "Moving established ClickHouse migration history and progress is not atomic; keep its original table name.",
        });
      }
      if (yield* diffMigrations({ news, output })) return { action: "update" };
      if ((news.dropOnDelete ?? false) !== output.dropOnDelete) return { action: "update" };
    }),
    read: Effect.fn(function* ({ id, fqn, instanceId, olds, output, previous }) {
      const scope = output ?? olds;
      const sql = yield* connect(scope, olds).pipe(
        Effect.catchTag("ServiceNotFound", () => Effect.succeed(undefined)),
      );
      if (!sql) return;
      if (output) {
        const current = yield* observeAcknowledged(sql, output);
        return current ? attributes(output, current, output.dropOnDelete, output) : undefined;
      }
      const name = olds.name ?? (yield* createPhysicalName({ id, maxLength: 63, delimiter: "_" }));
      yield* requireName(name);
      const uuid = yield* generationUuid(fqn, instanceId);
      const recovered = yield* observe(sql, { uuid });
      if (recovered) {
        if (recovered.name !== name) {
          return yield* new DatabaseSafetyError({
            message: "The interrupted database was renamed; refusing to claim its new name.",
          });
        }
        // Recovery proves identity, not that the interrupted create acknowledged deletion.
        const input = migrationsInputOf(olds);
        return attributes(
          scope,
          recovered,
          false,
          input
            ? {
                migrationsTable: resolveMigrations({ input, stamped: {} }).table,
              }
            : undefined,
        );
      }
      const existing = yield* observe(sql, { name });
      if (!existing) return undefined;
      if (previous && sameScope(scope, previous) && existing.uuid === previous.uuid) {
        return yield* new DatabaseSafetyError({
          message: "A replacement cannot reuse its predecessor's database UUID. Choose a new name.",
        });
      }
      return Unowned(attributes(scope, existing, olds.dropOnDelete ?? false));
    }),
    reconcile: Effect.fn(function* ({ id, fqn, instanceId, news, output }) {
      const input = migrationsInputOf(news);
      const stamped = stampedOf(output);
      const resolved = input ? resolveMigrations({ input, stamped }) : undefined;
      if (resolved && output?.migrationsTable && resolved.table !== output.migrationsTable) {
        return yield* new DatabaseSafetyError({
          message:
            "Moving established ClickHouse migration history and progress is not atomic; keep its original table name.",
        });
      }
      if ((news.migrationsUser === undefined) !== (news.migrationsPassword === undefined)) {
        return yield* new DatabaseSafetyError({
          message: "Supply both migrationsUser and migrationsPassword, or neither.",
        });
      }
      if (
        output &&
        (!sameScope(news, output) || (news.name !== undefined && news.name !== output.name))
      ) {
        return yield* new DatabaseSafetyError({
          message: "Database identity changed without replacement.",
        });
      }
      const scope = output ?? news;
      const name =
        output?.name ??
        news.name ??
        (yield* createPhysicalName({ id, maxLength: 63, delimiter: "_" }));
      const uuid = output?.uuid ?? (yield* generationUuid(fqn, instanceId));
      yield* requireName(name);
      const sql = yield* connect(scope, news);
      let current = output
        ? yield* observeAcknowledged(sql, output)
        : yield* observe(sql, { uuid });
      if (!current) {
        if (yield* observe(sql, { name })) {
          return yield* new DatabaseSafetyError({
            message:
              "The database name is occupied by a different UUID. Explicit adoption is required.",
          });
        }
        // The UUID is locally generated or validated during admission; names stay parameters.
        yield* sql.command({
          sql: `CREATE DATABASE {name:Identifier} UUID '${uuid}'`,
          parameters: { name },
        });
        current = yield* observe(sql, { uuid }).pipe(
          Effect.repeat({
            schedule: Schedule.spaced("5 seconds"),
            times: 9,
            until: (row) => row !== undefined,
          }),
        );
      }
      if (!current || current.name !== name) {
        return yield* new DatabaseSafetyError({
          message: "The exact database UUID and name were not observed ready.",
        });
      }
      const run =
        input && resolved
          ? yield* runMigrations({
              input,
              stamped,
              withExecutor: (apply) =>
                Effect.gen(function* () {
                  const records = yield* readMigrationRecords(input.dir);
                  const client = yield* connect(
                    scope,
                    {
                      username: news.migrationsUser ?? news.username,
                      password: news.migrationsPassword ?? news.password,
                    },
                    name,
                  );
                  const executor = yield* makeMigrationExecutor(client, resolved.table, records);
                  yield* apply(executor);
                  // Bookkeeping can land while its completion checkpoint loses its reply.
                  yield* makeMigrationExecutor(client, resolved.table, records);
                }),
            })
          : undefined;
      return {
        ...attributes(scope, current, news.dropOnDelete ?? false),
        ...migrationsAttrs({ input, run, output }),
      };
    }),
    delete: Effect.fn(function* ({ olds, output }) {
      if (!output.dropOnDelete) return;
      yield* requireName(output.name);
      const sql = yield* connect(output, olds).pipe(
        Effect.catchTag("ServiceNotFound", () => Effect.succeed(undefined)),
      );
      if (!sql) return;
      const current = yield* observe(sql, { uuid: output.uuid });
      const named = yield* observe(sql, { name: output.name });
      if (!current && !named) return;
      if (!current || current.name !== output.name || !named || named.uuid !== output.uuid) {
        return yield* new DatabaseSafetyError({
          message: "DROP refused: the database name no longer identifies the acknowledged UUID.",
        });
      }
      // No CAS exists between this observation and DROP; external DDL writers must serialize.
      yield* sql.command({
        sql: "DROP DATABASE {name:Identifier}",
        parameters: { name: output.name },
      });
      const remaining = yield* observe(sql, { uuid: output.uuid }).pipe(
        Effect.repeat({
          schedule: Schedule.spaced("5 seconds"),
          times: 9,
          until: (row) => row === undefined,
        }),
      );
      if (remaining) {
        return yield* new DatabaseSafetyError({
          message:
            "The dropped database UUID is still visible. Inspect SQL before another destroy attempt.",
        });
      }
    }),
  });
