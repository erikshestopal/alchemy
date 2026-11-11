import * as Sql from "@distilled.cloud/clickhouse/SQL";
import * as Binding from "alchemy/Binding";
import * as Output from "alchemy/Output";
import { CurrentRuntimeContext, RuntimeContext } from "alchemy/RuntimeContext";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import type { Service } from "../Service.ts";

export interface ConnectOptions {
  /** Existing SQL user with separately granted ClickHouse permissions, not a Cloud API key. */
  username: string;
  /** Password of the supplied SQL user. Never defaults to the service's initial password. */
  password: Redacted.Redacted<string>;
  /** Default database; this does not restrict access to other databases. */
  database: string;
}

export interface ConnectClient {
  /** Buffered JSON rows. The SQL user's grants, not this method's name, enforce access. */
  query(
    statement: Sql.Statement,
  ): Effect.Effect<readonly unknown[], Sql.SqlError | Sql.SqlTransportError, RuntimeContext>;
  /** Execute a statement without retrying an uncertain write. */
  command(
    statement: Sql.Statement,
  ): Effect.Effect<void, Sql.SqlError | Sql.SqlTransportError, RuntimeContext>;
  /** Insert a complete payload without retrying an uncertain write. */
  insert(
    input: Sql.Insert,
  ): Effect.Effect<void, Sql.SqlError | Sql.SqlTransportError, RuntimeContext>;
}

/**
 * Connect a Platform host to a service using an existing SQL identity.
 * `id` must be a static name unique within this host.
 * Different identities on the same service need different names.
 *
 * ClickHouse grants on the supplied user enforce permissions. Neither the
 * default database nor `query` is a permission boundary: SQL can name other
 * databases or perform any operation the user is allowed to execute.
 * This binding creates no users or grants and injects no Cloud management keys.
 * All code in the host can access its injected SQL credentials.
 * ClickHouse's SQL HTTPS interface accepts a SQL username and password, not
 * the host's workload identity; supply a narrowly granted SQL identity.
 *
 * ### Querying from a host
 * **Example:** Bind a separately granted SQL user
 * ```typescript
 * // src/health.ts
 * import { Service } from "@alchemy.run/clickhouse/Service";
 * import { Connect, ConnectHttp } from "@alchemy.run/clickhouse/SQL/Connect";
 * import * as Cloudflare from "@alchemy.run/cloudflare";
 * import * as Config from "effect/Config";
 * import * as Effect from "effect/Effect";
 * import * as HttpServerResponse from "effect/http/HttpServerResponse";
 *
 * const service = Service.lookup({
 *   organizationId: "your-organization-uuid",
 *   serviceId: "your-service-uuid",
 * });
 * export default class Health extends Cloudflare.Worker<Health>()(
 *   "Health",
 *   { main: import.meta.url },
 *   Effect.gen(function* () {
 *     const sql = yield* Connect("Analytics", service, {
 *       username: "app_reader",
 *       password: Config.Redacted("ANALYTICS_SQL_PASSWORD"),
 *       database: "analytics",
 *     });
 *     return {
 *       fetch: Effect.gen(function* () {
 *         const rows = yield* sql.query({ sql: "SELECT 1 AS value" });
 *         return yield* HttpServerResponse.json(rows);
 *       }),
 *     };
 *   }).pipe(Effect.provide(ConnectHttp)),
 * ) {}
 * ```
 * Include the Worker in a stack with both providers and allow its egress in
 * the service's SQL access list.
 *
 * @binding
 * @product SQL
 */
export interface Connect extends Binding.Service<
  Connect,
  "ClickHouse.SQL.Connect",
  (id: string, service: Service, options: ConnectOptions) => Effect.Effect<ConnectClient>
> {}

export const Connect = Binding.Service<Connect>("ClickHouse.SQL.Connect");

/**
 * Host-independent HTTP implementation using Platform's environment channel.
 * Each call reads the host environment and creates a distilled SQL client;
 * no sockets, streams, or promises survive between requests. The transport
 * buffers responses, follows no redirects, and never retries uncertain writes.
 */
export const ConnectHttp = Layer.succeed(
  Connect,
  Effect.fn(function* (id: string, service: Service, options: ConnectOptions) {
    if (typeof id !== "string" || id.length === 0) {
      return yield* Effect.die(new Error("ClickHouse.SQL.Connect requires a static, nonempty id"));
    }
    // Encode rather than sanitize: names such as `Reader-A` and `Reader_A` must stay distinct.
    const scope = Array.from(id, (character) =>
      character.codePointAt(0)!.toString(16).toUpperCase(),
    ).join("_");
    const prefix = `CLICKHOUSE_SQL_${scope}`;
    const keys = {
      url: `${prefix}_URL`,
      username: `${prefix}_USERNAME`,
      password: `${prefix}_PASSWORD`,
      database: `${prefix}_DATABASE`,
    };

    if (!globalThis.__ALCHEMY_RUNTIME__) {
      const context = yield* CurrentRuntimeContext;
      if (!context) {
        return yield* Effect.die(new Error("ClickHouse.SQL.Connect requires a Platform host"));
      }
      if (Object.hasOwn(context.env, keys.url)) {
        return yield* Effect.die(new Error(`Duplicate ClickHouse SQL connection id: ${id}`));
      }
      const url = service.service.pipe(
        Output.mapEffect((details) => {
          const endpoint = details.endpoints?.find((endpoint) => endpoint.protocol === "https");
          return endpoint?.host && endpoint.port
            ? Effect.succeed(`https://${endpoint.host}:${endpoint.port}`)
            : Effect.die(new Error("ClickHouse service has no HTTPS SQL endpoint"));
        }),
      );
      const env = {
        [keys.url]: url,
        [keys.username]: Output.asOutput(options.username),
        [keys.password]: Output.asOutput(options.password),
        [keys.database]: Output.asOutput(options.database),
      };
      for (const [key, value] of Object.entries(env)) {
        const stored = yield* context.set(key, value);
        if (stored !== key) {
          return yield* Effect.die(
            new Error(`ClickHouse SQL host ${context.Type} rewrote a binding environment key`),
          );
        }
      }
    }

    const client = Effect.gen(function* () {
      const context = yield* RuntimeContext;
      const url = yield* context.get<string>(keys.url);
      const username = yield* context.get<string>(keys.username);
      const password = yield* context.get<string | Redacted.Redacted<string>>(keys.password);
      const database = yield* context.get<string>(keys.database);
      const secret = Redacted.isRedacted(password) ? Redacted.value(password) : password;
      if (
        typeof url !== "string" ||
        url.length === 0 ||
        typeof username !== "string" ||
        username.length === 0 ||
        typeof secret !== "string" ||
        secret.length === 0 ||
        typeof database !== "string" ||
        database.length === 0
      ) {
        return yield* new Sql.SqlError({
          message: "Missing or invalid ClickHouse SQL binding environment",
        });
      }
      return Sql.makeSqlClient({ url, username, password: Redacted.make(secret), database });
    });

    return {
      query: (statement) =>
        client.pipe(
          Effect.flatMap((sql) => sql.query(statement)),
          Effect.provide(FetchHttpClient.layer),
        ),
      command: (statement) =>
        client.pipe(
          Effect.flatMap((sql) => sql.command(statement)),
          Effect.provide(FetchHttpClient.layer),
        ),
      insert: (input) =>
        client.pipe(
          Effect.flatMap((sql) => sql.insert(input)),
          Effect.provide(FetchHttpClient.layer),
        ),
    } satisfies ConnectClient;
  }),
);
