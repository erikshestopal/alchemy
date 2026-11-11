import { Connect, ConnectHttp } from "@alchemy.run/clickhouse/SQL/Connect";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Redacted from "effect/Redacted";
import { Allowed, Denied, Service, username } from "./ConnectDatabase.ts";

/** The test grants its dedicated SQL user access after deploying the disposable databases. */
export default class ConnectWorker extends Cloudflare.Worker<ConnectWorker>()(
  "ClickHouseSqlConnectWorker",
  { main: import.meta.url },
  Effect.gen(function* () {
    const service = yield* Service;
    const allowed = yield* Allowed;
    const denied = yield* Denied;
    const database = yield* allowed.name;
    const deniedDatabase = yield* denied.name;
    const token = yield* Config.Redacted("CLICKHOUSE_SQL_TEST_HTTP_TOKEN");
    const sql = yield* Connect("Application", service, {
      username,
      password: Config.Redacted("CLICKHOUSE_SQL_TEST_PASSWORD"),
      database,
    });

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        if (request.headers.authorization !== `Bearer ${Redacted.value(token)}`) {
          return HttpServerResponse.empty({ status: 401 });
        }
        const databaseName = yield* database;
        const deniedDatabaseName = yield* deniedDatabase;
        if (request.method === "GET" && request.url === "/query") {
          const rows = yield* sql.query({
            sql: "SELECT currentUser() AS username, currentDatabase() AS database, value FROM alchemy_sql_binding_events ORDER BY value SETTINGS select_sequential_consistency = 1",
          });
          return yield* HttpServerResponse.json(rows);
        }
        if (request.method === "POST" && request.url === "/insert") {
          const values = yield* Effect.sync(() =>
            new TextEncoder().encode('{"value":"binding-worker"}\n'),
          );
          yield* sql.insert({
            database: databaseName,
            table: "alchemy_sql_binding_events",
            format: "JSONEachRow",
            values,
          });
          return HttpServerResponse.empty({ status: 204 });
        }
        if (request.method === "POST" && request.url === "/forbidden-ddl") {
          yield* sql.command({
            sql: "CREATE TABLE {database:Identifier}.alchemy_sql_binding_forbidden (value String) ENGINE = MergeTree ORDER BY tuple()",
            parameters: { database: databaseName },
          });
          return HttpServerResponse.empty({ status: 204 });
        }
        if (request.method === "POST" && request.url === "/cross-database") {
          const rows = yield* sql.query({
            sql: "SELECT value FROM {database:Identifier}.alchemy_sql_binding_events",
            parameters: { database: deniedDatabaseName },
          });
          return yield* HttpServerResponse.json(rows);
        }
        if (request.method === "POST" && request.url === "/readonly-bypass") {
          const rows = yield* sql.query({
            sql: "SELECT value FROM {database:Identifier}.alchemy_sql_binding_events SETTINGS readonly = 0",
            parameters: { database: deniedDatabaseName },
          });
          return yield* HttpServerResponse.json(rows);
        }
        return HttpServerResponse.empty({ status: 404 });
      }).pipe(
        Effect.catchTag(["ClickHouseSqlError", "ClickHouseSqlTransportError"], (error) =>
          HttpServerResponse.json(
            {
              error: error._tag,
              code: error._tag === "ClickHouseSqlError" ? error.code : undefined,
            },
            { status: 502 },
          ),
        ),
        Effect.orDie,
      ),
    };
  }).pipe(Effect.provide(ConnectHttp)),
) {}
