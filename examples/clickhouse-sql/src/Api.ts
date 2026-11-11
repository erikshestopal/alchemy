import * as ClickHouse from "@alchemy.run/clickhouse";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Redacted from "effect/Redacted";
import { Database, Service } from "./database.ts";

export default class Api extends Cloudflare.Worker<Api>()(
  "Api",
  { main: import.meta.url },
  Effect.gen(function* () {
    const service = yield* Service;
    const database = yield* Database;
    const username = yield* Config.String("CLICKHOUSE_APP_USERNAME");
    const password = yield* Config.Redacted("CLICKHOUSE_APP_PASSWORD");
    const token = yield* Config.Redacted("CLICKHOUSE_HTTP_TOKEN");
    if (!username || username === "default" || Redacted.value(token).length < 32) {
      return yield* Effect.die(
        new Error("Supply a non-default SQL identity and an HTTP token of at least 32 characters."),
      );
    }
    const sql = yield* ClickHouse.SQL.Connect("Application", service, {
      username,
      password,
      database: database.name,
    });

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        if (request.headers.authorization !== `Bearer ${Redacted.value(token)}`) {
          return HttpServerResponse.empty({ status: 401 });
        }
        if (request.method === "GET" && request.url === "/events") {
          const rows = yield* sql.query({
            sql: "SELECT currentUser() AS username, currentDatabase() AS database, message FROM events ORDER BY message SETTINGS select_sequential_consistency = 1",
          });
          return yield* HttpServerResponse.json(rows);
        }
        if (request.method === "POST" && request.url === "/events") {
          const message = yield* request.text;
          if (message.length === 0 || message.length > 4096) {
            return HttpServerResponse.empty({ status: 400 });
          }
          yield* sql.command({
            sql: "INSERT INTO events (message) SETTINGS async_insert = 0 VALUES ({message:String})",
            parameters: { message },
          });
          return HttpServerResponse.empty({ status: 204 });
        }
        return HttpServerResponse.empty({ status: 404 });
      }).pipe(
        Effect.catchTag(["ClickHouseSqlError", "ClickHouseSqlTransportError"], (error) =>
          HttpServerResponse.json({ error: error._tag }, { status: 502 }),
        ),
        Effect.orDie,
      ),
    };
  }).pipe(Effect.provide(ClickHouse.SQL.ConnectHttp)),
) {}
