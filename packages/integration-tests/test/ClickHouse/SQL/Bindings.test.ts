import * as ClickHouse from "@alchemy.run/clickhouse";
import * as Cloudflare from "@alchemy.run/cloudflare";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Services from "@distilled.cloud/clickhouse/service";
import * as Sql from "@distilled.cloud/clickhouse/SQL";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { Allowed, Denied, Service, username } from "./fixtures/ConnectDatabase.ts";
import ConnectWorker from "./fixtures/ConnectWorker.ts";

const { test } = Test.make({
  providers: Layer.mergeAll(Cloudflare.providers(), ClickHouse.providers()),
  dev: false,
});

test.provider.skipIf(process.env.CLICKHOUSE_TEST_SLOW !== "1")(
  "SQL.Connect delivers the supplied SQL identity to a deployed Worker and enforces its table grants across requests",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const infrastructure = Effect.gen(function* () {
        const service = yield* Service;
        const allowed = yield* Allowed;
        const denied = yield* Denied;
        return { service, allowed, denied };
      });
      const created = yield* stack.deploy(infrastructure);
      const database = created.allowed.name;
      const token = yield* Config.Redacted("CLICKHOUSE_SQL_TEST_HTTP_TOKEN");
      const password = yield* Config.Redacted("CLICKHOUSE_SQL_TEST_PASSWORD");
      const endpoint = created.service.service.endpoints?.find(
        (entry) => entry.protocol === "https",
      );
      if (!endpoint?.host || !endpoint.port || !created.service.password) {
        return yield* Effect.fail(
          new Error("The disposable service needs its initial SQL password and HTTPS endpoint."),
        );
      }
      const admin = Sql.makeSqlClient({
        url: `https://${endpoint.host}:${endpoint.port}`,
        username: "default",
        password: created.service.password,
      });
      for (const name of [database, created.denied.name]) {
        yield* admin.command({
          sql: "CREATE TABLE {database:Identifier}.alchemy_sql_binding_events (value String) ENGINE = MergeTree ORDER BY value",
          parameters: { database: name },
        });
      }
      yield* admin.command({
        sql: "INSERT INTO {database:Identifier}.alchemy_sql_binding_events SETTINGS async_insert = 0 VALUES ('baseline')",
        parameters: { database },
      });
      // Fixture setup only; SQL.Connect never provisions SQL users or inherits default's grants.
      yield* admin.command({
        sql: "CREATE USER {username:Identifier} IDENTIFIED WITH sha256_password BY {password:String} SETTINGS async_insert = 1, wait_for_async_insert = 0, async_insert_busy_timeout_ms = 120000, async_insert_use_adaptive_busy_timeout = 0",
        parameters: { username, password: Redacted.value(password) },
      });
      yield* admin.command({
        sql: "GRANT SELECT, INSERT ON {database:Identifier}.alchemy_sql_binding_events TO {username:Identifier}",
        parameters: { database, username },
      });

      const { worker } = yield* stack.deploy(
        Effect.gen(function* () {
          yield* infrastructure;
          return { worker: yield* ConnectWorker };
        }),
      );
      expect(worker.url).toMatch(/^https:\/\//);
      const url = worker.url;
      const client = yield* HttpClient.HttpClient;
      // This is the fixture's own endpoint, not a provider API.
      const unauthorized = yield* Test.getWhenReady(`${url}/query`, { times: 5 });
      expect(unauthorized.status).toBe(401);
      const deniedWrite = yield* client.execute(HttpClientRequest.post(`${url}/insert`));
      expect(deniedWrite.status).toBe(401);

      const query = HttpClientRequest.get(`${url}/query`).pipe(
        HttpClientRequest.bearerToken(token),
      );
      const initial = yield* Test.executeWhenReady(query, { times: 5 });
      expect(initial.status).toBe(200);
      expect(yield* initial.json).toEqual([{ username, database, value: "baseline" }]);

      // Never retry a POST or the test body: a lost insert response is uncertain.
      const inserted = yield* client.execute(
        HttpClientRequest.post(`${url}/insert`).pipe(HttpClientRequest.bearerToken(token)),
      );
      expect(inserted.status).toBe(204);
      const expected = [
        { username, database, value: "baseline" },
        { username, database, value: "binding-worker" },
      ];
      const afterInsert = yield* client.execute(query);
      expect(afterInsert.status).toBe(200);
      expect(yield* afterInsert.json).toEqual(expected);

      for (const route of ["forbidden-ddl", "cross-database", "readonly-bypass"]) {
        const denied = yield* client.execute(
          HttpClientRequest.post(`${url}/${route}`).pipe(HttpClientRequest.bearerToken(token)),
        );
        expect(denied.status).toBe(502);
        expect(yield* denied.json).toEqual({ error: "ClickHouseSqlError", code: 497 });
        const allowed = yield* client.execute(query);
        expect(allowed.status).toBe(200);
        expect(yield* allowed.json).toEqual(expected);
      }
      expect(
        yield* admin.query({
          sql: "SELECT value FROM {database:Identifier}.alchemy_sql_binding_events ORDER BY value SETTINGS select_sequential_consistency = 1",
          parameters: { database },
        }),
      ).toEqual([{ value: "baseline" }, { value: "binding-worker" }]);
      yield* stack.destroy();
      expect(
        yield* Services.getInstance({
          organizationId: created.service.organizationId,
          serviceId: created.service.serviceId,
        }).pipe(
          Effect.as(false),
          Effect.catchTag("ServiceNotFound", () => Effect.succeed(true)),
        ),
      ).toBe(true);
    }),
  {
    tags: [
      "live",
      "provider:clickhouse",
      "provider:clickhouse:sql",
      "provider:cloudflare",
      "provider:cloudflare:worker",
    ],
    timeout: 900_000,
    retry: 0,
    exclusive: "clickhouse:disposable-services",
  },
);
