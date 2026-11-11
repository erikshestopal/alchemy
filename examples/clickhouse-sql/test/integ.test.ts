import { expect } from "bun:test";
import * as ClickHouse from "@alchemy.run/clickhouse";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import * as Services from "@distilled.cloud/clickhouse/service";
import { makeSqlClient } from "@distilled.cloud/clickhouse/SQL";
import * as Alchemy from "alchemy";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Layer from "effect/Layer";
import Stack from "../alchemy.run.ts";

const { test, afterAll, deploy, destroy } = Test.make({
  providers: Layer.mergeAll(ClickHouse.providers(), Cloudflare.providers()),
  state: Alchemy.localState(),
  dev: false,
});

const ClickHouseHttp = Layer.mergeAll(
  ClickHouse.ClickHouseAuth,
  ClickHouse.fromAuthProvider(),
  FetchHttpClient.layer,
);
const testOptions = {
  tags: ["live", "provider:clickhouse", "provider:clickhouse:sql", "provider:cloudflare"],
  timeout: 900_000,
  retry: 0,
};

const disabled =
  process.env.CLICKHOUSE_TEST_SLOW !== "1" ||
  process.env.CLICKHOUSE_SQL_DESTROY_DATA !== "true" ||
  !process.env.ALCHEMY_TEST_STAGE ||
  !process.env.CLICKHOUSE_SQL_EXAMPLE_TEST_SERVICE_ID ||
  !!process.env.ALCHEMY_TEST_DEV ||
  !process.env.CLICKHOUSE_APP_USERNAME ||
  !process.env.CLICKHOUSE_APP_PASSWORD ||
  !process.env.CLICKHOUSE_HTTP_TOKEN;

afterAll.skipIf(disabled)(
  Effect.gen(function* () {
    const organizationId = yield* Config.String("CLICKHOUSE_ORGANIZATION_ID");
    const serviceId = yield* Config.String("CLICKHOUSE_SQL_EXAMPLE_TEST_SERVICE_ID");
    yield* destroy(Stack);
    const missing = yield* Services.getInstance({ organizationId, serviceId }).pipe(
      Effect.as(false),
      Effect.catchTag("ServiceNotFound", () => Effect.succeed(true)),
    );
    expect(missing).toBe(true);
  }).pipe(Effect.provide(ClickHouseHttp)),
  { timeout: 120_000 },
);

// Bootstrap this disposable test stage and grant the supplied SQL identity before opting in.
test.skipIf(disabled)(
  "live deployed Worker uses the migrated database and supplied SQL identity, authenticates requests, and preserves rows on redeploy",
  Effect.gen(function* () {
    const organizationId = yield* Config.String("CLICKHOUSE_ORGANIZATION_ID");
    const fixtureId = yield* Config.String("CLICKHOUSE_SQL_EXAMPLE_TEST_SERVICE_ID");
    const username = yield* Config.String("CLICKHOUSE_APP_USERNAME");
    const password = yield* Config.Redacted("CLICKHOUSE_APP_PASSWORD");
    const token = yield* Config.Redacted("CLICKHOUSE_HTTP_TOKEN");
    const { result } = yield* Services.getInstance({ organizationId, serviceId: fixtureId });
    expect(result?.id).toBe(fixtureId);
    const endpoint = result?.endpoints?.find((entry) => entry.protocol === "https");
    if (!endpoint?.host || !endpoint.port) {
      return yield* Effect.fail(new Error("The disposable fixture needs an HTTPS SQL endpoint."));
    }

    const deployed = yield* deploy(Stack);
    expect(deployed.serviceId).toBe(fixtureId);
    expect(deployed.url).toMatch(/^https:\/\//);
    const url = deployed.url.replace(/\/+$/, "");
    const client = yield* HttpClient.HttpClient;
    const query = HttpClientRequest.get(`${url}/events`).pipe(HttpClientRequest.bearerToken(token));
    const unauthorized = yield* Test.getWhenReady(`${url}/events`, { times: 5 });
    expect(unauthorized.status).toBe(401);
    const deniedWrite = yield* client.execute(
      HttpClientRequest.post(`${url}/events`).pipe(HttpClientRequest.bodyText("must-not-land")),
    );
    expect(deniedWrite.status).toBe(401);
    const initial = yield* Test.executeWhenReady(query, { times: 5 });
    expect(initial.status).toBe(200);
    expect(yield* initial.json).toEqual([
      { username, database: deployed.database, message: "baseline" },
    ]);

    const message = "worker: 'quotes', \\slashes\\; DROP DATABASE default; --";
    // Never retry this POST or the test body: a lost insert response is uncertain.
    const written = yield* client.execute(
      HttpClientRequest.post(`${url}/events`).pipe(
        HttpClientRequest.bearerToken(token),
        HttpClientRequest.bodyText(message),
      ),
    );
    expect(written.status).toBe(204);
    const expected = [
      { username, database: deployed.database, message: "baseline" },
      { username, database: deployed.database, message },
    ];
    const read = yield* client.execute(query);
    expect(read.status).toBe(200);
    expect(yield* read.json).toEqual(expected);

    const sql = makeSqlClient({
      url: `https://${endpoint.host}:${endpoint.port}`,
      username,
      password,
      database: deployed.database,
    });
    expect(
      yield* sql.query({
        sql: "SELECT message FROM events ORDER BY message SETTINGS select_sequential_consistency = 1",
      }),
    ).toEqual([{ message: "baseline" }, { message }]);

    const redeployed = yield* deploy(Stack);
    expect(redeployed.serviceId).toBe(fixtureId);
    expect(redeployed.databaseUuid).toBe(deployed.databaseUuid);
    const preserved = yield* Test.executeWhenReady(query, { times: 5 });
    expect(preserved.status).toBe(200);
    expect(yield* preserved.json).toEqual(expected);
  }).pipe(Effect.provide(ClickHouseHttp)),
  testOptions,
);
