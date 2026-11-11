import { expect } from "bun:test";
import * as ClickHouse from "@alchemy.run/clickhouse";
import * as Test from "@alchemy.run/test/Bun";
import * as Services from "@distilled.cloud/clickhouse/service";
import { makeSqlClient } from "@distilled.cloud/clickhouse/SQL";
import * as Alchemy from "alchemy";
import { DestroyError } from "alchemy/Apply";
import { DeleteInProgress } from "alchemy/Provider";
import * as Cause from "effect/Cause";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import Stack from "../alchemy.run.ts";

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: ClickHouse.providers(),
  state: Alchemy.localState(),
});

const ClickHouseHttp = Layer.mergeAll(
  ClickHouse.ClickHouseAuth,
  ClickHouse.fromAuthProvider(),
  FetchHttpClient.layer,
);

const stack = beforeAll(deploy(Stack), { timeout: 900_000 });

afterAll(
  Effect.gen(function* () {
    yield* destroy(Stack).pipe(
      Effect.retry({
        while: (error) =>
          error instanceof DestroyError &&
          error.failures.length > 0 &&
          error.failures.every((failure) =>
            failure.cause.reasons.every(
              (reason) => Cause.isFailReason(reason) && reason.error instanceof DeleteInProgress,
            ),
          ),
        schedule: Schedule.spaced("6 seconds"),
        times: 2,
      }),
    );
    const db = yield* stack;
    const missing = yield* Services.getInstance({
      organizationId: db.organizationId,
      serviceId: db.serviceId,
    }).pipe(
      Effect.as(false),
      Effect.catchTag("ServiceNotFound", () => Effect.succeed(true)),
    );
    expect(missing).toBe(true);
  }).pipe(Effect.provide(ClickHouseHttp)),
  // Cloud shutdown can outlast one bounded destroy; resume only that typed condition.
  { timeout: 900_000 },
);

test(
  "provisions a queryable database and preserves its data and credentials through rename and redeploy",
  Effect.gen(function* () {
    const db = yield* stack;
    const endpoint = db.service.endpoints?.find((endpoint) => endpoint.protocol === "https");
    expect(endpoint?.host).toBeTruthy();
    expect(db.password).toBeDefined();
    if (!endpoint?.host || !endpoint.port || !db.password) {
      return yield* Effect.fail(new Error("Missing HTTPS endpoint or generated password"));
    }
    const sql = makeSqlClient({
      url: `https://${endpoint.host}:${endpoint.port}`,
      username: "default",
      password: db.password,
    });
    yield* sql
      .query({ sql: "SELECT 1 AS ready" })
      .pipe(Effect.retry({ schedule: Schedule.spaced("3 seconds"), times: 8 }));
    yield* sql.command({ sql: "CREATE DATABASE alchemy_example" });
    yield* sql.command({
      sql: "CREATE TABLE alchemy_example.events (id UInt32, message String) ENGINE = MergeTree ORDER BY id",
    });
    const message = "ClickHouse Cloud: 'quotes', \\slashes\\ and SQL ; DROP DATABASE default; --";
    const values = yield* Effect.sync(() =>
      new TextEncoder().encode(
        [
          { id: 7, message },
          { id: 19, message: "neighbor" },
        ]
          .map((row) => JSON.stringify(row))
          .join("\n"),
      ),
    );
    yield* sql.insert({
      database: "alchemy_example",
      table: "events",
      format: "JSONEachRow",
      values,
    });
    expect(
      yield* sql.query({
        sql: "SELECT id, message FROM alchemy_example.events WHERE message = {message:String}",
        parameters: { message },
      }),
    ).toEqual([{ id: 7, message }]);
    const before = (yield* Services.getInstance({
      organizationId: db.organizationId,
      serviceId: db.serviceId,
    })).result!;
    const unchanged = yield* deploy(Stack);
    expect(unchanged.serviceId).toBe(db.serviceId);
    expect(Redacted.value(unchanged.password!)).toBe(Redacted.value(db.password));
    const updated = yield* deploy(Stack).pipe(
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromUnknown({ CLICKHOUSE_SERVICE_NAME: `${before.name} renamed` }).pipe(
          ConfigProvider.orElse(ConfigProvider.fromEnv()),
        ),
      ),
    );
    expect(updated.serviceId).toBe(db.serviceId);
    const after = (yield* Services.getInstance({
      organizationId: db.organizationId,
      serviceId: db.serviceId,
    })).result!;
    expect(after.name).toBe(`${before.name} renamed`);
    expect(after.ipAccessList).toEqual(before.ipAccessList);
    expect(after.minReplicaMemoryGb).toBe(8);
    expect(after.maxReplicaMemoryGb).toBe(8);
    expect(after.numReplicas).toBe(1);
    expect(yield* sql.query({ sql: "SELECT id FROM alchemy_example.events ORDER BY id" })).toEqual([
      { id: 7 },
      { id: 19 },
    ]);
  }).pipe(Effect.provide(ClickHouseHttp)),
  { timeout: 120_000 },
);
