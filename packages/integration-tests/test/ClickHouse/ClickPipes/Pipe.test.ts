import { Pipe, type PipeProps } from "@alchemy.run/clickhouse/ClickPipes/Pipe";
import { providers } from "@alchemy.run/clickhouse/Providers";
import { Service } from "@alchemy.run/clickhouse/Service";
import { Database } from "@alchemy.run/clickhouse/SQL/Database";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as ClickPipes from "@distilled.cloud/clickhouse/click_pipes";
import * as Services from "@distilled.cloud/clickhouse/service";
import { makeSqlClient } from "@distilled.cloud/clickhouse/SQL";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import type * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { expectDeployFails } from "../../lookup.ts";
import { testName } from "../../names.ts";

const { test } = Test.make({ providers: providers() });

// ClickHouse's documented schema-inference fixture contains {a: 1} and {a: "22"}.
// The stack owns its destination service/database; no external source account is needed.
test.provider.skipIf(
  process.env.CLICKHOUSE_TEST_SLOW !== "1" || !process.env.CLICKHOUSE_TEST_ORGANIZATION_ID,
)(
  "S3 pipe ingests, repairs name drift, replaces and preserves both destination tables",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
      const objectUrl = yield* Config.String("CLICKHOUSE_CLICKPIPES_TEST_OBJECT_URL").pipe(
        Config.withDefault(
          "https://datasets-documentation.s3.eu-west-3.amazonaws.com/json/sample.json",
        ),
      );
      const infrastructure = Effect.gen(function* () {
        const service = yield* Service("Service", {
          organizationId,
          provider: "aws",
          region: "us-east-1",
          minReplicaMemoryGb: 8,
          maxReplicaMemoryGb: 8,
          numReplicas: 1,
          idleScaling: true,
          idleTimeoutMinutes: 5,
          ipAccessList: [{ source: "0.0.0.0/0", description: "Disposable ingestion test" }],
        });
        const database = yield* Database("Database", {
          organizationId,
          serviceId: service.serviceId,
          username: "default",
          password: service.password.as<Redacted.Redacted<string>>(),
          dropOnDelete: true,
        });
        return { service, database };
      });
      const fixture = yield* stack.deploy(infrastructure);
      const serviceId = fixture.service.serviceId;
      const database = fixture.database.name;
      const table = "first_destination";
      const replacementTable = "replacement_destination";
      const password = fixture.service.password;
      if (!password)
        return yield* Effect.fail(
          new Error("The disposable service must retain its initial SQL password."),
        );
      const declare = (props: PipeProps, id = "Pipe") =>
        Effect.gen(function* () {
          yield* infrastructure;
          return yield* Pipe(id, props);
        });
      const sourceUrl = yield* Effect.try(() => new URL(objectUrl));
      expect(sourceUrl.protocol).toBe("https:");
      expect(sourceUrl.username + sourceUrl.password + sourceUrl.search + sourceUrl.hash).toBe("");
      expect(sourceUrl.hostname).toMatch(/(^|\.)s3([.-][a-z0-9-]+)?\.amazonaws\.com$/);
      const client = yield* HttpClient.HttpClient;
      // This is the public object supplied by the test fixture, not a cloud API.
      const sourceText = yield* client.get(objectUrl).pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.flatMap((response) => response.text),
        Effect.scoped,
      );
      expect(sourceText.length).toBeLessThanOrEqual(65_536);
      const sourceRows = yield* Schema.decodeUnknownEffect(
        Schema.Array(
          Schema.fromJsonString(Schema.Struct({ a: Schema.Union([Schema.String, Schema.Number]) })),
        ),
      )(sourceText.trim().split(/\r?\n/).filter(Boolean));
      expect(sourceRows.length).toBeGreaterThan(0);

      const scope = { organizationId, serviceId };
      const service = (yield* Services.getInstance(scope)).result;
      const endpoint = service?.endpoints?.find((value) => value.protocol === "https");
      if (!endpoint?.host || !endpoint.port)
        return yield* Effect.fail(new Error("The disposable service needs an HTTPS SQL endpoint."));
      const sql = makeSqlClient({
        url: `https://${endpoint.host}:${endpoint.port}`,
        username: "default",
        password,
      });
      for (const name of [table, replacementTable]) {
        yield* sql.command({
          sql: "CREATE TABLE {database:Identifier}.{table:Identifier} (message String) ENGINE = MergeTree ORDER BY message",
          parameters: { database, table: name },
        });
        yield* sql.command({
          sql: "INSERT INTO {database:Identifier}.{table:Identifier} SETTINGS async_insert = 0 VALUES ('baseline')",
          parameters: { database, table: name },
        });
      }
      const tableIdentity = (table: string) =>
        sql.query({
          sql: "SELECT toString(uuid) AS uuid, create_table_query FROM system.tables WHERE database = {database:String} AND name = {table:String}",
          parameters: { database, table },
        });
      const readRows = (table: string) =>
        sql
          .query({
            sql: "SELECT message, toString(count()) AS copies FROM {database:Identifier}.{table:Identifier} GROUP BY message ORDER BY message",
            parameters: { database, table },
          })
          .pipe(
            Effect.flatMap(
              Schema.decodeUnknownEffect(
                Schema.Array(Schema.Struct({ message: Schema.String, copies: Schema.String })),
              ),
            ),
          );
      const identities = yield* Effect.all([tableIdentity(table), tableIdentity(replacementTable)]);
      const before = yield* Effect.all([readRows(table), readRows(replacementTable)]);
      for (const rows of before) expect(rows.length).toBeGreaterThan(0);
      for (const identity of identities) expect(identity).toHaveLength(1);
      for (const name of [table, replacementTable])
        expect(
          yield* sql.query({
            sql: "SELECT name, type FROM system.columns WHERE database = {database:String} AND table = {table:String} ORDER BY position",
            parameters: { database, table: name },
          }),
        ).toEqual([{ name: "message", type: "String" }]);
      const props: PipeProps = {
        ...scope,
        source: {
          kind: "objectStorage",
          config: { type: "s3", format: "JSONEachRow", url: objectUrl, isContinuous: false },
        },
        destination: {
          database,
          table,
          managedTable: false,
          columns: [{ name: "message", type: "String" }],
        },
        fieldMappings: [{ sourceField: "a", destinationField: "message" }],
        state: "running",
      };
      const created = yield* stack.deploy(declare(props));
      const complete = (clickPipeId: string) =>
        ClickPipes.getClickPipe({ ...scope, clickPipeId }).pipe(
          Effect.repeat({
            schedule: Schedule.spaced("15 seconds"),
            times: 8,
            until: ({ result }) =>
              result?.state === "Completed" ||
              result?.state === "Failed" ||
              result?.state === "InternalError",
          }),
        );
      expect((yield* complete(created.clickPipeId)).result?.state).toBe("Completed");
      const ingested = yield* readRows(table);
      const expected = new Map(before[0].map((row) => [row.message, Number(row.copies)]));
      for (const row of sourceRows)
        expected.set(String(row.a), (expected.get(String(row.a)) ?? 0) + 1);
      for (const [message, copies] of expected)
        expect(
          Number(ingested.find((row) => row.message === message)?.copies ?? 0),
        ).toBeGreaterThanOrEqual(copies);

      const name = testName("alchemy-clickpipe-renamed");
      const declaration = declare({ ...props, name });
      expect((yield* stack.deploy(declaration)).clickPipeId).toBe(created.clickPipeId);
      yield* ClickPipes.updateClickPipe({
        ...scope,
        clickPipeId: created.clickPipeId,
        name: testName("out-of-band"),
      });
      expect((yield* stack.deploy(declaration)).clickPipeId).toBe(created.clickPipeId);
      expect(
        (yield* ClickPipes.getClickPipe({ ...scope, clickPipeId: created.clickPipeId })).result,
      ).toMatchObject({ name, state: "Completed" });
      expect((yield* stack.plan(declaration)).resources.Pipe.action).toBe("noop");
      yield* expectDeployFails(stack.plan(declare({ ...props, name }, "Foreign")), [
        "OwnedBySomeoneElse",
      ]);

      const replacement = declare({
        ...props,
        name,
        destination: { ...props.destination, table: replacementTable },
      });
      yield* expectDeployFails(
        stack.plan(
          declare({
            ...props,
            clickPipeId: created.clickPipeId,
            destination: { ...props.destination, table: replacementTable },
          }),
        ),
        ["PipeSafetyError", "same pipe UUID"],
      );
      expect((yield* stack.plan(replacement)).resources.Pipe.action).toBe("replace");
      const replaced = yield* stack.deploy(replacement);
      expect(replaced.clickPipeId).not.toBe(created.clickPipeId);
      expect((yield* complete(replaced.clickPipeId)).result?.state).toBe("Completed");
      const secondRows = yield* readRows(replacementTable);
      const secondExpected = new Map(before[1].map((row) => [row.message, Number(row.copies)]));
      for (const row of sourceRows)
        secondExpected.set(String(row.a), (secondExpected.get(String(row.a)) ?? 0) + 1);
      for (const [message, copies] of secondExpected)
        expect(
          Number(secondRows.find((row) => row.message === message)?.copies ?? 0),
        ).toBeGreaterThanOrEqual(copies);
      yield* stack.deploy(replacement);
      yield* stack.deploy(infrastructure);
      const remaining = yield* Stream.runCollect(ClickPipes.clickPipeGetList.items(scope));
      expect(
        remaining.some(
          (pipe) => pipe.id === created.clickPipeId || pipe.id === replaced.clickPipeId,
        ),
      ).toBe(false);
      expect(yield* Effect.all([tableIdentity(table), tableIdentity(replacementTable)])).toEqual(
        identities,
      );
      expect(yield* readRows(table)).toEqual(ingested);
      expect(yield* readRows(replacementTable)).toEqual(secondRows);
      yield* stack.destroy();
      expect(
        yield* Services.getInstance(scope).pipe(
          Effect.as(false),
          Effect.catchTag("ServiceNotFound", () => Effect.succeed(true)),
        ),
      ).toBe(true);
    }),
  {
    timeout: 900_000,
    retry: 0,
    exclusive: "clickhouse:disposable-services",
    tags: ["live", "provider:clickhouse", "provider:clickhouse:clickpipes"],
  },
);
