import { providers } from "@alchemy.run/clickhouse/Providers";
import { Database, type DatabaseAttributes } from "@alchemy.run/clickhouse/SQL/Database";
import { quoteIdentifier } from "@alchemy.run/sql/Migrations";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Services from "@distilled.cloud/clickhouse/service";
import * as Sql from "@distilled.cloud/clickhouse/SQL";
import { adopt } from "alchemy/AdoptPolicy";
import * as Output from "alchemy/Output";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import { expectDeployFails } from "../../lookup.ts";
import { testName } from "../../names.ts";

const { test } = Test.make({ providers: providers() });
const tags = ["live", "provider:clickhouse", "provider:clickhouse:sql"] as const;
// Opt-in also authorizes exact-UUID cleanup of databases created by these tests.
const disabled =
  process.env.CLICKHOUSE_ALLOW_SQL_DATABASE_TESTS !== "1" ||
  !process.env.CLICKHOUSE_DISPOSABLE_ORGANIZATION_ID ||
  !process.env.CLICKHOUSE_DISPOSABLE_SERVICE_ID ||
  !process.env.CLICKHOUSE_SQL_DATABASE_TEST_USERNAME ||
  !process.env.CLICKHOUSE_SQL_DATABASE_TEST_PASSWORD;

const connection = Effect.gen(function* () {
  const props = {
    organizationId: yield* Config.String("CLICKHOUSE_DISPOSABLE_ORGANIZATION_ID"),
    serviceId: yield* Config.String("CLICKHOUSE_DISPOSABLE_SERVICE_ID"),
    username: yield* Config.String("CLICKHOUSE_SQL_DATABASE_TEST_USERNAME"),
    password: yield* Config.Redacted("CLICKHOUSE_SQL_DATABASE_TEST_PASSWORD"),
  };
  const { result } = yield* Services.getInstance({
    organizationId: props.organizationId,
    serviceId: props.serviceId,
  });
  expect(result?.id).toBe(props.serviceId);
  if (result?.name === "standpoint-dev") {
    return yield* Effect.fail(
      new Error("SQL tests must not mutate the protected standpoint-dev service."),
    );
  }
  const endpoint = result?.endpoints?.find((endpoint) => endpoint.protocol === "https");
  if (!endpoint?.host || !endpoint.port) {
    return yield* Effect.fail(
      new Error("The disposable service needs an accessible HTTPS SQL endpoint."),
    );
  }
  return {
    props,
    sql: Sql.makeSqlClient({
      url: `https://${endpoint.host}:${endpoint.port}`,
      username: props.username,
      password: props.password,
    }),
  };
});

type Receipt = Pick<DatabaseAttributes, "name" | "uuid">;
type SqlClient = ReturnType<typeof Sql.makeSqlClient>;
const observe = (sql: SqlClient, name: string) =>
  sql
    .query({
      sql: "SELECT name, toString(uuid) AS uuid, engine FROM system.databases WHERE name = {name:String}",
      parameters: { name },
    })
    .pipe(
      Effect.flatMap(
        Schema.decodeUnknownEffect(
          Schema.Array(
            Schema.Struct({ name: Schema.String, uuid: Schema.String, engine: Schema.String }),
          ),
        ),
      ),
      Effect.map((rows) => {
        expect(rows.length).toBeLessThanOrEqual(1);
        return rows.at(0);
      }),
    );

const dropReceipt = Effect.fn(function* (sql: SqlClient, receipt: Receipt) {
  const current = yield* observe(sql, receipt.name);
  if (!current) return;
  if (current.uuid !== receipt.uuid) {
    return yield* Effect.fail(
      new Error("Test cleanup refused: database UUID does not match its receipt."),
    );
  }
  // These tests serialize all DDL on their own databases; name-based DROP has no CAS.
  yield* sql.command({
    sql: "DROP DATABASE {name:Identifier} SYNC",
    parameters: { name: receipt.name },
  });
  const remaining = yield* observe(sql, receipt.name).pipe(
    Effect.repeat({
      schedule: Schedule.spaced("1 second"),
      times: 9,
      until: (row) => row === undefined,
    }),
  );
  expect(remaining).toBeUndefined();
});

test.provider.skipIf(disabled)(
  "database migrations apply once, refuse rewritten and uncertain statements, and preserve retained data",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const { props: admin, sql } = yield* connection;
      const username = testName("alchemy_migrations", "_");
      expect(
        yield* sql.query({
          sql: "SELECT name FROM system.users WHERE name = {username:String}",
          parameters: { username },
        }),
      ).toEqual([]);
      yield* sql.command({
        sql: "CREATE USER {username:Identifier} IDENTIFIED WITH sha256_password BY {password:String} SETTINGS async_insert = 1, wait_for_async_insert = 0, async_insert_busy_timeout_ms = 120000, async_insert_use_adaptive_busy_timeout = 0",
        parameters: { username, password: Redacted.value(admin.password) },
      });
      yield* Effect.addFinalizer(() =>
        sql
          .command({
            sql: `DROP USER ${quoteIdentifier(username, "clickhouse")}`,
          })
          .pipe(Effect.orDie),
      );
      yield* sql.command({
        sql: `GRANT CURRENT GRANTS(CREATE TABLE, SELECT, INSERT ON *.*) TO ${quoteIdentifier(username, "clickhouse")}`,
      });
      const props = { ...admin, migrationsUser: username, migrationsPassword: admin.password };
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dir = yield* fs.makeTempDirectoryScoped();
      const first = [
        "CREATE TABLE evidence (value String) ENGINE = MergeTree ORDER BY value SETTINGS index_granularity = 8192;",
        "/* SETTINGS async_insert = 1; */ INSERT INTO evidence VALUES ('preserved');",
        "INSERT INTO evidence SELECT 'SETTINGS async_insert = 1' WHERE 0 SETTINGS max_threads = 1, `async_insert` = FALSE, \"wait_for_async_insert\" = TRUE, allow_settings_after_format_in_insert = FALSE;",
        "INSERT INTO evidence SELECT settings profile FROM (SELECT 'alias_acceptance' AS settings) WHERE 0 SETTINGS log_queries, param_x = values, “async_insert” = FALSE, “wait_for_async_insert” = TRUE;",
        "INSERT INTO evidence SELECT settings async_insert FROM (SELECT 'protected_alias' AS settings) WHERE 0 SETTINGS log_comment = ‘values’, max_threads = 1;",
      ].join("\n");
      yield* fs.writeFileString(path.join(dir, "0001.sql"), first);
      const receipts: Receipt[] = [];
      const table = "migration history`quoted";
      const declaration = Effect.gen(function* () {
        const retained = yield* Database("Retained", { ...props, migrations: { dir, table } });
        const disposable = yield* Database("Disposable", { ...props, dropOnDelete: true });
        return { retained, disposable };
      });
      const data = (name: string) =>
        sql.query({
          sql: "SELECT value FROM {database:Identifier}.{table:Identifier} ORDER BY value SETTINGS select_sequential_consistency = 1",
          parameters: { database: name, table: "evidence" },
        });

      yield* Effect.gen(function* () {
        const created = yield* stack.deploy(declaration);
        receipts.push(created.retained, created.disposable);
        expect(yield* data(created.retained.name)).toEqual([{ value: "preserved" }]);
        const history = () =>
          sql.query({
            sql: "SELECT name FROM {database:Identifier}.{table:Identifier} ORDER BY name SETTINGS select_sequential_consistency = 1",
            parameters: { database: created.retained.name, table },
          });
        expect(yield* history()).toEqual([{ name: "0001.sql" }]);
        expect(created.retained.uuid).toMatch(
          /^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
        );
        expect(created.disposable.uuid).not.toBe(created.retained.uuid);
        expect(created.retained.dropOnDelete).toBe(false);
        expect((yield* observe(sql, created.retained.name))?.uuid).toBe(created.retained.uuid);
        expect((yield* observe(sql, created.disposable.name))?.uuid).toBe(created.disposable.uuid);
        yield* expectDeployFails(
          stack.plan(
            Database("Retained", {
              ...props,
              migrations: { dir, table: "history_a" },
            }),
          ),
          ["Moving established ClickHouse migration history"],
        );
        yield* fs.remove(path.join(dir, "0001.sql"));
        yield* expectDeployFails(stack.deploy(declaration), ["RewrittenMigrationHistoryError"]);
        yield* fs.writeFileString(path.join(dir, "0001.sql"), first);
        yield* fs.writeFileString(
          path.join(dir, "0002.sql"),
          'INSERT INTO evidence SETTINGS input_format_skip_unknown_fields = 1 FORMAT JSONEachRow\n{"value":"new","SETTINGS":{"async_insert":true,"wait_for_async_insert":false}};',
        );
        expect((yield* stack.plan(declaration)).resources.Retained.action).toBe("update");
        yield* stack.deploy(declaration);
        expect(yield* history()).toEqual([{ name: "0001.sql" }, { name: "0002.sql" }]);
        expect(yield* data(created.retained.name)).toEqual([
          { value: "new" },
          { value: "preserved" },
        ]);
        const plan = yield* stack.plan(declaration);
        expect(plan.resources.Retained.action).toBe("noop");
        expect(plan.resources.Disposable.action).toBe("noop");
        const redeployed = yield* stack.deploy(declaration);
        expect(redeployed.retained.uuid).toBe(created.retained.uuid);
        expect(redeployed.disposable.uuid).toBe(created.disposable.uuid);
        expect(yield* data(created.retained.name)).toEqual([
          { value: "new" },
          { value: "preserved" },
        ]);
        const progress = () =>
          sql.query({
            sql: "SELECT name, hash, statement, status FROM {database:Identifier}.{table:Identifier} ORDER BY name, statement, status SETTINGS select_sequential_consistency = 1",
            parameters: { database: created.retained.name, table: `${table}__progress` },
          });
        const progressBefore = yield* progress();
        expect(progressBefore.length).toBeGreaterThan(0);
        for (const conflict of [
          "INSERT INTO evidence SETTINGS log_queries, async_insert = 1, wait_for_async_insert = 0 VALUES ('rejected');",
          "INSERT INTO evidence SETTINGS param_x = values, async_insert = 1, wait_for_async_insert = 0 VALUES ('rejected');",
          "INSERT INTO evidence SETTINGS param_x.part = foo.values, async_insert = 1, wait_for_async_insert = 0 VALUES ('rejected');",
          "INSERT INTO evidence SETTINGS “async_insert” = 1, “wait_for_async_insert” = 0 VALUES ('rejected');",
          "INSERT INTO evidence SETTINGS log_comment = ‘values’, async_insert = 1, wait_for_async_insert = 0 VALUES ('rejected');",
          "INSERT INTO evidence SETTINGS async_insert = 1 VALUES ('rejected');",
          "INSERT INTO evidence SETTINGS max_threads = 1, `async_insert` = TRUE VALUES ('rejected');",
          "/* offsets include this comment */ INSERT INTO evidence SETTINGS \"wait_for_async_insert\" = FALSE VALUES ('rejected');",
          "SET wait_for_async_insert = 0;",
          "SET async_insert = DEFAULT;",
          "SET wait_for_async_insert = {wait:UInt8};",
          "SET async_insert = '0';",
          "SET allow_settings_after_format_in_insert = TRUE;",
          'INSERT INTO evidence SETTINGS allow_settings_after_format_in_insert = 1 FORMAT JSONEachRow\n{"value":"rejected"};',
          "INSERT INTO evidence SETTINGS \"profile\" = 'default' VALUES ('rejected');",
          "ALTER TABLE evidence RESET SETTING async_insert;",
          "SET `async\\x5finsert` = 1;",
        ]) {
          yield* fs.writeFileString(
            path.join(dir, "0003.sql"),
            `INSERT INTO evidence VALUES ('must_not_run');\n${conflict}`,
          );
          yield* expectDeployFails(stack.deploy(declaration), [
            "MigrationError",
            "0003.sql",
            "protected setting",
          ]);
          expect(yield* data(created.retained.name)).toEqual([
            { value: "new" },
            { value: "preserved" },
          ]);
          expect(yield* history()).toEqual([{ name: "0001.sql" }, { name: "0002.sql" }]);
          expect(yield* progress()).toEqual(progressBefore);
        }
        yield* fs.remove(path.join(dir, "0003.sql"));
        yield* fs.writeFileString(path.join(dir, "0001.sql"), `${first}\n-- rewritten`);
        yield* expectDeployFails(stack.deploy(declaration), ["RewrittenMigrationHistoryError"]);
        yield* fs.writeFileString(path.join(dir, "0001.sql"), first);
        yield* fs.writeFileString(
          path.join(dir, "0003.sql"),
          "INSERT INTO evidence SELECT 'before_failure'; INSERT INTO missing_migration_table SELECT 'must fail';",
        );
        yield* expectDeployFails(stack.deploy(declaration), ["MigrationError"]);
        yield* expectDeployFails(stack.deploy(declaration), ["uncertain outcome"]);
        yield* fs.rename(path.join(dir, "0003.sql"), path.join(dir, "0004.sql"));
        yield* expectDeployFails(stack.deploy(declaration), [
          "RewrittenMigrationHistoryError",
          "progress",
        ]);
        yield* fs.remove(path.join(dir, "0004.sql"));
        yield* expectDeployFails(stack.deploy(declaration), [
          "RewrittenMigrationHistoryError",
          "progress",
        ]);
        expect(yield* data(created.retained.name)).toEqual([
          { value: "before_failure" },
          { value: "new" },
          { value: "preserved" },
        ]);
        const renamed = yield* stack.plan(
          Database("Disposable", {
            ...props,
            name: `${created.disposable.name}_next`,
            dropOnDelete: true,
          }),
        );
        expect(renamed.resources.Disposable.action).toBe("replace");
        const replacement = yield* stack.deploy(
          Database("Disposable", {
            ...props,
            name: `${created.disposable.name}_next`,
            dropOnDelete: true,
          }),
        );
        receipts.push(replacement);
        expect(replacement.uuid).not.toBe(created.disposable.uuid);
        expect((yield* observe(sql, replacement.name))?.uuid).toBe(replacement.uuid);
        expect(yield* observe(sql, created.disposable.name)).toBeUndefined();

        yield* stack.destroy();
        expect((yield* observe(sql, created.retained.name))?.uuid).toBe(created.retained.uuid);
        expect(yield* data(created.retained.name)).toEqual([
          { value: "before_failure" },
          { value: "new" },
          { value: "preserved" },
        ]);
        expect(yield* observe(sql, created.disposable.name)).toBeUndefined();
        expect(yield* observe(sql, replacement.name)).toBeUndefined();
      }).pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            yield* stack.destroy();
            for (const receipt of receipts) yield* dropReceipt(sql, receipt);
          }).pipe(Effect.orDie),
        ),
      );
      yield* stack.destroy();
    }),
  { tags, timeout: 120_000, retry: 0 },
);

test.provider.skipIf(disabled)(
  "foreign names need adoption and a changed managed UUID blocks deploy and destroy without touching its replacement",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const { props, sql } = yield* connection;
      const [foreignIdentity, replacementIdentity] = yield* sql
        .query({
          sql: "SELECT toString(generateUUIDv4()) AS uuid FROM numbers(2)",
        })
        .pipe(
          Effect.flatMap(
            Schema.decodeUnknownEffect(
              Schema.Array(
                Schema.Struct({
                  uuid: Schema.String,
                }),
              ),
            ),
          ),
        );
      if (!foreignIdentity || !replacementIdentity)
        return yield* Effect.fail(new Error("Missing fixture UUID receipts."));
      expect(replacementIdentity.uuid).not.toBe(foreignIdentity.uuid);
      const foreign = {
        name: testName("alchemy_sql_foreign", "_"),
        uuid: foreignIdentity.uuid,
      };
      const replacementUuid = replacementIdentity.uuid;
      let foreignCreated = false;
      let guarded: DatabaseAttributes | undefined;
      const createReceipt = (receipt: Receipt) =>
        sql.command({
          sql: `CREATE DATABASE {name:Identifier} UUID '${receipt.uuid}'`,
          parameters: { name: receipt.name },
        });

      yield* Effect.gen(function* () {
        // Never take over a holdover from another run, even if its test name matches.
        expect(yield* observe(sql, foreign.name)).toBeUndefined();
        foreignCreated = true;
        yield* createReceipt(foreign);
        expect((yield* observe(sql, foreign.name))?.uuid).toBe(foreign.uuid);
        const declaration = Database("Foreign", {
          ...props,
          name: foreign.name,
          dropOnDelete: true,
        });
        yield* expectDeployFails(stack.deploy(declaration), ["OwnedBySomeoneElse"]);
        expect((yield* observe(sql, foreign.name))?.uuid).toBe(foreign.uuid);
        const admitted = yield* stack.deploy(declaration.pipe(adopt(true)));
        expect(admitted.uuid).toBe(foreign.uuid);
        const unnamed = yield* stack.deploy(Database("Foreign", { ...props, dropOnDelete: true }));
        expect(unnamed.name).toBe(foreign.name);
        expect(unnamed.uuid).toBe(foreign.uuid);
        yield* expectDeployFails(
          stack.deploy(
            Effect.gen(function* () {
              const dependency = yield* Database("CredentialDependency", {
                ...props,
                dropOnDelete: true,
              });
              return yield* Database("Foreign", {
                ...props,
                password: dependency.uuid.pipe(
                  Output.map(() => Redacted.make("invalid-disposable-test-password")),
                ),
                dropOnDelete: true,
              });
            }),
          ),
          ["ClickHouseSqlError"],
        );
        const corrected = yield* stack.deploy(
          Database("Foreign", { ...props, dropOnDelete: true }),
        );
        expect(corrected.uuid).toBe(foreign.uuid);
        yield* stack.destroy();
        expect(yield* observe(sql, foreign.name)).toBeUndefined();
        foreignCreated = false;

        guarded = yield* stack.deploy(Database("Guarded", { ...props, dropOnDelete: true }));
        const original = guarded;
        yield* dropReceipt(sql, original);
        yield* createReceipt({ name: original.name, uuid: replacementUuid });
        expect((yield* observe(sql, original.name))?.uuid).toBe(replacementUuid);
        yield* expectDeployFails(
          stack.deploy(Database("Guarded", { ...props, dropOnDelete: true })),
          ["ClickHouseSqlDatabaseSafetyError", "identity was renamed or replaced"],
        );
        yield* expectDeployFails(stack.destroy(), [
          "ClickHouseSqlDatabaseSafetyError",
          "DROP refused",
        ]);
        expect((yield* observe(sql, original.name))?.uuid).toBe(replacementUuid);
      }).pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            if (guarded) {
              const current = yield* observe(sql, guarded.name);
              if (current && current.uuid !== guarded.uuid && current.uuid !== replacementUuid) {
                return yield* Effect.fail(
                  new Error(
                    "Test restore refused: an unacknowledged database occupies the guarded name.",
                  ),
                );
              }
              if (current?.uuid === replacementUuid) {
                yield* dropReceipt(sql, { name: guarded.name, uuid: replacementUuid });
              }
              // Restore only the empty, test-owned UUID so normal stack deletion can finish.
              if (current?.uuid !== guarded.uuid) yield* createReceipt(guarded);
            }
            yield* stack.destroy();
            if (foreignCreated) yield* dropReceipt(sql, foreign);
            if (guarded) yield* dropReceipt(sql, guarded);
          }).pipe(Effect.orDie),
        ),
      );
      yield* stack.destroy();
    }),
  { tags, timeout: 120_000, retry: 0 },
);

test.provider.skipIf(disabled)(
  "interrupted creates and missing acknowledged databases converge without state surgery, while failed updates cannot authorize DROP",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const { props, sql } = yield* connection;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dir = yield* fs.makeTempDirectoryScoped();
      const name = testName("alchemy_sql_recovery", "_");
      const base = { ...props, name, migrations: { dir, table: "recovery history" } };
      expect(yield* observe(sql, name)).toBeUndefined();
      yield* expectDeployFails(
        stack.deploy(
          Database("Recovery", {
            ...base,
            migrationsUser: props.username,
          }),
        ),
        ["Supply both migrationsUser"],
      );
      expect(yield* observe(sql, name)).toBeUndefined();

      yield* fs.writeFileString(path.join(dir, "0001.sql"), "SELECT 'unterminated");
      yield* expectDeployFails(stack.deploy(Database("Recovery", base)), ["unterminated"]);
      const interrupted = yield* observe(sql, name);
      expect(interrupted).toBeDefined();
      yield* fs.writeFileString(
        path.join(dir, "0001.sql"),
        "CREATE TABLE evidence (value String) ENGINE = MergeTree ORDER BY value; INSERT INTO evidence SETTINGS async_insert = 0 VALUES ('recovered');",
      );
      const recovered = yield* stack.deploy(Database("Recovery", base));
      expect(recovered.uuid).toBe(interrupted?.uuid);
      yield* dropReceipt(sql, recovered);
      expect((yield* stack.plan(Database("Recovery", base))).resources.Recovery.action).toBe(
        "update",
      );
      const recreated = yield* stack.deploy(Database("Recovery", base));
      expect(recreated.uuid).toBe(recovered.uuid);
      expect(
        yield* sql.query({
          sql: "SELECT value FROM {database:Identifier}.evidence SETTINGS select_sequential_consistency = 1",
          parameters: { database: name },
        }),
      ).toEqual([{ value: "recovered" }]);

      yield* fs.writeFileString(path.join(dir, "0002.sql"), "SELECT 'unterminated");
      yield* expectDeployFails(
        stack.deploy(Database("Recovery", { ...base, dropOnDelete: true })),
        ["unterminated"],
      );
      yield* stack.destroy();
      expect((yield* observe(sql, name))?.uuid).toBe(recreated.uuid);
      yield* dropReceipt(sql, recreated);
      yield* stack.destroy();
    }),
  { tags, timeout: 120_000, retry: 0 },
);
