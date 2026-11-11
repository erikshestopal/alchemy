import {
  MigrationError,
  RewrittenMigrationHistoryError,
  quoteIdentifier,
  scanSqlScript,
  tableColumns,
  type MigrationRecord,
  type ScannedStatement,
  type SqlExecutor,
} from "@alchemy.run/sql/Migrations";
import type * as Sql from "@distilled.cloud/clickhouse/SQL";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Schema from "effect/Schema";

type SqlClient = ReturnType<typeof Sql.makeSqlClient>;
const rowsSchema = Schema.Array(Schema.Record(Schema.String, Schema.Unknown));
const progressSchema = Schema.Array(
  Schema.Struct({
    name: Schema.String,
    hash: Schema.String,
    statement: Schema.Number,
    status: Schema.String,
  }),
);

const durableSettings = new Map<string, ReadonlyArray<string>>([
  ["async_insert", ["0", "false"]],
  ["wait_for_async_insert", ["1", "true"]],
  ["allow_settings_after_format_in_insert", ["0", "false"]],
]);

// Statement settings override HTTP settings; only explicit, canonical controls are provably safe.
const conflictingSetting = (statement: ScannedStatement): string | undefined => {
  const tokens = Array.from(statement.code.matchAll(/[A-Za-z_0-9]+|[^\s]/g), (match) => {
    const source = statement.sql.slice(match.index);
    const quote = source[0];
    if (match[0] === "_" && (quote === "`" || quote === '"' || quote === "“")) {
      const quoted = /^(?:`(?:\\[\s\S]|``|[^`])*`|"(?:\\[\s\S]|""|[^"])*"|“[^”]*”)/.exec(
        source,
      )?.[0];
      const key = quoted?.slice(1, -1);
      // Escaped setting names fail closed instead of partially decoding ClickHouse's escapes.
      return { word: "", key: key && /^[A-Za-z_0-9]+$/.test(key) ? key.toLowerCase() : "" };
    }
    if (match[0] === "_" && (quote === "'" || quote === "$" || quote === "‘")) return { word: "" };
    const word = match[0].toLowerCase();
    return { word, key: /^[A-Za-z_][A-Za-z_0-9]*$/.test(word) ? word : undefined };
  });
  const closing = new Set([")", "]", "}"]);
  const opening = new Set(["(", "[", "{"]);
  const boundaries = new Set(["values", "format", "select", "with", "from", "settings"]);
  // VALUES/FORMAT bodies are data, not settings; skip the INSERT target before finding them.
  const insert = tokens.findIndex(
    (token, index) => token.word === "insert" && tokens[index + 1]?.word === "into",
  );
  let sourceStart = insert + 2;
  if (tokens[sourceStart]?.word === "table") sourceStart++;
  if (tokens[sourceStart]?.word === "function") sourceStart++;
  sourceStart++;
  while (tokens[sourceStart]?.word === ".") sourceStart += 2;
  while (tokens[sourceStart]?.word === "(") {
    let depth = 0;
    do {
      if (tokens[sourceStart]?.word === "(") depth++;
      if (tokens[sourceStart]?.word === ")") depth--;
      sourceStart++;
    } while (sourceStart < tokens.length && depth > 0);
  }
  let depth = 0;
  let select = false;
  for (let index = 0; index < tokens.length; index++) {
    const word = tokens[index]!.word;
    if (opening.has(word)) depth++;
    if (closing.has(word)) depth--;
    if (insert >= 0 && index >= sourceStart && depth === 0) {
      if (!select && (word === "values" || word === "format")) break;
      if (word === "select" || word === "with" || word === "from") select = true;
    }
    // SETTINGS may be the first selected column, not a settings clause.
    let precedingSelect = index - 1;
    if (["all", "distinct"].includes(tokens[precedingSelect]?.word ?? "")) precedingSelect--;
    if (
      word === "settings" &&
      tokens[precedingSelect]?.word === "select" &&
      (precedingSelect === 0 ||
        (insert >= 0 && precedingSelect === sourceStart) ||
        tokens[precedingSelect - 1]?.word === "(")
    )
      continue;
    const reset = word === "setting" && tokens[index - 1]?.word === "reset";
    if (
      word !== "settings" &&
      !(word === "set" && index === 0) &&
      !(word === "setting" && (reset || tokens[index - 1]?.word === "modify"))
    )
      continue;
    let cursor = index + 1;
    while (cursor < tokens.length) {
      let key = tokens[cursor]?.key;
      if (key === undefined) break;
      if (key === "") return "opaque quoted settings identifier";
      while (tokens[cursor + 1]?.word === ".") {
        const part = tokens[cursor + 2]?.key;
        if (!part) return "opaque compound settings identifier";
        key += `.${part}`;
        cursor += 2;
      }
      if (key === "profile" && tokens[cursor + 1]?.word === "=") return key;
      const safe = durableSettings.get(key);
      if (safe) {
        const value = tokens[cursor + 2]?.word;
        const after = tokens[cursor + 3]?.word;
        if (
          reset ||
          tokens[cursor + 1]?.word !== "=" ||
          !safe.includes(value ?? "") ||
          (after !== undefined && after !== "," && after !== ")" && !boundaries.has(after))
        )
          return key;
      }
      if (reset || tokens[cursor + 1]?.word !== "=") {
        cursor++;
      } else {
        cursor += 2;
        const valueStart = cursor;
        let nesting = 0;
        for (; cursor < tokens.length; cursor++) {
          const next = tokens[cursor]!.word;
          if (
            cursor > valueStart &&
            nesting === 0 &&
            (next === "," ||
              closing.has(next) ||
              (tokens[cursor - 1]?.word !== "." && boundaries.has(next)))
          )
            break;
          if (opening.has(next)) nesting++;
          if (closing.has(next)) nesting--;
        }
      }
      if (tokens[cursor]?.word !== ",") break;
      cursor++;
    }
  }
};

/** ClickHouse has no DDL transaction or conditional write; deployments must serialize. */
export const makeMigrationExecutor = (
  client: SqlClient,
  table: string,
  records: ReadonlyArray<MigrationRecord>,
) =>
  Effect.gen(function* () {
    const http = (yield* HttpClient.HttpClient).pipe(
      HttpClient.mapRequest(
        HttpClientRequest.setUrlParams({
          async_insert: "0",
          wait_for_async_insert: "1",
          allow_settings_after_format_in_insert: "0",
        }),
      ),
    );
    if (!table.trim() || table.includes("\0")) {
      return yield* new MigrationError({
        message: "Migration history needs a nonempty SQL table identifier.",
      });
    }
    const failure = (cause: Sql.SqlError | Sql.SqlTransportError | Schema.SchemaError) =>
      new MigrationError({
        message:
          cause._tag === "ClickHouseSqlError"
            ? `ClickHouse migration failed: ${cause.message}${cause.code === undefined ? "" : ` (SQL code ${cause.code})`}.`
            : cause._tag === "ClickHouseSqlTransportError"
              ? "ClickHouse migration response was lost; inspect statement progress before retrying."
              : "ClickHouse returned unreadable migration history.",
        cause,
      });
    const query: SqlExecutor["query"] = (sql, params) =>
      params?.length
        ? Effect.fail(
            new MigrationError({
              message: "ClickHouse history uses literal SQL, not positional parameters.",
            }),
          )
        : client
            .query({
              sql: `${sql.replace(/;\s*$/, "")} SETTINGS select_sequential_consistency = 1`,
            })
            .pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(rowsSchema)),
              Effect.map((rows) => [...rows]),
              Effect.mapError(failure),
              Effect.provideService(HttpClient.HttpClient, http),
            );
    const command = (sql: string) =>
      client
        .command({ sql })
        .pipe(Effect.mapError(failure), Effect.provideService(HttpClient.HttpClient, http));
    const batch: SqlExecutor["batch"] = (statements) =>
      Effect.forEach(statements, command, { discard: true });
    const progressName = `${table}__progress`;
    const progress = quoteIdentifier(progressName, "clickhouse");
    const executor: SqlExecutor = {
      dialect: "clickhouse",
      transactionalDdl: false,
      query,
      batch,
      applyMigration: (record, bookkeeping) =>
        Effect.gen(function* () {
          const scanned = scanSqlScript(record.sql, "clickhouse");
          if (scanned.some((statement) => statement.unterminated)) {
            return yield* new MigrationError({
              message: `Migration ${record.name} has an unterminated quote or comment.`,
            });
          }
          if (
            scanned.some((statement) =>
              /^\s*(USE|BEGIN|COMMIT|ROLLBACK|START\s+TRANSACTION)\b/i.test(statement.code),
            )
          ) {
            return yield* new MigrationError({
              message: `Migration ${record.name} requests session or transaction state, which ClickHouse HTTPS migrations do not support.`,
            });
          }
          for (const statement of scanned) {
            const setting = conflictingSetting(statement);
            if (setting) {
              return yield* new MigrationError({
                message: `Migration ${record.name} overrides a protected setting (${setting}). Keep async_insert = 0, wait_for_async_insert = 1, allow_settings_after_format_in_insert = 0, and do not switch settings profiles.`,
              });
            }
          }
          const statements = [...scanned.map((statement) => statement.text), bookkeeping];
          yield* command(`CREATE TABLE IF NOT EXISTS ${progress} (
          name String, hash String, statement UInt32, status String
        ) ENGINE = MergeTree ORDER BY (name, statement)`);
          const observed = yield* client
            .query({
              sql: `SELECT name, hash, statement, status FROM ${progress} WHERE name = {name:String} SETTINGS select_sequential_consistency = 1`,
              parameters: { name: record.name },
            })
            .pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(progressSchema)),
              Effect.mapError(failure),
              Effect.provideService(HttpClient.HttpClient, http),
            );
          if (
            observed.some(
              (row) =>
                row.hash !== record.hash ||
                !Number.isSafeInteger(row.statement) ||
                row.statement < 0 ||
                row.statement >= statements.length ||
                (row.status !== "intent" && row.status !== "complete"),
            )
          ) {
            return yield* new MigrationError({
              message: `Migration ${record.name} has changed or inconsistent progress. Restore the original file and inspect ${progressName}.`,
            });
          }
          for (let index = 0; index < statements.length; index++) {
            const prior = observed.filter((row) => row.statement === index);
            if (
              prior.length === 2 &&
              prior.filter((row) => row.status === "intent").length === 1 &&
              prior.filter((row) => row.status === "complete").length === 1
            )
              continue;
            if (prior.length > 0) {
              return yield* new MigrationError({
                message: `Migration ${record.name}, statement ${index + 1} has an uncertain outcome. Inspect ${progressName} and the database; Alchemy will not replay it.`,
              });
            }
            const checkpoint = (status: string) =>
              client
                .command({
                  sql: `INSERT INTO ${progress} (name, hash, statement, status) SETTINGS async_insert = 0 VALUES ({name:String}, {hash:String}, {statement:UInt32}, {status:String})`,
                  parameters: { name: record.name, hash: record.hash, statement: index, status },
                })
                .pipe(Effect.mapError(failure), Effect.provideService(HttpClient.HttpClient, http));
            // A durable intent precedes every nontransactional statement, including bookkeeping.
            yield* checkpoint("intent");
            yield* command(statements[index]!);
            yield* checkpoint("complete");
          }
        }),
    };
    if ((yield* tableColumns(executor, progressName)).length > 0) {
      const observed = yield* client
        .query({
          sql: `SELECT name, hash, statement, status FROM ${progress} SETTINGS select_sequential_consistency = 1`,
        })
        .pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(progressSchema)),
          Effect.mapError(failure),
          Effect.provideService(HttpClient.HttpClient, http),
        );
      const local = new Map(records.map((record) => [record.name, record]));
      for (const row of observed) {
        const record = local.get(row.name);
        if (record?.hash !== row.hash) {
          return yield* new RewrittenMigrationHistoryError({
            changed: record ? [row.name] : [],
            removed: record ? [] : [row.name],
            message: `Migration ${row.name} has changed or is missing from the local directory. Restore the original file and inspect ${progressName}.`,
          });
        }
        const count = scanSqlScript(record.sql, "clickhouse").length + 1;
        const prior = observed.filter(
          (other) => other.name === row.name && other.statement === row.statement,
        );
        if (
          !Number.isSafeInteger(row.statement) ||
          row.statement < 0 ||
          row.statement >= count ||
          prior.length !== 2 ||
          prior.filter((other) => other.status === "intent").length !== 1 ||
          prior.filter((other) => other.status === "complete").length !== 1 ||
          Array.from({ length: row.statement }, (_, index) => index).some(
            (index) =>
              !observed.some(
                (other) =>
                  other.name === row.name &&
                  other.statement === index &&
                  other.status === "complete",
              ),
          )
        ) {
          return yield* new MigrationError({
            message: `Migration ${row.name} has inconsistent progress or an uncertain outcome. Inspect ${progressName}; Alchemy will not replay it.`,
          });
        }
      }
    }
    return executor;
  });
