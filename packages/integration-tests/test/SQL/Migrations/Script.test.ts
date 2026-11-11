import { scanSqlScript, splitSqlScript, type ScriptDialect } from "@alchemy.run/sql/Migrations";
import { describe, expect, it } from "@alchemy.run/test";
import * as Effect from "effect/Effect";

const tags = { tags: ["unit", "local"] };

const split = (sql: string, dialect: ScriptDialect) =>
  Effect.sync(() => splitSqlScript(sql, dialect));

describe("splitSqlScript", () => {
  it.effect(
    "splits on semicolons and drops terminators and blank statements",
    () =>
      Effect.gen(function* () {
        for (const dialect of ["postgres", "mysql", "googlesql", "clickhouse"] as const) {
          expect(yield* split("SELECT 1;\n\n;SELECT 2;  ", dialect)).toEqual([
            "SELECT 1",
            "SELECT 2",
          ]);
        }
      }),
    tags,
  );

  it.effect(
    "removes comments, including `;` inside them",
    () =>
      Effect.gen(function* () {
        expect(
          yield* split(
            "-- header; still a comment\nCREATE TABLE a (id int) /* x; y */; SELECT 1",
            "postgres",
          ),
        ).toEqual(["CREATE TABLE a (id int)", "SELECT 1"]);
      }),
    tags,
  );

  it.effect(
    "treats `#` as a comment in MySQL and GoogleSQL, an operator in PostgreSQL",
    () =>
      Effect.gen(function* () {
        expect(yield* split("SELECT 1 # c; d\n+ 1; SELECT 2", "googlesql")).toEqual([
          "SELECT 1  \n+ 1",
          "SELECT 2",
        ]);
        expect(yield* split("SELECT 1 # c; d\n+ 1; SELECT 2", "mysql")).toEqual([
          "SELECT 1  \n+ 1",
          "SELECT 2",
        ]);
        expect(yield* split("SELECT '{}'::jsonb #> '{}'; SELECT 2", "postgres")).toEqual([
          "SELECT '{}'::jsonb #> '{}'",
          "SELECT 2",
        ]);
      }),
    tags,
  );

  it.effect(
    "needs whitespace after `--` for a MySQL comment",
    () =>
      Effect.gen(function* () {
        expect(yield* split("SELECT 1--1; SELECT 2", "mysql")).toEqual(["SELECT 1--1", "SELECT 2"]);
        expect(yield* split("SELECT 1 -- c; d\n; SELECT 2", "mysql")).toEqual([
          "SELECT 1",
          "SELECT 2",
        ]);
      }),
    tags,
  );

  it.effect(
    "nests block comments in PostgreSQL and ClickHouse",
    () =>
      Effect.gen(function* () {
        expect(yield* split("/* a /* b; */ c; */ SELECT 1; SELECT 2", "postgres")).toEqual([
          "SELECT 1",
          "SELECT 2",
        ]);
        expect(yield* split("/* a /* b; */ c; */ SELECT 1; SELECT 2", "clickhouse")).toEqual([
          "SELECT 1",
          "SELECT 2",
        ]);
        expect(yield* split("/* a /* b */ SELECT 1; SELECT 2", "googlesql")).toEqual([
          "SELECT 1",
          "SELECT 2",
        ]);
      }),
    tags,
  );

  it.effect(
    "keeps `;` inside PostgreSQL strings, E-strings, identifiers, and dollar quotes",
    () =>
      Effect.gen(function* () {
        expect(
          yield* split(
            "INSERT INTO t VALUES ('it''s; me', E'esc\\'; still', \"we\"\"ird;\"); SELECT $$a;b$$, $tag$c;$$d$tag$",
            "postgres",
          ),
        ).toEqual([
          "INSERT INTO t VALUES ('it''s; me', E'esc\\'; still', \"we\"\"ird;\")",
          "SELECT $$a;b$$, $tag$c;$$d$tag$",
        ]);
        // A word ending in `e` before a quote is not an E-string prefix.
        expect(yield* split("SELECT CASE WHEN x THEN 'a\\' ELSE'b'; SELECT 2", "postgres")).toEqual(
          ["SELECT CASE WHEN x THEN 'a\\' ELSE'b'", "SELECT 2"],
        );
      }),
    tags,
  );

  it.effect(
    "keeps `;` inside MySQL strings with backslash and doubled-quote escapes",
    () =>
      Effect.gen(function* () {
        expect(
          yield* split("INSERT INTO `t;x` VALUES ('a\\';b', 'c'';d', \"e;f\"); SELECT 2", "mysql"),
        ).toEqual(["INSERT INTO `t;x` VALUES ('a\\';b', 'c'';d', \"e;f\")", "SELECT 2"]);
      }),
    tags,
  );

  it.effect(
    "keeps semicolons inside ClickHouse escaped quotes, Unicode quotes and heredocs",
    () =>
      Effect.gen(function* () {
        const quoted = "SELECT 'it''s; -- text', 'a\\';b', \"col\\\";#x\", `col\\`;x`, 'c\\\\'";
        const unicode = "SELECT “col; -- # \\\\”, ‘value; -- # \\’ FROM “table; /* data */”";
        const heredoc = "SELECT $tag$raw; ' \\ -- /* $$ $other$ # text$tag$";
        expect(yield* split(`${quoted}; ${unicode}; ${heredoc}; SELECT 2`, "clickhouse")).toEqual([
          quoted,
          unicode,
          heredoc,
          "SELECT 2",
        ]);
      }),
    tags,
  );

  it.effect(
    "removes ClickHouse line comments without requiring whitespace",
    () =>
      Effect.gen(function* () {
        expect(
          yield* split(
            "--header;\nSELECT 1; # comment;\nSELECT 2; #!comment;\nSELECT 3; //comment;\nSELECT 4",
            "clickhouse",
          ),
        ).toEqual(["SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4"]);
      }),
    tags,
  );

  it.effect(
    "keeps `;` inside GoogleSQL strings, triple quotes, raw strings, and identifiers",
    () =>
      Effect.gen(function* () {
        expect(
          yield* split(
            "INSERT INTO `t;x` (a) VALUES ('it\\'s; -- no comment'); SELECT '''tri;ple ''q'' ''', \"\"\"x;\"\"\"",
            "googlesql",
          ),
        ).toEqual([
          "INSERT INTO `t;x` (a) VALUES ('it\\'s; -- no comment')",
          "SELECT '''tri;ple ''q'' ''', \"\"\"x;\"\"\"",
        ]);
        // In a raw string a backslash doesn't escape, so `r'a\'` ends there.
        expect(yield* split("SELECT r'a\\'; SELECT 2", "googlesql")).toEqual([
          "SELECT r'a\\'",
          "SELECT 2",
        ]);
      }),
    tags,
  );
});

describe("scanSqlScript", () => {
  it.effect(
    "masks literals and comments at the same offsets as the source",
    () =>
      Effect.sync(() => {
        const [statement] = scanSqlScript("CREATE INDEX \"i;x\" /* c */ ON t ('lit')", "postgres");
        expect(statement!.code.length).toEqual(statement!.sql.length);
        // `"i;x"` (5) masks to `_` + 4 spaces; ` /* c */ ` (9) blanks out.
        expect(statement!.code).toEqual(`CREATE INDEX _${" ".repeat(4 + 9)}ON t (_    )`);
        const on = statement!.code.indexOf(" ON ");
        expect(statement!.sql.slice(on)).toEqual(" ON t ('lit')");
      }),
    tags,
  );
});

describe("scanSqlScript alignment", () => {
  it.effect(
    "masks ClickHouse heredocs and quoted names while preserving keyword offsets",
    () =>
      Effect.sync(() => {
        const [statement] = scanSqlScript(
          '/* skip */ SELECT $body$DROP; \' \\ -- # /*$body$, `ALTER; TABLE`, "DELETE;", “INSERT;”, ‘VALUES;’ FROM t',
          "clickhouse",
        );
        expect(statement!.code.length).toEqual(statement!.sql.length);
        expect(statement!.code).not.toMatch(/DROP|ALTER|TABLE|DELETE|INSERT|VALUES/);
        const at = statement!.code.indexOf("FROM");
        expect(statement!.sql.slice(at)).toEqual("FROM t");
        expect(statement!.unterminated).toBeUndefined();
      }),
    tags,
  );

  it.effect(
    "flags unclosed ClickHouse quotes, heredocs, and nested or trailing comments",
    () =>
      Effect.sync(() => {
        for (const sql of [
          "SELECT 'oops\\",
          'SELECT "oops',
          "SELECT `oops",
          "SELECT “oops",
          "SELECT ‘oops",
          "SELECT $tag$oops$$",
        ]) {
          expect(scanSqlScript(sql, "clickhouse")[0]!.unterminated).toBe("quote");
        }
        for (const sql of ["SELECT 1 /* outer /* inner */", "/* oops", "SELECT 1; /* oops"]) {
          expect(scanSqlScript(sql, "clickhouse").at(-1)!.unterminated).toBe("comment");
        }
      }),
    tags,
  );

  it.effect(
    "keeps `code` aligned with `sql` across leading whitespace and comments",
    () =>
      Effect.sync(() => {
        const statements = scanSqlScript(
          "SELECT 1;\n  -- next\n  CREATE INDEX i ON t (a)",
          "postgres",
        );
        const second = statements[1]!;
        expect(second.code.length).toEqual(second.sql.length);
        const at = second.code.indexOf("CREATE");
        expect(second.sql.slice(at, at + 6)).toEqual("CREATE");
        expect(second.text).toEqual("CREATE INDEX i ON t (a)");
      }),
    tags,
  );

  it.effect(
    "flags a quote or comment that never closes",
    () =>
      Effect.sync(() => {
        expect(scanSqlScript("SELECT 'oops", "postgres")[0]!.unterminated).toEqual("quote");
        expect(scanSqlScript("SELECT $$oops", "postgres")[0]!.unterminated).toEqual("quote");
        expect(scanSqlScript("SELECT 1 /* oops", "googlesql")[0]!.unterminated).toEqual("comment");
        expect(scanSqlScript("SELECT 'ok'", "postgres")[0]!.unterminated).toBeUndefined();
      }),
    tags,
  );
});
