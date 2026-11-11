import { makeSqlClient } from "@distilled.cloud/clickhouse/SQL";
import * as Alchemist from "alchemy/Alchemist";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import Stack from "./alchemy.run.ts";

Effect.gen(function* () {
  const stage = yield* Config.String("ALCHEMY_STAGE").pipe(Config.withDefault("dev"));
  const plan = yield* Alchemist.plan({ stack: Stack, stage, operation: "deploy" });
  const db = yield* Alchemist.apply(plan);
  const endpoint = db.service.endpoints?.find((endpoint) => endpoint.protocol === "https");
  if (!endpoint?.host || !endpoint.port || !db.password) {
    return yield* Effect.fail(
      new Error("The service has no HTTPS endpoint or saved SQL password."),
    );
  }
  const sql = makeSqlClient({
    url: `https://${endpoint.host}:${endpoint.port}`,
    username: "default",
    password: db.password,
  });
  const rows = yield* sql.query({
    sql: "SELECT {message:String} AS message, version() AS version",
    parameters: { message: "Connected to ClickHouse Cloud" },
  });
  yield* Console.log(rows);
}).pipe(Effect.provide(Alchemist.layer()), Effect.scoped, Effect.runPromise);
