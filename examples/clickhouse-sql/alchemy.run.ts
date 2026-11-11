import * as ClickHouse from "@alchemy.run/clickhouse";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import Api from "./src/Api.ts";
import { Database, Service } from "./src/database.ts";

export default Alchemy.Stack(
  "ClickHouseSqlExample",
  {
    providers: Layer.mergeAll(ClickHouse.providers(), Cloudflare.providers()),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const service = yield* Service;
    const database = yield* Database;
    const api = yield* Api;
    return {
      organizationId: service.organizationId,
      serviceId: service.serviceId,
      database: database.name,
      databaseUuid: database.uuid,
      url: api.url.as<string>(),
    };
  }),
);
