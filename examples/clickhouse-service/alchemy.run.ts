import * as ClickHouse from "@alchemy.run/clickhouse";
import * as Alchemy from "alchemy";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";

export default Alchemy.Stack(
  "ClickHouseServiceExample",
  { providers: ClickHouse.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const organizationId = yield* Config.String("CLICKHOUSE_ORGANIZATION_ID");
    const cidr = yield* Config.String("CLICKHOUSE_ALLOWED_CIDR");
    const name = yield* Config.String("CLICKHOUSE_SERVICE_NAME").pipe(
      Config.withDefault("Alchemy analytics"),
    );
    const db = yield* ClickHouse.Service("Database", {
      organizationId,
      name,
      provider: "aws",
      region: "us-east-1",
      minReplicaMemoryGb: 8,
      maxReplicaMemoryGb: 8,
      numReplicas: 1,
      idleScaling: true,
      idleTimeoutMinutes: 5,
      ipAccessList: [{ source: cidr, description: "Application access" }],
    });
    return { organizationId, serviceId: db.serviceId, service: db.service, password: db.password };
  }),
);
