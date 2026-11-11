import * as ClickHouse from "@alchemy.run/clickhouse";
import * as Output from "alchemy/Output";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";

export const Service = Effect.gen(function* () {
  const organizationId = yield* Config.String("CLICKHOUSE_ORGANIZATION_ID");
  const deployCidr = yield* Config.String("CLICKHOUSE_DEPLOY_CIDR");
  const workerCidr = yield* Config.String("CLICKHOUSE_WORKER_CIDR");
  const destroyData = yield* Config.Boolean("CLICKHOUSE_SQL_DESTROY_DATA").pipe(
    Config.withDefault(false),
  );
  return yield* ClickHouse.Service("Service", {
    organizationId,
    provider: "aws",
    region: "us-east-1",
    minReplicaMemoryGb: 8,
    maxReplicaMemoryGb: 8,
    numReplicas: 1,
    idleScaling: true,
    idleTimeoutMinutes: 5,
    ipAccessList: [
      { source: deployCidr, description: "Deployment SQL access" },
      { source: workerCidr, description: "Worker SQL access" },
    ],
  }).pipe(RemovalPolicy.retain(!destroyData));
});

export const Database = Effect.gen(function* () {
  const service = yield* Service;
  const destroyData = yield* Config.Boolean("CLICKHOUSE_SQL_DESTROY_DATA").pipe(
    Config.withDefault(false),
  );
  return yield* ClickHouse.SQL.Database("Analytics", {
    organizationId: service.organizationId,
    serviceId: service.serviceId,
    username: "default",
    password: service.password.pipe(
      Output.mapEffect((password) =>
        password
          ? Effect.succeed(password)
          : Effect.die(new Error("The example requires its saved initial SQL password.")),
      ),
    ),
    migrations: "./migrations",
    dropOnDelete: destroyData,
  });
});
