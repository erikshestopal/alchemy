import * as ClickHouse from "@alchemy.run/clickhouse";
import * as Output from "alchemy/Output";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";

export const Service = Effect.gen(function* () {
  return yield* ClickHouse.Service("SqlBindingService", {
    organizationId: yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID"),
    provider: "aws",
    region: "us-east-1",
    minReplicaMemoryGb: 8,
    maxReplicaMemoryGb: 8,
    numReplicas: 1,
    idleScaling: true,
    idleTimeoutMinutes: 5,
    ipAccessList: [
      {
        source: yield* Config.String("CLICKHOUSE_SQL_TEST_DEPLOY_CIDR"),
        description: "SQL test deployer",
      },
      {
        source: yield* Config.String("CLICKHOUSE_SQL_TEST_WORKER_CIDR"),
        description: "SQL test Worker controlled egress",
      },
    ],
  });
});

const database = (id: string) =>
  Effect.gen(function* () {
    const service = yield* Service;
    return yield* ClickHouse.SQL.Database(id, {
      organizationId: service.organizationId,
      serviceId: service.serviceId,
      username: "default",
      password: service.password.pipe(
        Output.mapEffect((password) =>
          password
            ? Effect.succeed(password)
            : Effect.die(
                new Error("The disposable SQL fixture requires its service's initial password."),
              ),
        ),
      ),
      dropOnDelete: true,
    });
  });

export const Allowed = database("Allowed");
export const Denied = database("Denied");
export const username = "alchemy_sql_app";
