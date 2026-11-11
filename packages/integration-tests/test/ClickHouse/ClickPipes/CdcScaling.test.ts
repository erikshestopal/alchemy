import { ClickHouseAuth } from "@alchemy.run/clickhouse/AuthProvider";
import { CdcScaling } from "@alchemy.run/clickhouse/ClickPipes/CdcScaling";
import { fromAuthProvider } from "@alchemy.run/clickhouse/Credentials";
import { providers } from "@alchemy.run/clickhouse/Providers";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as ClickPipes from "@distilled.cloud/clickhouse/click_pipes";
import { adopt } from "alchemy/AdoptPolicy";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import { expectDeployFails } from "../../lookup.ts";

const { test } = Test.make({ providers: providers() });

test.skipIf(
  !process.env.CLICKHOUSE_DISPOSABLE_ORGANIZATION_ID ||
    !process.env.CLICKHOUSE_DISPOSABLE_SERVICE_ID,
)(
  "CDC scaling reports its typed prerequisite on a fixture without CDC infrastructure",
  Effect.gen(function* () {
    const organizationId = yield* Config.String("CLICKHOUSE_DISPOSABLE_ORGANIZATION_ID");
    const serviceId = yield* Config.String("CLICKHOUSE_DISPOSABLE_SERVICE_ID");
    const error = yield* ClickPipes.getClickPipeCdcScaling({ organizationId, serviceId }).pipe(
      Effect.flip,
    );
    expect(error._tag).toBe("CdcInfrastructureNotProvisioned");
  }).pipe(
    Effect.provide(Layer.mergeAll(ClickHouseAuth, fromAuthProvider(), FetchHttpClient.layer)),
  ),
  { tags: ["live", "provider:clickhouse", "provider:clickhouse:clickpipes"] },
);

// Requires an existing database ClickPipe to provision the service-wide scaler.
test.provider.skipIf(
  process.env.CLICKHOUSE_TEST_SLOW !== "1" ||
    !process.env.CLICKHOUSE_CLICKPIPES_TEST_ORGANIZATION_ID ||
    !process.env.CLICKHOUSE_CLICKPIPES_TEST_SERVICE_ID,
)(
  "CDC scaling requires adoption, updates, redeploys unchanged and restores its baseline",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_CLICKPIPES_TEST_ORGANIZATION_ID");
      const serviceId = yield* Config.String("CLICKHOUSE_CLICKPIPES_TEST_SERVICE_ID");
      const scope = { organizationId, serviceId };
      const before = (yield* ClickPipes.getClickPipeCdcScaling(scope)).result;
      if (before?.replicaCpuMillicores === undefined || before.replicaMemoryGb === undefined)
        return yield* Effect.fail(
          new Error("The disposable service needs a provisioned CDC scaler."),
        );
      const cpu = before.replicaCpuMillicores === 2000 ? 1000 : 2000;
      const target = { ...scope, replicaCpuMillicores: cpu, replicaMemoryGb: cpu / 250 };
      yield* expectDeployFails(stack.plan(CdcScaling("Scaling", target)), ["OwnedBySomeoneElse"]);
      const declaration = CdcScaling("Scaling", target).pipe(adopt(true));
      const deployed = yield* stack.deploy(declaration);
      expect(deployed.initialScaling).toEqual(before);
      expect((yield* ClickPipes.getClickPipeCdcScaling(scope)).result).toMatchObject({
        replicaCpuMillicores: cpu,
        replicaMemoryGb: cpu / 250,
      });
      expect((yield* stack.plan(declaration)).resources.Scaling.action).toBe("noop");
      yield* stack.deploy(declaration);
      yield* stack.destroy();
      expect((yield* ClickPipes.getClickPipeCdcScaling(scope)).result).toEqual(before);
    }),
  {
    timeout: 1_500_000,
    exclusive: "clickhouse:disposable-service",
    tags: ["live", "provider:clickhouse", "provider:clickhouse:clickpipes"],
  },
);
