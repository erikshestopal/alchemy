import * as ClickHouse from "@alchemy.run/clickhouse";
import { ScalingSchedule } from "@alchemy.run/clickhouse/ScalingSchedule";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Service from "@distilled.cloud/clickhouse/service";
import { adopt } from "alchemy/AdoptPolicy";
import * as Drift from "alchemy/Drift";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import { expectDeployFails } from "../lookup.ts";

const { test } = Test.make({ providers: ClickHouse.providers() });

test.provider.skipIf(
  !process.env.CLICKHOUSE_DISPOSABLE_SERVICE_ID || !process.env.CLICKHOUSE_TEST_ORGANIZATION_ID,
)(
  "disposable scaling schedule converges without replacing its restoration baseline",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
      const serviceId = yield* Config.String("CLICKHOUSE_DISPOSABLE_SERVICE_ID");
      const scope = { organizationId, serviceId };
      const entries = Service.getScalingSchedule(scope).pipe(
        Effect.flatMap(({ result }) =>
          result
            ? Effect.succeed(result.entries.map(({ id, isActiveNow, ...entry }) => entry))
            : Effect.fail(new Error("The disposable service must return its schedule.")),
        ),
        Effect.catchTag("ScalingScheduleNotFound", () => Effect.succeed([])),
      );
      const before = yield* entries;
      const baseline = (yield* Service.getInstance(scope)).result;
      if (!baseline) return yield* Effect.fail(new Error("The disposable service must exist."));
      const first = {
        ...scope,
        entries: [
          { name: `alchemy-${stack.stage}`, weekdays: [1], startHourUtc: 9, endHourUtc: 17 },
        ],
      };
      const updated = {
        ...scope,
        entries: [
          { name: `alchemy-${stack.stage}`, weekdays: [2], startHourUtc: 10, endHourUtc: 16 },
        ],
      };
      const declare = (props: typeof first) => ScalingSchedule("Schedule", props).pipe(adopt(true));
      const created = yield* stack.deploy(declare(first));
      expect(created.baseline.original).toHaveLength(before.length);
      expect(yield* entries).toMatchObject(first.entries);
      yield* stack.deploy(declare(updated));
      expect(yield* entries).toMatchObject(updated.entries);

      yield* Service.upsertScalingSchedule(first);
      const recovered = yield* stack.deploy(declare(first));
      expect(recovered.baseline.original).toEqual(created.baseline.original);
      expect(yield* entries).toMatchObject(first.entries);
      expect((yield* stack.plan(declare(first))).resources.Schedule?.action).toBe("noop");
      yield* stack.deploy(declare(first));

      yield* Service.upsertScalingSchedule(updated);
      yield* Drift.repair({ name: stack.name, stage: stack.stage }).pipe(
        Effect.provide(stack.state),
      );
      expect(yield* entries).toMatchObject(first.entries);
      yield* Service.upsertScalingSchedule(updated);
      yield* expectDeployFails(stack.destroy(), ["ScalingScheduleSafetyError"]);
      expect(yield* entries).toMatchObject(updated.entries);
      yield* Service.upsertScalingSchedule(first);
      yield* stack.destroy();
      const restored = yield* entries;
      expect(restored).toHaveLength(before.length);
      expect(restored).toEqual(expect.arrayContaining(before));
      expect((yield* Service.getInstance(scope)).result).toMatchObject({
        minReplicaMemoryGb: baseline.minReplicaMemoryGb,
        maxReplicaMemoryGb: baseline.maxReplicaMemoryGb,
        numReplicas: baseline.numReplicas,
        idleScaling: baseline.idleScaling,
        idleTimeoutMinutes: baseline.idleTimeoutMinutes,
      });
      yield* stack.destroy();
    }),
  {
    timeout: 120_000,
    exclusive: "clickhouse:disposable-service-settings",
    tags: ["live", "provider:clickhouse", "provider:clickhouse:schedule"],
  },
);
