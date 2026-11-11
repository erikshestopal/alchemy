import * as ClickHouse from "@alchemy.run/clickhouse";
import {
  BackupConfiguration,
  type BackupConfigurationProps,
} from "@alchemy.run/clickhouse/BackupConfiguration";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Backups from "@distilled.cloud/clickhouse/backup";
import { adopt } from "alchemy/AdoptPolicy";
import * as Drift from "alchemy/Drift";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import { expectDeployFails } from "../lookup.ts";

const { test } = Test.make({ providers: ClickHouse.providers() });

test.provider.skipIf(
  !process.env.CLICKHOUSE_DISPOSABLE_ORGANIZATION_ID ||
    !process.env.CLICKHOUSE_DISPOSABLE_SERVICE_ID,
)(
  "disposable backup settings converge, reject uncertain resets and restore the original values",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_DISPOSABLE_ORGANIZATION_ID");
      const serviceId = yield* Config.String("CLICKHOUSE_DISPOSABLE_SERVICE_ID");
      const scope = { organizationId, serviceId };
      const configuration = Effect.gen(function* () {
        const { result } = yield* Backups.getBackupConfiguration(scope);
        if (
          !result ||
          typeof result.backupPeriodInHours !== "number" ||
          typeof result.backupRetentionPeriodInHours !== "number"
        )
          return yield* Effect.fail(
            new Error("The disposable service must return its backup interval and retention."),
          );
        return result;
      });
      const before = yield* configuration;
      const retention = before.backupRetentionPeriodInHours === 336 ? 168 : 336;
      const otherRetention = retention === 336 ? 168 : 336;
      const first = { ...scope, backupPeriodInHours: 12, backupRetentionPeriodInHours: retention };
      const updated = { ...first, backupRetentionPeriodInHours: otherRetention };
      const declare = (props: BackupConfigurationProps) =>
        BackupConfiguration("Backups", props).pipe(adopt(true));
      const created = yield* stack.deploy(declare(first));
      expect(created.baseline.backupRetentionPeriodInHours?.original).toBe(
        before.backupRetentionPeriodInHours,
      );
      expect(yield* configuration).toMatchObject({
        backupPeriodInHours: 12,
        backupRetentionPeriodInHours: retention,
      });
      yield* stack.deploy(declare(updated));
      expect(yield* configuration).toMatchObject({
        backupPeriodInHours: 12,
        backupRetentionPeriodInHours: otherRetention,
      });
      expect((yield* configuration).backupStartTime).toBe(before.backupStartTime);
      yield* expectDeployFails(stack.plan(declare({ ...updated, backupStartTime: "02:00" })), [
        "ClickHouseBackupConfigurationSafetyError",
      ]);

      yield* Backups.updateBackupConfiguration({
        ...scope,
        backupRetentionPeriodInHours: retention,
      });
      const recovered = yield* stack.deploy(declare(first));
      expect(recovered.baseline.backupRetentionPeriodInHours?.original).toBe(
        before.backupRetentionPeriodInHours,
      );
      expect(recovered.baseline.backupPeriodInHours?.original).toBe(before.backupPeriodInHours);
      expect(yield* configuration).toMatchObject({
        backupPeriodInHours: 12,
        backupRetentionPeriodInHours: retention,
      });
      yield* Backups.updateBackupConfiguration({
        ...scope,
        backupRetentionPeriodInHours: otherRetention,
      });
      yield* Drift.repair({ name: stack.name, stage: stack.stage }).pipe(
        Effect.provide(stack.state),
      );
      expect((yield* configuration).backupRetentionPeriodInHours).toBe(retention);

      const reset = { ...first, backupRetentionPeriodInHours: null };
      yield* stack.deploy(declare(reset));
      const resetValue = (yield* configuration).backupRetentionPeriodInHours;
      expect(typeof resetValue).toBe("number");
      expect((yield* configuration).backupPeriodInHours).toBe(12);
      expect((yield* configuration).backupStartTime).toBe(before.backupStartTime);
      expect((yield* stack.plan(declare(reset))).resources.Backups?.action).toBe("noop");
      yield* stack.deploy(declare(reset));

      const foreign = resetValue === 48 ? 72 : 48;
      yield* Backups.updateBackupConfiguration({ ...scope, backupRetentionPeriodInHours: foreign });
      yield* expectDeployFails(stack.deploy(declare(reset), { force: true }), [
        "ClickHouseBackupConfigurationSafetyError",
      ]);
      expect((yield* configuration).backupRetentionPeriodInHours).toBe(foreign);
      const explicit = { ...first, backupRetentionPeriodInHours: foreign };
      const acknowledged = yield* stack.deploy(declare(explicit));
      expect(acknowledged.baseline.backupRetentionPeriodInHours?.original).toBe(
        before.backupRetentionPeriodInHours,
      );
      expect((yield* stack.plan(declare(explicit))).resources.Backups?.action).toBe("noop");

      const laterForeign = [96, 120, 144].find(
        (value) => value !== before.backupRetentionPeriodInHours,
      )!;
      yield* Backups.updateBackupConfiguration({
        ...scope,
        backupRetentionPeriodInHours: laterForeign,
      });
      yield* expectDeployFails(stack.destroy(), ["ClickHouseBackupConfigurationSafetyError"]);
      expect((yield* configuration).backupRetentionPeriodInHours).toBe(laterForeign);
      yield* Backups.updateBackupConfiguration({ ...scope, backupRetentionPeriodInHours: foreign });
      yield* stack.destroy();
      expect(yield* configuration).toEqual(before);
      yield* stack.destroy();
    }),
  {
    timeout: 120_000,
    exclusive: "clickhouse:disposable-service-settings",
    tags: ["live", "provider:clickhouse", "provider:clickhouse:backup"],
  },
);
