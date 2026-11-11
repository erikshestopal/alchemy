import * as ClickHouse from "@alchemy.run/clickhouse";
import { ServiceSetting } from "@alchemy.run/clickhouse/ServiceSetting";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Service from "@distilled.cloud/clickhouse/service";
import { adopt } from "alchemy/AdoptPolicy";
import * as Drift from "alchemy/Drift";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import { expectDeployFails } from "../lookup.ts";

const { test } = Test.make({ providers: ClickHouse.providers() });

test.skipIf(
  !process.env.CLICKHOUSE_DISPOSABLE_SERVICE_ID || !process.env.CLICKHOUSE_TEST_ORGANIZATION_ID,
)(
  "settings API reports its typed entitlement rejection on an unentitled fixture",
  Effect.gen(function* () {
    const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
    const serviceId = yield* Config.String("CLICKHOUSE_DISPOSABLE_SERVICE_ID");
    const error = yield* Service.serviceClickhouseSettingsListGet({
      organizationId,
      serviceId,
    }).pipe(Effect.flip);
    expect(error._tag).toBe("ServiceSettingsNotEnabled");
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        ClickHouse.ClickHouseAuth,
        ClickHouse.fromAuthProvider(),
        FetchHttpClient.layer,
      ),
    ),
  ),
  { tags: ["live", "provider:clickhouse", "provider:clickhouse:service-setting"] },
);

test.provider.skipIf(
  !process.env.CLICKHOUSE_DISPOSABLE_SERVICE_ID ||
    !process.env.CLICKHOUSE_TEST_ORGANIZATION_ID ||
    !process.env.CLICKHOUSE_DISPOSABLE_SETTING_NAME,
)(
  "disposable setting converges, preserves neighbors and restores its original override",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
      const serviceId = yield* Config.String("CLICKHOUSE_DISPOSABLE_SERVICE_ID");
      const name = yield* Config.String("CLICKHOUSE_DISPOSABLE_SETTING_NAME");
      const scope = { organizationId, serviceId };
      const settings = Effect.gen(function* () {
        const { result } = yield* Service.serviceClickhouseSettingsListGet(scope);
        if (!result?.settings)
          return yield* Effect.fail(new Error("The disposable service must return its settings."));
        return result.settings;
      });
      const before = yield* settings;
      expect(before.some((setting) => setting.name === name)).toBe(false);
      // The disposable beta setting must accept both string values "1" and "2".
      const declared = { ...scope, name, value: "1" };
      const created = yield* stack.deploy(ServiceSetting("Setting", declared));
      expect(created.baseline.original).toBe(null);
      expect((yield* settings).find((setting) => setting.name === name)?.value).toBe("1");

      yield* Service.updateServiceClickhouseSettings({ ...scope, settings: { [name]: "2" } });
      const updated = { ...declared, value: "2" };
      const recovered = yield* stack.deploy(ServiceSetting("Setting", updated));
      expect(recovered.baseline.original).toBe(null);
      expect(recovered.baseline.managed).toBe("2");
      expect((yield* settings).find((setting) => setting.name === name)?.value).toBe("2");
      expect(
        (yield* stack.plan(ServiceSetting("Setting", updated))).resources.Setting?.action,
      ).toBe("noop");
      yield* stack.deploy(ServiceSetting("Setting", updated));
      yield* expectDeployFails(
        stack.plan(ServiceSetting("Setting", { ...updated, name: `${name}-other` })),
        ["ServiceSettingSafetyError"],
      );

      yield* Service.updateServiceClickhouseSettings({ ...scope, settings: { [name]: "1" } });
      yield* Drift.repair({ name: stack.name, stage: stack.stage }).pipe(
        Effect.provide(stack.state),
      );
      expect((yield* settings).find((setting) => setting.name === name)?.value).toBe("2");
      expect((yield* settings).filter((setting) => setting.name !== name)).toEqual(before);

      yield* Service.updateServiceClickhouseSettings({ ...scope, settings: { [name]: "1" } });
      yield* expectDeployFails(stack.destroy(), ["ServiceSettingSafetyError"]);
      expect((yield* settings).find((setting) => setting.name === name)?.value).toBe("1");
      yield* Service.updateServiceClickhouseSettings({ ...scope, settings: { [name]: "2" } });
      yield* stack.destroy();
      expect(yield* settings).toEqual(before);
      yield* stack.destroy();

      yield* Service.updateServiceClickhouseSettings({ ...scope, settings: { [name]: "1" } });
      yield* expectDeployFails(stack.deploy(ServiceSetting("Setting", updated)), [
        "OwnedBySomeoneElse",
      ]);
      const adopted = yield* stack.deploy(ServiceSetting("Setting", updated).pipe(adopt(true)));
      expect(adopted.baseline.original).toBe("1");
      yield* stack.destroy();
      expect((yield* settings).find((setting) => setting.name === name)?.value).toBe("1");
      yield* Service.deleteServiceClickhouseSetting({ ...scope, settingName: name });
      expect(yield* settings).toEqual(before);
      yield* stack.destroy();
    }),
  {
    timeout: 120_000,
    exclusive: "clickhouse:disposable-service-settings",
    tags: ["live", "provider:clickhouse", "provider:clickhouse:service-setting"],
  },
);
