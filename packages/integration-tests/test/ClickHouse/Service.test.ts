import * as ClickHouse from "@alchemy.run/clickhouse";
import { Service, type ServiceProps } from "@alchemy.run/clickhouse/Service";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Retry from "@distilled.cloud/clickhouse/Retry";
import * as Services from "@distilled.cloud/clickhouse/service";
import * as Alchemy from "alchemy";
import { adopt } from "alchemy/AdoptPolicy";
import * as Alchemist from "alchemy/Alchemist";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import { expectDeployFails } from "../lookup.ts";
import { testName } from "../names.ts";

const { test } = Test.make({ providers: ClickHouse.providers() });
const live = test.provider.skipIf(process.env.CLICKHOUSE_TEST_SLOW !== "1");
const options = {
  timeout: 900_000,
  retry: 0,
  exclusive: "clickhouse:disposable-services",
  tags: ["live", "provider:clickhouse", "provider:clickhouse:service"],
} as const;

const missing = (organizationId: string, serviceId: string) =>
  Services.getInstance({ organizationId, serviceId }).pipe(
    Effect.as(false),
    Effect.catchTag("ServiceNotFound", () => Effect.succeed(true)),
  );

live(
  "partial updates preserve neighboring settings and explicit empty sets clear them",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
      const initial: ServiceProps = {
        organizationId,
        provider: "aws",
        region: "us-east-1",
        minReplicaMemoryGb: 8,
        maxReplicaMemoryGb: 8,
        numReplicas: 1,
        idleScaling: true,
        idleTimeoutMinutes: 15,
        ipAccessList: [{ source: "192.0.2.0/24", description: "Test only" }],
        tags: { purpose: "test", flag: undefined },
      };
      const created = yield* stack.deploy(Service("Service", initial));
      expect(Redacted.isRedacted(created.password)).toBe(true);
      const identity = { organizationId, serviceId: created.serviceId };
      const before = (yield* Services.getInstance(identity)).result!;
      const renamed: ServiceProps = {
        organizationId,
        provider: "aws",
        region: "us-east-1",
        name: `${before.name!.slice(0, 42)} renamed`,
      };
      const updated = yield* stack.deploy(Service("Service", renamed));
      expect(updated.serviceId).toBe(created.serviceId);
      expect(Redacted.value(updated.password!)).toBe(Redacted.value(created.password!));
      const after = (yield* Services.getInstance(identity)).result!;
      expect(after.name).toBe(renamed.name);
      expect(after.ipAccessList).toEqual(before.ipAccessList);
      expect(after.tags).toEqual(before.tags);
      expect(after.idleTimeoutMinutes).toBe(15);

      const clear = { ...renamed, ipAccessList: [], privateEndpointIds: [], tags: {} };
      yield* stack.deploy(Service("Service", clear));
      const cleared = (yield* Services.getInstance(identity)).result!;
      expect(cleared.ipAccessList).toEqual([]);
      expect(cleared.privateEndpointIds).toEqual([]);
      expect(cleared.tags?.some(({ key }) => key === "purpose" || key === "flag")).toBe(false);
      expect((yield* stack.plan(Service("Service", clear))).resources.Service.action).toBe("noop");
      yield* stack.deploy(Service("Service", clear));
      yield* stack.destroy();
      expect(yield* missing(organizationId, created.serviceId)).toBe(true);
    }),
  options,
);

for (const [provider, region, replicas] of [
  ["aws", "us-east-1", 1],
  ["gcp", "us-east1", 2],
  ["azure", "eastus2", 2],
] as const) {
  live(
    `${provider} creates and updates memory, replica and idle scaling without replacement`,
    (stack) =>
      Effect.gen(function* () {
        yield* stack.destroy();
        const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
        const initial: ServiceProps = {
          organizationId,
          provider,
          region,
          autoscalingMode: "vertical",
          minReplicaMemoryGb: 8,
          maxReplicaMemoryGb: 8,
          numReplicas: replicas,
          idleScaling: true,
          idleTimeoutMinutes: 5,
          ipAccessList: [{ source: "192.0.2.0/24", description: "Options test only" }],
          tags: { purpose: "configuration-test" },
        };
        const created = yield* stack.deploy(Service("Service", initial));
        expect(Redacted.isRedacted(created.password)).toBe(true);
        const identity = { organizationId, serviceId: created.serviceId };
        const before = (yield* Services.getInstance(identity)).result!;
        expect(before).toMatchObject({
          provider,
          region,
          minReplicaMemoryGb: 8,
          maxReplicaMemoryGb: 8,
          numReplicas: replicas,
          idleScaling: true,
          idleTimeoutMinutes: 5,
        });
        expect((yield* stack.plan(Service("Service", initial))).resources.Service.action).toBe(
          "noop",
        );
        const { numReplicas: _fixedCount, ...props } = initial;
        const scaled: ServiceProps = {
          ...props,
          maxReplicaMemoryGb: 16,
          minReplicas: replicas,
          maxReplicas: replicas,
          idleTimeoutMinutes: 10,
        };
        const updated = yield* stack.deploy(Service("Service", scaled));
        expect(updated.serviceId).toBe(created.serviceId);
        expect(Redacted.value(updated.password!)).toBe(Redacted.value(created.password!));
        const after = (yield* Services.getInstance(identity)).result!;
        expect(after.minReplicaMemoryGb).toBe(8);
        expect(after.maxReplicaMemoryGb).toBe(16);
        expect(after.minReplicas ?? after.numReplicas).toBe(replicas);
        expect(after.maxReplicas ?? after.numReplicas).toBe(replicas);
        expect(after.idleTimeoutMinutes).toBe(10);
        expect(after.ipAccessList).toEqual(before.ipAccessList);
        expect(after.tags).toEqual(before.tags);
        expect((yield* stack.plan(Service("Service", scaled))).resources.Service.action).toBe(
          "noop",
        );
        yield* stack.destroy();
        expect(yield* missing(organizationId, created.serviceId)).toBe(true);
      }),
    options,
  );
}

live(
  "an interrupted create resumes the same tagged generation without a new service or password reset",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
      const props: ServiceProps = {
        organizationId,
        provider: "aws",
        region: "us-east-1",
        name: testName("alchemy-interrupted-create"),
        minReplicaMemoryGb: 8,
        maxReplicaMemoryGb: 8,
        numReplicas: 1,
        ipAccessList: [],
      };
      const fiber = yield* stack.deploy(Service("Service", props)).pipe(Effect.forkChild);
      const created = yield* Services.instanceGetList({ organizationId }).pipe(
        Effect.map(({ result }) => result?.find(({ name }) => name === props.name)),
        Effect.repeat({
          schedule: Schedule.spaced("500 millis"),
          times: 120,
          until: (value) => !!value?.id,
        }),
      );
      expect(created?.id).toBeTruthy();
      yield* Fiber.interrupt(fiber);
      const interrupted = yield* Fiber.await(fiber);
      expect(Exit.isFailure(interrupted)).toBe(true);
      const recovered = yield* stack.deploy(Service("Service", props));
      expect(recovered.serviceId).toBe(created!.id);
      expect(recovered.password).toBeUndefined();
      const matches = (yield* Services.instanceGetList({ organizationId })).result!.filter(
        ({ name }) => name === props.name,
      );
      expect(matches.map(({ id }) => id)).toEqual([created!.id]);
      expect((yield* stack.plan(Service("Service", props))).resources.Service.action).toBe("noop");
      yield* stack.destroy();
      expect(yield* missing(organizationId, recovered.serviceId)).toBe(true);
    }),
  options,
);

live(
  "a missing managed service is recreated by drift repair",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
      const props: ServiceProps = {
        organizationId,
        provider: "aws",
        region: "us-east-1",
        minReplicaMemoryGb: 8,
        maxReplicaMemoryGb: 8,
        numReplicas: 1,
        ipAccessList: [],
      };
      const created = yield* stack.deploy(Service("Service", props));
      yield* Services.updateInstanceState({
        organizationId,
        serviceId: created.serviceId,
        command: "stop",
      });
      const stopped = yield* Services.getInstance({
        organizationId,
        serviceId: created.serviceId,
      }).pipe(
        Effect.repeat({
          schedule: Schedule.spaced("10 seconds"),
          times: 30,
          until: ({ result }) => result?.state === "stopped",
        }),
      );
      expect(stopped.result?.state).toBe("stopped");
      yield* Services.deleteInstance({ organizationId, serviceId: created.serviceId });
      const gone = yield* missing(organizationId, created.serviceId).pipe(
        Effect.repeat({ schedule: Schedule.spaced("2 seconds"), times: 30, until: Boolean }),
      );
      expect(gone).toBe(true);
      const definition = Alchemy.Stack(
        stack.name,
        { state: Alchemy.localState(), providers: ClickHouse.providers() },
        Service("Service", props),
      );
      const scan = yield* Alchemist.Drift.scan({ stack: definition, stage: stack.stage });
      expect(scan.resources.find(({ fqn }) => fqn === "Service")?.status).toBe("missing");
      yield* Alchemist.Drift.execute(scan);
      const recovered = yield* stack.deploy(Service("Service", props));
      expect(recovered.serviceId).not.toBe(created.serviceId);
      expect(
        (yield* Services.getInstance({ organizationId, serviceId: recovered.serviceId })).result
          ?.state,
      ).toMatch(/^(running|idle)$/);
      yield* stack.destroy();
      expect(yield* missing(organizationId, recovered.serviceId)).toBe(true);
    }),
  options,
);

live(
  "region replacement creates a new generation and removes the original without requiring omitted options",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
      const initial: ServiceProps = {
        organizationId,
        provider: "aws",
        region: "us-east-1",
        minReplicaMemoryGb: 8,
        maxReplicaMemoryGb: 8,
        numReplicas: 1,
        ipAccessList: [],
      };
      const created = yield* stack.deploy(Service("Service", initial));
      const replacement = { ...initial, region: "us-west-2" };
      expect((yield* stack.plan(Service("Service", replacement))).resources.Service.action).toBe(
        "replace",
      );
      const replaced = yield* stack.deploy(Service("Service", replacement));
      expect(replaced.serviceId).not.toBe(created.serviceId);
      expect(replaced.service.region).toBe("us-west-2");
      expect(yield* missing(organizationId, created.serviceId)).toBe(true);
      const found = yield* stack.deploy(
        Effect.gen(function* () {
          const service = yield* Service("Service", replacement);
          const lookup = Service.lookup({ organizationId, serviceId: service.serviceId });
          return { serviceId: lookup.serviceId };
        }),
      );
      expect(found.serviceId).toBe(replaced.serviceId);
      yield* stack.destroy();
      expect(yield* missing(organizationId, replaced.serviceId)).toBe(true);
    }),
  options,
);

live(
  "an exact foreign UUID requires adoption and never recovers its one-time password",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_TEST_ORGANIZATION_ID");
      const foreign = yield* Services.createInstance({
        organizationId,
        name: testName("alchemy-foreign-service"),
        provider: "aws",
        region: "us-east-1",
        minReplicaMemoryGb: 8,
        maxReplicaMemoryGb: 8,
        numReplicas: 1,
        ipAccessList: [],
      }).pipe(Retry.none);
      const serviceId = foreign.result?.service?.id;
      expect(serviceId).toBeTruthy();
      if (!serviceId)
        return yield* Effect.fail(new Error("Foreign fixture returned no service UUID."));
      const props: ServiceProps = {
        organizationId,
        serviceId,
        provider: "aws",
        region: "us-east-1",
      };
      const declaration = Service("Service", props);
      yield* expectDeployFails(stack.deploy(declaration), ["OwnedBySomeoneElse"]);
      expect((yield* Services.getInstance({ organizationId, serviceId })).result?.id).toBe(
        serviceId,
      );
      const adopted = yield* stack.deploy(declaration.pipe(adopt(true)));
      expect(adopted.serviceId).toBe(serviceId);
      expect(adopted.password).toBeUndefined();
      expect((yield* stack.plan(declaration)).resources.Service.action).toBe("noop");
      yield* stack.destroy();
      expect(yield* missing(organizationId, serviceId)).toBe(true);
    }),
  options,
);
