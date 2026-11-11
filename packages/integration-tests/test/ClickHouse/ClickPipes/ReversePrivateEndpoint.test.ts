import { ReversePrivateEndpoint } from "@alchemy.run/clickhouse/ClickPipes/ReversePrivateEndpoint";
import { providers } from "@alchemy.run/clickhouse/Providers";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as ClickPipes from "@distilled.cloud/clickhouse/click_pipes";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import { expectDeployFails } from "../../lookup.ts";

const { test } = Test.make({ providers: providers() });

// The endpoint service must allow ClickHouse's principal without manual acceptance.
test.provider.skipIf(
  process.env.CLICKHOUSE_TEST_SLOW !== "1" ||
    !process.env.CLICKHOUSE_CLICKPIPES_TEST_ORGANIZATION_ID ||
    !process.env.CLICKHOUSE_CLICKPIPES_TEST_SERVICE_ID ||
    !process.env.CLICKHOUSE_CLICKPIPES_TEST_VPC_ENDPOINT_SERVICE,
)(
  "reverse endpoint updates DNS, protects foreign targets, replaces and deletes",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_CLICKPIPES_TEST_ORGANIZATION_ID");
      const serviceId = yield* Config.String("CLICKHOUSE_CLICKPIPES_TEST_SERVICE_ID");
      const vpcEndpointServiceName = yield* Config.String(
        "CLICKHOUSE_CLICKPIPES_TEST_VPC_ENDPOINT_SERVICE",
      );
      const scope = { organizationId, serviceId };
      const props = {
        ...scope,
        description: "Alchemy test",
        target: { type: "VPC_ENDPOINT_SERVICE", vpcEndpointServiceName },
      } as const;
      const created = yield* stack.deploy(ReversePrivateEndpoint("Endpoint", props));
      expect(
        (yield* ClickPipes.getClickPipeReversePrivateEndpoint({
          ...scope,
          reversePrivateEndpointId: created.reversePrivateEndpointId,
        })).result,
      ).toMatchObject({ id: created.reversePrivateEndpointId, vpcEndpointServiceName });
      yield* expectDeployFails(stack.plan(ReversePrivateEndpoint("Foreign", props)), [
        "OwnedBySomeoneElse",
      ]);
      const mappings = [{ privateDnsName: "source.alchemy-test.internal" }];
      const updated = ReversePrivateEndpoint("Endpoint", {
        ...props,
        customPrivateDnsMappings: mappings,
      });
      expect((yield* stack.deploy(updated)).reversePrivateEndpointId).toBe(
        created.reversePrivateEndpointId,
      );
      expect(
        (yield* ClickPipes.getClickPipeReversePrivateEndpoint({
          ...scope,
          reversePrivateEndpointId: created.reversePrivateEndpointId,
        })).result?.customPrivateDnsMappings,
      ).toEqual(mappings);
      expect((yield* stack.plan(updated)).resources.Endpoint.action).toBe("noop");
      yield* stack.deploy(updated);
      const cleared = ReversePrivateEndpoint("Endpoint", {
        ...props,
        customPrivateDnsMappings: [],
      });
      yield* stack.deploy(cleared);
      expect(
        (yield* ClickPipes.getClickPipeReversePrivateEndpoint({
          ...scope,
          reversePrivateEndpointId: created.reversePrivateEndpointId,
        })).result?.customPrivateDnsMappings,
      ).toEqual([]);
      const replacement = ReversePrivateEndpoint("Endpoint", {
        ...props,
        description: "Alchemy replacement",
      });
      yield* expectDeployFails(
        stack.plan(
          ReversePrivateEndpoint("Endpoint", {
            ...props,
            reversePrivateEndpointId: created.reversePrivateEndpointId,
            description: "Alchemy replacement",
          }),
        ),
        ["ReversePrivateEndpointSafetyError", "same endpoint UUID"],
      );
      expect((yield* stack.plan(replacement)).resources.Endpoint.action).toBe("replace");
      const replaced = yield* stack.deploy(replacement);
      expect(replaced.reversePrivateEndpointId).not.toBe(created.reversePrivateEndpointId);
      yield* stack.destroy();
      const remaining = yield* ClickPipes.clickPipeReversePrivateEndpointGetList(scope).pipe(
        Effect.repeat({
          schedule: Schedule.spaced("1 minute"),
          times: 10,
          until: ({ result }) =>
            result !== undefined &&
            !result.some(
              (endpoint) =>
                endpoint.id === created.reversePrivateEndpointId ||
                endpoint.id === replaced.reversePrivateEndpointId,
            ),
        }),
      );
      expect(
        remaining.result?.some(
          (endpoint) =>
            endpoint.id === created.reversePrivateEndpointId ||
            endpoint.id === replaced.reversePrivateEndpointId,
        ),
      ).toBe(false);
    }),
  {
    timeout: 3_000_000,
    exclusive: "clickhouse:disposable-service",
    tags: ["live", "provider:clickhouse", "provider:clickhouse:clickpipes"],
  },
);
