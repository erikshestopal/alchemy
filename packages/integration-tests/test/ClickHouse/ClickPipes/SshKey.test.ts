import { SshKey, type SshKeyProps } from "@alchemy.run/clickhouse/ClickPipes/SshKey";
import { providers } from "@alchemy.run/clickhouse/Providers";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as ClickPipes from "@distilled.cloud/clickhouse/click_pipes";
import * as Retry from "@distilled.cloud/clickhouse/Retry";
import * as Command from "alchemy/Command";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import { expectDeployFails } from "../../lookup.ts";

const { test } = Test.make({ providers: Layer.merge(providers(), Command.providers()) });

test.provider.skipIf(
  process.env.CLICKHOUSE_TEST_SLOW !== "1" ||
    !process.env.CLICKHOUSE_CLICKPIPES_TEST_ORGANIZATION_ID ||
    !process.env.CLICKHOUSE_CLICKPIPES_TEST_SERVICE_ID,
)(
  "SSH key redeploy, replacement and foreign-name protection on a disposable service",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const organizationId = yield* Config.String("CLICKHOUSE_CLICKPIPES_TEST_ORGANIZATION_ID");
      const serviceId = yield* Config.String("CLICKHOUSE_CLICKPIPES_TEST_SERVICE_ID");
      const scope = { organizationId, serviceId };
      const props = { ...scope, host: "bastion.example.invalid", port: 22, username: "clickpipes" };
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const file = path.join(yield* fs.makeTempDirectoryScoped(), "installed-key");
      const declare = (props: SshKeyProps) =>
        Effect.gen(function* () {
          const key = yield* SshKey("Key", props);
          yield* Command.Exec("Consumer", {
            command: 'printf "%s\\n%s" "$KEY_ID" "$PUBLIC_KEY" > "$DESTINATION"',
            shell: true,
            memo: false,
            env: { KEY_ID: key.sshKeyId, PUBLIC_KEY: key.publicKey, DESTINATION: file },
          });
          return key;
        });
      const declaration = declare(props);
      const created = yield* stack.deploy(declaration);
      expect(created.publicKey).toMatch(/^ssh-/);
      const observed = (yield* ClickPipes.getClickPipeSshKey({
        ...scope,
        sshKeyId: created.sshKeyId,
      })).result;
      expect(observed).toMatchObject({
        id: created.sshKeyId,
        host: props.host,
        port: 22,
        username: props.username,
      });
      expect((yield* stack.deploy(declaration)).sshKeyId).toBe(created.sshKeyId);
      expect((yield* stack.plan(declaration)).resources.Key.action).toBe("noop");
      yield* expectDeployFails(
        stack.plan(declare({ ...props, sshKeyId: created.sshKeyId, host: "new.example.invalid" })),
        ["SshKeySafetyError", "same SSH key UUID"],
      );
      expect(
        (yield* ClickPipes.getClickPipeSshKey({ ...scope, sshKeyId: created.sshKeyId })).result?.id,
      ).toBe(created.sshKeyId);
      yield* ClickPipes.deleteClickPipeSshKey({ ...scope, sshKeyId: created.sshKeyId }).pipe(
        Retry.none,
      );
      const removed = yield* ClickPipes.clickPipeSshKeyGetList(scope).pipe(
        Effect.repeat({
          schedule: Schedule.spaced("15 seconds"),
          times: 8,
          until: ({ result }) =>
            result !== undefined && !result.some((key) => key.id === created.sshKeyId),
        }),
      );
      expect(removed.result?.some((key) => key.id === created.sshKeyId)).toBe(false);
      const recovered = yield* stack.deploy(declaration);
      expect(recovered.sshKeyId).not.toBe(created.sshKeyId);
      expect(recovered.publicKey).not.toBe(created.publicKey);
      expect(yield* fs.readFileString(file)).toBe(`${recovered.sshKeyId}\n${recovered.publicKey}`);
      if (!observed?.name)
        return yield* Effect.fail(new Error("SSH key response omitted its name."));
      yield* expectDeployFails(stack.plan(SshKey("Foreign", { ...props, name: observed.name })), [
        "OwnedBySomeoneElse",
      ]);
      const replacement = declare({ ...props, host: "replacement.example.invalid" });
      expect((yield* stack.plan(replacement)).resources.Key.action).toBe("replace");
      const replaced = yield* stack.deploy(replacement);
      expect(replaced.sshKeyId).not.toBe(recovered.sshKeyId);
      expect(replaced.publicKey).not.toBe(recovered.publicKey);
      expect(
        (yield* ClickPipes.getClickPipeSshKey({ ...scope, sshKeyId: replaced.sshKeyId })).result,
      ).toMatchObject({ host: "replacement.example.invalid" });
      yield* stack.destroy();
      const remaining = yield* ClickPipes.clickPipeSshKeyGetList(scope).pipe(
        Effect.repeat({
          schedule: Schedule.spaced("15 seconds"),
          times: 8,
          until: ({ result }) =>
            result !== undefined &&
            !result.some((key) =>
              [created.sshKeyId, recovered.sshKeyId, replaced.sshKeyId].includes(key.id!),
            ),
        }),
      );
      expect(
        remaining.result?.some((key) =>
          [created.sshKeyId, recovered.sshKeyId, replaced.sshKeyId].includes(key.id!),
        ),
      ).toBe(false);
    }),
  {
    timeout: 300_000,
    exclusive: "clickhouse:disposable-service",
    tags: ["live", "provider:clickhouse", "provider:clickhouse:clickpipes"],
  },
);
