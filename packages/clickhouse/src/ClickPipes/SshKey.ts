import * as ClickPipes from "@distilled.cloud/clickhouse/click_pipes";
import * as Retry from "@distilled.cloud/clickhouse/Retry";
import { OwnedBySomeoneElse, Unowned } from "alchemy/AdoptPolicy";
import { isResolved } from "alchemy/Diff";
import { createPhysicalName } from "alchemy/PhysicalName";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { Providers } from "../Providers.ts";
import { markedDescription, waitForObserved } from "./internal.ts";

export interface SshKeyProps {
  /** Organization containing the service. */
  organizationId: string;
  /** Service containing the key. */
  serviceId: string;
  /** Existing key UUID for explicit adoption. */
  sshKeyId?: string;
  /** Immutable key name. A physical name is generated when omitted. */
  name?: string;
  /** Immutable description, at most 180 characters; Alchemy reserves 75 for its ownership marker. */
  description?: string;
  /** SSH bastion hostname. */
  host: string;
  /** SSH bastion port. */
  port: number;
  /** SSH username on the bastion. */
  username: string;
}

export interface SshKeyAttributes {
  /** Organization containing the service. */
  organizationId: string;
  /** Service containing the key. */
  serviceId: string;
  /** Key UUID. */
  sshKeyId: string;
  /** Public key to install on the bastion. The private key remains with ClickHouse. */
  publicKey: string;
  /** Current validation status, if returned. */
  status: string | undefined;
  /** Most recent validation message, if returned. */
  statusMessage: string | undefined;
  /** Current key name, if returned. */
  name: string | undefined;
}

export interface SshKey extends Resource<
  "ClickHouse.ClickPipes.SshKey",
  SshKeyProps,
  SshKeyAttributes,
  never,
  Providers
> {}

/**
 * A service-scoped SSH key for a ClickPipe bastion. ClickHouse generates the
 * keypair; install the returned public key on the bastion before validating it.
 * Creation never performs a connectivity check. These operations are documented
 * by the ClickHouse Terraform provider, not the public OpenAPI specification.
 *
 * ### Create a key
 * **Example:** Generate a public key for a bastion
 * ```typescript
 * import * as Alchemy from "alchemy";
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 * import * as Effect from "effect/Effect";
 *
 * export default Alchemy.Stack("Ingestion", {
 *   providers: ClickHouse.providers(), state: Alchemy.localState(),
 * }, Effect.gen(function* () {
 *   return yield* ClickHouse.ClickPipes.SshKey("Bastion", {
 *     organizationId: "00000000-0000-4000-8000-000000000001",
 *     serviceId: "00000000-0000-4000-8000-000000000002",
 *     host: "bastion.example.com", port: 22, username: "clickpipes",
 *   });
 * }));
 * ```
 *
 * ### Ownership and replacement
 * Keys have no ownership tags. New keys carry an `[alchemy:<sha256>]` suffix
 * in their description, binding the full stack/stage/FQN, generation and create
 * configuration. Only an exact marker match recovers an interrupted create;
 * names and parent-service ownership never prove ownership. Explicit adoption requires the key UUID and
 * `Alchemy.AdoptPolicy.adopt(true)`. All configuration is immutable: changing it replaces
 * the key, deleting the old key first, and requires installing the new public
 * key. A failed create is never automatically retried. Older unmarked keys
 * require their exact UUID and explicit adoption.
 * Deleting this resource deletes the key but does not modify the bastion.
 *
 * @see https://github.com/ClickHouse/terraform-provider-clickhouse/tree/dc26b13d87ba0ef0b75205ddf9decc0a0e1e64e2/internal/api
 *
 * @resource
 * @product ClickPipes
 */
export const SshKey = Resource<SshKey>("ClickHouse.ClickPipes.SshKey");

export class SshKeySafetyError extends Data.TaggedError("SshKeySafetyError")<{
  message: string;
}> {}

type Scope = { organizationId: string; serviceId: string };

const list = Effect.fn(function* (scope: Scope) {
  const { result } = yield* ClickPipes.clickPipeSshKeyGetList({
    organizationId: scope.organizationId,
    serviceId: scope.serviceId,
  }).pipe(Effect.catchTag("ClickPipesServiceNotFound", () => Effect.succeed({ result: [] })));
  if (!result || result.some((key) => !key.id)) {
    return yield* new SshKeySafetyError({
      message: "SSH key listing did not include complete identities.",
    });
  }
  return result;
});

const attributes = Effect.fn(function* (scope: Scope, key: ClickPipes.ClickPipeSshKey) {
  if (!key.id || !key.publicKey) {
    return yield* new SshKeySafetyError({
      message: "SSH key response omitted its ID or public key.",
    });
  }
  return {
    ...scope,
    sshKeyId: key.id,
    publicKey: key.publicKey,
    status: key.status,
    statusMessage: key.statusMessage,
    name: key.name,
  };
});

const assertConfiguration = Effect.fn(function* (
  key: ClickPipes.ClickPipeSshKey,
  props: SshKeyProps,
) {
  const description = yield* keyDescription(props);
  if (
    key.host !== props.host ||
    key.port !== props.port ||
    key.username !== props.username ||
    (props.name !== undefined && key.name !== props.name) ||
    (key.description !== description && key.description !== props.description)
  ) {
    return yield* new SshKeySafetyError({
      message: "The existing key does not match the immutable desired configuration.",
    });
  }
});

const keyDescription = (props: SshKeyProps) =>
  markedDescription(props.description, {
    organizationId: props.organizationId,
    serviceId: props.serviceId,
    name: props.name,
    description: props.description,
    host: props.host,
    port: props.port,
    username: props.username,
  });

/** Validate connectivity only after installing the public key on the bastion. */
export const validateSshKey = (scope: Scope & { sshKeyId: string }) =>
  ClickPipes.validateClickPipeSshKey({ ...scope, body: {} }).pipe(Retry.none);

export const SshKeyProvider = () =>
  Provider.succeed(SshKey, {
    nuke: { skip: true },
    stables: ["organizationId", "serviceId", "sshKeyId", "publicKey"],
    diff: Effect.fn(function* ({ news, output }) {
      if (!isResolved(news) || !output) return;
      const key = (yield* list(output)).find((item) => item.id === output.sshKeyId);
      if (!key) return { action: "update", stables: ["organizationId", "serviceId"] };
      if (
        news.organizationId !== output.organizationId ||
        news.serviceId !== output.serviceId ||
        (news.sshKeyId !== undefined && news.sshKeyId !== output.sshKeyId) ||
        (news.name !== undefined && key.name !== news.name) ||
        (key.description !== news.description &&
          key.description !== (yield* keyDescription(news))) ||
        key.host !== news.host ||
        key.port !== news.port ||
        key.username !== news.username
      ) {
        if (news.sshKeyId === output.sshKeyId)
          return yield* new SshKeySafetyError({
            message: "A replacement cannot reuse the same SSH key UUID. Omit sshKeyId first.",
          });
        return { action: "replace", deleteFirst: true } as const;
      }
    }),
    read: Effect.fn(function* ({ id, olds, output, previous }) {
      const scope = {
        organizationId: output?.organizationId ?? olds.organizationId,
        serviceId: output?.serviceId ?? olds.serviceId,
      };
      const keys = yield* list(scope);
      const keyId = output?.sshKeyId ?? olds.sshKeyId;
      const name = olds.name ?? (yield* createPhysicalName({ id, maxLength: 64 }));
      const description = yield* keyDescription(olds);
      const owned = keys.filter((key) => key.description === description);
      const matches = keyId
        ? keys.filter((key) => key.id === keyId)
        : owned.length
          ? owned
          : keys.filter((key) => key.name === name);
      if (matches.length > 1)
        return yield* new SshKeySafetyError({
          message: "Multiple SSH keys match; select a UUID for adoption.",
        });
      const key = matches[0];
      if (!key) return;
      const result = yield* attributes(scope, key);
      if (
        output ||
        key.description === (yield* keyDescription(olds)) ||
        (previous?.organizationId === scope.organizationId &&
          previous.serviceId === scope.serviceId &&
          previous.sshKeyId === result.sshKeyId)
      )
        return result;
      if (!keyId)
        return yield* new OwnedBySomeoneElse({
          message: "Select the exact key UUID and opt into adoption.",
          resourceType: SshKey.Type,
          logicalId: id,
          physicalName: result.sshKeyId,
        });
      return Unowned(result);
    }),
    reconcile: Effect.fn(function* ({ id, news, output }) {
      if (
        output &&
        (news.organizationId !== output.organizationId ||
          news.serviceId !== output.serviceId ||
          (news.sshKeyId !== undefined && news.sshKeyId !== output.sshKeyId))
      )
        return yield* new SshKeySafetyError({
          message: "SSH key identity changed without replacement.",
        });
      const scope = { organizationId: news.organizationId, serviceId: news.serviceId };
      const keys = yield* list(scope);
      const keyId = output?.sshKeyId ?? news.sshKeyId;
      const name = news.name ?? (yield* createPhysicalName({ id, maxLength: 64 }));
      const description = yield* keyDescription(news);
      const owned = keys.filter((key) => key.description === description);
      const matches = keyId
        ? keys.filter((key) => key.id === keyId)
        : owned.length
          ? owned
          : keys.filter((key) => key.name === name);
      if (matches.length > 1)
        return yield* new SshKeySafetyError({
          message: "Multiple SSH keys match; select a UUID for adoption.",
        });
      const existing = matches[0];
      if (existing) {
        if (!output && existing.description !== (yield* keyDescription(news))) {
          const attrs = yield* attributes(scope, existing);
          return yield* new OwnedBySomeoneElse({
            message: "Select the exact key UUID and opt into adoption.",
            resourceType: SshKey.Type,
            logicalId: id,
            physicalName: attrs.sshKeyId,
          });
        }
        yield* assertConfiguration(existing, news);
        return yield* attributes(scope, existing);
      }
      if (news.sshKeyId)
        return yield* new SshKeySafetyError({
          message: "The explicitly selected SSH key is missing; remove sshKeyId to create one.",
        });
      if (description.length > 255)
        return yield* new SshKeySafetyError({
          message:
            "SSH description plus Alchemy's 75-character ownership suffix exceeds 255 characters.",
        });
      const { result } = yield* ClickPipes.createClickPipeSshKey({
        ...scope,
        name,
        description: yield* keyDescription(news),
        host: news.host,
        port: news.port,
        username: news.username,
      }).pipe(Retry.none);
      if (!result)
        return yield* new SshKeySafetyError({
          message: "SSH key creation returned no key; inspect the service before retrying.",
        });
      return yield* attributes(scope, result);
    }),
    delete: Effect.fn(function* ({ output }) {
      const scope = { organizationId: output.organizationId, serviceId: output.serviceId };
      const keys = yield* list(scope);
      if (!keys.some((key) => key.id === output.sshKeyId)) return;
      yield* ClickPipes.deleteClickPipeSshKey({ ...scope, sshKeyId: output.sshKeyId }).pipe(
        Retry.none,
        Effect.catchTag("ClickPipeSshKeyNotFound", () => Effect.void),
      );
      yield* waitForObserved({
        description: `SSH key ${output.sshKeyId} deletion`,
        observe: list(scope),
        ready: (keys) => !keys.some((key) => key.id === output.sshKeyId),
      });
    }),
  });
