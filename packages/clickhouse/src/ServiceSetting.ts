import * as Retry from "@distilled.cloud/clickhouse/Retry";
import * as Service from "@distilled.cloud/clickhouse/service";
import { Unowned } from "alchemy/AdoptPolicy";
import { isResolved } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { Providers } from "./Providers.ts";

export interface ServiceSettingProps {
  /** Organization containing the service. Remove the control before changing it. */
  organizationId: string;
  /** Service containing the setting. Remove the control before changing it. */
  serviceId: string;
  /** Name of the single setting to manage. Remove the control before changing it. */
  name: string;
  /** Explicit setting value. An empty string is a value; null is not supported. */
  value: string | number;
}

export interface ServiceSettingAttributes {
  /** Organization containing the service. */
  organizationId: string;
  /** Service containing the setting. */
  serviceId: string;
  /** Managed setting name. */
  name: string;
  /** Current explicit value, or null if no override exists. */
  value: string | number | null;
  /** Saved ownership, original override, and last acknowledged value. */
  baseline: {
    /** Owning resource address. */
    fqn: string;
    /** Owning resource instance. */
    instanceId: string;
    /** Original explicit override; null means the platform default was in effect. */
    original: string | number | null;
    /** Last acknowledged explicit override. */
    managed: string | number | null;
  };
}

export interface ServiceSetting extends Resource<
  "ClickHouse.ServiceSetting",
  ServiceSettingProps,
  ServiceSettingAttributes,
  never,
  Providers
> {}

/**
 * Manage one explicit ClickHouse service setting without changing other keys.
 * The beta settings API must be enabled for the organization by ClickHouse.
 * The deployment key needs permission to view and manage the service.
 * Some settings restart the ClickHouse server. Do not put secrets in values:
 * they are stored in resource state.
 *
 * Existing overrides require explicit adoption. The original override is
 * saved before writes and restored on destroy; when no override existed,
 * destroy resets this one name to the platform default. The effective default
 * itself is not captured. Redeploy converges to the explicit desired value,
 * including after a lost update response. Destroy refuses to overwrite later
 * foreign changes and keeps the original restoration baseline.
 * Remove this control before changing its organization, service or name.
 * The API provides no conditional write, so serialize concurrent writers.
 *
 * ### Manage one setting
 * **Example:** Adopt an existing override
 * ```typescript
 * import * as Alchemy from "alchemy";
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 *
 * const setting = yield* ClickHouse.ServiceSetting("Timeout", {
 *   organizationId: "00000000-0000-4000-8000-000000000001",
 *   serviceId: "00000000-0000-4000-8000-000000000002",
 *   name: "keep_alive_timeout",
 *   value: 30,
 * }).pipe(Alchemy.AdoptPolicy.adopt(true));
 * ```
 *
 * @resource
 * @product Service
 */
export const ServiceSetting = Resource<ServiceSetting>("ClickHouse.ServiceSetting");

export class ServiceSettingSafetyError extends Data.TaggedError("ServiceSettingSafetyError")<{
  message: string;
  reason:
    | "invalid-input"
    | "incomplete-response"
    | "invalid-baseline"
    | "scope-change"
    | "not-converged"
    | "foreign-change";
  settingName?: string;
  expected?: string | number | null;
  observed?: string | number | null;
}> {}

const validValue = (value: unknown): value is string | number =>
  typeof value === "string" || (typeof value === "number" && Number.isFinite(value));

const validate = (props: ServiceSettingProps) =>
  props.organizationId?.trim() &&
  props.serviceId?.trim() &&
  props.name?.trim() &&
  validValue(props.value)
    ? Effect.void
    : Effect.fail(
        new ServiceSettingSafetyError({
          reason: "invalid-input",
          message: "A scope, setting name and non-null value are required.",
        }),
      );

const observe = Effect.fn(function* (
  scope: Pick<ServiceSettingProps, "organizationId" | "serviceId" | "name">,
) {
  const { result } = yield* Service.serviceClickhouseSettingsListGet({
    organizationId: scope.organizationId,
    serviceId: scope.serviceId,
  });
  if (
    !result?.settings ||
    result.settings.some(
      (setting) => typeof setting.name !== "string" || !validValue(setting.value),
    )
  ) {
    return yield* new ServiceSettingSafetyError({
      reason: "incomplete-response",
      message: "The service did not return a complete settings list.",
    });
  }
  const matches = result.settings.filter((setting) => setting.name === scope.name);
  if (matches.length > 1)
    return yield* new ServiceSettingSafetyError({
      reason: "incomplete-response",
      settingName: scope.name,
      message: "The settings list contains duplicate names.",
    });
  return matches[0]?.value ?? null;
});

const baselineOf = Effect.fn(function* (
  output: ServiceSettingAttributes | undefined,
  owner: { fqn: string; instanceId: string },
) {
  const baseline = output?.baseline;
  if (
    !baseline ||
    baseline.fqn !== owner.fqn ||
    baseline.instanceId !== owner.instanceId ||
    (baseline.original !== null && !validValue(baseline.original)) ||
    (baseline.managed !== null && !validValue(baseline.managed))
  ) {
    return yield* new ServiceSettingSafetyError({
      reason: "invalid-baseline",
      message:
        "A checkpointed setting baseline is required; recover through read/adoption before writing.",
    });
  }
  return baseline;
});

const sameScope = (props: ServiceSettingProps, output: ServiceSettingAttributes) =>
  props.organizationId === output.organizationId &&
  props.serviceId === output.serviceId &&
  props.name === output.name;

export const ServiceSettingProvider = () =>
  Provider.succeed(ServiceSetting, {
    nuke: { singleton: true },
    stables: ["organizationId", "serviceId", "name"],
    diff: Effect.fn(function* ({ news, output, fqn, instanceId }) {
      if (!isResolved(news)) return;
      yield* validate(news);
      if (!output) return;
      const baseline = yield* baselineOf(output, { fqn, instanceId });
      if (!sameScope(news, output))
        return yield* new ServiceSettingSafetyError({
          reason: "scope-change",
          message: "Restore and remove this control before changing its scope or setting name.",
        });
      if (output.value !== news.value || baseline.managed !== news.value)
        return { action: "update" } as const;
    }),
    read: Effect.fn(function* ({ olds, output, fqn, instanceId }) {
      const scope = output ?? olds;
      if (!output) yield* validate(olds);
      const current = yield* observe(scope);
      if (output) {
        yield* baselineOf(output, { fqn, instanceId });
        return { ...output, value: current };
      }
      const attrs: ServiceSettingAttributes = {
        organizationId: scope.organizationId,
        serviceId: scope.serviceId,
        name: scope.name,
        value: current,
        baseline: { fqn, instanceId, original: current, managed: current },
      };
      return current === null ? attrs : Unowned(attrs);
    }),
    reconcile: Effect.fn(function* ({ news, output, fqn, instanceId }) {
      yield* validate(news);
      const baseline = yield* baselineOf(output, { fqn, instanceId });
      if (!output)
        return yield* new ServiceSettingSafetyError({
          reason: "invalid-baseline",
          message: "A persisted baseline is required.",
        });
      if (!sameScope(news, output))
        return yield* new ServiceSettingSafetyError({
          reason: "scope-change",
          message: "Restore and remove this control before changing its scope or setting name.",
        });
      const current = yield* observe(output);
      if (current !== news.value) {
        yield* Service.updateServiceClickhouseSettings({
          organizationId: output.organizationId,
          serviceId: output.serviceId,
          settings: { [output.name]: news.value },
        }).pipe(Retry.none);
      }
      const updated = yield* observe(output);
      if (updated !== news.value)
        return yield* new ServiceSettingSafetyError({
          reason: "not-converged",
          settingName: output.name,
          expected: news.value,
          observed: updated,
          message: "The service setting did not converge.",
        });
      return { ...output, value: updated, baseline: { ...baseline, managed: updated } };
    }),
    delete: Effect.fn(function* ({ output, fqn, instanceId }) {
      const baseline = yield* baselineOf(output, { fqn, instanceId });
      const serviceMissing = yield* Service.getInstance({
        organizationId: output.organizationId,
        serviceId: output.serviceId,
      }).pipe(
        Effect.as(false),
        Effect.catchTag("ServiceNotFound", () => Effect.succeed(true)),
      );
      if (serviceMissing) return;
      const current = yield* observe(output);
      if (current === baseline.original) return;
      if (current !== baseline.managed)
        return yield* new ServiceSettingSafetyError({
          reason: "foreign-change",
          settingName: output.name,
          expected: baseline.managed,
          observed: current,
          message: "Refusing to restore an externally changed setting.",
        });
      const scope = { organizationId: output.organizationId, serviceId: output.serviceId };
      if (baseline.original === null) {
        yield* Service.deleteServiceClickhouseSetting({ ...scope, settingName: output.name }).pipe(
          Retry.none,
        );
      } else {
        yield* Service.updateServiceClickhouseSettings({
          ...scope,
          settings: { [output.name]: baseline.original },
        }).pipe(Retry.none);
      }
      if ((yield* observe(output)) !== baseline.original)
        return yield* new ServiceSettingSafetyError({
          reason: "not-converged",
          settingName: output.name,
          expected: baseline.original,
          message: "The original setting override was not restored.",
        });
    }),
  });
