import * as Backups from "@distilled.cloud/clickhouse/backup";
import * as Retry from "@distilled.cloud/clickhouse/Retry";
import * as Service from "@distilled.cloud/clickhouse/service";
import { Unowned } from "alchemy/AdoptPolicy";
import { isResolved } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { Providers } from "./Providers.ts";

export interface BackupConfigurationProps {
  /** Existing organization; remove the control before changing it. */
  organizationId: string;
  /** Existing service; remove the control before changing it. */
  serviceId: string;
  /** Backup interval in hours. Omit to preserve; null asks ClickHouse to reset it. */
  backupPeriodInHours?: number | null;
  /** Retention in hours, a whole number of days from 24 to 1080. Null resets it. */
  backupRetentionPeriodInHours?: number | null;
  /** UTC time in HH:MM. Setting a time also resets the backup interval to 24 hours. */
  backupStartTime?: string | null;
}

type Field<T> = { original: T; managed: T; reset: boolean };
export interface BackupConfigurationAttributes {
  /** Organization owning the service. */
  organizationId: string;
  /** Service whose backup settings are managed. */
  serviceId: string;
  /** Last observed values of the managed fields, without changing their restoration baseline. */
  observed?: Backups.BackupConfiguration;
  /** Checkpointed original and last observed values. */
  baseline: {
    fqn: string;
    instanceId: string;
    backupPeriodInHours?: Field<number>;
    backupRetentionPeriodInHours?: Field<number>;
    backupStartTime?: Field<string>;
  };
}

export interface BackupConfiguration extends Resource<
  "ClickHouse.BackupConfiguration",
  BackupConfigurationProps,
  BackupConfigurationAttributes,
  never,
  Providers
> {}

/**
 * Manage selected backup settings of an existing ClickHouse Cloud service.
 * Adoption checkpoints the original values; destroy restores them only while
 * they still match the last values this control wrote. Omission leaves a field
 * alone; null asks ClickHouse to reset it. Remove the control before changing
 * its service or managed fields. Missing original or reset values fail safely.
 *
 * Setting a backup start time resets the interval to 24 hours. This control
 * refuses that change unless the observed interval is already 24 hours, and
 * refuses a conflicting declared interval. There is no conditional write;
 * serialize writers. Redeploy converges explicit values after a lost update
 * response. An uncertain reset needs an explicit recovery value because its
 * platform default cannot safely be inferred.
 *
 * ### Adopt backup retention
 * **Example:** Keep backups for a week
 * ```typescript
 * import * as Alchemy from "alchemy";
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 *
 * const backups = yield* ClickHouse.BackupConfiguration("Backups", {
 *   organizationId: "00000000-0000-4000-8000-000000000001",
 *   serviceId: "00000000-0000-4000-8000-000000000002",
 *   backupRetentionPeriodInHours: 168,
 * }).pipe(Alchemy.AdoptPolicy.adopt(true));
 * ```
 *
 * @resource
 * @product Backup
 */
export const BackupConfiguration = Resource<BackupConfiguration>("ClickHouse.BackupConfiguration");

export class BackupConfigurationSafetyError extends Data.TaggedError(
  "ClickHouseBackupConfigurationSafetyError",
)<{
  message: string;
  reason:
    | "invalid-input"
    | "incomplete-response"
    | "invalid-baseline"
    | "scope-change"
    | "interval-side-effect"
    | "ambiguous-reset"
    | "foreign-change"
    | "not-converged";
  field?: keyof Backups.BackupConfiguration;
  expected?: string | number;
  observed?: string | number;
}> {}
const fail = (reason: BackupConfigurationSafetyError["reason"], message: string) =>
  Effect.fail(new BackupConfigurationSafetyError({ reason, message }));

const validate = (p: BackupConfigurationProps) => {
  const period = p.backupPeriodInHours;
  const retention = p.backupRetentionPeriodInHours;
  const start = p.backupStartTime;
  return typeof p.organizationId === "string" &&
    !!p.organizationId.trim() &&
    typeof p.serviceId === "string" &&
    !!p.serviceId.trim() &&
    (period !== undefined || retention !== undefined || start !== undefined) &&
    (period == null || (typeof period === "number" && Number.isFinite(period) && period > 0)) &&
    (retention == null ||
      (typeof retention === "number" &&
        Number.isInteger(retention) &&
        retention >= 24 &&
        retention <= 1080 &&
        retention % 24 === 0)) &&
    (start == null || (typeof start === "string" && /^([01]\d|2[0-3]):[0-5]\d$/.test(start)))
    ? Effect.void
    : fail("invalid-input", "Invalid backup settings, organization or service.");
};

const observe = Effect.fn(function* (identity: { organizationId: string; serviceId: string }) {
  const { result } = yield* Backups.getBackupConfiguration({
    organizationId: identity.organizationId,
    serviceId: identity.serviceId,
  });
  if (!result)
    return yield* fail("incomplete-response", "ClickHouse omitted the backup configuration.");
  return result;
});

const checkBaseline = Effect.fn(function* (
  output: BackupConfigurationAttributes | undefined,
  owner: { fqn: string; instanceId: string },
) {
  const b = output?.baseline;
  if (
    !output ||
    !b ||
    b.fqn !== owner.fqn ||
    b.instanceId !== owner.instanceId ||
    (!b.backupPeriodInHours && !b.backupRetentionPeriodInHours && !b.backupStartTime) ||
    (b.backupPeriodInHours &&
      (typeof b.backupPeriodInHours.original !== "number" ||
        typeof b.backupPeriodInHours.managed !== "number" ||
        typeof b.backupPeriodInHours.reset !== "boolean")) ||
    (b.backupRetentionPeriodInHours &&
      (typeof b.backupRetentionPeriodInHours.original !== "number" ||
        typeof b.backupRetentionPeriodInHours.managed !== "number" ||
        typeof b.backupRetentionPeriodInHours.reset !== "boolean")) ||
    (b.backupStartTime &&
      (typeof b.backupStartTime.original !== "string" ||
        typeof b.backupStartTime.managed !== "string" ||
        typeof b.backupStartTime.reset !== "boolean"))
  )
    return yield* fail(
      "invalid-baseline",
      "A checkpointed backup baseline is required; recover through read/adoption before writing.",
    );
  return b;
});

const checkScope = (p: BackupConfigurationProps, o: BackupConfigurationAttributes) =>
  p.organizationId === o.organizationId &&
  p.serviceId === o.serviceId &&
  (p.backupPeriodInHours !== undefined) === (o.baseline.backupPeriodInHours !== undefined) &&
  (p.backupRetentionPeriodInHours !== undefined) ===
    (o.baseline.backupRetentionPeriodInHours !== undefined) &&
  (p.backupStartTime !== undefined) === (o.baseline.backupStartTime !== undefined)
    ? Effect.void
    : fail(
        "scope-change",
        "Restore and remove this control before changing its service or managed fields.",
      );

const observedFields = Effect.fn(function* (
  b: BackupConfigurationAttributes["baseline"],
  c: Backups.BackupConfiguration,
) {
  if (
    (b.backupPeriodInHours && typeof c.backupPeriodInHours !== "number") ||
    (b.backupRetentionPeriodInHours && typeof c.backupRetentionPeriodInHours !== "number") ||
    (b.backupStartTime && typeof c.backupStartTime !== "string")
  )
    return yield* fail("incomplete-response", "ClickHouse omitted a managed backup setting.");
  return {
    ...(b.backupPeriodInHours ? { backupPeriodInHours: c.backupPeriodInHours } : {}),
    ...(b.backupRetentionPeriodInHours
      ? { backupRetentionPeriodInHours: c.backupRetentionPeriodInHours }
      : {}),
    ...(b.backupStartTime ? { backupStartTime: c.backupStartTime } : {}),
  };
});

const needs = <T>(
  field: Field<T> | undefined,
  desired: T | null | undefined,
  current = field?.managed,
) =>
  field !== undefined &&
  (desired === null ? !field.reset || current !== field.managed : desired !== current);

const checkReset = <T extends string | number>(
  field: Field<T> | undefined,
  desired: T | null | undefined,
  current: T | undefined,
  name: keyof Backups.BackupConfiguration,
) =>
  field && desired === null && current !== field.managed
    ? Effect.fail(
        new BackupConfigurationSafetyError({
          reason: "ambiguous-reset",
          field: name,
          expected: field.managed,
          observed: current,
          message:
            "A reset's default cannot be inferred after drift or a lost response; declare an explicit value to recover.",
        }),
      )
    : Effect.void;

const checkStart = (
  c: Backups.BackupConfiguration,
  start: string | null | undefined,
  period: number | null | undefined,
) =>
  start === undefined || (c.backupPeriodInHours === 24 && (period === undefined || period === 24))
    ? Effect.void
    : fail(
        "interval-side-effect",
        "Setting backupStartTime changes the interval; it must already be 24 hours and not conflict with the desired interval.",
      );

const checkRestoration = Effect.fn(function* (
  b: BackupConfigurationAttributes["baseline"],
  c: Backups.BackupConfiguration,
) {
  for (const field of [
    "backupPeriodInHours",
    "backupRetentionPeriodInHours",
    "backupStartTime",
  ] as const) {
    const saved = b[field];
    if (saved && c[field] !== saved.original && c[field] !== saved.managed)
      return yield* new BackupConfigurationSafetyError({
        reason: "foreign-change",
        field,
        expected: saved.managed,
        observed: c[field],
        message: "Refusing to restore externally changed backup settings.",
      });
  }
});

export const BackupConfigurationProvider = () =>
  Provider.succeed(BackupConfiguration, {
    nuke: { singleton: true },
    stables: ["organizationId", "serviceId"],
    diff: Effect.fn(function* ({ news, output, fqn, instanceId }) {
      if (!isResolved(news)) return;
      if (!output) return yield* validate(news);
      const b = yield* checkBaseline(output, { fqn, instanceId });
      yield* checkScope(news, output);
      yield* validate(news);
      const c = output.observed;
      if (
        needs(b.backupPeriodInHours, news.backupPeriodInHours, c?.backupPeriodInHours) ||
        needs(
          b.backupRetentionPeriodInHours,
          news.backupRetentionPeriodInHours,
          c?.backupRetentionPeriodInHours,
        ) ||
        needs(b.backupStartTime, news.backupStartTime, c?.backupStartTime) ||
        needs(b.backupPeriodInHours, news.backupPeriodInHours) ||
        needs(b.backupRetentionPeriodInHours, news.backupRetentionPeriodInHours) ||
        needs(b.backupStartTime, news.backupStartTime)
      )
        return { action: "update" } as const;
    }),
    read: Effect.fn(function* ({ olds, output, fqn, instanceId }) {
      if (!output) yield* validate(olds);
      const identity = output ?? olds;
      const c = yield* observe(identity);
      if (output) {
        const b = yield* checkBaseline(output, { fqn, instanceId });
        return { ...output, observed: yield* observedFields(b, c) };
      }
      if (
        (olds.backupPeriodInHours !== undefined && typeof c.backupPeriodInHours !== "number") ||
        (olds.backupRetentionPeriodInHours !== undefined &&
          typeof c.backupRetentionPeriodInHours !== "number") ||
        (olds.backupStartTime !== undefined && typeof c.backupStartTime !== "string")
      )
        return yield* fail(
          "incomplete-response",
          "ClickHouse omitted a managed backup setting's original value.",
        );
      return Unowned({
        organizationId: identity.organizationId,
        serviceId: identity.serviceId,
        baseline: {
          fqn,
          instanceId,
          ...(olds.backupPeriodInHours !== undefined && c.backupPeriodInHours !== undefined
            ? {
                backupPeriodInHours: {
                  original: c.backupPeriodInHours,
                  managed: c.backupPeriodInHours,
                  reset: false,
                },
              }
            : {}),
          ...(olds.backupRetentionPeriodInHours !== undefined &&
          c.backupRetentionPeriodInHours !== undefined
            ? {
                backupRetentionPeriodInHours: {
                  original: c.backupRetentionPeriodInHours,
                  managed: c.backupRetentionPeriodInHours,
                  reset: false,
                },
              }
            : {}),
          ...(olds.backupStartTime !== undefined && c.backupStartTime !== undefined
            ? {
                backupStartTime: {
                  original: c.backupStartTime,
                  managed: c.backupStartTime,
                  reset: false,
                },
              }
            : {}),
        },
      });
    }),
    reconcile: Effect.fn(function* ({ news, output, fqn, instanceId }) {
      const b = yield* checkBaseline(output, { fqn, instanceId });
      if (!output)
        return yield* fail("invalid-baseline", "A persisted adoption baseline is required.");
      yield* checkScope(news, output);
      yield* validate(news);
      const c = yield* observe(output);
      yield* observedFields(b, c);
      yield* checkReset(
        b.backupPeriodInHours,
        news.backupPeriodInHours,
        c.backupPeriodInHours,
        "backupPeriodInHours",
      );
      yield* checkReset(
        b.backupRetentionPeriodInHours,
        news.backupRetentionPeriodInHours,
        c.backupRetentionPeriodInHours,
        "backupRetentionPeriodInHours",
      );
      yield* checkReset(
        b.backupStartTime,
        news.backupStartTime,
        c.backupStartTime,
        "backupStartTime",
      );
      const changes = {
        ...(needs(b.backupPeriodInHours, news.backupPeriodInHours, c.backupPeriodInHours)
          ? { backupPeriodInHours: news.backupPeriodInHours }
          : {}),
        ...(needs(
          b.backupRetentionPeriodInHours,
          news.backupRetentionPeriodInHours,
          c.backupRetentionPeriodInHours,
        )
          ? { backupRetentionPeriodInHours: news.backupRetentionPeriodInHours }
          : {}),
        ...(needs(b.backupStartTime, news.backupStartTime, c.backupStartTime)
          ? { backupStartTime: news.backupStartTime }
          : {}),
      };
      if ("backupStartTime" in changes)
        yield* checkStart(c, changes.backupStartTime, news.backupPeriodInHours);
      if (Object.keys(changes).length)
        yield* Backups.updateBackupConfiguration({
          organizationId: output.organizationId,
          serviceId: output.serviceId,
          ...changes,
        }).pipe(Retry.none);
      const updated = yield* observe(output);
      // Only a successful reset in this pass can establish a previously unknown default.
      yield* checkReset(
        "backupPeriodInHours" in changes ? undefined : b.backupPeriodInHours,
        news.backupPeriodInHours,
        updated.backupPeriodInHours,
        "backupPeriodInHours",
      );
      yield* checkReset(
        "backupRetentionPeriodInHours" in changes ? undefined : b.backupRetentionPeriodInHours,
        news.backupRetentionPeriodInHours,
        updated.backupRetentionPeriodInHours,
        "backupRetentionPeriodInHours",
      );
      yield* checkReset(
        "backupStartTime" in changes ? undefined : b.backupStartTime,
        news.backupStartTime,
        updated.backupStartTime,
        "backupStartTime",
      );
      if (
        (b.backupPeriodInHours &&
          (typeof updated.backupPeriodInHours !== "number" ||
            (news.backupPeriodInHours !== null &&
              updated.backupPeriodInHours !== news.backupPeriodInHours))) ||
        (b.backupRetentionPeriodInHours &&
          (typeof updated.backupRetentionPeriodInHours !== "number" ||
            (news.backupRetentionPeriodInHours !== null &&
              updated.backupRetentionPeriodInHours !== news.backupRetentionPeriodInHours))) ||
        (b.backupStartTime &&
          (typeof updated.backupStartTime !== "string" ||
            (news.backupStartTime !== null && updated.backupStartTime !== news.backupStartTime)))
      )
        return yield* fail(
          "not-converged",
          "Backup settings did not converge or ClickHouse omitted the reset value.",
        );
      return {
        ...output,
        observed: yield* observedFields(b, updated),
        baseline: {
          ...b,
          ...(b.backupPeriodInHours && updated.backupPeriodInHours !== undefined
            ? {
                backupPeriodInHours: {
                  ...b.backupPeriodInHours,
                  managed: updated.backupPeriodInHours,
                  reset: news.backupPeriodInHours === null,
                },
              }
            : {}),
          ...(b.backupRetentionPeriodInHours && updated.backupRetentionPeriodInHours !== undefined
            ? {
                backupRetentionPeriodInHours: {
                  ...b.backupRetentionPeriodInHours,
                  managed: updated.backupRetentionPeriodInHours,
                  reset: news.backupRetentionPeriodInHours === null,
                },
              }
            : {}),
          ...(b.backupStartTime && updated.backupStartTime !== undefined
            ? {
                backupStartTime: {
                  ...b.backupStartTime,
                  managed: updated.backupStartTime,
                  reset: news.backupStartTime === null,
                },
              }
            : {}),
        },
      };
    }),
    delete: Effect.fn(function* ({ output, fqn, instanceId }) {
      const b = yield* checkBaseline(output, { fqn, instanceId });
      const serviceMissing = yield* Service.getInstance({
        organizationId: output.organizationId,
        serviceId: output.serviceId,
      }).pipe(
        Effect.as(false),
        Effect.catchTag("ServiceNotFound", () => Effect.succeed(true)),
      );
      if (serviceMissing) return;
      let c = yield* observe(output);
      yield* checkRestoration(b, c);
      if (b.backupStartTime && c.backupStartTime !== b.backupStartTime.original) {
        // Restore the start time before the interval: writing it forces a 24-hour interval.
        yield* checkStart(c, b.backupStartTime.original, undefined);
        yield* Backups.updateBackupConfiguration({
          organizationId: output.organizationId,
          serviceId: output.serviceId,
          backupStartTime: b.backupStartTime.original,
        }).pipe(Retry.none);
        c = yield* observe(output);
        yield* checkRestoration(b, c);
        if (c.backupStartTime !== b.backupStartTime.original)
          return yield* fail("not-converged", "The original backup start time was not restored.");
      }
      const changes = {
        ...(b.backupPeriodInHours && c.backupPeriodInHours !== b.backupPeriodInHours.original
          ? { backupPeriodInHours: b.backupPeriodInHours.original }
          : {}),
        ...(b.backupRetentionPeriodInHours &&
        c.backupRetentionPeriodInHours !== b.backupRetentionPeriodInHours.original
          ? { backupRetentionPeriodInHours: b.backupRetentionPeriodInHours.original }
          : {}),
      };
      if (Object.keys(changes).length) {
        yield* Backups.updateBackupConfiguration({
          organizationId: output.organizationId,
          serviceId: output.serviceId,
          ...changes,
        }).pipe(Retry.none);
        const restored = yield* observe(output);
        if (
          (b.backupPeriodInHours &&
            restored.backupPeriodInHours !== b.backupPeriodInHours.original) ||
          (b.backupRetentionPeriodInHours &&
            restored.backupRetentionPeriodInHours !== b.backupRetentionPeriodInHours.original) ||
          (b.backupStartTime && restored.backupStartTime !== b.backupStartTime.original)
        )
          return yield* fail("not-converged", "Original backup settings were not restored.");
      }
    }),
  });
