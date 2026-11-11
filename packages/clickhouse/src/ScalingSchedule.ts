import * as Retry from "@distilled.cloud/clickhouse/Retry";
import * as Service from "@distilled.cloud/clickhouse/service";
import { Unowned } from "alchemy/AdoptPolicy";
import { deepEqual, isResolved } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { Providers } from "./Providers.ts";

/** A recurring UTC scaling window. Unspecified fields inherit the service baseline. */
export interface ScalingScheduleEntry {
  /** Human-readable entry name. */
  name: string;
  /** UTC weekdays, Sunday 0 through Saturday 6. */
  weekdays: number[];
  /** Inclusive start hour, 0–23 UTC. */
  startHourUtc: number;
  /** Exclusive end hour, 1–24 UTC; an earlier hour crosses midnight. */
  endHourUtc: number;
  /** Scaling mode; omitted means vertical. */
  autoscalingMode?: "vertical" | "horizontal";
  /** Minimum memory per replica in GiB; specify both bounds or neither. */
  minReplicaMemoryGb?: number;
  /** Maximum memory per replica in GiB; specify both bounds or neither. */
  maxReplicaMemoryGb?: number;
  /** Fixed vertical replica count; do not combine with replica bounds. */
  numReplicas?: number;
  /** Minimum replica count; specify both bounds or neither. */
  minReplicas?: number;
  /** Maximum replica count; specify both bounds or neither. */
  maxReplicas?: number;
  /** Whether idle scaling is enabled during this entry. */
  idleScaling?: boolean;
  /** Idle timeout in minutes, at least 5. */
  idleTimeoutMinutes?: number;
}

export interface ScalingScheduleProps {
  /** Organization containing the service. Remove the control before changing it. */
  organizationId: string;
  /** Service with the schedule. Remove the control before changing it. */
  serviceId: string;
  /** Complete non-overlapping schedule, with up to ten entries; empty clears it. */
  entries: ScalingScheduleEntry[];
}

export interface ScalingScheduleAttributes {
  /** Organization containing the service. */
  organizationId: string;
  /** Service with the schedule. */
  serviceId: string;
  /** Last observed schedule entries, excluding server-generated IDs and activity state. */
  entries: ScalingScheduleEntry[];
  /** Ownership, original schedule, and last acknowledged schedule. */
  baseline: {
    /** Owning resource address. */
    fqn: string;
    /** Owning instance identity. */
    instanceId: string;
    /** Original complete schedule. */
    original: ScalingScheduleEntry[];
    /** Last acknowledged complete schedule. */
    managed: ScalingScheduleEntry[];
  };
}

export interface ScalingSchedule extends Resource<
  "ClickHouse.ScalingSchedule",
  ScalingScheduleProps,
  ScalingScheduleAttributes,
  never,
  Providers
> {}

/**
 * Manage a service's complete weekly autoscaling schedule. The service's
 * baseline sizing is separate and is never written by this control. Entries
 * may inherit baseline fields. Scheduling is best-effort; a saved entry does
 * not guarantee quota when it activates. Scheduled autoscaling entitlement
 * is required, and horizontal scaling has its own entitlement. The API enforces service-profile
 * restrictions and tier-specific limits when it applies entries.
 *
 * Existing schedules require explicit adoption. Adoption saves the original
 * entries before writing and destroy restores them. A schedule created by this
 * control is cleared on destroy. Redeploy converges to the declared entries,
 * including after a lost update response; destroy refuses later foreign edits.
 * Remove this control before changing scope and serialize writers;
 * there is no CAS.
 *
 * ### Adopt a schedule
 * **Example:** Run at a fixed size on weekdays
 * ```typescript
 * import * as Alchemy from "alchemy";
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 *
 * const schedule = yield* ClickHouse.ScalingSchedule("Workdays", {
 *   organizationId: "00000000-0000-4000-8000-000000000001",
 *   serviceId: "00000000-0000-4000-8000-000000000002",
 *   entries: [{ name: "workday", weekdays: [1, 2, 3, 4, 5], startHourUtc: 9,
 *     endHourUtc: 17, minReplicaMemoryGb: 16, maxReplicaMemoryGb: 16 }],
 * }).pipe(Alchemy.AdoptPolicy.adopt(true));
 * ```
 *
 * @resource
 * @product Service
 */
export const ScalingSchedule = Resource<ScalingSchedule>("ClickHouse.ScalingSchedule");

export class ScalingScheduleSafetyError extends Data.TaggedError("ScalingScheduleSafetyError")<{
  message: string;
  reason:
    | "invalid-input"
    | "overlap"
    | "incomplete-response"
    | "invalid-baseline"
    | "scope-change"
    | "not-converged"
    | "foreign-change";
  expected?: ScalingScheduleEntry[];
  observed?: ScalingScheduleEntry[];
}> {}

const normalize = (entry: ScalingScheduleEntry): ScalingScheduleEntry => ({
  name: entry.name,
  weekdays: [...entry.weekdays].sort((a, b) => a - b),
  startHourUtc: entry.startHourUtc,
  endHourUtc: entry.endHourUtc,
  autoscalingMode: entry.autoscalingMode ?? "vertical",
  ...(entry.minReplicaMemoryGb !== undefined
    ? { minReplicaMemoryGb: entry.minReplicaMemoryGb }
    : {}),
  ...(entry.maxReplicaMemoryGb !== undefined
    ? { maxReplicaMemoryGb: entry.maxReplicaMemoryGb }
    : {}),
  ...(entry.numReplicas !== undefined
    ? { minReplicas: entry.numReplicas, maxReplicas: entry.numReplicas }
    : {}),
  ...(entry.minReplicas !== undefined ? { minReplicas: entry.minReplicas } : {}),
  ...(entry.maxReplicas !== undefined ? { maxReplicas: entry.maxReplicas } : {}),
  ...(entry.idleScaling !== undefined ? { idleScaling: entry.idleScaling } : {}),
  ...(entry.idleTimeoutMinutes !== undefined
    ? { idleTimeoutMinutes: entry.idleTimeoutMinutes }
    : {}),
});

const canonical = (entries: ScalingScheduleEntry[]) =>
  entries
    .map(normalize)
    .sort(
      (a, b) =>
        a.name.localeCompare(b.name) ||
        a.startHourUtc - b.startHourUtc ||
        a.endHourUtc - b.endHourUtc ||
        a.weekdays.join(",").localeCompare(b.weekdays.join(",")),
    );

const validate = (props: ScalingScheduleProps) =>
  Effect.gen(function* () {
    if (
      !props.organizationId?.trim() ||
      !props.serviceId?.trim() ||
      !Array.isArray(props.entries) ||
      props.entries.length > 10
    ) {
      return yield* new ScalingScheduleSafetyError({
        reason: "invalid-input",
        message: "A service and at most ten schedule entries are required.",
      });
    }
    const occupied = new Set<number>();
    for (const entry of props.entries) {
      const mem = (v: unknown) =>
        typeof v === "number" && Number.isInteger(v) && v >= 8 && v <= 356 && v % 4 === 0;
      const count = (v: unknown) => typeof v === "number" && Number.isInteger(v) && v >= 1;
      const minMemory = entry?.minReplicaMemoryGb;
      const maxMemory = entry?.maxReplicaMemoryGb;
      const min = entry?.minReplicas;
      const max = entry?.maxReplicas;
      const fixed = entry?.numReplicas;
      const mode = entry?.autoscalingMode ?? "vertical";
      if (
        !entry ||
        typeof entry.name !== "string" ||
        !entry.name.trim() ||
        !Array.isArray(entry.weekdays) ||
        !entry.weekdays.length ||
        entry.weekdays.some((d) => !Number.isInteger(d) || d < 0 || d > 6) ||
        new Set(entry.weekdays).size !== entry.weekdays.length ||
        !Number.isInteger(entry.startHourUtc) ||
        entry.startHourUtc < 0 ||
        entry.startHourUtc > 23 ||
        !Number.isInteger(entry.endHourUtc) ||
        entry.endHourUtc < 1 ||
        entry.endHourUtc > 24 ||
        entry.startHourUtc === entry.endHourUtc ||
        (entry.autoscalingMode !== undefined &&
          entry.autoscalingMode !== "vertical" &&
          entry.autoscalingMode !== "horizontal") ||
        (minMemory === undefined) !== (maxMemory === undefined) ||
        (minMemory !== undefined &&
          (!mem(minMemory) || !mem(maxMemory) || minMemory > maxMemory!)) ||
        (min === undefined) !== (max === undefined) ||
        (min !== undefined && (!count(min) || !count(max) || min > max!)) ||
        (fixed !== undefined &&
          (!count(fixed) || min !== undefined || max !== undefined || mode !== "vertical")) ||
        (mode === "vertical" && min !== undefined && min !== max) ||
        (mode === "horizontal" && (minMemory === undefined || minMemory !== maxMemory)) ||
        (entry.idleScaling !== undefined && typeof entry.idleScaling !== "boolean") ||
        (entry.idleTimeoutMinutes !== undefined &&
          (!Number.isInteger(entry.idleTimeoutMinutes) || entry.idleTimeoutMinutes < 5))
      ) {
        return yield* new ScalingScheduleSafetyError({
          reason: "invalid-input",
          message: "Invalid schedule entry or scaling bounds.",
        });
      }
      for (const day of entry.weekdays) {
        const hours = (entry.endHourUtc - entry.startHourUtc + 24) % 24 || 24;
        for (let hour = 0; hour < hours; hour++) {
          const slot = (day * 24 + entry.startHourUtc + hour) % 168;
          if (occupied.has(slot))
            return yield* new ScalingScheduleSafetyError({
              reason: "overlap",
              message: "Schedule entries overlap.",
            });
          occupied.add(slot);
        }
      }
    }
  });

const observe = Effect.fn(function* (
  scope: Pick<ScalingScheduleProps, "organizationId" | "serviceId">,
) {
  const response = yield* Service.getScalingSchedule({
    organizationId: scope.organizationId,
    serviceId: scope.serviceId,
  }).pipe(Effect.catchTag("ScalingScheduleNotFound", () => Effect.succeed(undefined)));
  if (!response) return [];
  const { result } = response;
  if (!result || !Array.isArray(result.entries))
    return yield* new ScalingScheduleSafetyError({
      reason: "incomplete-response",
      message: "The service did not return a complete schedule.",
    });
  const entries = canonical(result.entries);
  yield* validate({ ...scope, entries });
  return entries;
});

const baselineOf = Effect.fn(function* (
  output: ScalingScheduleAttributes | undefined,
  owner: { fqn: string; instanceId: string },
) {
  const baseline = output?.baseline;
  if (
    !baseline ||
    baseline.fqn !== owner.fqn ||
    baseline.instanceId !== owner.instanceId ||
    !Array.isArray(baseline.original) ||
    !Array.isArray(baseline.managed)
  )
    return yield* new ScalingScheduleSafetyError({
      reason: "invalid-baseline",
      message:
        "A checkpointed schedule baseline is required; recover through read/adoption before writing.",
    });
  return baseline;
});

export const ScalingScheduleProvider = () =>
  Provider.succeed(ScalingSchedule, {
    nuke: { singleton: true },
    stables: ["organizationId", "serviceId"],
    diff: Effect.fn(function* ({ news, output, fqn, instanceId }) {
      if (!isResolved(news)) return;
      yield* validate(news);
      if (!output) return;
      const baseline = yield* baselineOf(output, { fqn, instanceId });
      if (news.organizationId !== output.organizationId || news.serviceId !== output.serviceId)
        return yield* new ScalingScheduleSafetyError({
          reason: "scope-change",
          message: "Restore and remove the schedule before changing scope.",
        });
      const desired = canonical(news.entries);
      if (!deepEqual(output.entries, desired) || !deepEqual(baseline.managed, desired))
        return { action: "update" } as const;
    }),
    read: Effect.fn(function* ({ olds, output, fqn, instanceId }) {
      if (!output) yield* validate(olds);
      const scope = output ?? olds;
      const current = yield* observe(scope);
      if (output) {
        yield* baselineOf(output, { fqn, instanceId });
        return { ...output, entries: current };
      }
      const attrs: ScalingScheduleAttributes = {
        organizationId: scope.organizationId,
        serviceId: scope.serviceId,
        entries: current,
        baseline: { fqn, instanceId, original: current, managed: current },
      };
      return current.length ? Unowned(attrs) : attrs;
    }),
    reconcile: Effect.fn(function* ({ news, output, fqn, instanceId }) {
      yield* validate(news);
      const baseline = yield* baselineOf(output, { fqn, instanceId });
      if (!output)
        return yield* new ScalingScheduleSafetyError({
          reason: "invalid-baseline",
          message: "A persisted baseline is required.",
        });
      if (news.organizationId !== output.organizationId || news.serviceId !== output.serviceId)
        return yield* new ScalingScheduleSafetyError({
          reason: "scope-change",
          message: "Restore and remove the schedule before changing scope.",
        });
      const current = yield* observe(output);
      const desired = canonical(news.entries);
      if (!deepEqual(current, desired))
        yield* Service.upsertScalingSchedule({
          organizationId: output.organizationId,
          serviceId: output.serviceId,
          entries: desired,
        }).pipe(Retry.none);
      const updated = yield* observe(output);
      if (!deepEqual(updated, desired))
        return yield* new ScalingScheduleSafetyError({
          reason: "not-converged",
          expected: desired,
          observed: updated,
          message: "The schedule did not converge.",
        });
      return { ...output, entries: updated, baseline: { ...baseline, managed: updated } };
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
      if (deepEqual(current, baseline.original)) return;
      if (!deepEqual(current, baseline.managed))
        return yield* new ScalingScheduleSafetyError({
          reason: "foreign-change",
          expected: baseline.managed,
          observed: current,
          message: "Refusing to restore an externally changed schedule.",
        });
      yield* Service.upsertScalingSchedule({
        organizationId: output.organizationId,
        serviceId: output.serviceId,
        entries: baseline.original,
      }).pipe(Retry.none);
      if (!deepEqual(yield* observe(output), baseline.original))
        return yield* new ScalingScheduleSafetyError({
          reason: "not-converged",
          expected: baseline.original,
          message: "The original schedule was not restored.",
        });
    }),
  });
