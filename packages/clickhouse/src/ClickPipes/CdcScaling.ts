import * as ClickPipes from "@distilled.cloud/clickhouse/click_pipes";
import { OwnedBySomeoneElse, Unowned } from "alchemy/AdoptPolicy";
import { isResolved } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { Providers } from "../Providers.ts";
import { waitForObserved } from "./internal.ts";

export interface CdcScalingProps {
  /** Organization containing the service. Restore and remove the control before changing it. */
  organizationId: string;
  /** Service whose database ClickPipes share this infrastructure. Restore and remove the control before changing it. */
  serviceId: string;
  /** CPU in millicores, from 1000 to 32000 in increments of 1000. */
  replicaCpuMillicores: number;
  /** Memory in GiB, from 4 to 128; must equal four times the CPU core count. */
  replicaMemoryGb: number;
}

interface Scaling {
  /** CPU in millicores. */
  replicaCpuMillicores: number;
  /** Memory in GiB. */
  replicaMemoryGb: number;
}

export interface CdcScalingAttributes extends Scaling {
  /** Organization containing the service. */
  organizationId: string;
  /** Service whose database ClickPipes share this infrastructure. */
  serviceId: string;
  /** Scaling observed before adoption, restored on destroy if it is safe. */
  initialScaling: Scaling;
  /** Last scaling successfully managed by this resource. */
  managedScaling: Scaling;
}

export interface CdcScaling extends Resource<
  "ClickHouse.ClickPipes.CdcScaling",
  CdcScalingProps,
  CdcScalingAttributes,
  never,
  Providers
> {}

/**
 * Manage the shared scaling of all database ClickPipes in one service.
 * A database pipe must already be provisioned. Streaming pipes have separate
 * scaling. The API reports configuration before scaling has fully propagated.
 *
 * ### Manage shared scaling
 * **Example:** Adopt the existing service-wide scaler
 * ```typescript
 * import * as Alchemy from "alchemy";
 * import * as ClickHouse from "@alchemy.run/clickhouse";
 * import * as Effect from "effect/Effect";
 *
 * export default Alchemy.Stack("Ingestion", {
 *   providers: ClickHouse.providers(),
 *   state: Alchemy.localState(),
 * }, Effect.gen(function* () {
 *   return yield* ClickHouse.ClickPipes.CdcScaling("DatabaseScaling", {
 *     organizationId: "00000000-0000-4000-8000-000000000001",
 *     serviceId: "00000000-0000-4000-8000-000000000002",
 *     replicaCpuMillicores: 2000,
 *     replicaMemoryGb: 8,
 *   }).pipe(Alchemy.AdoptPolicy.adopt(true));
 * }));
 * ```
 *
 * Adoption captures the existing scaling. Destroy restores it only if the
 * current value still matches the last managed value. There is no delete API.
 * Use one control per service and serialize changes from other writers; the
 * API has no conditional write. Restore and remove this control before changing
 * its organization or service, so the original baseline stays durable.
 * A scaling change may take minutes;
 * a deploy polls for up to ten minutes, matching the official Terraform provider.
 * Scaling writes set an absolute value; retries retain the SDK's bounded policy.
 *
 * @see https://api.clickhouse.cloud/v1
 *
 * @resource
 * @product ClickPipes
 */
export const CdcScaling = Resource<CdcScaling>("ClickHouse.ClickPipes.CdcScaling");

export class CdcScalingSafetyError extends Data.TaggedError("CdcScalingSafetyError")<{
  message: string;
}> {}

const same = (a: Scaling, b: Scaling) =>
  a.replicaCpuMillicores === b.replicaCpuMillicores && a.replicaMemoryGb === b.replicaMemoryGb;

const observe = Effect.fn(function* (scope: { organizationId: string; serviceId: string }) {
  const { result } = yield* ClickPipes.getClickPipeCdcScaling({
    organizationId: scope.organizationId,
    serviceId: scope.serviceId,
  });
  if (result?.replicaCpuMillicores === undefined || result.replicaMemoryGb === undefined) {
    return yield* new CdcScalingSafetyError({
      message: "The API did not return a complete CDC scaling baseline.",
    });
  }
  return {
    replicaCpuMillicores: result.replicaCpuMillicores,
    replicaMemoryGb: result.replicaMemoryGb,
  };
});

const validate = (props: CdcScalingProps) =>
  props.replicaCpuMillicores >= 1000 &&
  props.replicaCpuMillicores <= 32000 &&
  props.replicaCpuMillicores % 1000 === 0 &&
  props.replicaMemoryGb === (props.replicaCpuMillicores / 1000) * 4
    ? Effect.void
    : Effect.fail(
        new CdcScalingSafetyError({
          message:
            "CPU must be 1000–32000 millicores in steps of 1000; memory must be four GiB per core.",
        }),
      );

const waitForScaling = Effect.fn(function* (
  scope: { organizationId: string; serviceId: string },
  desired: Scaling,
) {
  return yield* waitForObserved({
    description: "Service-wide CDC scaling",
    observe: observe(scope),
    ready: (value) => same(value, desired),
    slow: true,
  });
});

export const CdcScalingProvider = () =>
  Provider.succeed(CdcScaling, {
    nuke: { skip: true },
    stables: ["organizationId", "serviceId", "initialScaling"],
    diff: Effect.fn(function* ({ news, output }) {
      if (!isResolved(news)) return;
      yield* validate(news);
      if (
        output &&
        (news.organizationId !== output.organizationId || news.serviceId !== output.serviceId)
      ) {
        return yield* new CdcScalingSafetyError({
          message: "Restore and remove CDC scaling before changing its organization or service.",
        });
      }
      if (output) {
        const current = yield* observe(news);
        if (!same(current, news)) return { action: "update" } as const;
      }
    }),
    read: Effect.fn(function* ({ olds, output }) {
      const scope = output ?? olds;
      const current = yield* observe(scope);
      if (output) return { ...output, ...current };
      return Unowned({
        organizationId: scope.organizationId,
        serviceId: scope.serviceId,
        ...current,
        initialScaling: current,
        managedScaling: current,
      });
    }),
    reconcile: Effect.fn(function* ({ news, output }) {
      yield* validate(news);
      if (!output) {
        return yield* new OwnedBySomeoneElse({
          message: "Existing service-wide CDC scaling requires explicit adoption.",
          resourceType: CdcScaling.Type,
        });
      }
      if (news.organizationId !== output.organizationId || news.serviceId !== output.serviceId) {
        return yield* new CdcScalingSafetyError({
          message: "Restore and remove CDC scaling before changing its organization or service.",
        });
      }
      const current = yield* observe(news);
      if (!same(current, output.managedScaling) && !same(current, news)) {
        return yield* new CdcScalingSafetyError({
          message: "CDC scaling changed outside this resource.",
        });
      }
      if (!same(current, news)) {
        yield* ClickPipes.updateClickPipeCdcScaling({
          organizationId: news.organizationId,
          serviceId: news.serviceId,
          replicaCpuMillicores: news.replicaCpuMillicores,
          replicaMemoryGb: news.replicaMemoryGb,
        });
      }
      const observed = yield* waitForScaling(news, news);
      return { ...output, ...observed, managedScaling: observed };
    }),
    delete: Effect.fn(function* ({ output }) {
      const current = yield* observe(output);
      if (same(current, output.initialScaling)) return;
      if (!same(current, output.managedScaling)) {
        return yield* new CdcScalingSafetyError({
          message: "Refusing to overwrite externally changed CDC scaling.",
        });
      }
      yield* ClickPipes.updateClickPipeCdcScaling({
        organizationId: output.organizationId,
        serviceId: output.serviceId,
        ...output.initialScaling,
      });
      yield* waitForScaling(output, output.initialScaling);
    }),
  });
