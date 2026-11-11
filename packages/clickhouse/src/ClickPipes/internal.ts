import { ResourceContext } from "alchemy/ResourceContext";
import { createInternalTags } from "alchemy/Tags";
import { sha256Object } from "alchemy/Util/sha256";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

class ClickPipesNotReady extends Data.TaggedError("ClickPipesNotReady")<{
  message: string;
}> {}

// These APIs have no tags. Bind the marker to the full owner, generation and create intent.
export const markedDescription = Effect.fn(function* (
  description: string | undefined,
  intent: object,
) {
  const context = yield* ResourceContext;
  const tags = yield* createInternalTags(context.fqn);
  const hash = yield* sha256Object({
    tags,
    type: context.type,
    instanceId: context.instanceId,
    intent,
  });
  return `${description ?? ""}\n[alchemy:${hash}]`;
});

// Poll reads, never mutations: a lost write reply must be inspected on the next deploy.
export const waitForObserved = <A, E, R>(options: {
  description: string;
  observe: Effect.Effect<A, E, R>;
  ready: (value: A) => boolean;
  failed?: (value: A) => boolean;
  slow?: boolean;
}) =>
  Effect.gen(function* () {
    const value = yield* options.observe.pipe(
      Effect.repeat({
        schedule: Schedule.spaced(options.slow ? "1 minute" : "15 seconds"),
        times: options.slow ? 10 : 8,
        until: (value) => options.ready(value) || options.failed?.(value) === true,
      }),
    );
    if (!options.ready(value))
      return yield* new ClickPipesNotReady({
        message: `${options.description} did not converge; inspect ClickHouse before retrying.`,
      });
    return value;
  });
