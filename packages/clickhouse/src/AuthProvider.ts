import { DEFAULT_API_BASE_URL } from "@distilled.cloud/clickhouse/Credentials";
import { getEnv, getEnvRedactedRequired } from "alchemy/Auth/Env";
import {
  makeStoredAuthProvider,
  storedSecret,
  storedValueText,
  type StoredAuthConfig,
} from "alchemy/Auth/StoredAuthProvider";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";

export const CLICKHOUSE_AUTH_PROVIDER_NAME = "ClickHouse";

export type ClickHouseAuthConfig = StoredAuthConfig;

export interface ClickHouseResolvedCredentials {
  type: "apiKey";
  apiKey: Redacted.Redacted<string>;
  apiSecret: Redacted.Redacted<string>;
  apiBaseUrl: string;
  source: { type: ClickHouseAuthConfig["method"] | "env"; details?: string };
}

const clickhouseAuth = makeStoredAuthProvider<ClickHouseResolvedCredentials>({
  provider: CLICKHOUSE_AUTH_PROVIDER_NAME,
  fields: [
    { name: "apiKey", label: "ClickHouse Cloud API key", secret: true },
    { name: "apiSecret", label: "ClickHouse Cloud API secret", secret: true },
    {
      name: "apiBaseUrl",
      label: "ClickHouse Cloud API base URL",
      optional: true,
      placeholder: DEFAULT_API_BASE_URL,
    },
  ],
  toResolved: (values, source) => ({
    type: "apiKey",
    apiKey: storedSecret(values.apiKey) ?? Redacted.make(""),
    apiSecret: storedSecret(values.apiSecret) ?? Redacted.make(""),
    apiBaseUrl: storedValueText(values.apiBaseUrl) ?? DEFAULT_API_BASE_URL,
    source: { type: source },
  }),
  readEnvironment: Effect.all({
    apiKey: getEnvRedactedRequired("CLICKHOUSE_CLOUD_API_KEY"),
    apiSecret: getEnvRedactedRequired("CLICKHOUSE_CLOUD_API_SECRET"),
    apiBaseUrl: getEnv("CLICKHOUSE_CLOUD_API_BASE_URL"),
  }).pipe(
    Effect.map(({ apiKey, apiSecret, apiBaseUrl }) => ({
      type: "apiKey" as const,
      apiKey,
      apiSecret,
      apiBaseUrl: apiBaseUrl ?? DEFAULT_API_BASE_URL,
      source: { type: "env" as const },
    })),
  ),
  environment: [
    { name: "CLICKHOUSE_CLOUD_API_KEY", required: true, secret: true },
    { name: "CLICKHOUSE_CLOUD_API_SECRET", required: true, secret: true },
    { name: "CLICKHOUSE_CLOUD_API_BASE_URL", required: false },
  ],
});

/** Register ClickHouse Cloud API-key authentication with Alchemy profiles. */
export const ClickHouseAuth = clickhouseAuth.layer;
