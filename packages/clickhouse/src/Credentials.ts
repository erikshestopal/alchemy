import { Credentials } from "@distilled.cloud/clickhouse/Credentials";
import { ConfigError } from "@distilled.cloud/core/errors";
import { lazyCredentials } from "alchemy/Auth/Resolve";
import * as Layer from "effect/Layer";
import {
  CLICKHOUSE_AUTH_PROVIDER_NAME,
  type ClickHouseResolvedCredentials,
} from "./AuthProvider.ts";

export {
  Credentials,
  CredentialsFromEnv,
  credentials,
} from "@distilled.cloud/clickhouse/Credentials";

/** Resolve credentials lazily from the selected Alchemy profile or CI environment. */
export const fromAuthProvider = () =>
  Layer.effect(
    Credentials,
    lazyCredentials<ClickHouseResolvedCredentials, ConfigError>()(
      CLICKHOUSE_AUTH_PROVIDER_NAME,
      ({ apiKey, apiSecret, apiBaseUrl }) => ({ apiKey, apiSecret, apiBaseUrl }),
    ),
  );
