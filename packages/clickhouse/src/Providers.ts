import { CredentialsStoreLive } from "alchemy/Auth/Credentials";
import { ProfileStoreLive } from "alchemy/Auth/Profile";
import * as Provider from "alchemy/Provider";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import { ClickHouseAuth } from "./AuthProvider.ts";
import * as Credentials from "./Credentials.ts";
import { Service, ServiceProvider } from "./Service.ts";

export class Providers extends Provider.ProviderCollection<Providers>()("ClickHouse") {}

const makeProviders = () =>
  Layer.effect(Providers, Provider.collection([Service])).pipe(
    Layer.provide(ServiceProvider()),
    Layer.provideMerge(Credentials.fromAuthProvider()),
    Layer.provideMerge(FetchHttpClient.layer),
    Layer.provideMerge(ClickHouseAuth),
    Layer.provideMerge(ProfileStoreLive),
    Layer.provideMerge(CredentialsStoreLive),
    Layer.orDie,
  );

let cached: ReturnType<typeof makeProviders> | undefined;

/** ClickHouse services, HTTP client and profile-aware credentials. */
export const providers = () => (cached ??= makeProviders());
