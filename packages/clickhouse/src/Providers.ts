import { CredentialsStoreLive } from "alchemy/Auth/Credentials";
import { ProfileStoreLive } from "alchemy/Auth/Profile";
import * as Provider from "alchemy/Provider";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import { ClickHouseAuth } from "./AuthProvider.ts";
import { BackupConfiguration, BackupConfigurationProvider } from "./BackupConfiguration.ts";
import { CdcScaling, CdcScalingProvider } from "./ClickPipes/CdcScaling.ts";
import { Pipe, PipeProvider } from "./ClickPipes/Pipe.ts";
import {
  ReversePrivateEndpoint,
  ReversePrivateEndpointProvider,
} from "./ClickPipes/ReversePrivateEndpoint.ts";
import { SshKey, SshKeyProvider } from "./ClickPipes/SshKey.ts";
import * as Credentials from "./Credentials.ts";
import { ScalingSchedule, ScalingScheduleProvider } from "./ScalingSchedule.ts";
import { Service, ServiceProvider } from "./Service.ts";
import { ServiceSetting, ServiceSettingProvider } from "./ServiceSetting.ts";
import { Database, DatabaseProvider } from "./SQL/Database.ts";

export class Providers extends Provider.ProviderCollection<Providers>()("ClickHouse") {}

const makeProviders = () =>
  Layer.effect(
    Providers,
    Provider.collection([
      Service,
      ScalingSchedule,
      ServiceSetting,
      CdcScaling,
      Pipe,
      ReversePrivateEndpoint,
      SshKey,
      BackupConfiguration,
      Database,
    ]),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        ServiceProvider(),
        ScalingScheduleProvider(),
        ServiceSettingProvider(),
        CdcScalingProvider(),
        PipeProvider(),
        ReversePrivateEndpointProvider(),
        SshKeyProvider(),
        BackupConfigurationProvider(),
        DatabaseProvider(),
      ),
    ),
    Layer.provideMerge(Credentials.fromAuthProvider()),
    Layer.provideMerge(FetchHttpClient.layer),
    Layer.provideMerge(ClickHouseAuth),
    Layer.provideMerge(ProfileStoreLive),
    Layer.provideMerge(CredentialsStoreLive),
    Layer.orDie,
  );

let cached: ReturnType<typeof makeProviders> | undefined;

/** ClickHouse databases, ingestion, HTTP client and profile-aware credentials. */
export const providers = () => (cached ??= makeProviders());
