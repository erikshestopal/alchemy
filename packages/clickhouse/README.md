# @alchemy.run/clickhouse

Provision ClickHouse Cloud services with Alchemy, then connect over HTTPS SQL.
Manage sizing, network access and idle policy without replacing the database.

Managed Postgres, Functions/UDFs and BYOC are outside this provider's scope.
External PostgreSQL ingestion through ClickPipes remains supported.

## Install

```sh
npm install alchemy @alchemy.run/clickhouse effect
```

## Provision a database service

```typescript
// alchemy.run.ts
import * as ClickHouse from "@alchemy.run/clickhouse";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";

export default Alchemy.Stack(
  "Analytics",
  { providers: ClickHouse.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const db = yield* ClickHouse.Service("Database", {
      organizationId: "your-organization-uuid",
      provider: "aws",
      region: "us-east-1",
      minReplicaMemoryGb: 8,
      maxReplicaMemoryGb: 8,
      numReplicas: 1,
      idleScaling: true,
      idleTimeoutMinutes: 5,
      ipAccessList: [{ source: "192.0.2.10/32", description: "Replace with your application IP" }],
    });
    return { serviceId: db.serviceId, service: db.service, password: db.password };
  }),
);
```

Run `alchemy deploy`. The output includes HTTPS and native endpoints and a
redacted SQL password. Replace the example CIDR with your application's egress
IP; an empty list does not grant public access. Protect the local state:
ClickHouse reveals the SQL password only once.

The repository's `examples/clickhouse-service` includes a runnable stack,
SQL query command and live lifecycle test.
`ClickHouse.SQL.Database` supports deploy-time migrations from SQL files.
See the [SQL guide](https://alchemy.run/clickhouse/services/sql) for an example
and nontransactional recovery limits.

## Authentication

Run `alchemy profile edit --add ClickHouse` to store a Cloud API key and secret.
For CI, set `CLICKHOUSE_CLOUD_API_KEY` and `CLICKHOUSE_CLOUD_API_SECRET`.
The key needs `control-plane:organization:manage` on the target organization.
These are Cloud management credentials, not SQL credentials.

Destroy deletes the service and its data. A slow Cloud shutdown can return
`DeleteInProgress`; keep state and retry destroy after shutdown completes.
Existing services are never adopted by matching name.

[Setup](https://alchemy.run/clickhouse/setup) ·
[Service guide](https://alchemy.run/clickhouse/services/service)
