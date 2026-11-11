# ClickHouse SQL from a Cloudflare Worker

Create a ClickHouse Cloud service, apply two database migrations, and query or
insert events through an authenticated, deployed Cloudflare Worker. The service
uses one 8 GiB replica in AWS us-east-1 and is billable even when retained.

The service's initial `default` SQL credential is used only for database creation
and migrations. The Worker requires a **separate, supplied SQL identity** with
preconfigured grants. This example does not create SQL users, grant privileges,
reset passwords, or inject Cloud management keys into the Worker.

## Configure

Run commands from this directory after installing the repository's workspace
dependencies. Configure both ClickHouse and Cloudflare in the same Alchemy
profile:

```sh
pnpm alchemy profile edit --add ClickHouse
pnpm alchemy profile edit --add Cloudflare
```

For CI, supply `CLICKHOUSE_CLOUD_API_KEY`, `CLICKHOUSE_CLOUD_API_SECRET`,
`CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`. Use a ClickHouse management
identity allowed to create, read, update and delete services in your organization.

Supply these example settings through environment variables or a secret
provider. `.env` is also supported; keep it and `.alchemy/state` out of Git.

| Variable | Value |
| --- | --- |
| `CLICKHOUSE_ORGANIZATION_ID` | Your ClickHouse Cloud organization UUID. |
| `CLICKHOUSE_DEPLOY_CIDR` | Deployment machine's public egress CIDR, such as your IP followed by `/32`. |
| `CLICKHOUSE_WORKER_CIDR` | A CIDR that permits your deployed Worker's HTTPS egress. |
| `CLICKHOUSE_APP_USERNAME` | A SQL user other than `default`, not a management API key. |
| `CLICKHOUSE_APP_PASSWORD` | That user's SQL password, read with `Config.Redacted`. |
| `CLICKHOUSE_HTTP_TOKEN` | A private bearer token of at least 32 characters, read with `Config.Redacted`. |
| `CLICKHOUSE_SQL_DESTROY_DATA` | Optional; defaults to `false`. See cleanup before enabling. |

A normal Worker does not have one fixed public egress address. Use a Worker
deployment with controlled egress and allow only that egress CIDR. Cloudflare's
inbound proxy IP ranges are not Worker egress ranges. Keep the deploy CIDR
reachable for migrations and destruction; do not open SQL access to the internet.

## Deploy, then grant application access

1. Supply a distinct application SQL username and its intended password.
2. Deploy the stack:

   ```sh
   pnpm run deploy --stage dev
   ```

   Outputs contain the organization UUID, service UUID, generated database name
   and UUID, and Worker URL. No SQL password is returned as a stack output.
3. In that service's SQL access administration, create or select the supplied
   application identity and grant `SELECT` and `INSERT` on the output database's
   `events` table. Use an administrator authorized to manage SQL users and
   grants; the example makes no claim that the initial `default` user can do so.
   Confirm the identity has no unwanted grants, including grants on other databases.

The first deploy creates the schema before application grants exist. Until you
configure those grants, authenticated `/events` requests fail with HTTP 502.
Neither `SQL.Connect` nor its default database grants or restricts SQL access.
The Worker gives this identity no DDL endpoint and accepts no arbitrary SQL.

## Use the deployed API

Set `API_URL` to the stack's Worker URL. Every request requires the bearer token:

```sh
curl --fail-with-body "$API_URL/events" \
  -H "Authorization: Bearer $CLICKHOUSE_HTTP_TOKEN"
```

The first response contains `baseline`, inserted by `0002_seed.sql`. Each row
also reports the actual SQL username and database.

```sh
curl --fail-with-body -X POST "$API_URL/events" \
  -H "Authorization: Bearer $CLICKHOUSE_HTTP_TOKEN" \
  -H 'Content-Type: text/plain' --data-binary 'hello from the Worker'
```

A successful insert returns 204. Read again to see both rows. Inserts use typed
SQL parameters and synchronous acknowledgment. **Do not automatically retry
POST requests**: a lost response does not prove the insert failed. The endpoint
accepts 1–4096 characters and is a demonstration API, not a rate-limited ingestion
service. Add request-size and abuse controls before exposing it broadly.

## Change the schema

Add a new `.sql` file under `migrations/` and redeploy the same stage. Never edit,
remove or rename an applied file. Unchanged redeploys preserve rows without
repeating the seed. ClickHouse DDL is not transactional; if a statement fails or
its response is lost, inspect SQL and `__alchemy_migrations__progress` before
repairing progress and resuming. Do not replay a captured failed plan. Serialize
deploys and external DDL writers for this database.

## Clean up deliberately

By default, `destroy` removes the Worker and forgets the SQL database and
service, but **retains the service and all SQL data**. Retained compute remains
billable. Keep a secure backup of state before destruction: forgetting the
service also loses Alchemy's saved initial SQL password, and a later declaration
does not automatically reclaim retained resources.

For a disposable stack, enable deletion and deploy that policy before destroying:

```sh
export CLICKHOUSE_SQL_DESTROY_DATA=true
pnpm run deploy --stage dev
pnpm run destroy --stage dev
```

This sets database `dropOnDelete: true` and removes service retention. Destroy
then deletes the database, its tables and the whole service. Replacements with
this policy can also destroy data; they do not copy it. If service shutdown
outlasts the bounded delete wait, inspect the service and rerun only `destroy`
after it stops. Keep the same state, profile and stage until deletion succeeds.

## Opt-in live test

The deployed-host test has not been executed yet. It is not local-workerd
evidence, and these instructions do not claim it passes.

The test needs a manually prepared **disposable stack in its own test stage**,
because SQL user creation and grants are deliberately outside the example.
It uses that stage's existing service and destroys it, including all its data.
Do not point it at a production or shared service.

1. Set a unique `ALCHEMY_TEST_STAGE`, such as `test_yourname_clickhouse_sql`.
2. Set `CLICKHOUSE_SQL_DESTROY_DATA=true`, configure the settings above, and
   deploy with `pnpm run deploy --stage "$ALCHEMY_TEST_STAGE"`.
3. Grant the supplied application identity access as above. Leave `events`
   containing only the seeded `baseline` row; do not exercise POST manually.
4. Set `CLICKHOUSE_SQL_EXAMPLE_TEST_SERVICE_ID` to that stack's service UUID.
5. Set `CLICKHOUSE_TEST_SLOW=1` and run `pnpm test` from this directory.

Unset `ALCHEMY_TEST_DEV`; the test explicitly deploys a host and requires an
HTTPS URL. Use `ALCHEMY_PROFILE` when selecting a non-default profile. All gate
variables must be in the test process environment, not just `.env`.

The test checks unauthenticated reads and writes, actual SQL identity, migrated
seed data, a parameterized insert through HTTP, an independent SQL read, and
data preservation on redeploy. GET readiness waits are bounded; writes and the
test body have no retries. Cleanup does not honor `NO_DESTROY`. The test checks
`ServiceNotFound` after destroying its service; a teardown timeout remains a
failure, not a skipped check. After a failure, inspect the state and data before
resuming cleanup or rebuilding a fresh fixture.
