# ClickHouse Cloud service

Provision an 8 GiB ClickHouse Cloud service with one replica in AWS us-east-1,
query it over HTTPS, and destroy it when finished. Idle scaling starts after
five minutes. Cloud usage is billable.

## Run

Set `CLICKHOUSE_CLOUD_API_KEY` and `CLICKHOUSE_CLOUD_API_SECRET` through your
secret manager, or run `alchemy profile edit --add ClickHouse`.

From this directory, set the organization UUID and your application's egress
CIDR, then deploy:

```sh
export CLICKHOUSE_ORGANIZATION_ID="your-organization-uuid"
export CLICKHOUSE_ALLOWED_CIDR="your-public-ip/32"
pnpm run deploy
pnpm run query
```

`query` deploys the same stack idempotently, reads its saved SQL password, and
prints a parameterized SQL result and the server version. It uses stage `dev`
unless `ALCHEMY_STAGE` is set. Use the same stage for every command.

The Cloud API key is not a SQL password. A new service reveals its `default`
SQL user's password once; Alchemy keeps it redacted in state. Protect
`.alchemy/state` and do not commit it. Use restricted SQL users for applications.

Change `CLICKHOUSE_SERVICE_NAME` and redeploy to rename in place. Changing
provider, region or other create-only settings can replace the service and
destroy its data.

```sh
pnpm run destroy
```

Destroy deletes the service and all its data. If Cloud shutdown exceeds one
bounded wait, retry the same command after it stops. Do not discard state.
In non-interactive automation, pass `--yes` to approve deletion.

## Live test

With the same configuration, `pnpm test` provisions the stack, creates a SQL
database and table, inserts and queries rows, redeploys, renames in place, and
verifies data and password preservation. Cleanup resumes slow shutdowns and
checks that the service UUID is gone through the Cloud API.

Use a distinct `CLICKHOUSE_SERVICE_NAME` and `ALCHEMY_TEST_STAGE` for the test;
it deletes that stack. Test stages default to `test_$USER`, independently of
the CLI's `ALCHEMY_STAGE`. SQL schemas
in this example are created by the test, not an Alchemy schema or migration
resource.
