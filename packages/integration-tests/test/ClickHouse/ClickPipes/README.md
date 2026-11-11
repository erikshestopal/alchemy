# Live ingestion fixtures

All four suites call public `stack.deploy`, `stack.plan` and `stack.destroy`,
and verify remote state through distilled. They require `CLICKHOUSE_TEST_SLOW=1`;
no lifecycle is simulated. The S3 lifecycle passed with its self-provisioned
fixture, including ingestion, drift repair, replacement, destination-table
preservation and service teardown. The SSH suite previously passed against a
disposable Cloud service, including missing-key recreation with dependent outputs,
replacement and teardown. CDC mutation and reverse-endpoint lifecycles remain
unverified.

## S3 ingestion

Set `CLICKHOUSE_TEST_ORGANIZATION_ID` to an authorized test organization and run:

```sh
CLICKHOUSE_TEST_SLOW=1 pnpm test:integration test/ClickHouse/ClickPipes/Pipe.test.ts --profile testing --retry 0 --concurrency 1
```

The test provisions one disposable AWS us-east-1 ClickHouse service, a database
and two destination tables with baseline rows. It uses ClickHouse's public
19-byte JSONEachRow example at
`https://datasets-documentation.s3.eu-west-3.amazonaws.com/json/sample.json`,
containing `{"a":1}` and `{"a":"22"}`. No AWS credentials, pre-existing service,
SQL password or destination tables are required. An optional
`CLICKHOUSE_CLICKPIPES_TEST_OBJECT_URL` overrides the source with an unsigned
HTTPS S3 object of at most 64KB containing an `a` string/number field.

The test removes the pipe and checks both table identities, schemas and data
before destroying its database and service. Service deletion is verified through
the Cloud API. The test temporarily permits public SQL access to this disposable
service; its generated password remains redacted in stack state.

## Other ingestion fixtures

- **SSH keys:** `CLICKHOUSE_CLICKPIPES_TEST_ORGANIZATION_ID` and
  `CLICKHOUSE_CLICKPIPES_TEST_SERVICE_ID` select a disposable service.
  Key generation does not connect to the placeholder bastion. Connectivity
  validation needs a reachable bastion with the generated public key installed;
  that fixture is not provided.
- **CDC scaling:** the same service must already contain a database ClickPipe,
  so its shared CDC scaler exists. The suite adopts, changes and restores it.
  Do not point it at a service shared with other scaling writers.
- **Reverse endpoints:** additionally set
  `CLICKHOUSE_CLICKPIPES_TEST_VPC_ENDPOINT_SERVICE` to a disposable AWS endpoint
  service that permits ClickHouse's principal and needs no manual acceptance.

Kafka, Kinesis, Pub/Sub, PostgreSQL, MySQL, MongoDB and BigQuery remain supported
source types. Their live lifecycle tests need reachable, isolated source fixtures,
authentication and grants, and distinct disposable destination tables. Those
fixtures are not provided here; neither source-side cleanup nor hosted retention
of caller-managed source artifacts has been proved live. PostgreSQL additionally
needs read access to `pg_replication_slots` and `pg_publication` to distinguish
pipe-managed artifact cleanup from retention of caller-managed artifacts.
BigQuery needs a dedicated staging prefix and bucket-list access to inspect
initial-load cleanup. Pub/Sub and workload identity can require vendor preview
entitlements; no exact typed entitlement rejection has been observed in these suites.

Interrupted-create recovery for description-marked SSH keys and reverse endpoints
also remains unproved live. Pipes have no ownership-marker field: an ambiguous
creation requires inspection and explicit UUID adoption, never name-only recovery.
