# Native Athena execution

The default portable binary and image retain the Athena control-plane API, but
accepted queries finish with an explicit execution-unavailable error. They never
return simulated successful query results.

Build the optional Linux amd64 or arm64 image from an ordinary repository checkout:

```sh
docker build -f docker/Dockerfile.athena -t kumo-athena .
docker run --rm -p 4566:4566 kumo-athena
```

For a native source build, use CGO and `go build -tags athena_native ./cmd/kumo`.
Set `POLYGLOT_SQL_FFI_PATH` to the absolute path of the matching platform library.
An absent, unloadable, or wrong-version library fails queries explicitly. There
are no runtime downloads, translation subprocesses, or DuckDB extension installs.

## Supported execution contract

Queries run asynchronously against the registered Kumo Glue and S3 stores. Poll
`GetQueryExecution` for a terminal state; `StopQueryExecution` cancels the owned
execution context. Shutdown cancels active jobs. Interrupted persisted jobs become
failed on restart rather than appearing permanently running.

The initial native subset is a single Athena SELECT (including CTEs and aggregate
queries) over unpartitioned Glue tables in `AwsDataCatalog`. Database and table
names use ASCII identifiers. Supported scalar columns are strings, numeric types,
booleans, dates, timestamps, and decimal types. Supported table inputs are Parquet,
newline JSON using OpenX/Hive JSON SerDe, and a single-column RegexSerDe with
`input.regex=(.*)` for raw lines. Gzip inputs are supported by their `.gz` suffix.
Views, partitions, complex column types, arbitrary SQL table functions, DDL/DML,
execution parameters, and unsupported result encryption/ACL options fail explicitly.

`GetQueryResults` includes a header row and uses query-bound pagination tokens.
SQL NULL is an absent `VarCharValue`; an empty SQL string is a present empty value.
Rows and optional S3 CSV output come from one materialized result. CSV has the
usual empty-field ambiguity; the API and Parquet preserve NULL independently.

`UNLOAD (<SELECT>) TO 's3://bucket/prefix/' WITH (format='PARQUET')` writes one
Snappy Parquet object into an empty destination prefix. Its rows come from the
same materialization exposed by result pagination. Other UNLOAD options are not
implemented. Output writes must succeed before the query succeeds.

Athena compatibility macros preserve aggregate FILTER clauses, `max_by` with a
NULL value at the greatest ordering key, regex extraction's NULL on no match,
regex replacement dollar-number captures, UTC `to_iso8601`, and `fail` errors.
This is a tested-subset goal, not a claim of complete Athena dialect coverage.
Native execution uses one private DuckDB database per query, two engine threads,
and a 1 GB engine memory limit. Input objects and API result rows also occupy Go
memory; this emulator is not a production-scale query service or security sandbox.

## Dependency provenance and notices

- DuckDB Go driver: `github.com/duckdb/duckdb-go/v2 v2.10505.0` (DuckDB 1.5.5).
- Translator SDK: `github.com/tobilg/polyglot/packages/go v0.12.0`.
- Native translator: official [Polyglot v0.12.0 release](https://github.com/tobilg/polyglot/releases/tag/v0.12.0),
  matching source commit `8b728c154648fd3e92d328183df1e5ac5008746f`.
- The Dockerfile pins both Linux archives to their published SHA256 checksums.
  It does not build or extract Polyglot source/tooling archives.

The native image includes upstream DuckDB/driver and Polyglot license notices in
`/usr/share/licenses/kumo-athena`. The portable Dockerfile and upstream Go toolchain
remain unchanged by this optional target.
