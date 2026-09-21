//go:build athena_native && cgo

package athena

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/sivchari/kumo/internal/service/s3"
)

const engineDescription = "DuckDB 1.5.5 / Polyglot 0.12.0 (supported Athena subset)"

var unloadQuery = regexp.MustCompile(`(?is)^\s*UNLOAD\s*\((.*)\)\s+TO\s+'((?:''|[^'])*)'\s+WITH\s*\(\s*format\s*=\s*'PARQUET'\s*\)\s*;?\s*$`)

// One private database owns each execution; all API pages and output files read
// the same materialized relation, including nondeterministic SELECT expressions.
func executeQuery(ctx context.Context, request *QueryExecution) (*ResultSet, int64, error) {
	if len(request.ExecutionParameters) > 0 {
		return nil, 0, fmt.Errorf("execution parameters are not supported")
	}
	if request.ResultConfiguration != nil && (request.ResultConfiguration.EncryptionConfiguration != nil || request.ResultConfiguration.ACLConfiguration != nil || request.ResultConfiguration.ExpectedBucketOwner != "") {
		return nil, 0, fmt.Errorf("result encryption, ACL and owner options are not supported")
	}
	database := "default"
	if request.QueryExecutionContext != nil {
		if request.QueryExecutionContext.Catalog != "" && request.QueryExecutionContext.Catalog != "AwsDataCatalog" {
			return nil, 0, fmt.Errorf("only AwsDataCatalog is supported")
		}
		if request.QueryExecutionContext.Database != "" {
			database = request.QueryExecutionContext.Database
		}
	}
	if !simpleSQLName.MatchString(database) {
		return nil, 0, fmt.Errorf("unsupported database name")
	}
	query := request.Query
	unloadLocation := ""
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "UNLOAD") {
		match := unloadQuery.FindStringSubmatch(query)
		if match == nil {
			return nil, 0, fmt.Errorf("supported UNLOAD syntax requires only format='PARQUET'")
		}
		query = match[1]
		unloadLocation = strings.ReplaceAll(match[2], "''", "'")
	}
	translated, err := translateSelect(query)
	if err != nil {
		return nil, 0, err
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, 0, fmt.Errorf("open query engine: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("open query connection: %w", err)
	}
	defer conn.Close()
	if err := initializeEngine(ctx, conn, database); err != nil {
		return nil, 0, err
	}
	if err := validateSelect(ctx, conn, translated); err != nil {
		return nil, 0, err
	}
	tables, err := duckdb.GetTableNames(conn, translated, true)
	if err != nil {
		return nil, 0, fmt.Errorf("discover query tables: %w", err)
	}
	catalog, objects, err := catalogStores()
	if err != nil {
		return nil, 0, err
	}
	tempDir, err := os.MkdirTemp("", "kumo-athena-")
	if err != nil {
		return nil, 0, fmt.Errorf("create query workspace: %w", err)
	}
	defer os.RemoveAll(tempDir)
	var scanned int64
	for _, table := range tables {
		bytes, bindErr := bindCatalogTable(ctx, conn, catalog, objects, table, database, tempDir)
		scanned += bytes
		if bindErr != nil {
			return nil, scanned, bindErr
		}
	}
	if _, err := conn.ExecContext(ctx, "CREATE TEMP TABLE kumo_query_result AS SELECT * FROM ("+translated+") AS query_result"); err != nil {
		return nil, scanned, fmt.Errorf("execute SELECT: %w", err)
	}
	result, err := readExecutionResult(ctx, conn)
	if err != nil {
		return nil, scanned, err
	}
	if err := publishExecutionResult(ctx, conn, objects, request, result, unloadLocation, tempDir); err != nil {
		return nil, scanned, err
	}
	return result, scanned, nil
}

func initializeEngine(ctx context.Context, conn *sql.Conn, database string) error {
	statements := []string{
		"SET TimeZone='UTC'", "SET autoinstall_known_extensions=false", "SET autoload_known_extensions=false",
		"SET memory_limit='1GB'", "SET threads=2",
		"CREATE SCHEMA IF NOT EXISTS " + sqlIdentifier(database), "SET schema=" + sqlString(database),
		"CREATE MACRO kumo_max_by(value, ordering) AS arg_max_null(value, ordering)",
		"CREATE MACRO kumo_regexp_extract(value, pattern, group_index := 0) AS CASE WHEN regexp_matches(value, pattern) THEN regexp_extract(value, pattern, group_index) ELSE NULL END",
		`CREATE MACRO kumo_regexp_replace(value, pattern, replacement := '') AS regexp_replace(value, pattern, regexp_replace(replacement, '\$(\d+)', '\\\1', 'g'), 'g')`,
		"CREATE MACRO kumo_to_iso8601(value) AS CASE WHEN typeof(value)='DATE' THEN strftime(value,'%Y-%m-%d') ELSE strftime(value,'%Y-%m-%dT%H:%M:%S.%gZ') END",
		"CREATE MACRO kumo_fail(message) AS error(message)",
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize query engine: %w", err)
		}
	}
	return nil
}

// Parse, never execute, user text when checking the boundary. Reject table
// functions so queries cannot bypass the Glue/S3 resolver to read arbitrary files.
func validateSelect(ctx context.Context, conn *sql.Conn, query string) error {
	var encoded string
	if err := conn.QueryRowContext(ctx, "SELECT CAST(json_serialize_sql("+sqlString(query)+") AS VARCHAR)").Scan(&encoded); err != nil {
		return fmt.Errorf("parse translated SQL: %w", err)
	}
	var parsed struct {
		Error      bool              `json:"error"`
		Statements []json.RawMessage `json:"statements"`
	}
	if err := json.Unmarshal([]byte(encoded), &parsed); err != nil {
		return fmt.Errorf("decode parsed SQL: %w", err)
	}
	if parsed.Error || len(parsed.Statements) != 1 {
		return fmt.Errorf("exactly one supported SELECT is required")
	}
	var statement any
	if err := json.Unmarshal(parsed.Statements[0], &statement); err != nil {
		return fmt.Errorf("decode SELECT: %w", err)
	}
	return rejectTableFunctions(statement)
}

func rejectTableFunctions(value any) error {
	switch v := value.(type) {
	case map[string]any:
		if v["type"] == "TABLE_FUNCTION" {
			return fmt.Errorf("SQL table functions are not supported; use Glue tables")
		}
		for _, child := range v {
			if err := rejectTableFunctions(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := rejectTableFunctions(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func readExecutionResult(ctx context.Context, conn *sql.Conn) (*ResultSet, error) {
	rows, err := conn.QueryContext(ctx, "SELECT * FROM kumo_query_result")
	if err != nil {
		return nil, fmt.Errorf("read materialized result: %w", err)
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("read result schema: %w", err)
	}
	result := &ResultSet{ResultSetMetadata: &ResultSetMetadata{}}
	header := Row{}
	for _, column := range columns {
		result.ResultSetMetadata.ColumnInfo = append(result.ResultSetMetadata.ColumnInfo, ColumnInfo{Name: column.Name(), Label: column.Name(), Type: strings.ToLower(column.DatabaseTypeName()), Nullable: "NULLABLE"})
		header.Data = append(header.Data, Datum{VarCharValue: column.Name()})
	}
	result.Rows = append(result.Rows, header)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan result: %w", err)
		}
		row := Row{Data: make([]Datum, len(values))}
		for i, value := range values {
			if value == nil {
				row.Data[i].Null = true
				continue
			}
			switch typed := value.(type) {
			case time.Time:
				row.Data[i].VarCharValue = typed.UTC().Format("2006-01-02 15:04:05.999999999")
			case []byte:
				row.Data[i].VarCharValue = string(typed)
			default:
				row.Data[i].VarCharValue = fmt.Sprint(value)
			}
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate result: %w", err)
	}
	return result, nil
}

func publishExecutionResult(ctx context.Context, conn *sql.Conn, objects s3.Storage, request *QueryExecution, result *ResultSet, unloadLocation, tempDir string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish cancelled result: %w", err)
	}
	if unloadLocation != "" {
		bucket, prefix, err := s3Location(unloadLocation)
		if err != nil {
			return err
		}
		entries, _, err := objects.ListObjects(ctx, bucket, prefix, "", 1)
		if err != nil {
			return fmt.Errorf("inspect UNLOAD destination: %w", err)
		}
		if len(entries) > 0 {
			return fmt.Errorf("UNLOAD destination must be empty")
		}
		file := filepath.Join(tempDir, "result.parquet")
		if _, err := conn.ExecContext(ctx, "COPY kumo_query_result TO "+sqlString(file)+" (FORMAT PARQUET, COMPRESSION SNAPPY)"); err != nil {
			return fmt.Errorf("encode Parquet: %w", err)
		}
		input, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("open Parquet output: %w", err)
		}
		defer input.Close()
		if _, err := objects.PutObject(ctx, bucket, resultObjectKey(prefix, request.QueryExecutionID+".parquet"), input, map[string]string{"Content-Type": "application/vnd.apache.parquet"}); err != nil {
			return fmt.Errorf("store Parquet: %w", err)
		}
	}
	if request.ResultConfiguration == nil || request.ResultConfiguration.OutputLocation == "" {
		return nil
	}
	bucket, prefix, err := s3Location(request.ResultConfiguration.OutputLocation)
	if err != nil {
		return err
	}
	var data strings.Builder
	writer := csv.NewWriter(&data)
	for _, row := range result.Rows {
		values := make([]string, len(row.Data))
		for i, datum := range row.Data {
			values[i] = datum.VarCharValue
		}
		if err := writer.Write(values); err != nil {
			return fmt.Errorf("encode result CSV: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("finish result CSV: %w", err)
	}
	if _, err := objects.PutObject(ctx, bucket, resultObjectKey(prefix, request.QueryExecutionID+".csv"), strings.NewReader(data.String()), map[string]string{"Content-Type": "text/csv"}); err != nil {
		return fmt.Errorf("store result CSV: %w", err)
	}
	return nil
}

// A bucket-root destination must not acquire an unintended leading slash.
func resultObjectKey(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return strings.TrimRight(prefix, "/") + "/" + name
}
