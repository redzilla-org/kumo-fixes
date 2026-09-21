//go:build athena_native && cgo

package athena

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/sivchari/kumo/internal/service"
	"github.com/sivchari/kumo/internal/service/glue"
	"github.com/sivchari/kumo/internal/service/s3"
)

func catalogStores() (glue.Storage, s3.Storage, error) {
	var catalog glue.Storage
	var objects s3.Storage
	for _, svc := range service.Services() {
		if provider, ok := svc.(interface{ Storage() glue.Storage }); ok && svc.Name() == "glue" {
			catalog = provider.Storage()
		}
		if provider, ok := svc.(interface{ Storage() s3.Storage }); ok && svc.Name() == "s3" {
			objects = provider.Storage()
		}
	}
	if catalog == nil || objects == nil {
		return nil, nil, fmt.Errorf("Athena requires registered Glue and S3 stores")
	}
	return catalog, objects, nil
}

func s3Location(location string) (string, string, error) {
	parsed, err := url.Parse(location)
	if err != nil || parsed.Scheme != "s3" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", fmt.Errorf("unsupported S3 location %q", location)
	}
	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}

func sqlString(value string) string     { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
func sqlIdentifier(value string) string { return "\"" + strings.ReplaceAll(value, "\"", "\"\"") + "\"" }

var simpleSQLName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var decimalType = regexp.MustCompile(`^decimal\([0-9]+,[0-9]+\)$`)

func glueColumnType(raw string) (string, error) {
	typeName := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), " ", ""))
	switch typeName {
	case "string", "varchar", "char":
		return "VARCHAR", nil
	case "tinyint", "smallint", "int", "integer", "bigint", "float", "double", "boolean", "date", "timestamp":
		return strings.ToUpper(typeName), nil
	}
	if decimalType.MatchString(typeName) {
		return strings.ToUpper(typeName), nil
	}
	return "", fmt.Errorf("unsupported Glue column type %q", raw)
}

// Materialize only the referenced Glue table. Objects come from the emulator's
// storage, never remote AWS or a SQL-supplied filesystem location.
func bindCatalogTable(ctx context.Context, conn *sql.Conn, catalog glue.Storage, objects s3.Storage, reference, database, tempDir string) (int64, error) {
	parts := strings.Split(reference, ".")
	if len(parts) == 1 {
		parts = []string{database, parts[0]}
	}
	if len(parts) != 2 || !simpleSQLName.MatchString(parts[0]) || !simpleSQLName.MatchString(parts[1]) {
		return 0, fmt.Errorf("unsupported catalog table name %q", reference)
	}
	table, err := catalog.GetTable(ctx, "", parts[0], parts[1])
	if err != nil {
		return 0, fmt.Errorf("resolve Glue table %s: %w", reference, err)
	}
	if len(table.PartitionKeys) > 0 || table.StorageDescriptor == nil || table.ViewOriginalText != "" || table.ViewExpandedText != "" {
		return 0, fmt.Errorf("partitioned tables and Glue views are not supported")
	}
	descriptor := table.StorageDescriptor
	if len(descriptor.Columns) == 0 {
		return 0, fmt.Errorf("Glue table has no columns")
	}
	definitions := make([]string, 0, len(descriptor.Columns))
	columns := make([]string, 0, len(descriptor.Columns))
	projections := make([]string, 0, len(descriptor.Columns))
	for _, column := range descriptor.Columns {
		typeName, typeErr := glueColumnType(column.Type)
		if typeErr != nil {
			return 0, typeErr
		}
		definitions = append(definitions, sqlIdentifier(column.Name)+" "+typeName)
		columns = append(columns, sqlString(column.Name)+":"+sqlString(typeName))
		projections = append(projections, "CAST("+sqlIdentifier(column.Name)+" AS "+typeName+") AS "+sqlIdentifier(column.Name))
	}
	if _, err := conn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+sqlIdentifier(parts[0])); err != nil {
		return 0, fmt.Errorf("create local schema: %w", err)
	}
	name := sqlIdentifier(parts[0]) + "." + sqlIdentifier(parts[1])
	if _, err := conn.ExecContext(ctx, "CREATE TABLE "+name+" ("+strings.Join(definitions, ",")+")"); err != nil {
		return 0, fmt.Errorf("create catalog table: %w", err)
	}
	bucket, prefix, err := s3Location(descriptor.Location)
	if err != nil {
		return 0, err
	}
	listed, _, err := objects.ListObjects(ctx, bucket, prefix, "", int(^uint(0)>>1))
	if err != nil {
		return 0, fmt.Errorf("list table objects: %w", err)
	}
	var files []string
	var scanned int64
	for _, entry := range listed {
		if strings.HasSuffix(entry.Key, "/") || strings.HasPrefix(path.Base(entry.Key), "_") || strings.HasPrefix(path.Base(entry.Key), ".") {
			continue
		}
		if err := ctx.Err(); err != nil {
			return scanned, fmt.Errorf("read catalog: %w", err)
		}
		object, readErr := objects.GetObject(ctx, bucket, entry.Key)
		if readErr != nil {
			return scanned, fmt.Errorf("read table object: %w", readErr)
		}
		file, createErr := os.CreateTemp(tempDir, "input-*"+filepath.Ext(entry.Key))
		if createErr != nil {
			return scanned, fmt.Errorf("stage table object: %w", createErr)
		}
		_, writeErr := file.Write(object.Body)
		closeErr := file.Close()
		if writeErr != nil {
			return scanned, fmt.Errorf("stage object body: %w", writeErr)
		}
		if closeErr != nil {
			return scanned, fmt.Errorf("close staged object: %w", closeErr)
		}
		files = append(files, sqlString(file.Name()))
		scanned += object.Size
	}
	reader, err := catalogReader(descriptor, files, columns, projections)
	if err != nil {
		return scanned, err
	}
	if len(files) == 0 {
		return scanned, nil
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO "+name+" "+reader); err != nil {
		return scanned, fmt.Errorf("decode Glue table data: %w", err)
	}
	return scanned, nil
}

func catalogReader(descriptor *glue.StorageDescriptor, files, columns, projections []string) (string, error) {
	serde := ""
	if descriptor.SerdeInfo != nil {
		serde = descriptor.SerdeInfo.SerializationLibrary
	}
	fileList := "[" + strings.Join(files, ",") + "]"
	columnMap := "{" + strings.Join(columns, ",") + "}"
	switch {
	case strings.Contains(descriptor.InputFormat, "parquet") || strings.Contains(serde, "parquet"):
		return "SELECT " + strings.Join(projections, ",") + " FROM read_parquet(" + fileList + ")", nil
	case serde == "org.openx.data.jsonserde.JsonSerDe" || serde == "org.apache.hive.hcatalog.data.JsonSerDe":
		return "SELECT * FROM read_json(" + fileList + ", format='newline_delimited', columns=" + columnMap + ")", nil
	case serde == "org.apache.hadoop.hive.serde2.RegexSerDe":
		if len(columns) != 1 || descriptor.SerdeInfo.Parameters["input.regex"] != "(.*)" {
			return "", fmt.Errorf("only a single-column (.*) RegexSerDe is supported")
		}
		return "SELECT * FROM read_csv(" + fileList + ", header=false, auto_detect=false, delim=E'\\x1f', quote='', escape='', columns=" + columnMap + ")", nil
	default:
		return "", fmt.Errorf("unsupported Glue input format/serde %q / %q", descriptor.InputFormat, serde)
	}
}
