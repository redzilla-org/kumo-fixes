module github.com/sivchari/kumo

go 1.25.0

toolchain go1.26.8

require (
	github.com/aws/aws-sdk-go-v2 v1.43.4
	github.com/aws/aws-sdk-go-v2/config v1.32.17
	github.com/aws/aws-sdk-go-v2/credentials v1.19.16
	github.com/aws/aws-sdk-go-v2/service/acm v1.38.3
	github.com/aws/aws-sdk-go-v2/service/amplify v1.38.16
	github.com/aws/aws-sdk-go-v2/service/apigateway v1.39.3
	github.com/aws/aws-sdk-go-v2/service/apigatewayv2 v1.35.2
	github.com/aws/aws-sdk-go-v2/service/appmesh v1.35.14
	github.com/aws/aws-sdk-go-v2/service/appsync v1.53.7
	github.com/aws/aws-sdk-go-v2/service/athena v1.57.6
	github.com/aws/aws-sdk-go-v2/service/backup v1.55.2
	github.com/aws/aws-sdk-go-v2/service/cloudformation v1.76.1
	github.com/aws/aws-sdk-go-v2/service/cloudfront v1.67.4
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.66.3
	github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs v1.81.1
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.57.3
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.36.4
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.319.1
	github.com/aws/aws-sdk-go-v2/service/ecr v1.60.4
	github.com/aws/aws-sdk-go-v2/service/ecs v1.90.0
	github.com/aws/aws-sdk-go-v2/service/eventbridge v1.45.25
	github.com/aws/aws-sdk-go-v2/service/firehose v1.46.4
	github.com/aws/aws-sdk-go-v2/service/iam v1.58.1
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.43.7
	github.com/aws/aws-sdk-go-v2/service/kms v1.51.1
	github.com/aws/aws-sdk-go-v2/service/lambda v1.101.2
	github.com/aws/aws-sdk-go-v2/service/route53 v1.65.6
	github.com/aws/aws-sdk-go-v2/service/s3 v1.100.1
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.44.4
	github.com/aws/aws-sdk-go-v2/service/sesv2 v1.66.4
	github.com/aws/aws-sdk-go-v2/service/sfn v1.45.4
	github.com/aws/aws-sdk-go-v2/service/sns v1.42.4
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.27
	github.com/aws/aws-sdk-go-v2/service/ssm v1.73.4
	github.com/aws/aws-sdk-go-v2/service/sts v1.45.4
	github.com/duckdb/duckdb-go/v2 v2.10505.0
	github.com/fxamacker/cbor/v2 v2.9.0
	github.com/google/uuid v1.6.0
	github.com/spf13/cobra v1.10.2
	golang.org/x/crypto v0.47.0
)

require (
	github.com/apache/arrow-go/v18 v18.5.1 // indirect
	github.com/duckdb/duckdb-go-bindings v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-amd64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-arm64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/windows-amd64 v0.10505.0 // indirect
	github.com/ebitengine/purego v0.10.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/compress v1.18.3 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/telemetry v0.0.0-20260116145544-c6413dc483f5 // indirect
	golang.org/x/tools v0.41.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.16 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.36 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.35 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.21 // indirect
	github.com/aws/smithy-go v1.27.6 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	github.com/tobilg/polyglot/packages/go v0.12.0
	github.com/x448/float16 v0.8.4 // indirect
	golang.org/x/sys v0.40.0 // indirect
)
