-- ADR-002 verification 2: Flink writes an Iceberg table through Lakekeeper
-- onto SeaweedFS. scripts/flink-iceberg-check.sh runs this, then reads the
-- table back with Trino. S3 credentials come from the container environment.

SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

CREATE CATALOG lake WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://lakekeeper:8181/catalog',
    'warehouse' = 'lakehouse',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://seaweedfs:8333',
    's3.path-style-access' = 'true',
    'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lake.flink_check;

CREATE TABLE lake.flink_check.smoke (id BIGINT, note STRING);

INSERT INTO lake.flink_check.smoke VALUES (1, 'a'), (2, 'b'), (3, 'c');
