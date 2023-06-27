package com.markosindustries.parquito.protobuf;

/**
 * Configuration for how we convert between protobuf and parquet schemas.
 * @param enumsAsInt32 Rather than Parquet's default of enums as strings, store the integer value of enums - which preserves Protobuf's backwards compatibility guarantees around renames
 */
public record ProtobufParquetConfig(boolean enumsAsInt32) {}
