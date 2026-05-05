package com.markosindustries.parquito.protobuf;

/**
 * Configuration for how we convert between protobuf and parquet schemas.
 *
 * @param enumsAsInt32 Rather than Parquet's default of enums as strings, store the integer value of
 *     enums - which preserves Protobuf's backwards compatibility guarantees around renames
 */
public record ProtobufParquetConfig(int recursionLimit, boolean enumsAsInt32) {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private int recursionLimit = 100;
    private boolean enumsAsInt32 = false;

    public Builder withRecursionLimit(final int recursionLimit) {
      this.recursionLimit = recursionLimit;
      return this;
    }

    public Builder withEnumsAsInt32(final boolean enumsAsInt32) {
      this.enumsAsInt32 = enumsAsInt32;
      return this;
    }

    public ProtobufParquetConfig build() {
      return new ProtobufParquetConfig(recursionLimit, enumsAsInt32);
    }
  }
}
