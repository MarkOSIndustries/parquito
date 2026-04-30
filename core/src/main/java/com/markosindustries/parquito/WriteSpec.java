package com.markosindustries.parquito;

import org.apache.parquet.format.CompressionCodec;

public record WriteSpec(
    long maxRowsPerRowGroup,
    CompressionCodec compressionCodec,
    EncodingSelector encodingSelector,
    BloomFilterSelector bloomFilterSelector) {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private long maxRowsPerRowGroup = 1_000_000;
    private CompressionCodec compressionCodec = CompressionCodec.UNCOMPRESSED;
    private EncodingSelector encodingSelector = EncodingSelector.DEFAULT;
    private BloomFilterSelector bloomFilterSelector = BloomFilterSelector.DEFAULT;

    public Builder withMaxRowsPerRowGroup(final long maxRowsPerRowGroup) {
      this.maxRowsPerRowGroup = maxRowsPerRowGroup;
      return this;
    }

    public Builder withCompressionCodec(final CompressionCodec compressionCodec) {
      this.compressionCodec = compressionCodec;
      return this;
    }

    public Builder withEncodingSelector(final EncodingSelector encodingSelector) {
      this.encodingSelector = encodingSelector;
      return this;
    }

    public Builder withBloomFilterSelector(final BloomFilterSelector bloomFilterSelector) {
      this.bloomFilterSelector = bloomFilterSelector;
      return this;
    }

    public WriteSpec build() {
      return new WriteSpec(
          maxRowsPerRowGroup, compressionCodec, encodingSelector, bloomFilterSelector);
    }
  }
}
