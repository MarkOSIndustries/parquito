package com.markosindustries.parquito;

import org.apache.parquet.format.CompressionCodec;

public record WriteSpec(
    long targetBytesPerRowGroup,
    int targetBytesPerDataPage,
    long maxRowsPerRowGroup,
    int maxValuesPerDataPage,
    CompressionCodec compressionCodec,
    EncodingSelector encodingSelector,
    BloomFilterSelector bloomFilterSelector) {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private long targetBytesPerRowGroup = Bytes.fromGb(1);
    private int targetBytesPerDataPage = Bytes.fromKb(8);
    private long maxRowsPerRowGroup = 100_000;
    private int maxValuesPerDataPage = 32_767;
    private CompressionCodec compressionCodec = CompressionCodec.UNCOMPRESSED;
    private EncodingSelector encodingSelector = EncodingSelector.DEFAULT;
    private BloomFilterSelector bloomFilterSelector = BloomFilterSelector.DEFAULT;

    public Builder withTargetBytesPerRowGroup(final long targetBytesPerRowGroup) {
      this.targetBytesPerRowGroup = targetBytesPerRowGroup;
      return this;
    }

    public Builder withTargetBytesPerDataPage(final int targetBytesPerDataPage) {
      this.targetBytesPerDataPage = targetBytesPerDataPage;
      return this;
    }

    public Builder withMaxRowsPerRowGroup(final long maxRowsPerRowGroup) {
      this.maxRowsPerRowGroup = maxRowsPerRowGroup;
      return this;
    }

    public Builder withMaxValuesPerDataPage(final int maxValuesPerDataPage) {
      this.maxValuesPerDataPage = maxValuesPerDataPage;
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
          targetBytesPerRowGroup,
          targetBytesPerDataPage,
          maxRowsPerRowGroup,
          maxValuesPerDataPage,
          compressionCodec,
          encodingSelector,
          bloomFilterSelector);
    }
  }
}
