package com.markosindustries.parquito;

import org.apache.parquet.format.ColumnMetaData;

public interface BloomFilterSelector {
  boolean shouldWriteBloomFilter(
      final ColumnMetaData columnMetaData, long distinctValues, long totalValues, long totalNulls);

  BloomFilterSelector DEFAULT = new DefaultBloomFilterSelector();

  class DefaultBloomFilterSelector implements BloomFilterSelector {
    @Override
    public boolean shouldWriteBloomFilter(
        final ColumnMetaData columnMetaData,
        final long distinctValues,
        final long totalValues,
        final long totalNulls) {
      return false;
    }
  }
}
