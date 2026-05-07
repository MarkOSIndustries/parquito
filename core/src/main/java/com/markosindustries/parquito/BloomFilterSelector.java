package com.markosindustries.parquito;

import java.util.Optional;
import org.apache.parquet.format.Type;

/** Determines whether to write a bloom filter for a given column chunk */
public interface BloomFilterSelector {
  /**
   * This will be called for each ColumnChunk to decide whether to write a bloom filter for that
   * ColumnChunk, and if so - the desired false positive probability to be used to size the bloom
   * filter.
   *
   * @param type The type of data in the column
   * @param schemaPath The path within the schema of the column
   * @param distinctValues The number of distinct (unique) non-null values
   * @param totalValues The total values in the chunk (including nulls)
   * @param totalNulls The total nulls in the chunk
   * @return Optional.empty() for "no bloom filter" or an Optional.of(falsePositiveProbability) to
   *     indicate that a bloom filter should be created with enough bytes for the given false
   *     positive probability
   */
  Optional<Double> shouldWriteBloomFilter(
      final Type type,
      final ParquetSchemaPath schemaPath,
      long distinctValues,
      long totalValues,
      long totalNulls);

  BloomFilterSelector DEFAULT = new DefaultBloomFilterSelector();

  class DefaultBloomFilterSelector implements BloomFilterSelector {
    @Override
    public Optional<Double> shouldWriteBloomFilter(
        final Type type,
        final ParquetSchemaPath schemaPath,
        final long distinctValues,
        final long totalValues,
        final long totalNulls) {
      return Optional.empty();
    }
  }
}
