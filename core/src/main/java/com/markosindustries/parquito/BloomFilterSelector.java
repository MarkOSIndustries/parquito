package com.markosindustries.parquito;

import com.markosindustries.parquito.bloomfilter.SplitBlockBloomFilterImplementation;
import java.util.Optional;
import org.apache.parquet.format.Type;

/** Determines whether to write a bloom filter for a given column chunk */
public interface BloomFilterSelector {
  /**
   * This will be called for each ColumnChunk to decide whether to write a bloom filter for that
   * ColumnChunk, and if so - the size of the bloom filter in bytes. ⚠️ Number of bytes must be a
   * multiple of 8.
   *
   * @param type The type of data in the column
   * @param schemaPath The path within the schema of the column
   * @param distinctValues The number of distinct (unique) non-null values
   * @param totalValues The total values in the chunk (including nulls)
   * @param totalNulls The total nulls in the chunk
   * @return Optional.empty() for "no bloom filter" or an Optional.of(bytes) to indicate that a
   *     bloom filter should be created with the given size in bytes. You can use {@link
   *     #bytesRequiredFor(long, double)} to estimate.
   */
  Optional<Integer> shouldWriteBloomFilter(
      final Type type,
      final ParquetSchemaPath schemaPath,
      long distinctValues,
      long totalValues,
      long totalNulls);

  BloomFilterSelector DEFAULT = new DefaultBloomFilterSelector();

  class DefaultBloomFilterSelector implements BloomFilterSelector {
    @Override
    public Optional<Integer> shouldWriteBloomFilter(
        final Type type,
        final ParquetSchemaPath schemaPath,
        final long distinctValues,
        final long totalValues,
        final long totalNulls) {
      return Optional.empty();
    }
  }

  /**
   * Return the number of bytes required for a bloom filter
   *
   * @param distinctValueCount The number of distinct values <i>in the entire domain, not just this
   *     file or column</i>
   * @param falsePositiveProbability The desired false positive probability - ie: the chance the
   *     bloom filter will return "might contain" when in fact it does not contain a value.
   * @return The number of bytes required
   */
  static int bytesRequiredFor(
      final long distinctValueCount, final double falsePositiveProbability) {
    return SplitBlockBloomFilterImplementation.bytesRequiredFor(
        distinctValueCount, falsePositiveProbability);
  }
}
