package com.markosindustries.parquito;

import com.markosindustries.parquito.bloomfilter.SplitBlockBloomFilterImplementation;
import com.markosindustries.parquito.encoding.Maths;
import java.util.Map;
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

  BloomFilterSelector DEFAULT = new None();

  class None implements BloomFilterSelector {
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

  class FalsePositiveProbabilities implements BloomFilterSelector {
    private final Map<ParquetSchemaPath, Double> columnsWithFalsePositiveProbabilities;

    public FalsePositiveProbabilities(
        final Map<ParquetSchemaPath, Double> columnsWithFalsePositiveProbabilities) {
      this.columnsWithFalsePositiveProbabilities = columnsWithFalsePositiveProbabilities;
    }

    @Override
    public Optional<Integer> shouldWriteBloomFilter(
        final Type type,
        final ParquetSchemaPath schemaPath,
        final long distinctValues,
        final long totalValues,
        final long totalNulls) {
      return Optional.ofNullable(columnsWithFalsePositiveProbabilities.get(schemaPath))
          .map(fpp -> bytesRequiredFor(distinctValues, fpp));
    }
  }

  /**
   * Create bloom filters for each column chunk with a path present in the Map, and size it based on
   * the given false positive probability value and the number of distinct values being written.
   *
   * @param columnsWithFalsePositiveProbabilities column paths with corresponding false positive
   *     probabilities
   * @return A BloomFilterSelector which sizes the given columns using the given FPPs
   */
  static FalsePositiveProbabilities fpp(
      final Map<ParquetSchemaPath, Double> columnsWithFalsePositiveProbabilities) {
    return new FalsePositiveProbabilities(columnsWithFalsePositiveProbabilities);
  }

  class FixedSizes implements BloomFilterSelector {
    private final Map<ParquetSchemaPath, Integer> columnsWithByteSizes;

    public FixedSizes(final Map<ParquetSchemaPath, Integer> columnsWithByteSizes) {
      this.columnsWithByteSizes = columnsWithByteSizes;
      if (columnsWithByteSizes.values().stream()
          .anyMatch(bytes -> Maths.remainderDivPow2(bytes, 3) != 0)) {
        throw new IllegalArgumentException("Bloom filter byte sizes must be a multiple of 8");
      }
    }

    @Override
    public Optional<Integer> shouldWriteBloomFilter(
        final Type type,
        final ParquetSchemaPath schemaPath,
        final long distinctValues,
        final long totalValues,
        final long totalNulls) {
      return Optional.ofNullable(columnsWithByteSizes.get(schemaPath));
    }
  }

  /**
   * Create bloom filters for each column chunk with a path present in the Map, and make them
   * exactly the corresponding byte size specified, regardless of any other factors.
   *
   * <p>Bloom filter byte sizes must be a multiple of 8.
   *
   * @param columnsWithByteSizes column paths with corresponding byte sizes
   * @return A BloomFilterSelector which sizes the given columns as instructed.
   */
  static FixedSizes fixedSize(final Map<ParquetSchemaPath, Integer> columnsWithByteSizes) {
    return new FixedSizes(columnsWithByteSizes);
  }

  /**
   * Return the number of bytes required for a bloom filter
   *
   * @param distinctValueCount The number of distinct values it will represent.
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
