package com.markosindustries.parquito.bloomfilter;

import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.parquet.format.BloomFilterHeader;

public record BloomFilter(
    BloomFilterHashFunction hashFunction, BloomFilterImplementation bloomFilterImplementation) {

  public static BloomFilter from(BloomFilterHeader header, ByteBuffer bitset) {
    return new BloomFilter(
        BloomFilterHashFunctions.find(header.hash),
        BloomFilterAlgorithms.read(header.algorithm, bitset));
  }

  public <ReadAs> boolean mightContain(final ReadAs value) {
    final var hash = hashFunction.hash(value);
    return bloomFilterImplementation.mightContain(hash);
  }

  public <ReadAs> boolean mightContainAny(final Collection<ReadAs> values) {
    for (final var value : values) {
      final var hash = hashFunction.hash(value);
      if (bloomFilterImplementation.mightContain(hash)) {
        return true;
      }
    }
    return false;
  }
}
