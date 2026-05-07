package com.markosindustries.parquito.bloomfilter;

import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.parquet.format.BloomFilterAlgorithm;
import org.apache.parquet.format.BloomFilterCompression;
import org.apache.parquet.format.BloomFilterHash;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.SplitBlockAlgorithm;
import org.apache.parquet.format.Uncompressed;
import org.apache.parquet.format.XxHash;

public record BloomFilter(
    BloomFilterHeader header,
    ByteBuffer bitset,
    BloomFilterHashFunction hashFunction,
    BloomFilterImplementation bloomFilterImplementation)
    implements BloomFilterRead, BloomFilterWrite {

  public static BloomFilter create(final BloomFilterHeader header, final ByteBuffer bitset) {
    return new BloomFilter(
        header,
        bitset,
        BloomFilterHashFunctions.find(header.hash),
        BloomFilterAlgorithms.create(header.algorithm, bitset));
  }

  public static BloomFilter createEmpty(final int sizeInBytes) {
    final BloomFilterHash hashFunction = BloomFilterHash.XXHASH(new XxHash());
    final var algorithm = BloomFilterAlgorithm.BLOCK(new SplitBlockAlgorithm());
    final var bitset = ByteBuffer.allocate(sizeInBytes);
    final var header =
        new BloomFilterHeader(
            bitset.capacity(),
            algorithm,
            hashFunction,
            BloomFilterCompression.UNCOMPRESSED(new Uncompressed()));
    return create(header, bitset);
  }

  public <Value> boolean mightContain(final Value value) {
    final var hash = hashFunction.hash(value);
    return bloomFilterImplementation.mightContain(hash);
  }

  public <Value> boolean mightContainAny(final Collection<Value> values) {
    for (final var value : values) {
      final var hash = hashFunction.hash(value);
      if (bloomFilterImplementation.mightContain(hash)) {
        return true;
      }
    }
    return false;
  }

  public <Value> void insert(final Value value) {
    final var hash = hashFunction.hash(value);
    bloomFilterImplementation.insert(hash);
  }

  public <Value> void insertAll(final Iterable<Value> values) {
    for (final var value : values) {
      final var hash = hashFunction.hash(value);
      bloomFilterImplementation.insert(hash);
    }
  }
}
