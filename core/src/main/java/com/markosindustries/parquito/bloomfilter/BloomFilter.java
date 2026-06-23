package com.markosindustries.parquito.bloomfilter;

import com.markosindustries.parquito.ColumnValuesSet;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.nio.ByteBuffer;
import java.util.List;
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

  public <T> boolean mightContainAny(final ColumnValuesSet<T> values) {
    final var logicalTypeConverter = values.getLogicalTypeConverter();
    switch (logicalTypeConverter.getType()) {
      case BOOLEAN ->
          throw new UnsupportedOperationException("Bloom filter doesn't support Booleans");
      case INT32 -> {
        for (final var value : values.getInts()) {
          if (bloomFilterImplementation.mightContain(hashFunction.hash(value))) {
            return true;
          }
        }
      }
      case INT64 -> {
        for (final var value : values.getLongs()) {
          if (bloomFilterImplementation.mightContain(hashFunction.hash(value))) {
            return true;
          }
        }
      }
      case INT96 -> throw new UnsupportedOperationException("Can't handle int96 yet");
      case FLOAT -> {
        for (final var value : values.getFloats()) {
          if (bloomFilterImplementation.mightContain(hashFunction.hash(value))) {
            return true;
          }
        }
      }
      case DOUBLE -> {
        for (final var value : values.getDoubles()) {
          if (bloomFilterImplementation.mightContain(hashFunction.hash(value))) {
            return true;
          }
        }
      }
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
        for (final var value : values.getByteBuffers()) {
          if (bloomFilterImplementation.mightContain(hashFunction.hash(value))) {
            return true;
          }
        }
      }
    }

    return false;
  }

  public void insert(final ByteBuffer value) {
    final var hash = hashFunction.hash(value);
    bloomFilterImplementation.insert(hash);
  }

  public void insert(final double value) {
    final var hash = hashFunction.hash(value);
    bloomFilterImplementation.insert(hash);
  }

  public void insert(final float value) {
    final var hash = hashFunction.hash(value);
    bloomFilterImplementation.insert(hash);
  }

  public void insert(final int value) {
    final var hash = hashFunction.hash(value);
    bloomFilterImplementation.insert(hash);
  }

  public void insert(final long value) {
    final var hash = hashFunction.hash(value);
    bloomFilterImplementation.insert(hash);
  }

  public void insertAll(final List<ByteBuffer> values) {
    for (final var value : values) {
      final var hash = hashFunction.hash(value);
      bloomFilterImplementation.insert(hash);
    }
  }

  public void insertAll(final DoubleList values) {
    for (final var value : values) {
      final var hash = hashFunction.hash(value);
      bloomFilterImplementation.insert(hash);
    }
  }

  public void insertAll(final FloatList values) {
    for (final var value : values) {
      final var hash = hashFunction.hash(value);
      bloomFilterImplementation.insert(hash);
    }
  }

  public void insertAll(final IntList values) {
    for (final var value : values) {
      final var hash = hashFunction.hash(value);
      bloomFilterImplementation.insert(hash);
    }
  }

  public void insertAll(final LongList values) {
    for (final var value : values) {
      final var hash = hashFunction.hash(value);
      bloomFilterImplementation.insert(hash);
    }
  }
}
