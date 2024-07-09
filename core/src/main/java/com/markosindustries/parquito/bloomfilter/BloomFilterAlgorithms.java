package com.markosindustries.parquito.bloomfilter;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.parquet.format.BloomFilterAlgorithm;

public final class BloomFilterAlgorithms {
  private static final Map<
          BloomFilterAlgorithm._Fields, Function<ByteBuffer, BloomFilterImplementation>>
      REGISTERED_IMPLEMENTATIONS =
          new HashMap<>() {
            {
              put(BloomFilterAlgorithm._Fields.BLOCK, SplitBlockBloomFilterImplementation::new);
            }
          };

  public static synchronized void register(
      BloomFilterAlgorithm._Fields hashFunction,
      Function<ByteBuffer, BloomFilterImplementation> bloomFilterImplementationConstructor) {
    REGISTERED_IMPLEMENTATIONS.put(hashFunction, bloomFilterImplementationConstructor);
  }

  public static BloomFilterImplementation read(
      final BloomFilterAlgorithm algorithm, ByteBuffer bitset) {
    final var registeredImplementation = REGISTERED_IMPLEMENTATIONS.get(algorithm.getSetField());
    if (Objects.isNull(registeredImplementation)) {
      throw new UnsupportedOperationException(
          "Use "
              + BloomFilterAlgorithms.class.getName()
              + ".register() to add support for "
              + BloomFilterAlgorithm.class.getSimpleName()
              + " "
              + algorithm.getSetField().getFieldName());
    }
    return registeredImplementation.apply(bitset);
  }
}
