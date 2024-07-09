package com.markosindustries.parquito.bloomfilter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.parquet.format.BloomFilterHash;

public final class BloomFilterHashFunctions {
  private static final Map<BloomFilterHash._Fields, Supplier<BloomFilterHashFunction>>
      REGISTERED_HASH_FUNCTIONS =
          new HashMap<>() {
            {
              put(BloomFilterHash._Fields.XXHASH, BloomFilterHashFunction.XXH64::new);
            }
          };

  public static synchronized void register(
      BloomFilterHash._Fields hashFunction,
      Supplier<BloomFilterHashFunction> hashFunctionSupplier) {
    REGISTERED_HASH_FUNCTIONS.put(hashFunction, hashFunctionSupplier);
  }

  public static BloomFilterHashFunction find(final BloomFilterHash hashFunction) {
    final var registeredHashFunction = REGISTERED_HASH_FUNCTIONS.get(hashFunction.getSetField());
    if (Objects.isNull(registeredHashFunction)) {
      throw new UnsupportedOperationException(
          "Use "
              + BloomFilterHashFunctions.class.getName()
              + ".register() to add support for "
              + BloomFilterHash.class.getName()
              + " "
              + hashFunction.getSetField().getFieldName());
    }
    return registeredHashFunction.get();
  }
}
