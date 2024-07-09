package com.markosindustries.parquito.bloomfilter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import net.openhft.hashing.LongHashFunction;

public abstract class BloomFilterHashFunction {
  private final ByteBuffer valueAsBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);

  public <T> long hash(final T value) {
    try {
      if (value instanceof byte[] v) {
        return hashBytes(v);
      }
      if (value instanceof ByteBuffer v) {
        return hashBytes(v);
      }
      if (value instanceof String v) {
        return hashBytes(v.getBytes(StandardCharsets.UTF_8));
      }
      if (value instanceof Integer v) {
        return hashBytes(this.valueAsBytes.putInt(v).flip());
      }
      if (value instanceof Long v) {
        return hashBytes(this.valueAsBytes.putLong(v).flip());
      }
      if (value instanceof Float v) {
        return hashBytes(this.valueAsBytes.putFloat(v).flip());
      }
      if (value instanceof Double v) {
        return hashBytes(this.valueAsBytes.putDouble(v).flip());
      }

      throw new IllegalArgumentException("Can't hash values of type " + value.getClass().getName());
    } finally {
      this.valueAsBytes.clear();
    }
  }

  abstract long hashBytes(byte[] value);

  abstract long hashBytes(ByteBuffer value);

  static final class XXH64 extends BloomFilterHashFunction {
    private static final LongHashFunction IMPL = LongHashFunction.xx();

    @Override
    long hashBytes(final byte[] value) {
      return IMPL.hashBytes(value);
    }

    @Override
    long hashBytes(final ByteBuffer value) {
      return IMPL.hashBytes(value);
    }
  }
}
