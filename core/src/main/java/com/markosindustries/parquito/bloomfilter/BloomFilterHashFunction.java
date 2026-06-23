package com.markosindustries.parquito.bloomfilter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import net.openhft.hashing.LongHashFunction;

public abstract class BloomFilterHashFunction {
  private final ByteBuffer valueAsBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);

  public long hash(final ByteBuffer value) {
    return hashBytes(value);
  }

  public long hash(final double value) {
    return hashBytes(this.valueAsBytes.putDouble(value).flip());
  }

  public long hash(final float value) {
    return hashBytes(this.valueAsBytes.putFloat(value).flip());
  }

  public long hash(final int value) {
    return hashBytes(this.valueAsBytes.putInt(value).flip());
  }

  public long hash(final long value) {
    return hashBytes(this.valueAsBytes.putLong(value).flip());
  }

  abstract long hashBytes(ByteBuffer value);

  static final class XXH64 extends BloomFilterHashFunction {
    public static final XXH64 INSTANCE = new XXH64();

    private static final LongHashFunction IMPL = LongHashFunction.xx();

    @Override
    long hashBytes(final ByteBuffer value) {
      return IMPL.hashBytes(value);
    }
  }
}
