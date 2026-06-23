package com.markosindustries.parquito.types;

import java.nio.ByteBuffer;
import java.util.Arrays;

public abstract class AbstractLogicalTypeConverter<T> implements LogicalTypeConverter<T> {
  private final Class<T> convertedClass;

  public AbstractLogicalTypeConverter(Class<T> convertedClass) {
    this.convertedClass = convertedClass;
  }

  public Class<T> getConvertedClass() {
    return convertedClass;
  }

  @Override
  public T fromBoolean(final boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T fromByteBuffer(final ByteBuffer value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T fromFloat(final float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T fromDouble(final double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T fromInt32(final int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T fromInt64(final long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean toBoolean(final T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer toByteBuffer(final T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float toFloat(final T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double toDouble(final T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int toInt32(final T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long toInt64(final T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compareBoolean(final boolean value, final T referenceValue) {
    return Boolean.compare(value, toBoolean(referenceValue));
  }

  @Override
  public int compareByteBuffer(final ByteBuffer value, final T referenceValue) {
    return unsignedByteComparison(value, toByteBuffer(referenceValue));
  }

  @Override
  public int compareFloat(final float value, final T referenceValue) {
    return Float.compare(value, toFloat(referenceValue));
  }

  @Override
  public int compareDouble(final double value, final T referenceValue) {
    return Double.compare(value, toDouble(referenceValue));
  }

  @Override
  public int compareInt32(final int value, final T referenceValue) {
    return Integer.compare(value, toInt32(referenceValue));
  }

  @Override
  public int compareInt64(final long value, final T referenceValue) {
    return Long.compare(value, toInt64(referenceValue));
  }

  private static int unsignedByteComparison(final ByteBuffer o1, final ByteBuffer o2) {
    if (o1.hasArray() && o2.hasArray()) {
      return Arrays.compareUnsigned(
          o1.array(),
          o1.arrayOffset() + o1.position(),
          o1.arrayOffset() + o1.limit(),
          o2.array(),
          o2.arrayOffset() + o2.position(),
          o2.arrayOffset() + o2.limit());
    }

    final int o1Start = o1.position();
    final int o1Size = o1.limit() - o1Start;
    final int o2Start = o2.position();
    final int o2Size = o2.limit() - o2Start;
    int cmp;
    for (int o1Index = o1Start, o2Index = o2Start;
        o1Index < o1.limit() && o2Index < o2.limit();
        o1Index++, o2Index++) {
      cmp = Byte.compareUnsigned(o1.get(o1Index), o2.get(o2Index));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(o1Size, o2Size);
  }

  private static int unsignedLongComparison(final long o1, final long o2) {
    final int top63Cmp = Long.compare(o1 >>> 1, o2 >>> 1);
    if (top63Cmp != 0) {
      return top63Cmp;
    }
    return Long.compare(o1 & 1L, o2 & 1L);
  }
}
