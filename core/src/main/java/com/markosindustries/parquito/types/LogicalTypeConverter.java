package com.markosindustries.parquito.types;

import java.nio.ByteBuffer;
import org.apache.parquet.format.Type;

public interface LogicalTypeConverter<T> {
  Type getType();

  Class<T> getConvertedClass();

  T fromBoolean(final boolean value);

  T fromByteBuffer(final ByteBuffer value);

  T fromFloat(final float value);

  T fromDouble(final double value);

  T fromInt32(final int value);

  T fromInt64(final long value);

  boolean toBoolean(final T value);

  ByteBuffer toByteBuffer(final T value);

  float toFloat(final T value);

  double toDouble(final T value);

  int toInt32(final T value);

  long toInt64(final T value);

  int compareBoolean(final boolean value, T referenceValue);

  int compareByteBuffer(final ByteBuffer value, T referenceValue);

  int compareFloat(final float value, T referenceValue);

  int compareDouble(final double value, T referenceValue);

  int compareInt32(final int value, T referenceValue);

  int compareInt64(final long value, T referenceValue);
}
