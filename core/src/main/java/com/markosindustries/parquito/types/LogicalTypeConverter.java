package com.markosindustries.parquito.types;

import com.markosindustries.parquito.page.Values;
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

  // TODO - make this replace all the fromXXX methods
  default T from(Values values, int valueIndex) {
    return switch (getType()) {
      case BOOLEAN -> fromBoolean(values.getBoolean(valueIndex));
      case INT32 -> fromInt32(values.getInt32(valueIndex));
      case INT64 -> fromInt64(values.getInt64(valueIndex));
      case INT96 -> throw new UnsupportedOperationException("We can't currently handle Int96");
      case FLOAT -> fromFloat(values.getFloat(valueIndex));
      case DOUBLE -> fromDouble(values.getDouble(valueIndex));
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> fromByteBuffer(values.getByteBuffer(valueIndex));
    };
  }
}
