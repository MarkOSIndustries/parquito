package com.markosindustries.parquito.types;

import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.Type;

public class IdentityConversionStrategy implements ConversionStrategy {
  @Override
  public LogicalTypeConverter<?> converterFor(
      final Type type, final LogicalType logicalType, final int typeLength) {
    return switch (type) {
      case BOOLEAN -> BooleanConverter.INSTANCE;
      case INT32 -> IntegerConverter.INSTANCE;
      case INT64 -> LongConverter.INSTANCE;
      case INT96 -> throw new UnsupportedOperationException("Can't handle int96 yet");
      case FLOAT -> FloatConverter.INSTANCE;
      case DOUBLE -> DoubleConverter.INSTANCE;
      case BYTE_ARRAY -> ByteBufferConverter.VARIABLE_LENGTH;
      case FIXED_LEN_BYTE_ARRAY -> ByteBufferConverter.FIXED_LENGTH;
    };
  }
}
