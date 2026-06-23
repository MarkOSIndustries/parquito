package com.markosindustries.parquito.json.types;

import com.markosindustries.parquito.types.BooleanConverter;
import com.markosindustries.parquito.types.ConversionStrategy;
import com.markosindustries.parquito.types.DoubleConverter;
import com.markosindustries.parquito.types.DurationMicrosConverter;
import com.markosindustries.parquito.types.DurationMillisConverter;
import com.markosindustries.parquito.types.DurationNanosConverter;
import com.markosindustries.parquito.types.FloatConverter;
import com.markosindustries.parquito.types.InstantDatesConverter;
import com.markosindustries.parquito.types.InstantMicrosConverter;
import com.markosindustries.parquito.types.InstantMillisConverter;
import com.markosindustries.parquito.types.InstantNanosConverter;
import com.markosindustries.parquito.types.IntegerConverter;
import com.markosindustries.parquito.types.LogicalTypeConverter;
import com.markosindustries.parquito.types.LongConverter;
import com.markosindustries.parquito.types.StringConverter;
import com.markosindustries.parquito.types.UUIDConverter;
import com.markosindustries.parquito.types.UnsignedIntegerConverter;
import com.markosindustries.parquito.types.UnsignedLongConverter;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.Type;

public class JSONTypeConversionStrategy implements ConversionStrategy {
  public LogicalTypeConverter<?> converterFor(
      final Type type, final LogicalType logicalType, final int typeLength) {
    if (type == null) return null;
    if (logicalType == null || logicalType.isSetUNKNOWN()) {
      return switch (type) {
        case BOOLEAN -> BooleanConverter.INSTANCE;
        case INT32 -> IntegerConverter.INSTANCE;
        case INT64 -> LongConverter.INSTANCE;
        case INT96 -> throw new UnsupportedOperationException("Can't handle int96 yet");
        case FLOAT -> FloatConverter.INSTANCE;
        case DOUBLE -> DoubleConverter.INSTANCE;
        case BYTE_ARRAY -> Base64StringConverter.VARIABLE_LENGTH;
        case FIXED_LEN_BYTE_ARRAY -> Base64StringConverter.FIXED_LENGTH;
      };
    }

    return switch (logicalType.getSetField()) {
      case STRING, ENUM ->
          switch (type) {
            case BYTE_ARRAY -> StringConverter.VARIABLE_LENGTH;
            case FIXED_LEN_BYTE_ARRAY -> StringConverter.FIXED_LENGTH;
            default ->
                throw new IllegalArgumentException(
                    "Logical type INTEGER is not valid for type " + type);
          };
      case DATE -> InstantDatesConverter.INSTANCE;
      case TIME ->
          switch (logicalType.getTIME().unit.getSetField()) {
            case MILLIS -> DurationMillisConverter.INSTANCE;
            case MICROS -> DurationMicrosConverter.INSTANCE;
            case NANOS -> DurationNanosConverter.INSTANCE;
          };
      case TIMESTAMP ->
          switch (logicalType.getTIMESTAMP().unit.getSetField()) {
            case MILLIS -> InstantMillisConverter.INSTANCE;
            case MICROS -> InstantMicrosConverter.INSTANCE;
            case NANOS -> InstantNanosConverter.INSTANCE;
          };
      case INTEGER ->
          switch (type) {
            case INT32 ->
                logicalType.getINTEGER().isSigned
                    ? IntegerConverter.INSTANCE
                    : UnsignedIntegerConverter.INSTANCE;
            case INT64 ->
                logicalType.getINTEGER().isSigned
                    ? LongConverter.INSTANCE
                    : UnsignedLongConverter.INSTANCE;
            default ->
                throw new IllegalArgumentException(
                    "Logical type INTEGER is not valid for type " + type);
          };
      case UUID -> UUIDConverter.INSTANCE;

      default ->
          throw new IllegalArgumentException(
              "Can't currently read values of type " + type + " - logicalType " + logicalType);
    };
  }
}
