package com.markosindustries.parquito;

import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;

@FunctionalInterface
public interface EncodingSelector {
  Encoding selectEncoding(
      final ColumnMetaData columnMetaData, long distinctValues, long totalValues, long totalNulls);

  EncodingSelector DEFAULT = new DefaultEncodingSelector();

  class DefaultEncodingSelector implements EncodingSelector {
    @Override
    public Encoding selectEncoding(
        final ColumnMetaData columnMetaData,
        final long distinctValues,
        final long totalValues,
        final long totalNulls) {
      final var totalNonNull = totalValues - totalNulls;
      if (totalNonNull == 0) {
        return Encoding.PLAIN;
      }
      if (distinctValues << 1 <= totalNonNull) {
        return Encoding.RLE_DICTIONARY;
      }
      return switch (columnMetaData.type) {
        case BOOLEAN -> Encoding.RLE;
        case INT32 -> Encoding.DELTA_BINARY_PACKED;
        case INT64 -> Encoding.DELTA_BINARY_PACKED;
        case INT96 -> throw new UnsupportedOperationException("Can't handle int96 yet");
        case FLOAT -> Encoding.PLAIN;
        case DOUBLE -> Encoding.PLAIN;
        case BYTE_ARRAY -> Encoding.DELTA_LENGTH_BYTE_ARRAY;
        case FIXED_LEN_BYTE_ARRAY -> Encoding.DELTA_BYTE_ARRAY;
      };
    }
  }
}
