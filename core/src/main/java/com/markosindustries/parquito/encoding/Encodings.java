package com.markosindustries.parquito.encoding;

import org.apache.parquet.format.Encoding;

public class Encodings {
  public static <ReadAs extends Comparable<ReadAs>> ParquetEncoding<ReadAs> getDecoder(
      Encoding encoding) {
    return switch (encoding) {
      case PLAIN -> new PlainEncoding<>();
      case PLAIN_DICTIONARY, RLE_DICTIONARY -> new DictionaryEncoding<>();

      default -> throw new UnsupportedOperationException("Unsupported encoding: " + encoding);
        //      case BIT_PACKED -> null;
        //      case DELTA_BINARY_PACKED -> null;
        //      case DELTA_LENGTH_BYTE_ARRAY -> null;
        //      case DELTA_BYTE_ARRAY -> null;
        //      case BYTE_STREAM_SPLIT -> null;
    };
  }
}
