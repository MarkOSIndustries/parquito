package com.markosindustries.parquito.encoding;

import org.apache.parquet.format.Encoding;

public class Encodings {
  public static ParquetEncoding getEncoding(Encoding encoding) {
    return switch (encoding) {
      case PLAIN -> new PlainEncoding();
      case PLAIN_DICTIONARY, RLE_DICTIONARY -> new DictionaryEncoding();
      case DELTA_BINARY_PACKED -> new DeltaBinaryPackedEncoding();
      case DELTA_LENGTH_BYTE_ARRAY -> new DeltaLengthByteArrayEncoding();
      case DELTA_BYTE_ARRAY -> new DeltaByteArrayEncoding();
      case RLE -> new RLEBooleanEncoding();
      case BYTE_STREAM_SPLIT -> new ByteStreamSplitEncoding();
      default -> throw new UnsupportedOperationException("Unsupported encoding: " + encoding);
    };
  }
}
