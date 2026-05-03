package com.markosindustries.parquito.encoding;

import org.apache.parquet.format.Encoding;

public interface IntEncodings {
  ParquetIntEncoding INT_ENCODING_RLE = new RLEIntEncoding(true, false);
  ParquetIntEncoding INT_ENCODING_DICTIONARY_INDICES = new RLEIntEncoding(false, false);
  ParquetIntEncoding INT_ENCODING_DATA_PAGE_V2_LEVELS = new RLEIntEncoding(false, true);
  ParquetIntEncoding INT_ENCODING_BIT_PACKED = new BitPackedIntEncoding();
  ParquetIntEncoding INT_ENCODING_DELTA_BINARY_PACKED = new DeltaBinaryIntEncoding();

  static ParquetIntEncoding getDecoder(Encoding encoding) {
    return switch (encoding) {
      case RLE -> INT_ENCODING_RLE;
      case BIT_PACKED -> INT_ENCODING_BIT_PACKED;
      case DELTA_BINARY_PACKED -> INT_ENCODING_DELTA_BINARY_PACKED;

      default ->
          throw new UnsupportedOperationException("Unsupported integer encoding: " + encoding);
    };
  }
}
