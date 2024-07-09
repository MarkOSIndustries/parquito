package com.markosindustries.parquito.encoding;

import org.apache.parquet.format.Encoding;

public interface IntEncodings {
  static int bitWidth(int value) {
    return 32 - Integer.numberOfLeadingZeros(value);
  }

  ParquetIntEncoding INT_ENCODING_RLE = new RLEIntEncoding(true);
  ParquetIntEncoding INT_ENCODING_RLE_WITHOUT_LENGTH_HEADER = new RLEIntEncoding(false);
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
