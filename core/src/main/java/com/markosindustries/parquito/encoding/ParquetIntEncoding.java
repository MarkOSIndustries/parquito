package com.markosindustries.parquito.encoding;

import java.io.IOException;
import java.io.InputStream;

public interface ParquetIntEncoding {
  int[] decode(
      final int expectedValues, final int bitWidth, final InputStream decompressedPageStream)
      throws IOException;
}
