package com.markosindustries.parquito.encoding;

import java.io.IOException;
import java.io.InputStream;

public class DeltaBinaryIntEncoding implements ParquetIntEncoding {
  @Override
  public int[] decode(
      final int expectedValues, final int bitWidthIgnored, final InputStream decompressedPageStream)
      throws IOException {
    return DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageStream);
  }
}
