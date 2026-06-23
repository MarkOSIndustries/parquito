package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.arrays.FastArray32;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DeltaBinaryIntEncoding implements ParquetIntEncoding {
  @Override
  public int[] decode(
      final int expectedValues, final int bitWidthIgnored, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    return DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageBuffer);
  }

  @Override
  public void encode(
      final FastArray32 values,
      final int bitWidthIgnored,
      final OutputStream uncompressedPageStream)
      throws IOException {
    DeltaBinaryPackedEncoding.encodeFrom(values, uncompressedPageStream);
  }
}
