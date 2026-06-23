package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.arrays.FastArray;
import com.markosindustries.parquito.arrays.FastArray32;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface ParquetIntEncoding {
  int[] decode(
      final int expectedValues, final int bitWidth, final ByteBuffer decompressedPageBuffer)
      throws IOException;

  void encode(FastArray32 values, final int bitWidth, final OutputStream uncompressedPageStream)
      throws IOException;

  default void encode(int[] values, final int bitWidth, final OutputStream uncompressedPageStream)
      throws IOException {
    encode(FastArray.wrap(values), bitWidth, uncompressedPageStream);
  }

  default void encode(IntList values, final int bitWidth, final OutputStream uncompressedPageStream)
      throws IOException {
    encode(FastArray.wrap(values), bitWidth, uncompressedPageStream);
  }
}
