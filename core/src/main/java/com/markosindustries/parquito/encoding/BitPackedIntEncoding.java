package com.markosindustries.parquito.encoding;

import java.io.IOException;
import java.io.InputStream;

public class BitPackedIntEncoding implements ParquetIntEncoding {
  @Override
  public int[] decode(
      final int expectedValues, final int bitWidth, final InputStream decompressedPageStream)
      throws IOException {
    if (bitWidth < 0) {
      throw new IllegalArgumentException("Can't decode a bitWidth less than 0");
    }

    final var values = new int[expectedValues];

    if (bitWidth == 0 || expectedValues == 0) {
      return values;
    }

    int buffer = 0;
    int availableBits = 0;
    int mask = Maths.intMaskLowerBits(bitWidth);

    for (int index = 0; index < expectedValues; index++) {
      while (availableBits < bitWidth) {
        buffer <<= 8;
        buffer |= decompressedPageStream.read();
        availableBits += 8;
      }
      availableBits -= bitWidth;
      values[index] = (buffer >>> availableBits) & mask;
    }

    return values;
  }
}
