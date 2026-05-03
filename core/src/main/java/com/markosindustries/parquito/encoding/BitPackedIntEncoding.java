package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.arrays.FastArray32;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
        buffer <<= Maths.BITS_PER_BYTE;
        buffer |= decompressedPageStream.read();
        availableBits += Maths.BITS_PER_BYTE;
      }
      availableBits -= bitWidth;
      values[index] = (buffer >>> availableBits) & mask;
    }

    return values;
  }

  @Override
  public void encode(
      final FastArray32 values, final int bitWidth, final OutputStream uncompressedPageStream)
      throws IOException {
    if (bitWidth < 0) {
      throw new IllegalArgumentException("Can't encode a bitWidth less than 0");
    }

    final var valuesLength = values.length();
    if (bitWidth == 0 || valuesLength == 0) {
      return;
    }

    long buffer = 0;
    int availableBits = 0;
    int mask = Maths.intMaskLowerBits(bitWidth);
    for (var valuesIndex = 0; valuesIndex < valuesLength; ) {
      while (availableBits < Maths.BITS_PER_BYTE && valuesIndex < valuesLength) {
        buffer <<= bitWidth;
        buffer |= values.get32(valuesIndex++) & mask;
        availableBits += bitWidth;
      }
      if (availableBits >= Maths.BITS_PER_BYTE) {
        uncompressedPageStream.write((int) buffer);
        buffer >>>= Maths.BITS_PER_BYTE;
        availableBits -= Maths.BITS_PER_BYTE;
      }
    }
    if (availableBits > 0) {
      // pad the last byte with zeros
      buffer <<= Maths.BITS_PER_BYTE - availableBits;
      uncompressedPageStream.write((int) buffer);
    }
  }
}
