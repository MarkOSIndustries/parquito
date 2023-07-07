package com.markosindustries.parquito.encoding;

import com.clearspring.analytics.util.Varint;
import com.markosindustries.parquito.SpecifiedByteCountInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Run Length Encoding / Bit-Packing Hybrid
 * https://parquet.apache.org/docs/file-format/data-pages/encodings/#a-namerlearun-length-encoding--bit-packing-hybrid-rle--3
 */
public class RLEIntEncoding implements ParquetIntEncoding {
  private static final int HEADER_FLAG_BIT_PACKED = 1;

  private final boolean hasLengthHeader;

  public RLEIntEncoding(boolean hasLengthHeader) {
    this.hasLengthHeader = hasLengthHeader;
  }

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

    DataInputStream dataInput;
    if (hasLengthHeader) {
      final var length = LittleEndian.readInt(decompressedPageStream);
      dataInput =
          new DataInputStream(new SpecifiedByteCountInputStream(decompressedPageStream, length));
    } else {
      dataInput = new DataInputStream(decompressedPageStream);
    }

    for (int index = 0; index < expectedValues; ) {
      index += decodeNextRun(values, index, bitWidth, dataInput);
    }

    return values;
  }

  private int decodeNextRun(
      final int[] values, final int offset, final int bitWidth, final DataInput dataInput)
      throws IOException {
    final var header = Varint.readUnsignedVarInt(dataInput);
    if ((header & HEADER_FLAG_BIT_PACKED) == HEADER_FLAG_BIT_PACKED) {
      return decodeBitPackedRun(values, offset, (header >>> 1) << 3, bitWidth, dataInput);
    } else {
      return decodeRepeatedRun(values, offset, header >>> 1, bitWidth, dataInput);
    }
  }

  private int decodeBitPackedRun(
      final int[] values,
      final int offset,
      final int runLength,
      final int bitWidth,
      final DataInput dataInput)
      throws IOException {
    if (Maths.remainderDivPow2(runLength, 3) != 0) {
      throw new IllegalArgumentException(
          "Bit packed runs must have length which is a multiple of 8");
    }

    final var expectedValues = Math.min(values.length - offset, runLength);
    final long mask = Maths.longMaskLowerBits(bitWidth);
    long buffer = 0;
    int bitsAvailable = 0;
    for (int i = 0; i < expectedValues; i++) {
      while (bitsAvailable < bitWidth) {
        buffer |= ((long) dataInput.readUnsignedByte()) << bitsAvailable;
        bitsAvailable += 8;
      }
      int next = (int) (buffer & mask);
      buffer >>>= bitWidth;
      bitsAvailable -= bitWidth;

      values[offset + i] = next;
    }

    // The encoding demands runLength values, even if we don't need that many
    for (int wastedBits = bitWidth * (runLength - expectedValues);
        wastedBits > 7;
        wastedBits -= 8) {
      dataInput.readUnsignedByte();
    }

    return expectedValues;
  }

  private int decodeRepeatedRun(
      final int[] values,
      final int offset,
      final int runLength,
      final int bitWidth,
      final DataInput dataInput)
      throws IOException {
    final var expectedValues = Math.min(values.length - offset, runLength);

    int repeatedValue = 0;
    for (int shift = 0; shift < bitWidth; shift += 8) {
      repeatedValue |= (dataInput.readUnsignedByte() << shift);
    }

    for (int i = 0; i < expectedValues; i++) {
      values[offset + i] = repeatedValue;
    }

    return expectedValues;
  }
}
