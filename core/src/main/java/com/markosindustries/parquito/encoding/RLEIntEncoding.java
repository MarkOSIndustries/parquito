package com.markosindustries.parquito.encoding;

import com.clearspring.analytics.util.Varint;
import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.SpecifiedByteCountInputStream;
import com.markosindustries.parquito.arrays.FastArray;
import com.markosindustries.parquito.arrays.FastArray32;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Run Length Encoding / Bit-Packing Hybrid
 * https://parquet.apache.org/docs/file-format/data-pages/encodings/#a-namerlearun-length-encoding--bit-packing-hybrid-rle--3
 */
public class RLEIntEncoding implements ParquetIntEncoding {
  private static final int HEADER_FLAG_BIT_PACKED = 1;

  private final boolean hasLengthHeader;
  private final boolean omitsZeroBitWidthRuns;

  public RLEIntEncoding(final boolean hasLengthHeader, final boolean omitsZeroBitWidthRuns) {
    this.hasLengthHeader = hasLengthHeader;
    this.omitsZeroBitWidthRuns = omitsZeroBitWidthRuns;
  }

  @Override
  public int[] decode(
      final int expectedValues, final int bitWidth, final InputStream decompressedPageStream)
      throws IOException {
    if (bitWidth < 0) {
      throw new IllegalArgumentException("Can't decode a bitWidth less than 0");
    }

    final var values = new int[expectedValues];

    if (expectedValues == 0 || (omitsZeroBitWidthRuns && bitWidth == 0)) {
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
      final int[] values,
      final int offset,
      final int bitWidth,
      final DataInputStream dataInputStream)
      throws IOException {
    final var header = Varint.readUnsignedVarInt(dataInputStream);
    if ((header & HEADER_FLAG_BIT_PACKED) == HEADER_FLAG_BIT_PACKED) {
      return decodeBitPackedRun(values, offset, (header >>> 1) << 3, bitWidth, dataInputStream);
    } else {
      return decodeRepeatedRun(values, offset, header >>> 1, bitWidth, dataInputStream);
    }
  }

  private int decodeBitPackedRun(
      final int[] values,
      final int offset,
      final int runLength,
      final int bitWidth,
      final InputStream inputStream)
      throws IOException {
    final var expectedValues = Math.min(values.length - offset, runLength);

    readBitPacked(
        FastArray.slice(values, offset, expectedValues), bitWidth, runLength, inputStream);

    return expectedValues;
  }

  private int decodeRepeatedRun(
      final int[] values,
      final int offset,
      final int runLength,
      final int bitWidth,
      final InputStream inputStream)
      throws IOException {
    final var expectedValues = Math.min(values.length - offset, runLength);

    int repeatedValue = 0;
    for (int shift = 0; shift < bitWidth; shift += 8) {
      repeatedValue |= (inputStream.read() << shift);
    }

    for (int i = 0; i < expectedValues; i++) {
      values[offset + i] = repeatedValue;
    }

    return expectedValues;
  }

  @Override
  public void encode(
      final FastArray32 values, final int bitWidth, final OutputStream uncompressedPageStream)
      throws IOException {
    if (bitWidth < 0) {
      throw new IllegalArgumentException("Can't decode a bitWidth less than 0");
    }

    if (omitsZeroBitWidthRuns && bitWidth == 0) {
      return;
    }

    final var valuesLength = values.length();

    final var outputBufferStream =
        new ByteBufferOutputStream(
            valuesLength); // a bit tricky to know, but 1 byte per entry is a decent starting spot
    final var dataOutput = new DataOutputStream(outputBufferStream);

    var bitPackStartIndex = 0;
    for (int valuesIndex = 0; valuesIndex < valuesLength; ) {
      final var value = values.get32(valuesIndex);
      // Look ahead to see if there's any repeats - more than 1 is usually more efficient than
      // bitpacking
      int repeatedRunLength = 1;
      for (int j = valuesIndex + 1; j < values.length(); j++) {
        if (values.get32(j) == value) {
          repeatedRunLength++;
        } else {
          break;
        }
      }
      if (repeatedRunLength > 1) {
        if (bitPackStartIndex < valuesIndex) {
          writeBitPackedRun(
              values.slice32(bitPackStartIndex, valuesIndex - bitPackStartIndex),
              bitWidth,
              dataOutput);
        }
        writeRepeatedRun(value, bitWidth, repeatedRunLength, dataOutput);
        valuesIndex += repeatedRunLength;
        bitPackStartIndex = valuesIndex;
      } else {
        valuesIndex += 8; // always bitpack in multiples of 8
      }
    }
    if (bitPackStartIndex < valuesLength) {
      writeBitPackedRun(
          values.slice(bitPackStartIndex, valuesLength - bitPackStartIndex), bitWidth, dataOutput);
    }

    if (hasLengthHeader) {
      LittleEndian.writeInt(outputBufferStream.size(), uncompressedPageStream);
    }
    outputBufferStream.writeTo(uncompressedPageStream);
  }

  private static void writeRepeatedRun(
      final int value, final int bitWidth, final int runLength, final DataOutputStream dataOutput)
      throws IOException {
    final var header = runLength << 1;
    Varint.writeUnsignedVarInt(header, dataOutput);
    int buffer = value;
    for (int shift = 0; shift < bitWidth; shift += 8) {
      dataOutput.writeByte(buffer);
      buffer >>>= 8;
    }
  }

  private static void writeBitPackedRun(
      final FastArray values, final int bitWidth, final DataOutputStream dataOutput)
      throws IOException {
    final var runLengthDividedBy8 = Maths.ceilDivPow2(values.length(), 3);
    final var header = HEADER_FLAG_BIT_PACKED | (runLengthDividedBy8 << 1);
    Varint.writeUnsignedVarInt(header, dataOutput);
    writeBitPacked(values, bitWidth, runLengthDividedBy8 << 3, dataOutput);
  }

  public static void readBitPacked(
      final FastArray targetArray,
      final int bitWidth,
      final int runLength,
      final InputStream inputStream)
      throws IOException {
    if (Maths.remainderDivPow2(runLength, 3) != 0) {
      throw new IllegalArgumentException(
          "Bit packed runs must have length which is a multiple of 8");
    }

    final long mask = Maths.longMaskLowerBits(bitWidth);
    final int count = targetArray.length();
    long buffer = 0;
    int bitsAvailable = 0;
    for (int i = 0; i < count; i++) {
      while (bitsAvailable < bitWidth) {
        buffer |= ((long) inputStream.read()) << bitsAvailable;
        bitsAvailable += 8;
      }
      targetArray.set(i, buffer & mask);
      buffer >>>= bitWidth;
      bitsAvailable -= bitWidth;
    }

    // The encoding demands runLength values, even if we don't need that many
    for (int wastedBits = bitWidth * (runLength - count); wastedBits > 7; wastedBits -= 8) {
      //noinspection ResultOfMethodCallIgnored
      inputStream.read();
    }
  }

  public static void writeBitPacked(
      final FastArray sourceArray, final int bitWidth, final OutputStream outputStream)
      throws IOException {
    final var runLengthDividedBy8 = Maths.ceilDivPow2(sourceArray.length(), 3);
    writeBitPacked(sourceArray, bitWidth, runLengthDividedBy8 << 3, outputStream);
  }

  public static void writeBitPacked(
      final FastArray sourceArray,
      final int bitWidth,
      final int runLength,
      final OutputStream outputStream)
      throws IOException {
    final var valuesLength = sourceArray.length();

    long byteBuffer = 0;
    int packedBits = 0;
    for (var valueIndex = 0; valueIndex < valuesLength; valueIndex++) {
      var value = sourceArray.get(valueIndex);
      for (int valueBitsRemaining = bitWidth; valueBitsRemaining > 0; ) {
        final var bitsToGrab = Math.min(valueBitsRemaining, Maths.BITS_PER_LONG - packedBits);
        byteBuffer |= (value & Maths.longMaskLowerBits(bitsToGrab)) << packedBits;
        packedBits += bitsToGrab;
        valueBitsRemaining -= bitsToGrab;
        value >>>= bitsToGrab;
        while (packedBits >= Maths.BITS_PER_BYTE) {
          // Ignores all but lowest Maths.BITS_PER_BYTE bits
          outputStream.write((int) byteBuffer);
          packedBits -= Maths.BITS_PER_BYTE;
          byteBuffer >>>= Maths.BITS_PER_BYTE;
        }
      }
    }
    if (packedBits > 0) {
      outputStream.write((int) byteBuffer);
    }
    // The encoding demands runLength values, even if we don't need that many
    for (int wastedBits = bitWidth * (runLength - valuesLength); wastedBits > 7; wastedBits -= 8) {
      outputStream.write(0);
    }
  }
}
