package com.markosindustries.parquito.encoding;

import com.clearspring.analytics.util.Varint;
import com.markosindustries.parquito.ByteBufferOutputStream;
import com.markosindustries.parquito.ParquetIOException;
import com.markosindustries.parquito.arrays.FastArray;
import com.markosindustries.parquito.arrays.FastArray32;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

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
      final int expectedValues, final int bitWidth, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    if (bitWidth < 0) {
      throw new IllegalArgumentException("Can't decode a bitWidth less than 0");
    }

    final var values = new int[expectedValues];

    if (expectedValues == 0 || (omitsZeroBitWidthRuns && bitWidth == 0)) {
      return values;
    }

    decompressedPageBuffer.order(ByteOrder.LITTLE_ENDIAN);

    final ByteBuffer pageBuffer;
    if (hasLengthHeader) {
      final var length = decompressedPageBuffer.getInt();
      if (decompressedPageBuffer.remaining() < length) {
        throw new ParquetIOException("Not enough bytes to decode " + getClass().getSimpleName());
      }
    }

    for (int index = 0; index < expectedValues; ) {
      index += decodeNextRun(values, index, bitWidth, decompressedPageBuffer);
    }

    return values;
  }

  private int decodeNextRun(
      final int[] values,
      final int offset,
      final int bitWidth,
      final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final var header =
        Varint.readUnsignedVarInt(new DataInputFromByteBuffer(decompressedPageBuffer));
    if ((header & HEADER_FLAG_BIT_PACKED) == HEADER_FLAG_BIT_PACKED) {
      return decodeBitPackedRun(
          values, offset, (header >>> 1) << 3, bitWidth, decompressedPageBuffer);
    } else {
      return decodeRepeatedRun(values, offset, header >>> 1, bitWidth, decompressedPageBuffer);
    }
  }

  private int decodeBitPackedRun(
      final int[] values,
      final int offset,
      final int runLength,
      final int bitWidth,
      final ByteBuffer decompressedPageBuffer) {
    final var expectedValues = Math.min(values.length - offset, runLength);

    readBitPacked(
        FastArray.slice(values, offset, expectedValues),
        bitWidth,
        runLength,
        decompressedPageBuffer);

    return expectedValues;
  }

  private int decodeRepeatedRun(
      final int[] values,
      final int offset,
      final int runLength,
      final int bitWidth,
      final ByteBuffer decompressedPageBuffer) {
    final var expectedValues = Math.min(values.length - offset, runLength);

    int repeatedValue = 0;
    for (int shift = 0; shift < bitWidth; shift += 8) {
      repeatedValue |= ((0xFF & decompressedPageBuffer.get()) << shift);
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

  private static final byte[] maskUpperAtBitIndex =
      new byte[] {
        Maths.byteMaskUpperBits(8),
        Maths.byteMaskUpperBits(7),
        Maths.byteMaskUpperBits(6),
        Maths.byteMaskUpperBits(5),
        Maths.byteMaskUpperBits(4),
        Maths.byteMaskUpperBits(3),
        Maths.byteMaskUpperBits(2),
        Maths.byteMaskUpperBits(1),
      };
  private static final byte[] maskLowerForBitsRemaining;

  static {
    maskLowerForBitsRemaining = new byte[Maths.BITS_PER_LONG + 1];
    Arrays.fill(
        maskLowerForBitsRemaining,
        Maths.BITS_PER_BYTE,
        Maths.BITS_PER_LONG + 1,
        Maths.byteMaskLowerBits(8));
    for (var bitsRemaining = 0; bitsRemaining < Maths.BITS_PER_BYTE; bitsRemaining++) {
      maskLowerForBitsRemaining[bitsRemaining] = Maths.byteMaskLowerBits(bitsRemaining);
    }
  }

  private static final int[][] byteIndexIncrementFor;
  private static final int[][] nextBitIndexFor;

  static {
    byteIndexIncrementFor = new int[Maths.BITS_PER_BYTE][];
    nextBitIndexFor = new int[Maths.BITS_PER_BYTE][];
    for (var bitIndex = 0; bitIndex < Maths.BITS_PER_BYTE; bitIndex++) {
      byteIndexIncrementFor[bitIndex] = new int[Maths.BITS_PER_LONG + 1];
      nextBitIndexFor[bitIndex] = new int[Maths.BITS_PER_LONG + 1];
      for (var bitsNeeded = 0; bitsNeeded < byteIndexIncrementFor[bitIndex].length; bitsNeeded++) {
        final var nextBitIndex = bitsNeeded + bitIndex;
        byteIndexIncrementFor[bitIndex][bitsNeeded] = nextBitIndex >= Maths.BITS_PER_BYTE ? 1 : 0;
        nextBitIndexFor[bitIndex][bitsNeeded] =
            Math.min(nextBitIndex, Maths.BITS_PER_BYTE) % Maths.BITS_PER_BYTE;
      }
    }
  }

  public static void readBitPacked(
      final FastArray targetArray,
      final int bitWidth,
      final int runLength,
      final ByteBuffer decompressedPageBuffer) {
    if (Maths.remainderDivPow2(runLength, 3) != 0) {
      throw new IllegalArgumentException(
          "Bit packed runs must have length which is a multiple of 8");
    }

    final int expectedBytes = Maths.floorDivPow2(bitWidth * runLength, 3);

    final int valuesLength = targetArray.length();
    final var next8Buffer = new byte[bitWidth];
    int valueIndex = 0;
    for (int bytesRead = 0; bytesRead < expectedBytes; bytesRead += next8Buffer.length) {
      if (decompressedPageBuffer.remaining() < next8Buffer.length) {
        throw new ParquetIOException("Not enough bytes available");
      }
      decompressedPageBuffer.get(next8Buffer, 0, next8Buffer.length);
      int byteIndex = 0, bitIndex = 0;
      for (var i = 0; i < 8 && valueIndex < valuesLength; i++) {
        long nextValue = 0;
        int bitsNeeded = bitWidth;
        while (bitsNeeded > 0) {
          int nextByteIndex = byteIndex + byteIndexIncrementFor[bitIndex][bitsNeeded];
          int nextBitIndex = nextBitIndexFor[bitIndex][bitsNeeded];
          long extract =
              0xFF
                  & (next8Buffer[byteIndex]
                      & maskUpperAtBitIndex[bitIndex]
                      & (maskLowerForBitsRemaining[bitsNeeded] << bitIndex));
          int shiftLeft = (bitWidth - bitsNeeded) - bitIndex;
          nextValue |= shiftLeft > 0 ? extract << shiftLeft : extract >>> -shiftLeft;
          bitsNeeded -= Maths.BITS_PER_BYTE - bitIndex;
          byteIndex = nextByteIndex;
          bitIndex = nextBitIndex;
        }
        targetArray.set(valueIndex++, nextValue);
      }
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
    if (Maths.remainderDivPow2(runLength, 3) != 0) {
      throw new IllegalArgumentException(
          "Bit packed runs must have length which is a multiple of 8");
    }

    final int expectedBytes = Maths.floorDivPow2(bitWidth * runLength, 3);

    final var valuesLength = sourceArray.length();
    final var next8Buffer = new byte[bitWidth];
    int valueIndex = 0;
    for (int bytesWritten = 0; bytesWritten < expectedBytes; bytesWritten += next8Buffer.length) {
      int byteIndex = 0, bitIndex = 0;
      Arrays.fill(next8Buffer, (byte) 0);
      for (var i = 0; i < 8 && valueIndex < valuesLength; i++) {
        long nextValue = sourceArray.get(valueIndex++);
        int bitsAvailable = bitWidth;
        while (bitsAvailable > 0) {
          int nextByteIndex = byteIndex + byteIndexIncrementFor[bitIndex][bitsAvailable];
          int nextBitIndex = nextBitIndexFor[bitIndex][bitsAvailable];
          next8Buffer[byteIndex] |=
              (byte) ((nextValue & maskLowerForBitsRemaining[bitsAvailable]) << bitIndex);
          int shiftRight = Maths.BITS_PER_BYTE - bitIndex;
          nextValue >>>= shiftRight;
          bitsAvailable -= shiftRight;
          byteIndex = nextByteIndex;
          bitIndex = nextBitIndex;
        }
      }

      outputStream.write(next8Buffer);
    }
  }
}
