package com.markosindustries.parquito.encoding;

import static org.apache.parquet.format.Encoding.DELTA_BINARY_PACKED;

import com.clearspring.analytics.util.Varint;
import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.page.Values;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DeltaBinaryPackedEncoding<ReadAs> implements ParquetEncoding<ReadAs> {
  @Override
  public Values<ReadAs> decode(
      final int expectedValues,
      final int decompressedPageBytes,
      final InputStream decompressedPageStream,
      final ColumnChunkReader<ReadAs> columnChunkReader)
      throws IOException {
    if (expectedValues == 0) {
      return Values.empty();
    }

    final var readAsClass = columnChunkReader.getColumnType().parquetType().getReadAsClass();
    if (readAsClass.isAssignableFrom(Integer.class)) {
      final var values = DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageStream);
      return index -> readAsClass.cast(values[index]);
    }
    if (readAsClass.isAssignableFrom(Long.class)) {
      final var values = DeltaBinaryPackedEncoding.decode64(expectedValues, decompressedPageStream);
      return index -> readAsClass.cast(values[index]);
    }

    throw new UnsupportedOperationException(
        "Can't use " + DELTA_BINARY_PACKED + " with: " + readAsClass);
  }

  public static int[] decode32(final int expectedValues, final InputStream decompressedPageStream)
      throws IOException {
    final var values = new int[expectedValues];
    if (expectedValues == 0) {
      return values;
    }

    decodeInto(
        new TargetArray() {
          @Override
          public int length() {
            return expectedValues;
          }

          @Override
          public void set(final int index, final long value) {
            values[index] = (int) value;
          }
        },
        decompressedPageStream);

    return values;
  }

  public static long[] decode64(final int expectedValues, final InputStream decompressedPageStream)
      throws IOException {
    final var values = new long[expectedValues];
    if (expectedValues == 0) {
      return values;
    }

    decodeInto(
        new TargetArray() {
          @Override
          public int length() {
            return expectedValues;
          }

          @Override
          public void set(final int index, final long value) {
            values[index] = value;
          }
        },
        decompressedPageStream);

    return values;
  }

  interface TargetArray {
    int length();

    void set(int index, long value);
  }

  public static void decodeInto(TargetArray targetArray, final InputStream decompressedPageStream)
      throws IOException {
    final var dataInputStream = new DataInputStream(decompressedPageStream);
    final var valuesPerBlock = Varint.readUnsignedVarInt(dataInputStream);
    final var miniBlocksPerBlock = Varint.readUnsignedVarInt(dataInputStream);
    final var totalValueCount = Varint.readUnsignedVarInt(dataInputStream);

    if (totalValueCount != targetArray.length()) {
      throw new IllegalArgumentException(
          "Expected "
              + targetArray.length()
              + " but delta binary encoding block header says "
              + totalValueCount
              + " are present");
    }
    if (Maths.remainderDivPow2(valuesPerBlock / miniBlocksPerBlock, 5) != 0) {
      throw new IllegalArgumentException(
          "Expected values per block / miniBlocksPerBlock to be a multiple of 32 - but that's not true for "
              + valuesPerBlock
              + " / "
              + miniBlocksPerBlock);
    }

    final var valuesPerMiniBlock = valuesPerBlock / miniBlocksPerBlock;
    long previousValue = ZigZag.decode(Varint.readUnsignedVarLong(dataInputStream));
    targetArray.set(0, previousValue);

    final var bitWidthsForBlock = new int[miniBlocksPerBlock];
    for (int valuesSeen = 1; valuesSeen < totalValueCount; ) {
      // Read a block
      final long minDelta = ZigZag.decode(Varint.readUnsignedVarLong(dataInputStream));
      for (int miniBlockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        bitWidthsForBlock[miniBlockIdx] = dataInputStream.readUnsignedByte();
      }
      for (int miniBlockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        final var bitWidth = bitWidthsForBlock[miniBlockIdx];
        long mask = Maths.longMaskLowerBits(bitWidth);

        long buffer = 0;
        int availableBits = 0;

        for (int index = 0; index < valuesPerMiniBlock; index++, valuesSeen++) {
          // When the spec says miniblocks are bitpacked - they mean little-endian RLE hybrid
          // bitpacking, not big-endian legacy bitpacking
          while (availableBits < bitWidth) {
            buffer |= ((long) dataInputStream.readUnsignedByte()) << availableBits;
            availableBits += 8;
          }
          availableBits -= bitWidth;

          if (valuesSeen < targetArray.length()) {
            previousValue += minDelta + (buffer & mask);
            buffer >>>= bitWidth;
            targetArray.set(valuesSeen, previousValue);
          }
        }
      }
    }
  }
}
