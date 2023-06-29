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
    final var readAsClass = columnChunkReader.getColumnType().parquetType().getReadAsClass();
    if (!(readAsClass.isAssignableFrom(Integer.class)
        || readAsClass.isAssignableFrom(Long.class))) {
      throw new UnsupportedOperationException(
          "Can't use " + DELTA_BINARY_PACKED + " with: " + readAsClass);
    }

    if (expectedValues == 0) {
      return Values.empty();
    }
    final var values = decode(expectedValues, decompressedPageStream);
    return index -> readAsClass.cast(values[index]);
  }

  public static long[] decode(final int expectedValues, final InputStream decompressedPageStream)
      throws IOException {
    final var values = new long[expectedValues];
    if (expectedValues == 0) {
      return values;
    }

    final var dataInputStream = new DataInputStream(decompressedPageStream);
    final var valuesPerBlock = Varint.readUnsignedVarInt(dataInputStream);
    final var miniBlocksPerBlock = Varint.readUnsignedVarInt(dataInputStream);
    final var totalValueCount = Varint.readUnsignedVarInt(dataInputStream);

    if (totalValueCount != expectedValues) {
      throw new IllegalArgumentException(
          "Expected "
              + expectedValues
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
    values[0] = previousValue;

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
          while (availableBits < bitWidth) {
            buffer <<= 32;
            buffer |= dataInputStream.readInt();
            availableBits += 32;
          }

          availableBits -= bitWidth;

          previousValue += minDelta + ((buffer >>> availableBits) & mask);
          if (valuesSeen < values.length) {
            values[valuesSeen] = previousValue;
          }
        }
      }
    }

    return values;
  }
}
