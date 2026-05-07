package com.markosindustries.parquito.encoding;

import static org.apache.parquet.format.Encoding.DELTA_BINARY_PACKED;

import com.clearspring.analytics.util.Varint;
import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.arrays.FastArray;
import com.markosindustries.parquito.arrays.FastDictionary;
import com.markosindustries.parquito.page.Values;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
      return new Values<ReadAs>() {
        @Override
        public ReadAs get(final int index) {
          return readAsClass.cast(values[index]);
        }

        @Override
        public int count() {
          return values.length;
        }
      };
    }
    if (readAsClass.isAssignableFrom(Long.class)) {
      final var values = DeltaBinaryPackedEncoding.decode64(expectedValues, decompressedPageStream);
      return new Values<ReadAs>() {
        @Override
        public ReadAs get(final int index) {
          return readAsClass.cast(values[index]);
        }

        @Override
        public int count() {
          return expectedValues;
        }
      };
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

    decodeInto(FastArray.wrap(values), decompressedPageStream);

    return values;
  }

  public static long[] decode64(final int expectedValues, final InputStream decompressedPageStream)
      throws IOException {
    final var values = new long[expectedValues];
    if (expectedValues == 0) {
      return values;
    }

    decodeInto(FastArray.wrap(values), decompressedPageStream);

    return values;
  }

  static void decodeInto(FastArray targetArray, final InputStream decompressedPageStream)
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

        // When the spec says miniblocks are bitpacked - they mean little-endian RLE hybrid
        // bitpacking, not big-endian legacy bitpacking
        final var miniBlockSlice =
            targetArray.slice(
                valuesSeen, Math.min(valuesPerMiniBlock, totalValueCount - valuesSeen));
        RLEIntEncoding.readBitPacked(miniBlockSlice, bitWidth, valuesPerMiniBlock, dataInputStream);
        for (var i = 0; i < miniBlockSlice.length(); i++) {
          previousValue += miniBlockSlice.get(i) + minDelta;
          miniBlockSlice.set(i, previousValue);
        }
        valuesSeen += miniBlockSlice.length();
      }
    }
  }

  @Override
  public void encode(
      final FastDictionary<ReadAs, ?> values,
      final OutputStream uncompressedPageStream,
      final ColumnChunkWriter<ReadAs> columnChunkWriter)
      throws IOException {
    final var readAsClass = columnChunkWriter.getColumnType().parquetType().getReadAsClass();
    try {
      DeltaBinaryPackedEncoding.encodeFrom(values.asFastArray(), uncompressedPageStream);
    } catch (UnsupportedOperationException e) {
      throw new UnsupportedOperationException(
          "Can't use " + DELTA_BINARY_PACKED + " with: " + readAsClass, e);
    }
  }

  public static void encode32(final int[] values, final OutputStream uncompressedPageStream)
      throws IOException {
    encodeFrom(FastArray.wrap(values), uncompressedPageStream);
  }

  public static void encode64(final long[] values, final OutputStream uncompressedPageStream)
      throws IOException {
    encodeFrom(FastArray.wrap(values), uncompressedPageStream);
  }

  static void encodeFrom(FastArray sourceArray, final OutputStream uncompressedPageStream)
      throws IOException {
    // TODO figure this out based on sourceArray.length()
    final var valuesPerBlock = 128;
    final var miniBlocksPerBlock = 4;
    final var totalValueCount = sourceArray.length();

    final var dataOutputStream = new DataOutputStream(uncompressedPageStream);
    Varint.writeUnsignedVarInt(valuesPerBlock, dataOutputStream);
    Varint.writeUnsignedVarInt(miniBlocksPerBlock, dataOutputStream);
    Varint.writeUnsignedVarInt(totalValueCount, dataOutputStream);

    if (totalValueCount == 0) {
      return;
    }

    final var valuesPerMiniBlock = valuesPerBlock / miniBlocksPerBlock;
    long previousValue = sourceArray.get(0);
    Varint.writeUnsignedVarLong(ZigZag.encode(previousValue), dataOutputStream);

    final var bitWidthsForBlock = new int[miniBlocksPerBlock];
    final var deltasForBlock = new long[valuesPerBlock];
    for (int valuesSeen = 1; valuesSeen < totalValueCount; ) {
      // Write a block

      long minDelta = Long.MAX_VALUE;
      for (var blockIdx = 0; blockIdx < valuesPerBlock; blockIdx++) {
        if (valuesSeen < totalValueCount) {
          long nextValue = sourceArray.get(valuesSeen++);
          long nextDelta = nextValue - previousValue;
          previousValue = nextValue;
          if (nextDelta < minDelta) {
            minDelta = nextDelta;
          }
          deltasForBlock[blockIdx] = nextDelta;
        } else {
          deltasForBlock[blockIdx] = minDelta; // So that we get a zero after subtraction
        }
      }

      Varint.writeUnsignedVarLong(ZigZag.encode(minDelta), dataOutputStream);

      for (int miniBlockIdx = 0, blockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        bitWidthsForBlock[miniBlockIdx] = 0;
        for (var i = 0; i < valuesPerMiniBlock; i++, blockIdx++) {
          deltasForBlock[blockIdx] -= minDelta;
          final var bitWidth = Maths.bitWidth(deltasForBlock[blockIdx]);
          if (bitWidth > bitWidthsForBlock[miniBlockIdx]) {
            bitWidthsForBlock[miniBlockIdx] = bitWidth;
          }
        }

        dataOutputStream.writeByte(bitWidthsForBlock[miniBlockIdx]);
      }

      for (int miniBlockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        final var bitWidth = bitWidthsForBlock[miniBlockIdx];
        if (bitWidth == 0) {
          continue;
        }

        RLEIntEncoding.writeBitPacked(
            FastArray.slice(deltasForBlock, miniBlockIdx * valuesPerMiniBlock, valuesPerMiniBlock),
            bitWidth,
            dataOutputStream);
      }
    }
  }

  @Override
  public int refineBytesRequiredEstimate(
      final int valueCount,
      final int estimatedPlainBytesRequired,
      final ColumnChunkWriter<ReadAs> columnChunkWriter) {
    return Math.ceilDiv(Maths.bitWidth(valueCount) * valueCount, 8);
  }
}
