package com.markosindustries.parquito.encoding;

import com.markosindustries.parquito.ColumnChunkReader;
import com.markosindustries.parquito.arrays.FastArray;
import com.markosindustries.parquito.page.Values;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DeltaBinaryPackedEncoding implements ParquetEncoding {
  @Override
  public Values decode(
      final int expectedValues,
      final ByteBuffer decompressedPageBuffer,
      final ColumnChunkReader columnChunkReader)
      throws IOException {
    if (expectedValues == 0) {
      return Values.empty();
    }

    final var type = columnChunkReader.getColumnType().getType();
    return switch (type) {
      case INT32 -> {
        final var values =
            DeltaBinaryPackedEncoding.decode32(expectedValues, decompressedPageBuffer);
        yield new Values.Impl() {
          @Override
          public int getInt32(final int index) {
            return values[index];
          }

          @Override
          public int count() {
            return values.length;
          }
        };
      }
      case INT64 -> {
        final var values =
            DeltaBinaryPackedEncoding.decode64(expectedValues, decompressedPageBuffer);
        yield new Values.Impl() {
          @Override
          public long getInt64(final int index) {
            return values[index];
          }

          @Override
          public int count() {
            return expectedValues;
          }
        };
      }
      default ->
          throw new UnsupportedOperationException(
              this.getClass().getSimpleName() + " does not support type " + type);
    };
  }

  public static int[] decode32(final int expectedValues, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final var values = new int[expectedValues];
    if (expectedValues == 0) {
      return values;
    }

    decodeInto(FastArray.wrap(values), decompressedPageBuffer);

    return values;
  }

  public static long[] decode64(final int expectedValues, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final var values = new long[expectedValues];
    if (expectedValues == 0) {
      return values;
    }

    decodeInto(FastArray.wrap(values), decompressedPageBuffer);

    return values;
  }

  static void decodeInto(FastArray targetArray, final ByteBuffer decompressedPageBuffer)
      throws IOException {
    final var valuesPerBlock = VarInt.getUnsigned32(decompressedPageBuffer);
    final var miniBlocksPerBlock = VarInt.getUnsigned32(decompressedPageBuffer);
    final var totalValueCount = VarInt.getUnsigned32(decompressedPageBuffer);

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
    long previousValue = ZigZag.decode(VarInt.getUnsigned64(decompressedPageBuffer));
    targetArray.set(0, previousValue);

    final var bitWidthsForBlock = new int[miniBlocksPerBlock];
    for (int valuesSeen = 1; valuesSeen < totalValueCount; ) {
      // Read a block
      final long minDelta = ZigZag.decode(VarInt.getUnsigned64(decompressedPageBuffer));
      for (int miniBlockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        bitWidthsForBlock[miniBlockIdx] = 0xFF & decompressedPageBuffer.get();
      }
      for (int miniBlockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        final var bitWidth = bitWidthsForBlock[miniBlockIdx];

        // When the spec says miniblocks are bitpacked - they mean little-endian RLE hybrid
        // bitpacking, not big-endian legacy bitpacking
        final var miniBlockSlice =
            targetArray.slice(
                valuesSeen, Math.min(valuesPerMiniBlock, totalValueCount - valuesSeen));
        RLEIntEncoding.readBitPacked(
            miniBlockSlice, bitWidth, valuesPerMiniBlock, decompressedPageBuffer);
        for (var i = 0; i < miniBlockSlice.length(); i++) {
          previousValue += miniBlockSlice.get(i) + minDelta;
          miniBlockSlice.set(i, previousValue);
        }
        valuesSeen += miniBlockSlice.length();
      }
    }
  }

  @Override
  public void encode(final EncodingWritableValues values, final OutputStream uncompressedPageStream)
      throws IOException {
    final var fastArray =
        switch (values.getType()) {
          case INT32 -> FastArray.wrap(values.getInt32sAsIntList());
          case INT64 -> FastArray.wrap(values.getInt64sAsLongList());
          default ->
              throw new UnsupportedOperationException(
                  this.getClass().getSimpleName() + " does not support type " + values.getType());
        };
    DeltaBinaryPackedEncoding.encodeFrom(fastArray, uncompressedPageStream);
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

    VarInt.putUnsigned32(valuesPerBlock, uncompressedPageStream);
    VarInt.putUnsigned32(miniBlocksPerBlock, uncompressedPageStream);
    VarInt.putUnsigned32(totalValueCount, uncompressedPageStream);

    if (totalValueCount == 0) {
      return;
    }

    final var valuesPerMiniBlock = valuesPerBlock / miniBlocksPerBlock;
    long previousValue = sourceArray.get(0);
    VarInt.putUnsigned64(ZigZag.encode(previousValue), uncompressedPageStream);

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

      VarInt.putUnsigned64(ZigZag.encode(minDelta), uncompressedPageStream);

      for (int miniBlockIdx = 0, blockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        bitWidthsForBlock[miniBlockIdx] = 0;
        for (var i = 0; i < valuesPerMiniBlock; i++, blockIdx++) {
          deltasForBlock[blockIdx] -= minDelta;
          final var bitWidth = Maths.bitWidth(deltasForBlock[blockIdx]);
          if (bitWidth > bitWidthsForBlock[miniBlockIdx]) {
            bitWidthsForBlock[miniBlockIdx] = bitWidth;
          }
        }

        uncompressedPageStream.write(bitWidthsForBlock[miniBlockIdx]);
      }

      for (int miniBlockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        final var bitWidth = bitWidthsForBlock[miniBlockIdx];
        if (bitWidth == 0) {
          continue;
        }

        RLEIntEncoding.writeBitPacked(
            FastArray.slice(deltasForBlock, miniBlockIdx * valuesPerMiniBlock, valuesPerMiniBlock),
            bitWidth,
            uncompressedPageStream);
      }
    }
  }

  @Override
  public int refineBytesRequiredEstimate(
      final EncodingWritableValues values, final int estimatedPlainBytesRequired) {
    return Math.ceilDiv(Maths.bitWidth(values.length()) * values.length(), 8);
  }
}
