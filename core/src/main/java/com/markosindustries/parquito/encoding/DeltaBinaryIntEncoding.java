package com.markosindustries.parquito.encoding;

import com.clearspring.analytics.util.Varint;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DeltaBinaryIntEncoding implements ParquetIntEncoding {
  @Override
  public int[] decode(
      final int expectedValues, final int bitWidthIgnored, final InputStream decompressedPageStream)
      throws IOException {
    /*
     * Delta encoding consists of a header followed by blocks of delta encoded values binary packed. Each block is made of miniblocks, each of them binary packed with its own bit width.
     *
     * The header is defined as follows:
     *
     * <block size in values> <number of miniblocks in a block> <total value count> <first value>
     *
     *     the block size is a multiple of 128; it is stored as a ULEB128 int
     *     the miniblock count per block is a divisor of the block size such that their quotient, the number of values in a miniblock, is a multiple of 32; it is stored as a ULEB128 int
     *     the total value count is stored as a ULEB128 int
     *     the first value is stored as a zigzag ULEB128 int
     */
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

    /*
     * Each block contains
     *
     * <min delta> <list of bitwidths of miniblocks> <miniblocks>
     *    the min delta is a zigzag ULEB128 int (we compute a minimum as we need positive integers for bit packing)
     *    the bitwidth of each block is stored as a byte
     *    each miniblock is a list of bit packed ints according to the bit width stored at the begining of the block
     *
     * To encode a block, we will:
     *
     *    Compute the differences between consecutive elements. For the first element in the block, use the last element in the previous block or, in the case of the first block, use the first value of the whole sequence, stored in the header.
     *    Compute the frame of reference (the minimum of the deltas in the block). Subtract this min delta from all deltas in the block. This guarantees that all values are non-negative.
     *    Encode the frame of reference (min delta) as a zigzag ULEB128 int followed by the bit widths of the miniblocks and the delta values (minus the min delta) bit packed per miniblock.
     */

    final var values = new int[expectedValues];
    int previousValue = ZigZag.decode(Varint.readUnsignedVarInt(dataInputStream));
    int valuesSeen = 1;
    values[0] = previousValue;

    final var bitWidthsForBlock = new int[miniBlocksPerBlock];
    for (int valueIdx = 0; valueIdx < totalValueCount; valueIdx++) {
      // Read a block
      final var minDelta = ZigZag.decode(Varint.readUnsignedVarInt(dataInputStream));
      for (int miniBlockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        bitWidthsForBlock[miniBlockIdx] = dataInputStream.readUnsignedByte();
      }
      for (int miniBlockIdx = 0; miniBlockIdx < miniBlocksPerBlock; miniBlockIdx++) {
        final var bitWidth = bitWidthsForBlock[miniBlockIdx];
        int mask = Maths.intMaskLowerBits(bitWidth);

        long buffer = 0;
        int availableBits = 0;

        for (int index = 0; index < valuesPerMiniBlock; index++) {
          while (availableBits < bitWidth) {
            buffer <<= 32;
            buffer |= dataInputStream.readInt();
            availableBits += 32;
          }

          availableBits -= bitWidth;

          previousValue += minDelta + ((buffer >>> availableBits) & mask);
          values[valuesSeen++] = previousValue;
          if (valuesSeen == expectedValues) {
            return values;
          }
        }
      }
    }

    throw new IllegalArgumentException(
        "We somehow ran out of blocks before we found the expected number of values");
  }
}
