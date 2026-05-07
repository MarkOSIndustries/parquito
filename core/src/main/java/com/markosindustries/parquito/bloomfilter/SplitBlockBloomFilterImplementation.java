package com.markosindustries.parquito.bloomfilter;

import com.markosindustries.parquito.encoding.Maths;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

public class SplitBlockBloomFilterImplementation implements BloomFilterImplementation {
  // https://github.com/apache/parquet-format/blob/master/BloomFilter.md#technical-approach

  public static final int WORDS_IN_A_BLOCK = 8;
  private static final double ONE_OVER_WORDS_IN_A_BLOCK = 1d / WORDS_IN_A_BLOCK;

  public static int blocksRequiredFor(
      final long distinctValueCount, final double falsePositiveProbability) {
    final double bitsRequired =
        -Math.ceil(
            WORDS_IN_A_BLOCK
                * distinctValueCount
                / Math.log(1 - Math.pow(falsePositiveProbability, ONE_OVER_WORDS_IN_A_BLOCK)));
    return Math.ceilDiv(Maths.nextPowerOfTwo((int) (bitsRequired / Maths.BITS_PER_BYTE)), 64);
  }

  public static int bytesRequiredFor(
      final long distinctValueCount, final double falsePositiveProbability) {
    final var blocksRequired = blocksRequiredFor(distinctValueCount, falsePositiveProbability);
    return blocksRequired * WORDS_IN_A_BLOCK * 4;
  }

  private final ByteBuffer bitset;
  private final IntBuffer bitsetAsIntBuffer;
  private final Mask mask;

  private final Block[] blocks;

  public SplitBlockBloomFilterImplementation(final ByteBuffer bitset) {
    this.bitset = bitset.order(ByteOrder.LITTLE_ENDIAN);
    this.bitsetAsIntBuffer = this.bitset.asIntBuffer();
    if ((bitsetAsIntBuffer.limit() % WORDS_IN_A_BLOCK) != 0) {
      throw new IllegalArgumentException(
          "SBBF bitset did not contain an integer number of blocks (" + bitset.limit() + " bytes)");
    }
    this.mask = new Mask();
    this.blocks = new Block[bitsetAsIntBuffer.limit() / WORDS_IN_A_BLOCK];
    for (int i = 0; i < blocks.length; i++) {
      blocks[i] = new Block(bitsetAsIntBuffer.slice(i * WORDS_IN_A_BLOCK, WORDS_IN_A_BLOCK));
    }
  }

  @Override
  public boolean mightContain(final long hash) {
    final var hashTopBits = hash >>> 32;
    final var blockIndex = (int) ((hashTopBits * ((long) blocks.length)) >>> 32);

    return blocks[blockIndex].mightContain(this.mask.maskKey((int) hash));
  }

  @Override
  public void insert(final long hash) {
    final var hashTopBits = hash >>> 32;
    final var blockIndex = (int) ((hashTopBits * ((long) blocks.length)) >>> 32);

    blocks[blockIndex].insert(this.mask.maskKey((int) hash));
  }

  // TODO - SIMD using Java19+ Vectors?
  private static class Block {
    protected final IntBuffer words;

    public Block(final IntBuffer words) {
      this.words = words;
    }

    public boolean mightContain(final Block keyMask) {
      for (int i = 0; i < WORDS_IN_A_BLOCK; i++) {
        final var keyMaskWord = keyMask.words.get(i);
        if ((keyMaskWord & this.words.get(i)) != keyMaskWord) {
          return false;
        }
      }
      return true;
    }

    public void insert(final Block keyMask) {
      for (int i = 0; i < WORDS_IN_A_BLOCK; i++) {
        this.words.put(i, this.words.get(i) | keyMask.words.get(i));
      }
    }
  }

  private static class Mask extends Block {
    private static final int[] SALT =
        new int[] {
          0x47b6137b,
          0x44974d91,
          0x8824ad5b,
          0xa2b7289d,
          0x705495c7,
          0x2df1424b,
          0x9efc4947,
          0x5c6bfb31
        };

    public Mask() {
      super(IntBuffer.allocate(WORDS_IN_A_BLOCK));
    }

    public Mask maskKey(int hash) {
      for (int i = 0; i < WORDS_IN_A_BLOCK; i++) {
        words.put(i, 1 << ((hash * SALT[i]) >>> 27));
      }
      return this;
    }
  }
}
