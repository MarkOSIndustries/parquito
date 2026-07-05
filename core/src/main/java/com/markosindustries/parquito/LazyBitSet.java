package com.markosindustries.parquito;

import com.markosindustries.parquito.encoding.Maths;
import java.util.function.IntPredicate;

/**
 * Like {@link java.util.BitSet} but with a couple of extra constraints/optimisations - Size is
 * known up front, no dynamic resize - Each value can be lazily initialised - Lazy flag is kept in
 * same word as value
 */
public class LazyBitSet {
  private final long[] words;
  private final IntPredicate initIndex;

  public LazyBitSet(final int bits, final IntPredicate initIndex) {
    // We divide by 32 rather than 64 because we need twice the bits
    // High 32 are lazy flags, and low 32 are bit values
    words = new long[Maths.ceilDivPow2(bits, 5)];
    this.initIndex = initIndex;
  }

  public boolean get(int bitIndex) {
    int wordIndex = bitIndex >> 5;
    long valueMask = 0xFFFFFFFFL & (1 << bitIndex);
    long initMask = valueMask << 32;

    if ((words[wordIndex] & initMask) == 0) {
      if (initIndex.test(bitIndex)) {
        words[wordIndex] |= valueMask | initMask;
      } else {
        words[wordIndex] |= initMask;
      }
    }

    return (words[wordIndex] & valueMask) != 0;
  }
}
