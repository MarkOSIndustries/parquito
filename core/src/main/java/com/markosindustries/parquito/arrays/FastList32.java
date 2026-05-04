package com.markosindustries.parquito.arrays;

import com.markosindustries.parquito.encoding.Maths;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import java.util.BitSet;

public sealed interface FastList32 extends FastArray32
    permits IntListBoxed,
        IntListBoxless,
        FastArrayBitset,
        ShortListBoxless,
        ByteListBoxless,
        ZerosCounter {
  void add(final int value);

  FastList32 subList(int startOffsetInclusive, int endOffsetExclusive);

  static FastList32 createTightestFit(final int largestValuePossible) {
    final var bitWidth = Maths.bitWidth(largestValuePossible);
    if (bitWidth == 0) {
      return new ZerosCounter(0);
    }
    if (bitWidth == 1) {
      return new FastArrayBitset(new BitSet());
    }
    if (bitWidth <= Maths.BITS_PER_BYTE) {
      return new ByteListBoxless(new ByteArrayList());
    }
    if (bitWidth <= Maths.BITS_PER_SHORT) {
      return new ShortListBoxless(new ShortArrayList());
    }
    return new IntListBoxless(new IntArrayList());
  }
}
