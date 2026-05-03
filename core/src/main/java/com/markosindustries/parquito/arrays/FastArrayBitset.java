package com.markosindustries.parquito.arrays;

import java.util.BitSet;

public final class FastArrayBitset implements FastArray32 {
  private final BitSet bitset;
  private final int offset;
  private final int count;

  public FastArrayBitset(final BitSet bitset) {
    this(bitset, 0, bitset.length());
  }

  public FastArrayBitset(final BitSet bitset, final int offset, final int count) {
    this.bitset = bitset;
    this.offset = offset;
    this.count = count;
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new FastArrayBitset(bitset, this.offset + offset, count);
  }

  @Override
  public int get32(final int index) {
    return bitset.get(offset + index) ? 1 : 0;
  }

  @Override
  public void set32(final int index, final int value) {
    bitset.set(offset + index, value != 0);
  }

  @Override
  public int length() {
    return count;
  }
}
