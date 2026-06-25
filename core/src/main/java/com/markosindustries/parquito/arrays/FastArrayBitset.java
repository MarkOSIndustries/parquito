package com.markosindustries.parquito.arrays;

import java.util.BitSet;

public final class FastArrayBitset implements FastList32 {
  private final BitSet bitset;
  private final int offset;
  private int count;
  private final boolean fixedSize;

  public FastArrayBitset(final BitSet bitset) {
    this(bitset, 0, bitset.length(), false);
  }

  public FastArrayBitset(final BitSet bitset, final int offset, final int count) {
    this(bitset, offset, count, false);
  }

  public FastArrayBitset(
      final BitSet bitset, final int offset, final int count, final boolean fixedSize) {
    if (offset < 0) {
      throw new IllegalArgumentException(
          "Cannot slice BitSet(" + bitset.size() + ") with offset " + offset);
    }
    this.bitset = bitset;
    this.offset = offset;
    this.count = count;
    this.fixedSize = fixedSize;
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new FastArrayBitset(bitset, this.offset + offset, count, true);
  }

  @Override
  public FastList32 subList(final int startOffsetInclusive, final int endOffsetExclusive) {
    return new FastArrayBitset(
        bitset,
        this.offset + startOffsetInclusive,
        endOffsetExclusive - startOffsetInclusive,
        true);
  }

  @Override
  public int get32(final int index) {
    return bitset.get(offset + index) ? 1 : 0;
  }

  @Override
  public void set32(final int index, final int value) {
    final var idx = offset + index;
    if (idx >= count) {
      throw new IndexOutOfBoundsException("Can't modify outside of current BitSet extent");
    }
    bitset.set(idx, value != 0);
  }

  @Override
  public void add(final int value) {
    if (fixedSize) {
      throw new IndexOutOfBoundsException("Can't grow this BitSet");
    }
    bitset.set(count, value != 0);
    count++;
  }

  @Override
  public int length() {
    return count;
  }

  @Override
  public void clear() {
    if (fixedSize) {
      throw new IndexOutOfBoundsException("Can't clear this BitSet");
    }
    bitset.clear();
    count = 0;
  }
}
