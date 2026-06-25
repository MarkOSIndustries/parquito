package com.markosindustries.parquito.arrays;

import it.unimi.dsi.fastutil.ints.IntList;

public final class IntListBoxless implements FastList32 {
  private final IntList values;
  private final boolean fixedSize;

  public IntListBoxless(final IntList values) {
    this(values, false);
  }

  private IntListBoxless(final IntList values, final boolean fixedSize) {
    this.values = values;
    this.fixedSize = fixedSize;
  }

  @Override
  public int length() {
    return values.size();
  }

  @Override
  public void set32(final int index, final int value) {
    values.set(index, value);
  }

  @Override
  public int get32(final int index) {
    return values.getInt(index);
  }

  @Override
  public void add(final int value) {
    if (fixedSize) {
      throw new IndexOutOfBoundsException("Can't grow this IntListBoxless");
    }
    values.add(value);
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new IntListBoxless(values.subList(offset, offset + count), true);
  }

  @Override
  public FastList32 subList(final int startOffsetInclusive, final int endOffsetExclusive) {
    return new IntListBoxless(values.subList(startOffsetInclusive, endOffsetExclusive), true);
  }

  @Override
  public void clear() {
    if (fixedSize) {
      throw new IndexOutOfBoundsException("Can't clear this IntListBoxless");
    }
    values.clear();
  }
}
