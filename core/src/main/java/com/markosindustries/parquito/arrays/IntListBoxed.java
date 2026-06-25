package com.markosindustries.parquito.arrays;

import java.util.List;

public final class IntListBoxed implements FastList32 {
  private final List<Integer> values;
  private final boolean fixedSize;

  public IntListBoxed(final List<Integer> values) {
    this(values, false);
  }

  private IntListBoxed(final List<Integer> values, final boolean fixedSize) {
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
    return values.get(index);
  }

  @Override
  public void add(final int value) {
    if (fixedSize) {
      throw new IndexOutOfBoundsException("Can't grow this IntListBoxed");
    }
    values.add(value);
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new IntListBoxed(values.subList(offset, offset + count), true);
  }

  @Override
  public FastList32 subList(final int startOffsetInclusive, final int endOffsetExclusive) {
    return new IntListBoxed(values.subList(startOffsetInclusive, endOffsetExclusive), true);
  }

  @Override
  public void clear() {
    if (fixedSize) {
      throw new IndexOutOfBoundsException("Can't clear this IntListBoxed");
    }
    values.clear();
  }
}
