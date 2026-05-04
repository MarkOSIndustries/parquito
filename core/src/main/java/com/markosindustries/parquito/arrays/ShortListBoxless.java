package com.markosindustries.parquito.arrays;

import it.unimi.dsi.fastutil.shorts.ShortList;

public final class ShortListBoxless implements FastList32 {
  private final ShortList values;
  private final boolean fixedSize;

  public ShortListBoxless(final ShortList values) {
    this(values, false);
  }

  private ShortListBoxless(final ShortList values, final boolean fixedSize) {
    this.values = values;
    this.fixedSize = fixedSize;
  }

  @Override
  public int length() {
    return values.size();
  }

  @Override
  public void set32(final int index, final int value) {
    values.set(index, (short) value);
  }

  @Override
  public void add(final int value) {
    if (fixedSize) {
      throw new IndexOutOfBoundsException("Can't grow this ShortListBoxless");
    }
    values.add((short) value);
  }

  @Override
  public int get32(final int index) {
    return values.getShort(index);
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new ShortListBoxless(values.subList(offset, offset + count), true);
  }

  @Override
  public FastList32 subList(final int startOffsetInclusive, final int endOffsetExclusive) {
    return new ShortListBoxless(values.subList(startOffsetInclusive, endOffsetExclusive), true);
  }
}
