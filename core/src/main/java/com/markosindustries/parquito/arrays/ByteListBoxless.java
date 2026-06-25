package com.markosindustries.parquito.arrays;

import it.unimi.dsi.fastutil.bytes.ByteList;

public final class ByteListBoxless implements FastList32 {
  private final ByteList values;
  private final boolean fixedSize;

  public ByteListBoxless(final ByteList values) {
    this(values, false);
  }

  private ByteListBoxless(final ByteList values, final boolean fixedSize) {
    this.values = values;
    this.fixedSize = fixedSize;
  }

  @Override
  public int length() {
    return values.size();
  }

  @Override
  public void set32(final int index, final int value) {
    values.set(index, (byte) value);
  }

  @Override
  public void add(final int value) {
    if (fixedSize) {
      throw new IndexOutOfBoundsException("Can't grow this ByteListBoxless");
    }
    values.add((byte) value);
  }

  @Override
  public int get32(final int index) {
    return values.getByte(index);
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new ByteListBoxless(values.subList(offset, offset + count), true);
  }

  @Override
  public FastList32 subList(final int startOffsetInclusive, final int endOffsetExclusive) {
    return new ByteListBoxless(values.subList(startOffsetInclusive, endOffsetExclusive), true);
  }

  @Override
  public void clear() {
    if (fixedSize) {
      throw new IndexOutOfBoundsException("Can't clear this ByteListBoxed");
    }
    values.clear();
  }
}
