package com.markosindustries.parquito.arrays;

import it.unimi.dsi.fastutil.ints.IntList;

public final class IntListBoxless implements FastArray32 {
  private final IntList values;

  public IntListBoxless(final IntList values) {
    this.values = values;
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
  public FastArray32 slice32(final int offset, final int count) {
    return new com.markosindustries.parquito.arrays.IntListBoxless(
        values.subList(offset, offset + count));
  }
}
