package com.markosindustries.parquito.arrays;

import java.util.List;

public final class IntListBoxed implements FastArray32 {
  private final List<Integer> values;

  public IntListBoxed(final List<Integer> values) {
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
    return values.get(index);
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new com.markosindustries.parquito.arrays.IntListBoxed(
        values.subList(offset, offset + count));
  }
}
