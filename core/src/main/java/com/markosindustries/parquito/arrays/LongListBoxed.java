package com.markosindustries.parquito.arrays;

import java.util.List;

public final class LongListBoxed implements FastArray64 {
  private final List<Long> values;

  public LongListBoxed(final List<Long> values) {
    this.values = values;
  }

  @Override
  public int length() {
    return values.size();
  }

  @Override
  public void set(final int index, final long value) {
    values.set(index, value);
  }

  @Override
  public long get(final int index) {
    return values.get(index);
  }

  @Override
  public FastArray slice(final int offset, final int count) {
    return new com.markosindustries.parquito.arrays.LongListBoxed(
        values.subList(offset, offset + count));
  }
}
