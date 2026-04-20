package com.markosindustries.parquito.arrays;

import it.unimi.dsi.fastutil.longs.LongList;

public final class LongListBoxless implements FastArray64 {
  private final LongList values;

  public LongListBoxless(final LongList values) {
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
    return values.getLong(index);
  }

  @Override
  public FastArray slice(final int offset, final int count) {
    return new com.markosindustries.parquito.arrays.LongListBoxless(
        values.subList(offset, offset + count));
  }
}
