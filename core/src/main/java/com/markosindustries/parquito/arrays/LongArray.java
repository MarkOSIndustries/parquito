package com.markosindustries.parquito.arrays;

public final class LongArray implements FastArray64 {
  private final long[] values;

  public LongArray(final long[] values) {
    this.values = values;
  }

  @Override
  public int length() {
    return values.length;
  }

  @Override
  public void set(final int index, final long value) {
    values[index] = value;
  }

  @Override
  public long get(final int index) {
    return values[index];
  }

  @Override
  public FastArray slice(final int offset, final int count) {
    return new LongArraySlice(values, offset, count);
  }
}
