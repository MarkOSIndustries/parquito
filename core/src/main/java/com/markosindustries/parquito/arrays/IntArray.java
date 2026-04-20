package com.markosindustries.parquito.arrays;

public final class IntArray implements FastArray32 {
  private final int[] values;

  public IntArray(final int[] values) {
    this.values = values;
  }

  @Override
  public int length() {
    return values.length;
  }

  @Override
  public void set32(final int index, final int value) {
    values[index] = value;
  }

  @Override
  public int get32(final int index) {
    return values[index];
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new IntArraySlice(values, offset, count);
  }
}
