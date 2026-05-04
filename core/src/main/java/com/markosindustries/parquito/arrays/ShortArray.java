package com.markosindustries.parquito.arrays;

public final class ShortArray implements FastArray32 {
  private final short[] values;

  public ShortArray(final short[] values) {
    this.values = values;
  }

  @Override
  public int length() {
    return values.length;
  }

  @Override
  public void set32(final int index, final int value) {
    values[index] = (short) value;
  }

  @Override
  public int get32(final int index) {
    return values[index];
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new ShortArraySlice(values, offset, count);
  }
}
