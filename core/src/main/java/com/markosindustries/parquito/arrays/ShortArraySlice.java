package com.markosindustries.parquito.arrays;

public final class ShortArraySlice implements FastArray32 {
  private final short[] values;
  private final int offset;
  private final int count;

  public ShortArraySlice(final short[] values, int offset, int count) {
    if (offset < 0 || offset + count > values.length) {
      throw new IllegalArgumentException(
          "Cannot slice short["
              + values.length
              + "] with offset "
              + offset
              + " and count + "
              + count);
    }
    this.values = values;
    this.offset = offset;
    this.count = count;
  }

  @Override
  public int length() {
    return count;
  }

  @Override
  public void set32(final int index, final int value) {
    values[offset + index] = (short) value;
  }

  @Override
  public int get32(final int index) {
    return values[offset + index];
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new ShortArraySlice(values, this.offset + offset, count);
  }
}
