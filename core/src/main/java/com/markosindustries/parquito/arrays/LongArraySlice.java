package com.markosindustries.parquito.arrays;

public final class LongArraySlice implements FastArray64 {
  private final long[] values;
  private final int offset;
  private final int count;

  public LongArraySlice(final long[] values, int offset, int count) {
    if (offset < 0 || offset + count > values.length) {
      throw new IllegalArgumentException(
          "Cannot slice long["
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
  public void set(final int index, final long value) {
    values[offset + index] = value;
  }

  @Override
  public long get(final int index) {
    return values[offset + index];
  }

  @Override
  public FastArray slice(final int offset, final int count) {
    return new LongArraySlice(values, this.offset + offset, count);
  }
}
