package com.markosindustries.parquito.arrays;

public final class ByteArraySlice implements FastArray32 {
  private final byte[] values;
  private final int offset;
  private final int count;

  public ByteArraySlice(final byte[] values, int offset, int count) {
    if (offset < 0 || offset + count > values.length) {
      throw new IllegalArgumentException(
          "Cannot slice byte["
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
    values[offset + index] = (byte) value;
  }

  @Override
  public int get32(final int index) {
    return values[offset + index];
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new ByteArraySlice(values, this.offset + offset, count);
  }
}
