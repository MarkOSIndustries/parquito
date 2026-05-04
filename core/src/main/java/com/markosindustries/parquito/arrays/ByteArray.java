package com.markosindustries.parquito.arrays;

public final class ByteArray implements FastArray32 {
  private final byte[] values;

  public ByteArray(final byte[] values) {
    this.values = values;
  }

  @Override
  public int length() {
    return values.length;
  }

  @Override
  public void set32(final int index, final int value) {
    values[index] = (byte) value;
  }

  @Override
  public int get32(final int index) {
    return values[index];
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new ByteArraySlice(values, offset, count);
  }
}
