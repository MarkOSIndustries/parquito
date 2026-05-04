package com.markosindustries.parquito.arrays;

public final class ZerosCounter implements FastList32 {
  private int count;

  public ZerosCounter(int count) {
    if (count < 0) {
      throw new IllegalArgumentException("Can't count a negative number of zeros: " + count);
    }
    this.count = count;
  }

  @Override
  public int length() {
    return count;
  }

  @Override
  public void set32(final int index, final int value) {
    if (value != 0) {
      throw new IllegalArgumentException("Can't set a non-zero value in a zero counter: " + value);
    }
    count = Math.max(count, index);
  }

  @Override
  public void add(final int value) {
    if (value != 0) {
      throw new IllegalArgumentException("Can't add a non-zero value in a zero counter: " + value);
    }
    count++;
  }

  @Override
  public int get32(final int index) {
    return 0;
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return new ZerosCounter(count);
  }

  @Override
  public FastList32 subList(int startOffsetInclusive, int endOffsetExclusive) {
    return new ZerosCounter(endOffsetExclusive - startOffsetInclusive);
  }
}
