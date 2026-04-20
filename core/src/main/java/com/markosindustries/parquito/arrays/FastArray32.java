package com.markosindustries.parquito.arrays;

public sealed interface FastArray32 extends FastArray
    permits IntArray, IntArraySlice, IntListBoxless, IntListBoxed {
  @Override
  default FastArray slice(int offset, int count) {
    return slice32(offset, count);
  }

  @Override
  default long get(int index) {
    return get32(index);
  }

  @Override
  default void set(int index, long value) {
    set32(index, (int) value);
  }

  FastArray32 slice32(int offset, int count);

  int get32(int index);

  void set32(int index, int value);
}
