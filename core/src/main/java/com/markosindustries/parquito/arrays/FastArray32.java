package com.markosindustries.parquito.arrays;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;

public sealed interface FastArray32 extends FastArray
    permits FastDictionary32,
        IntArray,
        IntArraySlice,
        ShortArray,
        ShortArraySlice,
        ByteArray,
        ByteArraySlice,
        FastList32 {
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

  static <T> FastArray32 wrap(List<T> list, Class<T> elementClass) {
    if (list instanceof IntList) {
      return new IntListBoxless((IntList) list);
    }
    if (elementClass.isAssignableFrom(Integer.class)) {
      return new IntListBoxed((List<Integer>) list);
    }
    throw new UnsupportedOperationException(
        "Cannot use 32 bit Boxless access with " + list.getClass() + " of " + elementClass);
  }
}
