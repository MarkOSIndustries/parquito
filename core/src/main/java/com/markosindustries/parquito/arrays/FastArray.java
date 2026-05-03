package com.markosindustries.parquito.arrays;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.List;

/**
 * This exists to avoid boxing conversions for arrays of int[] and long[].
 *
 * <p>It also supports lists as an underlying type, and will attempt to avoid boxing there if
 * possible.
 */
public sealed interface FastArray permits FastArray32, FastArray64 {
  int length();

  void set(int index, long value);

  long get(int index);

  FastArray slice(int offset, int count);

  static FastArray32 wrap(int[] values) {
    return new IntArray(values);
  }

  static FastArray64 wrap(long[] values) {
    return new LongArray(values);
  }

  static FastArray32 wrap(IntList list) {
    return new IntListBoxless(list);
  }

  static FastArray64 wrap(LongList list) {
    return new LongListBoxed(list);
  }

  @SuppressWarnings("unchecked")
  static <T> FastArray wrap(List<T> list, Class<T> elementClass) {
    if (list instanceof IntList || elementClass.isAssignableFrom(Integer.class)) {
      return FastArray32.wrap(list, elementClass);
    }
    if (list instanceof LongList || elementClass.isAssignableFrom(Long.class)) {
      return FastArray64.wrap(list, elementClass);
    }
    throw new UnsupportedOperationException(
        "Cannot use Boxless access with " + list.getClass() + " of " + elementClass);
  }

  static FastArray32 slice(int[] values, int offset, int count) {
    return new IntArraySlice(values, offset, count);
  }

  static FastArray64 slice(long[] values, int offset, int count) {
    return new LongArraySlice(values, offset, count);
  }
}
