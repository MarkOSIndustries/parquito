package com.markosindustries.parquito.arrays;

import it.unimi.dsi.fastutil.longs.LongList;
import java.util.List;

public sealed interface FastArray64 extends FastArray
    permits LongArray, LongArraySlice, LongListBoxless, LongListBoxed, FastDictionary64 {
  static <T> FastArray64 wrap(List<T> list, Class<T> elementClass) {
    if (list instanceof LongList) {
      return new LongListBoxless((LongList) list);
    }
    if (elementClass.isAssignableFrom(Long.class)) {
      return new LongListBoxed((List<Long>) list);
    }
    throw new UnsupportedOperationException(
        "Cannot use 64 bit Boxless access with " + list.getClass() + " of " + elementClass);
  }
}
