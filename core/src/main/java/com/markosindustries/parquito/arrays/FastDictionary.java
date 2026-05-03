package com.markosindustries.parquito.arrays;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.Collection;
import java.util.List;

public sealed interface FastDictionary<T, D extends FastDictionary<T, D>> extends Collection<T>
    permits FastDictionary32, FastDictionary64, FastDictionaryObject {
  @SuppressWarnings("unchecked")
  static <T> FastDictionary<T, ?> wrap(
      final List<T> values, final FastArray32 indices, Class<T> elementClass) {
    if (values instanceof IntList || elementClass.isAssignableFrom(Integer.class)) {
      return (FastDictionary<T, ?>)
          new FastDictionary32(FastArray32.wrap(values, elementClass), indices);
    }
    if (values instanceof LongList || elementClass.isAssignableFrom(Long.class)) {
      return (FastDictionary<T, ?>)
          new FastDictionary64(FastArray64.wrap(values, elementClass), indices);
    }
    return new FastDictionaryObject<T>(values, indices);
  }

  D sliceDictionary(int offset, int count);

  T getAsObject(int index);

  int getIndex(int i);

  FastArray32 getIndices();

  int length();

  FastArray asFastArray();
}
