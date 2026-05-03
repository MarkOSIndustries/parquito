package com.markosindustries.parquito.arrays;

import java.util.AbstractCollection;
import java.util.List;

public final class FastDictionaryObject<T> extends AbstractCollection<T>
    implements FastDictionary<T, FastDictionaryObject<T>> {
  private final List<T> values;
  private final FastArray32 indices;

  public FastDictionaryObject(final List<T> values, final FastArray32 indices) {
    this.values = values;
    this.indices = indices;
  }

  @Override
  public FastDictionaryObject<T> sliceDictionary(final int offset, final int count) {
    return new FastDictionaryObject<>(values, indices.slice32(offset, count));
  }

  @Override
  public T getAsObject(final int index) {
    return values.get(indices.get32(index));
  }

  @Override
  public int getIndex(final int index) {
    return indices.get32(index);
  }

  @Override
  public FastArray32 getIndices() {
    return indices;
  }

  @Override
  public int length() {
    return indices.length();
  }

  @Override
  public FastArray asFastArray() {
    throw new UnsupportedOperationException("Cannot use a FastDictionaryObject as a FastArray");
  }

  @Override
  public java.util.Iterator<T> iterator() {
    return new FastDictionaryIterator<T>(this);
  }

  @Override
  public int size() {
    return length();
  }
}
