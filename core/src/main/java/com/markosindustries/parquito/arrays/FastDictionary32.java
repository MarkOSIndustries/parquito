package com.markosindustries.parquito.arrays;

import java.util.AbstractCollection;

public final class FastDictionary32 extends AbstractCollection<Integer>
    implements FastDictionary<Integer, FastDictionary32>, FastArray32 {
  private final FastArray32 values;
  private final FastArray32 indices;

  public FastDictionary32(final FastArray32 values, final FastArray32 indices) {
    this.values = values;
    this.indices = indices;
  }

  @Override
  public FastArray32 slice32(final int offset, final int count) {
    return sliceDictionary(offset, count);
  }

  @Override
  public FastDictionary32 sliceDictionary(final int offset, final int count) {
    return new FastDictionary32(values, indices.slice32(offset, count));
  }

  @Override
  public int get32(final int index) {
    return values.get32(indices.get32(index));
  }

  @Override
  public Integer getAsObject(final int index) {
    return get32(index);
  }

  @Override
  public int getIndex(final int index) {
    return indices.get32(index);
  }

  @Override
  public void set32(final int index, final int value) {
    throw new UnsupportedOperationException("Modifications to IntDictionary are not supported");
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
    return this;
  }

  @Override
  public java.util.Iterator<Integer> iterator() {
    return new FastDictionaryIterator<Integer>(this);
  }

  @Override
  public int size() {
    return length();
  }
}
