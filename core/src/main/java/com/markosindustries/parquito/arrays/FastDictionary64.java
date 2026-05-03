package com.markosindustries.parquito.arrays;

import java.util.AbstractCollection;

public final class FastDictionary64 extends AbstractCollection<Long>
    implements FastDictionary<Long, FastDictionary64>, FastArray64 {
  private final FastArray64 values;
  private final FastArray32 indices;

  public FastDictionary64(final FastArray64 values, final FastArray32 indices) {
    this.values = values;
    this.indices = indices;
  }

  @Override
  public FastArray slice(final int offset, final int count) {
    return sliceDictionary(offset, count);
  }

  @Override
  public FastDictionary64 sliceDictionary(final int offset, final int count) {
    return new FastDictionary64(values, indices.slice32(offset, count));
  }

  @Override
  public long get(final int index) {
    return values.get(indices.get32(index));
  }

  @Override
  public Long getAsObject(final int index) {
    return get(index);
  }

  @Override
  public int getIndex(final int index) {
    return indices.get32(index);
  }

  @Override
  public void set(final int index, final long value) {
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
  public java.util.Iterator<Long> iterator() {
    return new FastDictionaryIterator<Long>(this);
  }

  @Override
  public int size() {
    return length();
  }
}
