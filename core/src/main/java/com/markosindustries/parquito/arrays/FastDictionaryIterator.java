package com.markosindustries.parquito.arrays;

import java.util.Iterator;

public class FastDictionaryIterator<T> implements Iterator<T> {
  private final FastDictionary<T, ?> fastDictionary;
  private int index = 0;

  public FastDictionaryIterator(final FastDictionary<T, ?> fastDictionary) {
    this.fastDictionary = fastDictionary;
  }

  @Override
  public boolean hasNext() {
    return index < fastDictionary.length();
  }

  @Override
  public T next() {
    return fastDictionary.getAsObject(index++);
  }
}
