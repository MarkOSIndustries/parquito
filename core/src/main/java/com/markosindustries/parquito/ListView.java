package com.markosindustries.parquito;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.SortedSet;

public class ListView {
  public static <T> List<T> of(SortedSet<T> sortedSet) {
    return new List<T>() {
      @Override
      public int size() {
        return sortedSet.size();
      }

      @Override
      public boolean isEmpty() {
        return sortedSet.isEmpty();
      }

      @Override
      public boolean contains(final Object o) {
        return sortedSet.contains(o);
      }

      @Override
      public Iterator<T> iterator() {
        return sortedSet.iterator();
      }

      @Override
      public Object[] toArray() {
        return sortedSet.toArray();
      }

      @Override
      public <T1> T1[] toArray(final T1[] a) {
        return sortedSet.toArray(a);
      }

      @Override
      public boolean add(final T t) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public boolean remove(final Object o) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public boolean containsAll(final Collection<?> c) {
        return sortedSet.containsAll(c);
      }

      @Override
      public boolean addAll(final Collection<? extends T> c) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public boolean addAll(final int index, final Collection<? extends T> c) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public T get(final int index) {
        throw new UnsupportedOperationException("Cannot access SortedSet by index");
      }

      @Override
      public T set(final int index, final T element) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public void add(final int index, final T element) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public T remove(final int index) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public int indexOf(final Object o) {
        throw new UnsupportedOperationException("Cannot search a sorted set for an index");
      }

      @Override
      public int lastIndexOf(final Object o) {
        throw new UnsupportedOperationException("Cannot search a sorted set for an index");
      }

      @Override
      public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public ListIterator<T> listIterator(final int index) {
        throw new UnsupportedOperationException("This is a read only view");
      }

      @Override
      public List<T> subList(final int fromIndex, final int toIndex) {
        throw new UnsupportedOperationException("Cannot access SortedSet by index");
      }
    };
  }
}
