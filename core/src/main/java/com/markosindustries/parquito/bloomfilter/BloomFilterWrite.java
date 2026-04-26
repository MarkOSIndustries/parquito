package com.markosindustries.parquito.bloomfilter;

public interface BloomFilterWrite {
  <Value> void insert(final Value value);

  <Value> void insertAll(final Iterable<Value> values);
}
