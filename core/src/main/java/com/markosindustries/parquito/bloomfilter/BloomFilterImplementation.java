package com.markosindustries.parquito.bloomfilter;

public interface BloomFilterImplementation {
  boolean mightContain(long hash);

  void insert(long hash);
}
