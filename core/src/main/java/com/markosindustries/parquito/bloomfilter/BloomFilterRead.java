package com.markosindustries.parquito.bloomfilter;

import com.markosindustries.parquito.ColumnValuesSet;

public interface BloomFilterRead {
  <T> boolean mightContainAny(final ColumnValuesSet<T> values);
}
