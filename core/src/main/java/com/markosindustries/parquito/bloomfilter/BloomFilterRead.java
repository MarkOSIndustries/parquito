package com.markosindustries.parquito.bloomfilter;

import java.util.Collection;

public interface BloomFilterRead {
  <Value> boolean mightContain(final Value value);

  <Value> boolean mightContainAny(final Collection<Value> values);
}
