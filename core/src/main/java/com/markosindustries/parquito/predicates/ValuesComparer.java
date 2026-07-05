package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.page.Values;

@FunctionalInterface
public interface ValuesComparer {
  int compare(final Values values, final int index);
}
