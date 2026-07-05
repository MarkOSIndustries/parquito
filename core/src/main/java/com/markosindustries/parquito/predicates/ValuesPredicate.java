package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.page.Values;

@FunctionalInterface
public interface ValuesPredicate {
  boolean matches(final Values values, final int index);
}
