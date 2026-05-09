package com.markosindustries.parquito.predicates;

public class Intersection extends CompoundPredicate {
  public Intersection(ParquetPredicate... predicates) {
    super(predicates);
  }

  @Override
  public boolean matchesNextRow() {
    for (final ParquetPredicate predicate : predicates) {
      if (!predicate.matchesNextRow()) {
        return false;
      }
    }
    return true;
  }
}
