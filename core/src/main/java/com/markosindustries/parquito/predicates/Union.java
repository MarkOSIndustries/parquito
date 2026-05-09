package com.markosindustries.parquito.predicates;

public class Union extends CompoundPredicate {
  public Union(ParquetPredicate... predicates) {
    super(predicates);
  }

  @Override
  public boolean matchesNextRow() {
    for (final ParquetPredicate predicate : predicates) {
      if (predicate.matchesNextRow()) {
        return true;
      }
    }
    return false;
  }
}
