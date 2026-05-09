package com.markosindustries.parquito.predicates;

public class Not extends CompoundPredicate {
  private final ParquetPredicate predicate;

  public Not(final ParquetPredicate predicate) {
    super(predicate);
    this.predicate = predicate;
  }

  @Override
  public boolean matchesNextRow() {
    return !predicate.matchesNextRow();
  }
}
