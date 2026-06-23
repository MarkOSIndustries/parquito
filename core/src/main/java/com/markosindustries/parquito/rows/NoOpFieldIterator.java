package com.markosindustries.parquito.rows;

public class NoOpFieldIterator implements ParquetFieldIterator {
  public static final NoOpFieldIterator INSTANCE = new NoOpFieldIterator();

  @Override
  public int peekDefinitionLevel() {
    return 0;
  }

  @Override
  public int peekRepetitionLevel() {
    return 0;
  }

  @Override
  public void skipNextRow() {}

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public void visitNext(final FieldVisitor visitor) {}
}
