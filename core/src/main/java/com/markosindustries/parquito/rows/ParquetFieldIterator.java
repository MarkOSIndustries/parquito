package com.markosindustries.parquito.rows;

public interface ParquetFieldIterator {
  int peekDefinitionLevel();

  int peekRepetitionLevel();

  void skipNextRow();

  boolean hasNext();

  void visitNext(final FieldVisitor visitor);
}
