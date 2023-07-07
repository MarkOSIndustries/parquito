package com.markosindustries.parquito.rows;

import java.util.Iterator;

public interface ParquetFieldIterator<T> extends Iterator<T> {
  int peekDefinitionLevel();

  int peekRepetitionLevel();

  boolean nextRowMatches();

  void skipNextRow();
}
