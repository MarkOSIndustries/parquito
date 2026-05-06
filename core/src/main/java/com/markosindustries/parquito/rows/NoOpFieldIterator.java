package com.markosindustries.parquito.rows;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class NoOpFieldIterator implements ParquetFieldIterator<Void> {
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
  public boolean nextRowMatches() {
    return false;
  }

  @Override
  public void skipNextRow() {}

  @Override
  public boolean hasNext() {
    return false;
  }

  @SuppressFBWarnings("IT_NO_SUCH_ELEMENT")
  @Override
  public Void next() {
    return null;
  }
}
