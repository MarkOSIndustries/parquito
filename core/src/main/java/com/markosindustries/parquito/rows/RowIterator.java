package com.markosindustries.parquito.rows;

import java.util.Iterator;

public class RowIterator<Row> implements Iterator<Row> {
  private final ParquetFieldIterator<Row> iterator;

  public RowIterator(ParquetFieldIterator<Row> iterator) {
    this.iterator = iterator;
    advanceToNext();
  }

  private void advanceToNext() {
    while (iterator.hasNext() && !iterator.nextRowMatches()) {
      iterator.skipNextRow();
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Row next() {
    final var next = iterator.next();
    advanceToNext();
    return next;
  }
}
