package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.Reader;
import java.util.Iterator;

public class RowIterator<Row> implements Iterator<Row> {
  private final PushdownPredicates pushdownPredicates;
  private final ParquetFieldIterator iterator;
  private final Reader<Row> reader;

  public RowIterator(
      PushdownPredicates pushdownPredicates, ParquetFieldIterator iterator, Reader<Row> reader) {
    this.pushdownPredicates = pushdownPredicates;
    this.iterator = iterator;
    this.reader = reader;
    advanceToNext();
  }

  private void advanceToNext() {
    while (iterator.hasNext() && !pushdownPredicates.matchesNextRow()) {
      iterator.skipNextRow();
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Row next() {
    final var rowBuilder = reader.rowBuilder();
    iterator.visitNext(rowBuilder);
    advanceToNext();
    return rowBuilder.build();
  }
}
