package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.predicates.ColumnPredicate;
import com.markosindustries.parquito.predicates.ParquetPredicate;
import java.util.Arrays;

/** Takes a set of leaf predicates and gets them to the leaf iterators */
public class PushdownPredicates {
  private final ParquetPredicate topLevelPredicate;
  private final ColumnPredicate<?, ?>[] columnPredicates;
  private final int pathOffset;

  public PushdownPredicates(final ParquetPredicate topLevelPredicate) {
    this(
        topLevelPredicate, topLevelPredicate.columnPredicates().toArray(ColumnPredicate[]::new), 0);
  }

  private PushdownPredicates(
      final ParquetPredicate topLevelPredicate,
      final ColumnPredicate<?, ?>[] columnPredicates,
      final int pathOffset) {
    this.topLevelPredicate = topLevelPredicate;
    this.columnPredicates = columnPredicates;
    this.pathOffset = pathOffset;
  }

  public boolean matchesNextRow() {
    return topLevelPredicate.matchesNextRow();
  }

  public void newPage(final DataPageCursor dataPageCursor) {
    for (final var columnPredicate : columnPredicates) {
      columnPredicate.newPage(dataPageCursor);
    }
  }

  public PushdownPredicates forChild(final int childIndex) {
    return new PushdownPredicates(
        topLevelPredicate,
        Arrays.stream(columnPredicates)
            .filter(columnPredicate -> shouldPushDown(columnPredicate, childIndex))
            .toArray(ColumnPredicate[]::new),
        pathOffset + 1);
  }

  private boolean shouldPushDown(
      final ColumnPredicate<?, ?> columnPredicatePredicate, final int childIndex) {
    if (pathOffset < columnPredicatePredicate.getSchemaPath().getPathLength()) {
      return columnPredicatePredicate.getSchemaPath().getFieldIndexAtDepth(pathOffset)
          == childIndex;
    }
    return false;
  }
}
