package com.markosindustries.parquito.rows;

import com.markosindustries.parquito.ParquetPredicate;
import java.util.Arrays;

/** Takes a set of leaf predicates and gets them to the leaf iterators */
public class PushdownPredicates {
  private final ParquetPredicate topLevelPredicate;
  private final ParquetPredicate.Leaf<?>[] leafPredicates;
  private final int pathOffset;

  public PushdownPredicates(
      final ParquetPredicate topLevelPredicate,
      final ParquetPredicate.Leaf<?>[] leafPredicates,
      final int pathOffset) {
    this.topLevelPredicate = topLevelPredicate;
    this.leafPredicates = leafPredicates;
    this.pathOffset = pathOffset;
  }

  public boolean matchesNextRow() {
    return topLevelPredicate.matchesNextRow();
  }

  public void newPage(final DataPageCursor<?> dataPageCursor) {
    for (final var leafPredicate : leafPredicates) {
      leafPredicate.newPageUnsafe(dataPageCursor);
    }
  }

  public PushdownPredicates forChild(final int childIndex) {
    return new PushdownPredicates(
        topLevelPredicate,
        Arrays.stream(leafPredicates)
            .filter(leafPredicate -> shouldPushDown(leafPredicate, childIndex))
            .toArray(ParquetPredicate.Leaf[]::new),
        pathOffset + 1);
  }

  private boolean shouldPushDown(
      final ParquetPredicate.Leaf<?> leafPredicate, final int childIndex) {
    if (pathOffset < leafPredicate.getSchemaPath().pathAsFieldIndices.length) {
      return leafPredicate.getSchemaPath().pathAsFieldIndices[pathOffset] == childIndex;
    }
    return false;
  }
}
