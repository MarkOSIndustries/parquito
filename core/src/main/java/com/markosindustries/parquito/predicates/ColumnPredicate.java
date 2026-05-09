package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.DataPageCursor;
import com.markosindustries.parquito.rows.PredicateMaterialisedMatches;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;
import com.markosindustries.parquito.types.ColumnType;
import java.util.stream.Stream;

/**
 * A predicate
 *
 * @param <ReadAs>
 */
public abstract class ColumnPredicate<ReadAs, RowMatcher extends PredicateRowMatcher>
    implements ParquetPredicate {
  private final ColumnType<ReadAs> columnType;
  private final ReadAs comparator;
  private final ParquetSchemaPath schemaPath;
  private final RowMatcherConstructor<ReadAs, RowMatcher> rowMatcherConstructor;
  private PredicateRowMatcher rowMatcher;

  @FunctionalInterface
  public interface RowMatcherConstructor<ReadAs, RowMatcher extends PredicateRowMatcher> {
    RowMatcher create(
        final DataPageCursor<ReadAs> dataPageCursor,
        final PredicateMaterialisedMatches materialisedMatches,
        final boolean matchesNull);
  }

  ColumnPredicate(
      final ReadAs comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath,
      final RowMatcherConstructor<ReadAs, RowMatcher> rowMatcherConstructor) {
    this.comparator = comparator;
    this.columnType = columnType;
    this.schemaPath = schemaPath;
    this.rowMatcherConstructor = rowMatcherConstructor;
  }

  public SchemaTraversalSpec asSchemaTraversalSpec() {
    return asSchemaTraversalSpec(0);
  }

  private SchemaTraversalSpec asSchemaTraversalSpec(final int offset) {
    return new SchemaTraversalSpec() {
      @Override
      public boolean includesChild(final int childFieldIndex) {
        return schemaPath.getPathLength() > offset
            && schemaPath.getFieldIndexAtDepth(offset) == childFieldIndex;
      }

      @Override
      public SchemaTraversalSpec forChild(final int childFieldIndex) {
        return asSchemaTraversalSpec(offset + 1);
      }
    };
  }

  public ParquetSchemaPath getSchemaPath() {
    return schemaPath;
  }

  protected int compare(ReadAs value) {
    return columnType.compare(value, comparator);
  }

  public abstract boolean valueMatches(final ReadAs value);

  @Override
  public final boolean matchesNextRow() {
    if (rowMatcher == null) {
      throw new RuntimeException(
          "Something went wrong - we wanted to check a predicate against a row, but no data page has been encountered yet");
    }
    return rowMatcher.rowMatches();
  }

  @Override
  public final Stream<ColumnPredicate<?, ?>> columnPredicates() {
    return Stream.of(this);
  }

  public final void newPageUnsafe(final DataPageCursor<?> dataPageCursor) {
    //noinspection unchecked
    newPage((DataPageCursor<ReadAs>) dataPageCursor);
  }

  public final void newPage(final DataPageCursor<ReadAs> dataPageCursor) {
    final var materialisedMatches = dataPageCursor.getDataPage().getValues().materialise(this);
    final var matchesNull = valueMatches(null);
    this.rowMatcher =
        rowMatcherConstructor.create(dataPageCursor, materialisedMatches, matchesNull);
  }
}
