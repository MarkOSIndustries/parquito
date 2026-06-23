package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.DataPageCursor;
import com.markosindustries.parquito.rows.PredicateMaterialisedMatches;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

/**
 * A predicate
 *
 * @param <Converted>
 */
public abstract class ColumnPredicate<Converted, RowMatcher extends PredicateRowMatcher>
    implements ParquetPredicate {
  private final ConvertedColumnType<Converted> convertedColumnType;
  private final ParquetSchemaPath schemaPath;
  private final RowMatcherConstructor<RowMatcher> rowMatcherConstructor;
  private PredicateRowMatcher rowMatcher;

  @FunctionalInterface
  public interface RowMatcherConstructor<RowMatcher extends PredicateRowMatcher> {
    RowMatcher create(
        final DataPageCursor dataPageCursor,
        final PredicateMaterialisedMatches materialisedMatches,
        final boolean matchesNull);
  }

  public ColumnPredicate(
      final ConvertedColumnType<Converted> convertedColumnType,
      final ParquetSchemaPath schemaPath,
      final RowMatcherConstructor<RowMatcher> rowMatcherConstructor) {
    this.convertedColumnType = convertedColumnType;
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

  protected int compare(final boolean o1, final Converted o2) {
    return convertedColumnType.compare(o1, o2);
  }

  protected int compare(final ByteBuffer o1, final Converted o2) {
    return convertedColumnType.compare(o1, o2);
  }

  protected int compare(final double o1, final Converted o2) {
    return convertedColumnType.compare(o1, o2);
  }

  protected int compare(final float o1, final Converted o2) {
    return convertedColumnType.compare(o1, o2);
  }

  protected int compare(final int o1, final Converted o2) {
    return convertedColumnType.compare(o1, o2);
  }

  protected int compare(final long o1, final Converted o2) {
    return convertedColumnType.compare(o1, o2);
  }

  protected int compareNull(final Converted o2) {
    return convertedColumnType.compareNull(o2);
  }

  public abstract boolean valueMatches(final boolean value);

  public abstract boolean valueMatches(final ByteBuffer value);

  public abstract boolean valueMatches(final double value);

  public abstract boolean valueMatches(final float value);

  public abstract boolean valueMatches(final int value);

  public abstract boolean valueMatches(final long value);

  public abstract boolean nullMatches();

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

  public final void newPage(final DataPageCursor dataPageCursor) {
    final var materialisedMatches =
        dataPageCursor
            .getDataPage()
            .getValues()
            .materialise(this, this.convertedColumnType.logicalTypeConverter().getConvertedClass());
    this.rowMatcher =
        rowMatcherConstructor.create(dataPageCursor, materialisedMatches, nullMatches());
  }
}
