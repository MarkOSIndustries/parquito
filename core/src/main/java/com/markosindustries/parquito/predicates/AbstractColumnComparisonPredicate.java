package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

public abstract class AbstractColumnComparisonPredicate<
        Converted, RowMatcher extends PredicateRowMatcher>
    extends ColumnPredicate<Converted, RowMatcher> {
  private final ConvertedColumnType<Converted> convertedColumnType;

  public AbstractColumnComparisonPredicate(
      final ConvertedColumnType<Converted> convertedColumnType,
      final ParquetSchemaPath schemaPath,
      final RowMatcherConstructor<RowMatcher> rowMatcherConstructor) {
    super(schemaPath, rowMatcherConstructor);
    this.convertedColumnType = convertedColumnType;
  }

  public ValuesComparer makeValuesComparer(final Converted right) {
    return convertedColumnType.makeValuesComparer(right);
  }

  protected int compareNull(final Converted right) {
    return convertedColumnType.compareNull(right);
  }
}
