package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

public abstract class AbstractColumnEqualityPredicate<
        Converted, RowMatcher extends PredicateRowMatcher>
    extends ColumnPredicate<Converted, RowMatcher> {
  private final ConvertedColumnType<Converted> convertedColumnType;

  public AbstractColumnEqualityPredicate(
      final ConvertedColumnType<Converted> convertedColumnType,
      final ParquetSchemaPath schemaPath,
      final RowMatcherConstructor<RowMatcher> rowMatcherConstructor) {
    super(schemaPath, rowMatcherConstructor);
    this.convertedColumnType = convertedColumnType;
  }

  protected ValuesPredicate makeEqualsPredicate(final Converted right) {
    return convertedColumnType.makeValuesEqualityPredicate(right);
  }

  protected boolean equalsNull(final Converted right) {
    return convertedColumnType.compareNull(right) == 0;
  }
}
