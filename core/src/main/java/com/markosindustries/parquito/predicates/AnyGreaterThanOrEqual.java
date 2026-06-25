package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

/**
 * Matches a row if at least one value for the given column is greater than or equal to the
 * referenceValue
 *
 * @param <Converted> The type of value
 */
public class AnyGreaterThanOrEqual<Converted>
    extends ColumnPredicate<Converted, PredicateRowMatcher.AnyMatch> {
  private final Converted referenceValue;

  public AnyGreaterThanOrEqual(
      final Converted referenceValue,
      final ConvertedColumnType<Converted> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AnyMatch::new);
    this.referenceValue = referenceValue;
  }

  @Override
  public boolean valueMatches(final Values values, final int index) {
    return compare(values, index, referenceValue) >= 0;
  }

  @Override
  public boolean nullMatches() {
    return compareNull(referenceValue) >= 0;
  }
}
