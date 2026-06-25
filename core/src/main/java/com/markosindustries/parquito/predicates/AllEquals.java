package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

/**
 * Matches a row if ALL values for the given column equals the referenceValue
 *
 * @param <Converted> The type of value
 */
public class AllEquals<Converted> extends ColumnPredicate<Converted, PredicateRowMatcher.AllMatch> {
  private final Converted referenceValue;

  public AllEquals(
      final Converted referenceValue,
      final ConvertedColumnType<Converted> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AllMatch::new);
    this.referenceValue = referenceValue;
  }

  @Override
  public boolean valueMatches(final Values values, final int index) {
    return compare(values, index, referenceValue) == 0;
  }

  @Override
  public boolean nullMatches() {
    return compareNull(referenceValue) == 0;
  }
}
