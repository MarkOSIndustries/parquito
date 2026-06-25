package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

/**
 * Matches a row if none of the values for the given column equals the referenceValue
 *
 * @param <Converted> The type of value
 */
public class NoneEquals<Converted>
    extends ColumnPredicate<Converted, PredicateRowMatcher.NoneMatch> {
  private final Converted referenceValue;

  public NoneEquals(
      final Converted referenceValue,
      final ConvertedColumnType<Converted> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.NoneMatch::new);
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
