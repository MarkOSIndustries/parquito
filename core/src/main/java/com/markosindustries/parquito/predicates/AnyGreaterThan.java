package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

/**
 * Matches a row if at least one value for the given column is greater than the referenceValue
 *
 * @param <Converted> The type of value
 */
public class AnyGreaterThan<Converted>
    extends AbstractColumnComparisonPredicate<Converted, PredicateRowMatcher.AnyMatch> {
  private final Converted referenceValue;
  private final ValuesComparer comparer;

  public AnyGreaterThan(
      final Converted referenceValue,
      final ConvertedColumnType<Converted> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AnyMatch::new);
    this.referenceValue = referenceValue;
    this.comparer = makeValuesComparer(referenceValue);
  }

  @Override
  public boolean valueMatches(final Values values, final int index) {
    return comparer.compare(values, index) > 0;
  }

  @Override
  public boolean nullMatches() {
    return compareNull(referenceValue) > 0;
  }
}
