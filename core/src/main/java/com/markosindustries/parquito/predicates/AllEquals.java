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
public class AllEquals<Converted>
    extends AbstractColumnEqualityPredicate<Converted, PredicateRowMatcher.AllMatch> {
  private final Converted referenceValue;
  private final ValuesPredicate predicate;

  public AllEquals(
      final Converted referenceValue,
      final ConvertedColumnType<Converted> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AllMatch::new);
    this.referenceValue = referenceValue;
    this.predicate = makeEqualsPredicate(referenceValue);
  }

  @Override
  public boolean valueMatches(final Values values, final int index) {
    return predicate.matches(values, index);
  }

  @Override
  public boolean nullMatches() {
    return equalsNull(referenceValue);
  }
}
