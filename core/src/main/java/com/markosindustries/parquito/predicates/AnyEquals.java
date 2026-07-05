package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

/**
 * Matches a row if at least one value for the given column equals the referenceValue
 *
 * @param <Converted> The type of value
 */
public class AnyEquals<Converted>
    extends AbstractColumnEqualityPredicate<Converted, PredicateRowMatcher.AnyMatch> {
  private final Converted referenceValue;
  private final ValuesPredicate predicate;

  public AnyEquals(
      final Converted referenceValue,
      final ConvertedColumnType<Converted> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AnyMatch::new);
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
