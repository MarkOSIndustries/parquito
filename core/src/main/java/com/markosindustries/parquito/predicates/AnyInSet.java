package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ColumnValuesSet;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

/**
 * Matches a row if at least one value for the given column equals one of the values in the
 * referenceValues set
 *
 * @param <Converted> The type of value
 */
public class AnyInSet<Converted>
    extends AbstractColumnSetContainsPredicate<Converted, PredicateRowMatcher.AnyMatch> {
  public AnyInSet(
      final ColumnValuesSet<Converted> referenceValues, final ParquetSchemaPath schemaPath) {
    super(referenceValues, schemaPath, PredicateRowMatcher.AnyMatch::new);
  }

  @Override
  public boolean valueMatches(final Values values, final int index) {
    return referenceValues.contains(values, index);
  }

  @Override
  public boolean nullMatches() {
    return referenceValues.containsNull();
  }
}
