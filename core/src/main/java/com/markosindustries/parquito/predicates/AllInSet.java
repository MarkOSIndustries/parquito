package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ColumnValuesSet;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.PredicateRowMatcher;

/**
 * Matches a row if all values for the given column equal one of the values in the referenceValues
 * set
 *
 * @param <Converted> The type of value
 */
public class AllInSet<Converted>
    extends AbstractColumnSetContainsPredicate<Converted, PredicateRowMatcher.AllMatch> {
  public AllInSet(
      final ColumnValuesSet<Converted> referenceValues, final ParquetSchemaPath schemaPath) {
    super(referenceValues, schemaPath, PredicateRowMatcher.AllMatch::new);
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
