package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;

/**
 * Matches a row if ALL values for the given column equals the referenceValue
 *
 * @param <ReadAs> The type of value
 */
public class AllEquals<ReadAs>
    extends ColumnPredicate<ReadAs, PredicateRowMatcher.AllMatch<ReadAs>> {
  private final ReadAs referenceValue;

  public AllEquals(
      final ReadAs referenceValue,
      final ColumnType<ReadAs> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AllMatch::new);
    this.referenceValue = referenceValue;
  }

  @Override
  public boolean valueMatches(final ReadAs value) {
    return compare(value, referenceValue) == 0;
  }
}
