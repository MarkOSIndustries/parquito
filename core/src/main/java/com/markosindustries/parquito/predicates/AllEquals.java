package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;

/**
 * Matches a row if ALL values for the given column equals the comparator
 *
 * @param <ReadAs> The type of value
 */
public class AllEquals<ReadAs>
    extends ColumnPredicate<ReadAs, PredicateRowMatcher.AllMatch<ReadAs>> {
  public AllEquals(
      final ReadAs comparator, final ColumnType<ReadAs> columnType, ParquetSchemaPath schemaPath) {
    super(comparator, columnType, schemaPath, PredicateRowMatcher.AllMatch::new);
  }

  @Override
  public boolean valueMatches(final ReadAs value) {
    return compare(value) == 0;
  }
}
