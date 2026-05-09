package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;

/**
 * Matches a row if none of the values for the given column equals the comparator
 *
 * @param <ReadAs> The type of value
 */
public class NoneEquals<ReadAs>
    extends ColumnPredicate<ReadAs, PredicateRowMatcher.NoneMatch<ReadAs>> {
  public NoneEquals(
      final ReadAs comparator, final ColumnType<ReadAs> columnType, ParquetSchemaPath schemaPath) {
    super(comparator, columnType, schemaPath, PredicateRowMatcher.NoneMatch::new);
  }

  @Override
  public boolean valueMatches(final ReadAs value) {
    return compare(value) == 0;
  }
}
