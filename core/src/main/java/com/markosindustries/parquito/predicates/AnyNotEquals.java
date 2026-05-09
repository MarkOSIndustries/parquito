package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;

/**
 * Matches a row if at least one value for the given column does not equal the comparator
 *
 * @param <ReadAs> The type of value
 */
public class AnyNotEquals<ReadAs>
    extends ColumnPredicate<ReadAs, PredicateRowMatcher.AnyMatch<ReadAs>> {
  public AnyNotEquals(
      final ReadAs comparator, final ColumnType<ReadAs> columnType, ParquetSchemaPath schemaPath) {
    super(comparator, columnType, schemaPath, PredicateRowMatcher.AnyMatch::new);
  }

  @Override
  public boolean valueMatches(final ReadAs value) {
    return compare(value) != 0;
  }
}
