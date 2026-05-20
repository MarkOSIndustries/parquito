package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;

/**
 * Matches a row if at least one value for the given column is less than or equal to the
 * referenceValue
 *
 * @param <ReadAs> The type of value
 */
public class AnyLessThanOrEqual<ReadAs>
    extends ColumnPredicate<ReadAs, PredicateRowMatcher.AnyMatch<ReadAs>> {
  private final ReadAs referenceValue;

  public AnyLessThanOrEqual(
      final ReadAs referenceValue,
      final ColumnType<ReadAs> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AnyMatch::new);
    this.referenceValue = referenceValue;
  }

  @Override
  public boolean valueMatches(final ReadAs value) {
    return compare(value, referenceValue) <= 0;
  }
}
