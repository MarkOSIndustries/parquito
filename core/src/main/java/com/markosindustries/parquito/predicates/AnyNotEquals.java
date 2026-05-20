package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;

/**
 * Matches a row if at least one value for the given column does not equal the referenceValue
 *
 * @param <ReadAs> The type of value
 */
public class AnyNotEquals<ReadAs>
    extends ColumnPredicate<ReadAs, PredicateRowMatcher.AnyMatch<ReadAs>> {
  private final ReadAs referenceValue;

  public AnyNotEquals(
      final ReadAs referenceValue,
      final ColumnType<ReadAs> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AnyMatch::new);
    this.referenceValue = referenceValue;
  }

  @Override
  public boolean valueMatches(final ReadAs value) {
    return compare(value, referenceValue) != 0;
  }
}
