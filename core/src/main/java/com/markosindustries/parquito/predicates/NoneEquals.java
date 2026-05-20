package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;

/**
 * Matches a row if none of the values for the given column equals the referenceValue
 *
 * @param <ReadAs> The type of value
 */
public class NoneEquals<ReadAs>
    extends ColumnPredicate<ReadAs, PredicateRowMatcher.NoneMatch<ReadAs>> {
  private final ReadAs referenceValue;

  public NoneEquals(
      final ReadAs referenceValue,
      final ColumnType<ReadAs> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.NoneMatch::new);
    this.referenceValue = referenceValue;
  }

  @Override
  public boolean valueMatches(final ReadAs value) {
    return compare(value, referenceValue) == 0;
  }
}
