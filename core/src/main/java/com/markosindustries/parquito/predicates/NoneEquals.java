package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import java.nio.ByteBuffer;

/**
 * Matches a row if none of the values for the given column equals the referenceValue
 *
 * @param <Converted> The type of value
 */
public class NoneEquals<Converted>
    extends ColumnPredicate<Converted, PredicateRowMatcher.NoneMatch> {
  private final Converted referenceValue;

  public NoneEquals(
      final Converted referenceValue,
      final ConvertedColumnType<Converted> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.NoneMatch::new);
    this.referenceValue = referenceValue;
  }

  @Override
  public boolean valueMatches(final boolean value) {
    return compare(value, referenceValue) == 0;
  }

  @Override
  public boolean valueMatches(final ByteBuffer value) {
    return compare(value, referenceValue) == 0;
  }

  @Override
  public boolean valueMatches(final double value) {
    return compare(value, referenceValue) == 0;
  }

  @Override
  public boolean valueMatches(final float value) {
    return compare(value, referenceValue) == 0;
  }

  @Override
  public boolean valueMatches(final int value) {
    return compare(value, referenceValue) == 0;
  }

  @Override
  public boolean valueMatches(final long value) {
    return compare(value, referenceValue) == 0;
  }

  @Override
  public boolean nullMatches() {
    return compareNull(referenceValue) == 0;
  }
}
