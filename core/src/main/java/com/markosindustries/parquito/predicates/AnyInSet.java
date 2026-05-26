package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;
import java.util.HashSet;
import java.util.Set;

/**
 * Matches a row if at least one value for the given column equals one of the values in the
 * referenceValues set
 *
 * @param <ReadAs> The type of value
 */
public class AnyInSet<ReadAs>
    extends ColumnPredicate<ReadAs, PredicateRowMatcher.AnyMatch<ReadAs>> {
  private final Set<ReadAs> referenceValues;

  private AnyInSet(
      Set<ReadAs> referenceValues,
      final ColumnType<ReadAs> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AnyMatch::new);
    this.referenceValues = referenceValues;
    this.referenceValues.addAll(referenceValues);
  }

  @Override
  public boolean valueMatches(final ReadAs value) {
    return referenceValues.contains(value);
  }

  private static <ReadAs> Set<ReadAs> asTypedSet(
      final Set<?> referenceValues, final ColumnType<ReadAs> columnType) {
    final var set = new HashSet<ReadAs>(referenceValues.size());
    final var caster = columnType.parquetType().getReadAsClass();
    for (final var referenceValue : referenceValues) {
      set.add(caster.cast(referenceValue));
    }
    return set;
  }

  public static <ReadAs> AnyInSet<ReadAs> from(
      final Set<?> referenceValues,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyInSet<>(asTypedSet(referenceValues, columnType), columnType, schemaPath);
  }
}
