package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ColumnValuesSet;
import com.markosindustries.parquito.ConvertedColumnType;
import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import java.util.HashSet;
import java.util.Set;

/**
 * Matches a row if all values for the given column equal one of the values in the referenceValues
 * set
 *
 * @param <Converted> The type of value
 */
public class AllInSet<Converted> extends ColumnPredicate<Converted, PredicateRowMatcher.AllMatch> {
  private final ColumnValuesSet<Converted> referenceValues;

  private AllInSet(
      Set<Converted> referenceValues,
      final ConvertedColumnType<Converted> columnType,
      ParquetSchemaPath schemaPath) {
    super(columnType, schemaPath, PredicateRowMatcher.AllMatch::new);
    this.referenceValues =
        new ColumnValuesSet<>(columnType.logicalTypeConverter(), referenceValues);
  }

  @Override
  public boolean valueMatches(final Values values, final int index) {
    return referenceValues.contains(values, index);
  }

  @Override
  public boolean nullMatches() {
    return referenceValues.containsNull();
  }

  private static <Converted> Set<Converted> asTypedSet(
      final Set<?> referenceValues, final ConvertedColumnType<Converted> columnType) {
    final var set = new HashSet<Converted>();
    final var caster = columnType.logicalTypeConverter().getConvertedClass();
    for (final var referenceValue : referenceValues) {
      set.add(caster.cast(referenceValue));
    }
    return set;
  }

  public static <Converted> AllInSet<Converted> from(
      final Set<?> referenceValues,
      final ConvertedColumnType<Converted> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AllInSet<>(asTypedSet(referenceValues, columnType), columnType, schemaPath);
  }
}
