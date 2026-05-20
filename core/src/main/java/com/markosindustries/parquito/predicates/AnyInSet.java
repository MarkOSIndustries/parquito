package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.ParquetSchemaPath;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import java.util.Set;
import java.util.SortedSet;

/**
 * Matches a row if at least one value for the given column equals one of the values in the
 * referenceValues set
 *
 * @param <ReadAs> The type of value
 */
public class AnyInSet<ReadAs>
    extends ColumnPredicate<ReadAs, PredicateRowMatcher.AnyMatch<ReadAs>> {
  private final SortedSet<ReadAs> referenceValues;

  public AnyInSet(
      final Set<ReadAs> referenceValues,
      final ColumnType<ReadAs> columnType,
      ParquetSchemaPath schemaPath) {
    this(
        referenceValues instanceof SortedSet<ReadAs>
                && columnType
                    .getComparator()
                    .equals(((SortedSet<ReadAs>) referenceValues).comparator())
            ? (SortedSet<ReadAs>) referenceValues
            : asSortedSet(referenceValues, columnType),
        columnType,
        schemaPath);
  }

  private AnyInSet(
      SortedSet<ReadAs> referenceValues,
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

  private static <ReadAs> ObjectAVLTreeSet<ReadAs> asSortedSet(
      final Set<?> referenceValues, final ColumnType<ReadAs> columnType) {
    final var set = new ObjectAVLTreeSet<>(columnType.getComparator());
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
    return new AnyInSet<>(asSortedSet(referenceValues, columnType), columnType, schemaPath);
  }
}
