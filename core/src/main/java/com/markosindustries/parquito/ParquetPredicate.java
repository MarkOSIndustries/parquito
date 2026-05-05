package com.markosindustries.parquito;

import static com.markosindustries.parquito.ParquetPredicates.all;

import com.markosindustries.parquito.types.ColumnType;
import java.util.Arrays;
import java.util.BitSet;
import java.util.function.IntPredicate;

public interface ParquetPredicate<ReadAs> {
  BitSet includedChildren();

  default boolean includesChild(final int childFieldIndex) {
    return includedChildren().get(childFieldIndex);
  }

  ParquetPredicate<?> forChild(final int childFieldIndex);

  default boolean objectMatches(final Object value) {
    //noinspection unchecked
    return valueMatches((ReadAs) value);
  }

  boolean valueMatches(final ReadAs value);

  boolean branchMatches(final IntPredicate childMatchesNextRow);

  default SchemaTraversalSpec asSchemaTraversalSpec() {
    final ParquetPredicate<ReadAs> self = this;
    return new SchemaTraversalSpec() {
      @Override
      public boolean includesChild(final int childFieldIndex) {
        return self.includesChild(childFieldIndex);
      }

      @Override
      public SchemaTraversalSpec forChild(final int childFieldIndex) {
        return self.forChild(childFieldIndex).asSchemaTraversalSpec();
      }
    };
  }

  class All<ReadAs> implements ParquetPredicate<ReadAs> {
    private static final BitSet EMPTY_BITSET = new BitSet();

    @Override
    public BitSet includedChildren() {
      return EMPTY_BITSET;
    }

    @Override
    public boolean includesChild(final int childFieldIndex) {
      return false;
    }

    @Override
    public ParquetPredicate<?> forChild(final int childFieldIndex) {
      return this;
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return true;
    }

    @Override
    public boolean branchMatches(final IntPredicate childMatchesNextRow) {
      return true;
    }
  }

  class Union implements ParquetPredicate<Object> {
    private final ParquetPredicate<?>[] predicates;
    private final BitSet includedChildren;

    public Union(ParquetPredicate<?>... predicates) {
      this.predicates = predicates;
      this.includedChildren = new BitSet();
      for (final var predicate : predicates) {
        includedChildren.or(predicate.includedChildren());
      }
    }

    @Override
    public BitSet includedChildren() {
      return includedChildren;
    }

    @Override
    public ParquetPredicate<?> forChild(final int childFieldIndex) {
      final var childPredicates =
          Arrays.stream(predicates)
              .filter(predicate -> predicate.includesChild(childFieldIndex))
              .map(predicate -> predicate.forChild(childFieldIndex))
              .toArray(ParquetPredicate[]::new);
      if (childPredicates.length > 0) {
        return new Union(childPredicates);
      }
      return all();
    }

    @Override
    public boolean valueMatches(final Object value) {
      for (final ParquetPredicate<?> predicate : predicates) {
        if (predicate.objectMatches(value)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean branchMatches(final IntPredicate childMatchesNextRow) {
      return includedChildren.stream().anyMatch(childMatchesNextRow) || includedChildren.isEmpty();
    }
  }

  class Intersection implements ParquetPredicate<Object> {
    private final ParquetPredicate<?>[] predicates;
    private final BitSet includedChildren;

    public Intersection(ParquetPredicate<?>... predicates) {
      this.predicates = predicates;
      this.includedChildren = new BitSet();
      for (final var predicate : predicates) {
        includedChildren.or(predicate.includedChildren());
      }
    }

    @Override
    public BitSet includedChildren() {
      return includedChildren;
    }

    @Override
    public ParquetPredicate<?> forChild(final int childFieldIndex) {
      final var childPredicates =
          Arrays.stream(predicates)
              .filter(predicate -> predicate.includesChild(childFieldIndex))
              .map(predicate -> predicate.forChild(childFieldIndex))
              .toArray(ParquetPredicate[]::new);
      if (childPredicates.length > 0) {
        return new Intersection(childPredicates);
      }
      return all();
    }

    @Override
    public boolean valueMatches(final Object value) {
      for (final ParquetPredicate<?> predicate : predicates) {
        if (!predicate.objectMatches(value)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean branchMatches(final IntPredicate childMatchesNextRow) {
      return includedChildren.stream().allMatch(childMatchesNextRow);
    }
  }

  class Not<ReadAs> implements ParquetPredicate<ReadAs> {
    private final ParquetPredicate<ReadAs> predicate;

    public Not(final ParquetPredicate<ReadAs> predicate) {
      this.predicate = predicate;
    }

    @Override
    public BitSet includedChildren() {
      return predicate.includedChildren();
    }

    @Override
    public ParquetPredicate<?> forChild(final int childFieldIndex) {
      return predicate.forChild(childFieldIndex);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return !predicate.valueMatches(value);
    }

    @Override
    public boolean branchMatches(final IntPredicate childMatchesNextRow) {
      return !predicate.branchMatches(childMatchesNextRow);
    }
  }

  abstract class Leaf<ReadAs, L extends Leaf<ReadAs, L>> implements ParquetPredicate<ReadAs> {
    private final ColumnType<ReadAs> columnType;
    private final LeafConstructor<ReadAs, L> constructor;
    private final ReadAs comparator;
    private final int offset;
    private final ParquetSchemaPath schemaPath;
    private final BitSet includedChildren;

    @FunctionalInterface
    interface LeafConstructor<ReadAs, L> {
      L construct(
          final ReadAs comparator,
          final ColumnType<ReadAs> columnType,
          final ParquetSchemaPath schemaPath,
          final int offset);
    }

    Leaf(
        final LeafConstructor<ReadAs, L> constructor,
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        final ParquetSchemaPath schemaPath,
        final int offset) {
      this.constructor = constructor;
      this.comparator = comparator;
      this.columnType = columnType;
      this.offset = offset;
      this.schemaPath = schemaPath;
      this.includedChildren = new BitSet();
      if (schemaPath.path.length > offset) {
        includedChildren.set(schemaPath.pathAsFieldIndices[offset]);
      }
    }

    public BitSet includedChildren() {
      return includedChildren;
    }

    @Override
    public ParquetPredicate<?> forChild(final int childFieldIndex) {
      if (!includesChild(childFieldIndex)) {
        return all();
      }
      if (schemaPath.path.length > offset) {
        return constructor.construct(comparator, columnType, schemaPath, offset + 1);
      }

      return all();
    }

    protected int compare(ReadAs value) {
      return columnType.compare(value, comparator);
    }

    @Override
    public boolean branchMatches(final IntPredicate childMatchesNextRow) {
      if (schemaPath.path.length > offset) {
        return childMatchesNextRow.test(schemaPath.pathAsFieldIndices[offset]);
      }
      return true;
    }
  }

  class Equals<ReadAs> extends Leaf<ReadAs, Equals<ReadAs>> {
    public Equals(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath,
        int offset) {
      super(Equals::new, comparator, columnType, schemaPath, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) == 0;
    }
  }

  class NotEquals<ReadAs> extends Leaf<ReadAs, NotEquals<ReadAs>> {
    public NotEquals(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath,
        int offset) {
      super(NotEquals::new, comparator, columnType, schemaPath, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) != 0;
    }
  }

  class GreaterThan<ReadAs> extends Leaf<ReadAs, GreaterThan<ReadAs>> {
    public GreaterThan(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath,
        int offset) {
      super(GreaterThan::new, comparator, columnType, schemaPath, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) > 0;
    }
  }

  class GreaterThanOrEqual<ReadAs> extends Leaf<ReadAs, GreaterThanOrEqual<ReadAs>> {
    public GreaterThanOrEqual(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath,
        int offset) {
      super(GreaterThanOrEqual::new, comparator, columnType, schemaPath, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) >= 0;
    }
  }

  class LessThan<ReadAs> extends Leaf<ReadAs, LessThan<ReadAs>> {
    public LessThan(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath,
        int offset) {
      super(LessThan::new, comparator, columnType, schemaPath, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) < 0;
    }
  }

  class LessThanOrEqual<ReadAs> extends Leaf<ReadAs, LessThanOrEqual<ReadAs>> {
    public LessThanOrEqual(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath,
        int offset) {
      super(LessThanOrEqual::new, comparator, columnType, schemaPath, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) <= 0;
    }
  }
}
