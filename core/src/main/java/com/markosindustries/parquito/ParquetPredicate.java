package com.markosindustries.parquito;

import static com.markosindustries.parquito.ParquetPredicates.all;

import com.markosindustries.parquito.types.ColumnType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface ParquetPredicate<ReadAs> {
  Set<String> includedChildren();

  default boolean includesChild(final String child) {
    return includedChildren().contains(child);
  }

  ParquetPredicate<?> forChild(final String child);

  default boolean objectMatches(final Object value) {
    //noinspection unchecked
    return valueMatches((ReadAs) value);
  }

  boolean valueMatches(final ReadAs value);

  boolean branchMatches(final Function<String, Boolean> childMatchesNextRow);

  class All<ReadAs> implements ParquetPredicate<ReadAs> {
    @Override
    public Set<String> includedChildren() {
      return Collections.emptySet();
    }

    @Override
    public boolean includesChild(final String child) {
      return false;
    }

    @Override
    public ParquetPredicate<?> forChild(final String child) {
      return this;
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return true;
    }

    @Override
    public boolean branchMatches(final Function<String, Boolean> childMatchesNextRow) {
      return true;
    }
  }

  class Union implements ParquetPredicate<Object> {
    private final ParquetPredicate<?>[] predicates;
    private final Set<String> includedChildren;

    public Union(ParquetPredicate<?>... predicates) {
      this.predicates = predicates;
      this.includedChildren =
          Arrays.stream(predicates)
              .flatMap(p -> p.includedChildren().stream())
              .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<String> includedChildren() {
      return includedChildren;
    }

    @Override
    public ParquetPredicate<?> forChild(final String child) {
      final var childPredicates =
          Arrays.stream(predicates)
              .filter(predicates -> predicates.includesChild(child))
              .map(predicates -> predicates.forChild(child))
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
    public boolean branchMatches(final Function<String, Boolean> childMatchesNextRow) {
      for (final String child : includedChildren) {
        if (childMatchesNextRow.apply(child)) {
          return true;
        }
      }
      return includedChildren.isEmpty();
    }
  }

  class Intersection implements ParquetPredicate<Object> {
    private final ParquetPredicate<?>[] predicates;
    private final Set<String> includedChildren;

    public Intersection(ParquetPredicate<?>... predicates) {
      this.predicates = predicates;
      this.includedChildren =
          Arrays.stream(predicates)
              .flatMap(p -> p.includedChildren().stream())
              .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<String> includedChildren() {
      return includedChildren;
    }

    @Override
    public ParquetPredicate<?> forChild(final String child) {
      final var childPredicates =
          Arrays.stream(predicates)
              .filter(predicates -> predicates.includesChild(child))
              .map(predicates -> predicates.forChild(child))
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
    public boolean branchMatches(final Function<String, Boolean> childMatchesNextRow) {
      for (final String child : includedChildren) {
        if (!childMatchesNextRow.apply(child)) {
          return false;
        }
      }
      return true;
    }
  }

  abstract class Leaf<ReadAs, L extends Leaf<ReadAs, L>> implements ParquetPredicate<ReadAs> {
    private final ColumnType<ReadAs> columnType;
    private final LeafConstructor<ReadAs, L> constructor;
    private final ReadAs comparator;
    private final int offset;
    private final String[] path;
    private final Set<String> includedChildren;

    @FunctionalInterface
    interface LeafConstructor<ReadAs, L> {
      L construct(ReadAs comparator, ColumnType<ReadAs> columnType, String[] path, int offset);
    }

    protected Leaf(
        LeafConstructor<ReadAs, L> constructor,
        ReadAs comparator,
        ColumnType<ReadAs> columnType,
        String[] path,
        int offset) {
      this.constructor = constructor;
      this.comparator = comparator;
      this.columnType = columnType;
      this.offset = offset;
      this.path = path;
      this.includedChildren =
          path.length > offset ? Collections.singleton(path[offset]) : Collections.emptySet();
    }

    public Set<String> includedChildren() {
      return includedChildren;
    }

    @Override
    public ParquetPredicate<?> forChild(final String child) {
      if (!includesChild(child)) {
        return all();
      }
      if (path.length > offset) {
        return constructor.construct(comparator, columnType, path, offset + 1);
      }

      return all();
    }

    protected int compare(ReadAs value) {
      return columnType.compare(value, comparator);
    }

    @Override
    public boolean branchMatches(final Function<String, Boolean> childMatchesNextRow) {
      if (path.length > offset) {
        return childMatchesNextRow.apply(path[offset]);
      }
      return true;
    }
  }

  class Equals<ReadAs> extends Leaf<ReadAs, Equals<ReadAs>> {
    public Equals(
        final ReadAs comparator, final ColumnType<ReadAs> columnType, String[] path, int offset) {
      super(Equals::new, comparator, columnType, path, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) == 0;
    }
  }

  class GreaterThan<ReadAs> extends Leaf<ReadAs, GreaterThan<ReadAs>> {
    public GreaterThan(
        final ReadAs comparator, final ColumnType<ReadAs> columnType, String[] path, int offset) {
      super(GreaterThan::new, comparator, columnType, path, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) > 0;
    }
  }

  class GreaterThanOrEqual<ReadAs> extends Leaf<ReadAs, GreaterThanOrEqual<ReadAs>> {
    public GreaterThanOrEqual(
        final ReadAs comparator, final ColumnType<ReadAs> columnType, String[] path, int offset) {
      super(GreaterThanOrEqual::new, comparator, columnType, path, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) >= 0;
    }
  }

  class LessThan<ReadAs> extends Leaf<ReadAs, LessThan<ReadAs>> {
    public LessThan(
        final ReadAs comparator, final ColumnType<ReadAs> columnType, String[] path, int offset) {
      super(LessThan::new, comparator, columnType, path, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) < 0;
    }
  }

  class LessThanOrEqual<ReadAs> extends Leaf<ReadAs, LessThanOrEqual<ReadAs>> {
    public LessThanOrEqual(
        final ReadAs comparator, final ColumnType<ReadAs> columnType, String[] path, int offset) {
      super(LessThanOrEqual::new, comparator, columnType, path, offset);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) <= 0;
    }
  }
}
