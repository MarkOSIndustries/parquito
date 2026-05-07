package com.markosindustries.parquito;

import com.markosindustries.parquito.rows.DataPageCursor;
import com.markosindustries.parquito.rows.PredicateRowMatcher;
import com.markosindustries.parquito.types.ColumnType;
import java.util.Arrays;
import java.util.stream.Stream;

public interface ParquetPredicate {
  boolean matchesNextRow();

  SchemaTraversalSpec asSchemaTraversalSpec();

  Stream<Leaf<?>> leaves();

  class All implements ParquetPredicate {
    @Override
    public boolean matchesNextRow() {
      return true;
    }

    @Override
    public SchemaTraversalSpec asSchemaTraversalSpec() {
      return SchemaTraversalSpecs.none();
    }

    @Override
    public Stream<Leaf<?>> leaves() {
      return Stream.empty();
    }
  }

  class None implements ParquetPredicate {
    @Override
    public boolean matchesNextRow() {
      return false;
    }

    @Override
    public SchemaTraversalSpec asSchemaTraversalSpec() {
      return SchemaTraversalSpecs.none();
    }

    @Override
    public Stream<Leaf<?>> leaves() {
      return Stream.empty();
    }
  }

  abstract class CompoundPredicate implements ParquetPredicate {
    protected final ParquetPredicate[] predicates;

    public CompoundPredicate(ParquetPredicate... predicates) {
      this.predicates = predicates;
    }

    @Override
    public SchemaTraversalSpec asSchemaTraversalSpec() {
      return asSchemaTraversalSpec(
          Arrays.stream(predicates)
              .map(ParquetPredicate::asSchemaTraversalSpec)
              .toArray(SchemaTraversalSpec[]::new));
    }

    private SchemaTraversalSpec asSchemaTraversalSpec(final SchemaTraversalSpec[] childSpecs) {
      return new SchemaTraversalSpec() {
        @Override
        public boolean includesChild(final int childFieldIndex) {
          return Arrays.stream(childSpecs)
              .anyMatch(childSpec -> childSpec.includesChild(childFieldIndex));
        }

        @Override
        public SchemaTraversalSpec forChild(final int childFieldIndex) {
          return asSchemaTraversalSpec(
              Arrays.stream(childSpecs)
                  .filter(childSpec -> childSpec.includesChild(childFieldIndex))
                  .map(childSpec -> childSpec.forChild(childFieldIndex))
                  .toArray(SchemaTraversalSpec[]::new));
        }
      };
    }

    @Override
    public Stream<Leaf<?>> leaves() {
      return Arrays.stream(predicates).flatMap(ParquetPredicate::leaves);
    }
  }

  class Union extends CompoundPredicate {
    public Union(ParquetPredicate... predicates) {
      super(predicates);
    }

    @Override
    public boolean matchesNextRow() {
      for (final ParquetPredicate predicate : predicates) {
        if (predicate.matchesNextRow()) {
          return true;
        }
      }
      return false;
    }
  }

  class Intersection extends CompoundPredicate {
    public Intersection(ParquetPredicate... predicates) {
      super(predicates);
    }

    @Override
    public boolean matchesNextRow() {
      for (final ParquetPredicate predicate : predicates) {
        if (!predicate.matchesNextRow()) {
          return false;
        }
      }
      return true;
    }
  }

  class Not extends CompoundPredicate {
    private final ParquetPredicate predicate;

    public Not(final ParquetPredicate predicate) {
      super(predicate);
      this.predicate = predicate;
    }

    @Override
    public boolean matchesNextRow() {
      return !predicate.matchesNextRow();
    }
  }

  // TODO 3/2 different kinds of leaf now - Any/All/None (for repeateds)
  abstract class Leaf<ReadAs> implements ParquetPredicate {
    private final ColumnType<ReadAs> columnType;
    private final ReadAs comparator;
    private final ParquetSchemaPath schemaPath;
    private PredicateRowMatcher rowMatcher;

    Leaf(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        final ParquetSchemaPath schemaPath) {
      this.comparator = comparator;
      this.columnType = columnType;
      this.schemaPath = schemaPath;
    }

    public SchemaTraversalSpec asSchemaTraversalSpec() {
      return asSchemaTraversalSpec(0);
    }

    private SchemaTraversalSpec asSchemaTraversalSpec(final int offset) {
      return new SchemaTraversalSpec() {
        @Override
        public boolean includesChild(final int childFieldIndex) {
          return schemaPath.path.length > offset
              && schemaPath.pathAsFieldIndices[offset] == childFieldIndex;
        }

        @Override
        public SchemaTraversalSpec forChild(final int childFieldIndex) {
          return asSchemaTraversalSpec(offset + 1);
        }
      };
    }

    public ParquetSchemaPath getSchemaPath() {
      return schemaPath;
    }

    protected int compare(ReadAs value) {
      return columnType.compare(value, comparator);
    }

    public abstract boolean valueMatches(final ReadAs value);

    @Override
    public boolean matchesNextRow() {
      if (rowMatcher == null) {
        throw new RuntimeException("Something went wrong");
      }
      return rowMatcher.rowMatches();
    }

    @Override
    public Stream<Leaf<?>> leaves() {
      return Stream.of(this);
    }

    // TODO - can we split this visit stuff off into a separate class so predicate definition
    // doesn't really know mechanics of visiting data pages?
    public void newPageUnsafe(final DataPageCursor<?> dataPageCursor) {
      //noinspection unchecked
      newPage((DataPageCursor<ReadAs>) dataPageCursor);
    }

    public void newPage(final DataPageCursor<ReadAs> dataPageCursor) {
      final var materialisedMatches = dataPageCursor.getDataPage().getValues().materialise(this);
      final var matchesNull = valueMatches(null);
      this.rowMatcher =
          new PredicateRowMatcher.AnyMatch<>(dataPageCursor, materialisedMatches, matchesNull);
    }
  }

  class Equals<ReadAs> extends Leaf<ReadAs> {
    public Equals(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath) {
      super(comparator, columnType, schemaPath);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) == 0;
    }
  }

  class NotEquals<ReadAs> extends Leaf<ReadAs> {
    public NotEquals(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath) {
      super(comparator, columnType, schemaPath);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) != 0;
    }
  }

  class GreaterThan<ReadAs> extends Leaf<ReadAs> {
    public GreaterThan(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath) {
      super(comparator, columnType, schemaPath);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) > 0;
    }
  }

  class GreaterThanOrEqual<ReadAs> extends Leaf<ReadAs> {
    public GreaterThanOrEqual(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath) {
      super(comparator, columnType, schemaPath);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) >= 0;
    }
  }

  class LessThan<ReadAs> extends Leaf<ReadAs> {
    public LessThan(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath) {
      super(comparator, columnType, schemaPath);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) < 0;
    }
  }

  class LessThanOrEqual<ReadAs> extends Leaf<ReadAs> {
    public LessThanOrEqual(
        final ReadAs comparator,
        final ColumnType<ReadAs> columnType,
        ParquetSchemaPath schemaPath) {
      super(comparator, columnType, schemaPath);
    }

    @Override
    public boolean valueMatches(final ReadAs value) {
      return compare(value) <= 0;
    }
  }
}
