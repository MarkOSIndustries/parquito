package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;
import java.util.Arrays;
import java.util.stream.Stream;

public abstract class CompoundPredicate implements ParquetPredicate {
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
  public Stream<ColumnPredicate<?, ?>> columnPredicates() {
    return Arrays.stream(predicates).flatMap(ParquetPredicate::columnPredicates);
  }
}
