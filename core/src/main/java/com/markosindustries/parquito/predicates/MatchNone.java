package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.SchemaTraversalSpecs;
import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;
import java.util.stream.Stream;

/** Matches no rows without inspecting any columns */
public class MatchNone implements ParquetPredicate {
  @Override
  public boolean matchesNextRow() {
    return false;
  }

  @Override
  public SchemaTraversalSpec asSchemaTraversalSpec() {
    return SchemaTraversalSpecs.none();
  }

  @Override
  public Stream<ColumnPredicate<?, ?>> columnPredicates() {
    return Stream.empty();
  }
}
