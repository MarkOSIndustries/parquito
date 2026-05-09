package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.SchemaTraversalSpecs;
import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;
import java.util.stream.Stream;

/** Matches all rows without inspecting any columns */
public class MatchAll implements ParquetPredicate {
  @Override
  public boolean matchesNextRow() {
    return true;
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
