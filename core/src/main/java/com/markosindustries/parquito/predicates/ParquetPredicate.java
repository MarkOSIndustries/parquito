package com.markosindustries.parquito.predicates;

import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;
import java.util.stream.Stream;

public interface ParquetPredicate {
  boolean matchesNextRow();

  SchemaTraversalSpec asSchemaTraversalSpec();

  Stream<ColumnPredicate<?, ?>> columnPredicates();
}
