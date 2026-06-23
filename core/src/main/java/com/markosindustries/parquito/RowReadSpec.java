package com.markosindustries.parquito;

import com.markosindustries.parquito.predicates.ParquetPredicate;
import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;

public record RowReadSpec<Row>(
    Reader<Row> reader, ParquetPredicate predicate, SchemaTraversalSpec schemaTraversalSpec) {
  public RowReadSpec(Reader<Row> reader) {
    this(reader, ParquetPredicates.matchAll(), SchemaTraversalSpecs.all());
  }

  public RowReadSpec(Reader<Row> reader, ParquetPredicate predicate) {
    this(reader, predicate, SchemaTraversalSpecs.all());
  }

  public RowReadSpec(Reader<Row> reader, SchemaTraversalSpec schemaTraversalSpec) {
    this(reader, ParquetPredicates.matchAll(), schemaTraversalSpec);
  }
}
