package com.markosindustries.parquito;

import com.markosindustries.parquito.predicates.ParquetPredicate;
import com.markosindustries.parquito.schematraversal.SchemaTraversalSpec;

public record RowReadSpec<Repeated, Value>(
    Reader<Repeated, Value> reader,
    ParquetPredicate predicate,
    SchemaTraversalSpec schemaTraversalSpec) {
  public RowReadSpec(Reader<Repeated, Value> reader) {
    this(reader, ParquetPredicates.matchAll(), SchemaTraversalSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, ParquetPredicate predicate) {
    this(reader, predicate, SchemaTraversalSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, SchemaTraversalSpec schemaTraversalSpec) {
    this(reader, ParquetPredicates.matchAll(), schemaTraversalSpec);
  }
}
