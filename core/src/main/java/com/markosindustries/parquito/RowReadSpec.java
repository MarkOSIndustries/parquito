package com.markosindustries.parquito;

public record RowReadSpec<Repeated, Value>(
    Reader<Repeated, Value> reader,
    ParquetPredicate predicate,
    SchemaTraversalSpec schemaTraversalSpec) {
  public RowReadSpec(Reader<Repeated, Value> reader) {
    this(reader, ParquetPredicates.all(), SchemaTraversalSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, ParquetPredicate predicate) {
    this(reader, predicate, SchemaTraversalSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, SchemaTraversalSpec schemaTraversalSpec) {
    this(reader, ParquetPredicates.all(), schemaTraversalSpec);
  }
}
