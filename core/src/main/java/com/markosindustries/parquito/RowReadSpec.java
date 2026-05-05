package com.markosindustries.parquito;

public record RowReadSpec<Repeated, Value, ReadAs>(
    Reader<Repeated, Value> reader,
    ParquetPredicate<ReadAs> predicate,
    SchemaTraversalSpec schemaTraversalSpec) {
  public RowReadSpec(Reader<Repeated, Value> reader) {
    this(reader, ParquetPredicates.all(), SchemaTraversalSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, ParquetPredicate<ReadAs> predicate) {
    this(reader, predicate, SchemaTraversalSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, SchemaTraversalSpec schemaTraversalSpec) {
    this(reader, ParquetPredicates.all(), schemaTraversalSpec);
  }

  public RowReadSpec<?, ?, ?> forChild(final int childFieldIndex) {
    return new RowReadSpec<>(
        reader.forChild(childFieldIndex),
        predicate.forChild(childFieldIndex),
        schemaTraversalSpec.forChild(childFieldIndex));
  }

  public boolean includesChild(final int childFieldIndex) {
    return predicate.includesChild(childFieldIndex)
        || schemaTraversalSpec.includesChild(childFieldIndex);
  }
}
