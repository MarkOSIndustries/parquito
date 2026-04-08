package com.markosindustries.parquito;

public record RowReadSpec<Repeated, Value, ReadAs>(
    Reader<Repeated, Value> reader, ParquetPredicate<ReadAs> predicate, ColumnSpec columnSpec) {
  public RowReadSpec(Reader<Repeated, Value> reader) {
    this(reader, ParquetPredicates.all(), ColumnSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, ParquetPredicate<ReadAs> predicate) {
    this(reader, predicate, ColumnSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, ColumnSpec columnSpec) {
    this(reader, ParquetPredicates.all(), columnSpec);
  }

  public RowReadSpec<?, ?, ?> forChild(final int childFieldId) {
    return new RowReadSpec<>(
        reader.forChild(childFieldId),
        predicate.forChild(childFieldId),
        columnSpec.forChild(childFieldId));
  }

  public boolean includesChild(final int childFieldId) {
    return predicate.includesChild(childFieldId) || columnSpec.includesChild(childFieldId);
  }
}
