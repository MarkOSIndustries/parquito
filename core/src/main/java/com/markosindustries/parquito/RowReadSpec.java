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

  public RowReadSpec<?, ?, ?> forChild(final int childFieldIndex) {
    return new RowReadSpec<>(
        reader.forChild(childFieldIndex),
        predicate.forChild(childFieldIndex),
        columnSpec.forChild(childFieldIndex));
  }

  public boolean includesChild(final int childFieldIndex) {
    return predicate.includesChild(childFieldIndex) || columnSpec.includesChild(childFieldIndex);
  }
}
