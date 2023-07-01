package com.markosindustries.parquito;

public record RowReadSpec<Repeated, Value>(
    Reader<Repeated, Value> reader, RowPredicate rowPredicate, ColumnSpec columnSpec) {
  public RowReadSpec(Reader<Repeated, Value> reader) {
    this(reader, RowPredicate.ALL, ColumnSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, RowPredicate rowPredicate) {
    this(reader, rowPredicate, ColumnSpecs.all());
  }

  public RowReadSpec(Reader<Repeated, Value> reader, ColumnSpec columnSpec) {
    this(reader, RowPredicate.ALL, columnSpec);
  }

  public RowReadSpec<?, ?> forChild(final String child) {
    return new RowReadSpec<>(
        reader.forChild(child), rowPredicate.forChild(child), columnSpec.forChild(child));
  }

  public boolean includesChild(final String child) {
    return rowPredicate.includesChild(child) || columnSpec.includesChild(child);
  }
}
