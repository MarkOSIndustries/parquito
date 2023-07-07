package com.markosindustries.parquito;

import java.util.List;

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

  public RowReadSpec<?, ?, ?> forChild(final String child) {
    return new RowReadSpec<>(
        reader.forChild(child), predicate.forChild(child), columnSpec.forChild(child));
  }

  public boolean includesChild(final String child) {
    return predicate.includesChild(child) || columnSpec.includesChild(child);
  }

  public boolean rowPredicateIncludesPath(final List<String> path) {
    ParquetPredicate<?> currentPredicate = predicate;
    for (final String child : path) {
      if (currentPredicate.includesChild(child)) {
        currentPredicate = currentPredicate.forChild(child);
      } else {
        return false;
      }
    }
    return true;
  }

  public boolean columnSpecIncludesPath(final List<String> path) {
    ColumnSpec currentColumnSpec = columnSpec;
    for (final String child : path) {
      if (currentColumnSpec.includesChild(child)) {
        currentColumnSpec = currentColumnSpec.forChild(child);
      } else {
        return false;
      }
    }
    return true;
  }
}
