package com.markosindustries.parquito;

import com.markosindustries.parquito.types.ColumnType;

public class ParquetPredicates {
  static <ReadAs> ParquetPredicate.All<ReadAs> all() {
    return new ParquetPredicate.All<>();
  }

  public static ParquetPredicate<?> union(ParquetPredicate<?>... predicates) {
    return new ParquetPredicate.Union(predicates);
  }

  public static ParquetPredicate<?> intersection(ParquetPredicate<?>... predicates) {
    return new ParquetPredicate.Intersection(predicates);
  }

  public static <ReadAs> ParquetPredicate<ReadAs> equals(
      Object comparator, ColumnType<ReadAs> columnType, String... schemaPath) {
    return new ParquetPredicate.Equals<>((ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> equals(
      RowGroupReader rowGroupReader, Object comparator, String... schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath);
    return equals(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate<ReadAs> greaterThan(
      Object comparator, ColumnType<ReadAs> columnType, String... schemaPath) {
    return new ParquetPredicate.GreaterThan<>((ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> greaterThan(
      RowGroupReader rowGroupReader, Object comparator, String... schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath);
    return greaterThan(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate<ReadAs> greaterThanOrEqual(
      Object comparator, ColumnType<ReadAs> columnType, String... schemaPath) {
    return new ParquetPredicate.GreaterThanOrEqual<>(
        (ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> greaterThanOrEqual(
      RowGroupReader rowGroupReader, Object comparator, String... schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath);
    return greaterThanOrEqual(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate<ReadAs> lessThan(
      Object comparator, ColumnType<ReadAs> columnType, String... schemaPath) {
    return new ParquetPredicate.LessThan<>((ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> lessThan(
      RowGroupReader rowGroupReader, Object comparator, String... schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath);
    return lessThan(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate<ReadAs> lessThanOrEqual(
      Object comparator, ColumnType<ReadAs> columnType, String... schemaPath) {
    return new ParquetPredicate.LessThanOrEqual<>((ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> lessThanOrEqual(
      RowGroupReader rowGroupReader, Object comparator, String... schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath);
    return lessThanOrEqual(comparator, columnType, schemaPath);
  }
}
