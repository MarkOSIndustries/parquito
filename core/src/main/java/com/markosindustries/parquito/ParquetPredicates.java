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
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.Equals<>((ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> equals(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return equals(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate<ReadAs> greaterThan(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.GreaterThan<>((ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> greaterThan(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return greaterThan(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate<ReadAs> greaterThanOrEqual(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.GreaterThanOrEqual<>(
        (ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> greaterThanOrEqual(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return greaterThanOrEqual(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate<ReadAs> lessThan(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.LessThan<>((ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> lessThan(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return lessThan(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate<ReadAs> lessThanOrEqual(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.LessThanOrEqual<>((ReadAs) comparator, columnType, schemaPath, 0);
  }

  public static ParquetPredicate<?> lessThanOrEqual(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return lessThanOrEqual(comparator, columnType, schemaPath);
  }
}
