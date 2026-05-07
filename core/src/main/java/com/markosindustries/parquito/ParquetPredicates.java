package com.markosindustries.parquito;

import com.markosindustries.parquito.types.ColumnType;
import java.util.Collection;

public class ParquetPredicates {
  static ParquetPredicate.All all() {
    return new ParquetPredicate.All();
  }

  static ParquetPredicate.None none() {
    return new ParquetPredicate.None();
  }

  public static ParquetPredicate union(ParquetPredicate... predicates) {
    return new ParquetPredicate.Union(predicates);
  }

  public static ParquetPredicate union(Collection<? extends ParquetPredicate> predicates) {
    return union(predicates.toArray(ParquetPredicate[]::new));
  }

  public static ParquetPredicate intersection(ParquetPredicate... predicates) {
    return new ParquetPredicate.Intersection(predicates);
  }

  public static ParquetPredicate intersection(Collection<? extends ParquetPredicate> predicates) {
    return intersection(predicates.toArray(ParquetPredicate[]::new));
  }

  public static ParquetPredicate not(ParquetPredicate predicate) {
    return new ParquetPredicate.Not(predicate);
  }

  public static <ReadAs> ParquetPredicate equals(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.Equals<>((ReadAs) comparator, columnType, schemaPath);
  }

  public static ParquetPredicate equals(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return equals(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate notEquals(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.NotEquals<>((ReadAs) comparator, columnType, schemaPath);
  }

  public static ParquetPredicate notEquals(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return notEquals(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate greaterThan(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.GreaterThan<>((ReadAs) comparator, columnType, schemaPath);
  }

  public static ParquetPredicate greaterThan(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return greaterThan(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate greaterThanOrEqual(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.GreaterThanOrEqual<>((ReadAs) comparator, columnType, schemaPath);
  }

  public static ParquetPredicate greaterThanOrEqual(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return greaterThanOrEqual(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate lessThan(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.LessThan<>((ReadAs) comparator, columnType, schemaPath);
  }

  public static ParquetPredicate lessThan(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return lessThan(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate lessThanOrEqual(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new ParquetPredicate.LessThanOrEqual<>((ReadAs) comparator, columnType, schemaPath);
  }

  public static ParquetPredicate lessThanOrEqual(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return lessThanOrEqual(comparator, columnType, schemaPath);
  }
}
