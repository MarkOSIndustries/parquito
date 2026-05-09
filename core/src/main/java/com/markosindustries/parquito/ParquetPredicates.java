package com.markosindustries.parquito;

import com.markosindustries.parquito.predicates.AllEquals;
import com.markosindustries.parquito.predicates.AnyEquals;
import com.markosindustries.parquito.predicates.AnyGreaterThan;
import com.markosindustries.parquito.predicates.AnyGreaterThanOrEqual;
import com.markosindustries.parquito.predicates.AnyLessThan;
import com.markosindustries.parquito.predicates.AnyLessThanOrEqual;
import com.markosindustries.parquito.predicates.AnyNotEquals;
import com.markosindustries.parquito.predicates.Intersection;
import com.markosindustries.parquito.predicates.MatchAll;
import com.markosindustries.parquito.predicates.MatchNone;
import com.markosindustries.parquito.predicates.NoneEquals;
import com.markosindustries.parquito.predicates.Not;
import com.markosindustries.parquito.predicates.ParquetPredicate;
import com.markosindustries.parquito.predicates.Union;
import com.markosindustries.parquito.types.ColumnType;
import java.util.Collection;

public class ParquetPredicates {
  static MatchAll matchAll() {
    return new MatchAll();
  }

  static MatchNone matchNone() {
    return new MatchNone();
  }

  public static ParquetPredicate union(ParquetPredicate... predicates) {
    return new Union(predicates);
  }

  public static ParquetPredicate union(Collection<? extends ParquetPredicate> predicates) {
    return union(predicates.toArray(ParquetPredicate[]::new));
  }

  public static ParquetPredicate intersection(ParquetPredicate... predicates) {
    return new Intersection(predicates);
  }

  public static ParquetPredicate intersection(Collection<? extends ParquetPredicate> predicates) {
    return intersection(predicates.toArray(ParquetPredicate[]::new));
  }

  public static ParquetPredicate not(ParquetPredicate predicate) {
    return new Not(predicate);
  }

  public static <ReadAs> ParquetPredicate allEquals(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AllEquals<>(
        columnType.parquetType().getReadAsClass().cast(comparator), columnType, schemaPath);
  }

  public static ParquetPredicate allEquals(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return allEquals(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyEquals(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyEquals<>(
        columnType.parquetType().getReadAsClass().cast(comparator), columnType, schemaPath);
  }

  public static ParquetPredicate anyEquals(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyEquals(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyNotEquals(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyNotEquals<>(
        columnType.parquetType().getReadAsClass().cast(comparator), columnType, schemaPath);
  }

  public static ParquetPredicate anyNotEquals(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyNotEquals(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate noneEquals(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new NoneEquals<>(
        columnType.parquetType().getReadAsClass().cast(comparator), columnType, schemaPath);
  }

  public static ParquetPredicate noneEquals(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return noneEquals(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyGreaterThan(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyGreaterThan<>(
        columnType.parquetType().getReadAsClass().cast(comparator), columnType, schemaPath);
  }

  public static ParquetPredicate anyGreaterThan(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyGreaterThan(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyGreaterThanOrEqual(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyGreaterThanOrEqual<>(
        columnType.parquetType().getReadAsClass().cast(comparator), columnType, schemaPath);
  }

  public static ParquetPredicate anyGreaterThanOrEqual(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyGreaterThanOrEqual(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyLessThan(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyLessThan<>(
        columnType.parquetType().getReadAsClass().cast(comparator), columnType, schemaPath);
  }

  public static ParquetPredicate anyLessThan(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyLessThan(comparator, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyLessThanOrEqual(
      final Object comparator,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyLessThanOrEqual<>(
        columnType.parquetType().getReadAsClass().cast(comparator), columnType, schemaPath);
  }

  public static ParquetPredicate anyLessThanOrEqual(
      final RowGroupReader rowGroupReader,
      final Object comparator,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyLessThanOrEqual(comparator, columnType, schemaPath);
  }
}
