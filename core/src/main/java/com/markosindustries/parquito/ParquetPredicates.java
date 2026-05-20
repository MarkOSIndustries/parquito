package com.markosindustries.parquito;

import com.markosindustries.parquito.predicates.AllEquals;
import com.markosindustries.parquito.predicates.AllInSet;
import com.markosindustries.parquito.predicates.AnyEquals;
import com.markosindustries.parquito.predicates.AnyGreaterThan;
import com.markosindustries.parquito.predicates.AnyGreaterThanOrEqual;
import com.markosindustries.parquito.predicates.AnyInSet;
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
import java.util.Set;

public class ParquetPredicates {
  public static MatchAll matchAll() {
    return new MatchAll();
  }

  public static MatchNone matchNone() {
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
      final Object referenceValue,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AllEquals<>(
        columnType.parquetType().getReadAsClass().cast(referenceValue), columnType, schemaPath);
  }

  public static ParquetPredicate allEquals(
      final RowGroupReader rowGroupReader,
      final Object referenceValue,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return allEquals(referenceValue, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyEquals(
      final Object referenceValue,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyEquals<>(
        columnType.parquetType().getReadAsClass().cast(referenceValue), columnType, schemaPath);
  }

  public static ParquetPredicate anyEquals(
      final RowGroupReader rowGroupReader,
      final Object referenceValue,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyEquals(referenceValue, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyNotEquals(
      final Object referenceValue,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyNotEquals<>(
        columnType.parquetType().getReadAsClass().cast(referenceValue), columnType, schemaPath);
  }

  public static ParquetPredicate anyNotEquals(
      final RowGroupReader rowGroupReader,
      final Object referenceValue,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyNotEquals(referenceValue, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate noneEquals(
      final Object referenceValue,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new NoneEquals<>(
        columnType.parquetType().getReadAsClass().cast(referenceValue), columnType, schemaPath);
  }

  public static ParquetPredicate noneEquals(
      final RowGroupReader rowGroupReader,
      final Object referenceValue,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return noneEquals(referenceValue, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyGreaterThan(
      final Object referenceValue,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyGreaterThan<>(
        columnType.parquetType().getReadAsClass().cast(referenceValue), columnType, schemaPath);
  }

  public static ParquetPredicate anyGreaterThan(
      final RowGroupReader rowGroupReader,
      final Object referenceValue,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyGreaterThan(referenceValue, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyGreaterThanOrEqual(
      final Object referenceValue,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyGreaterThanOrEqual<>(
        columnType.parquetType().getReadAsClass().cast(referenceValue), columnType, schemaPath);
  }

  public static ParquetPredicate anyGreaterThanOrEqual(
      final RowGroupReader rowGroupReader,
      final Object referenceValue,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyGreaterThanOrEqual(referenceValue, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyLessThan(
      final Object referenceValue,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyLessThan<>(
        columnType.parquetType().getReadAsClass().cast(referenceValue), columnType, schemaPath);
  }

  public static ParquetPredicate anyLessThan(
      final RowGroupReader rowGroupReader,
      final Object referenceValue,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyLessThan(referenceValue, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyLessThanOrEqual(
      final Object referenceValue,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return new AnyLessThanOrEqual<>(
        columnType.parquetType().getReadAsClass().cast(referenceValue), columnType, schemaPath);
  }

  public static ParquetPredicate anyLessThanOrEqual(
      final RowGroupReader rowGroupReader,
      final Object referenceValue,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyLessThanOrEqual(referenceValue, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate anyInSet(
      final Set<?> referenceValues,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return AnyInSet.from(referenceValues, columnType, schemaPath);
  }

  public static ParquetPredicate anyInSet(
      final RowGroupReader rowGroupReader,
      final Set<?> referenceValues,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return anyInSet(referenceValues, columnType, schemaPath);
  }

  public static <ReadAs> ParquetPredicate allInSet(
      final Set<?> referenceValues,
      final ColumnType<ReadAs> columnType,
      final ParquetSchemaPath schemaPath) {
    return AllInSet.from(referenceValues, columnType, schemaPath);
  }

  public static ParquetPredicate allInSet(
      final RowGroupReader rowGroupReader,
      final Set<?> referenceValues,
      final ParquetSchemaPath schemaPath) {
    final var columnType = rowGroupReader.getColumnType(schemaPath).orElseThrow();
    return allInSet(referenceValues, columnType, schemaPath);
  }
}
