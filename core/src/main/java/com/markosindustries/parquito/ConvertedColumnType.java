package com.markosindustries.parquito;

import com.markosindustries.parquito.predicates.ValuesComparer;
import com.markosindustries.parquito.predicates.ValuesPredicate;
import com.markosindustries.parquito.types.LogicalTypeConverter;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.SortingColumn;

public record ConvertedColumnType<T>(
    ColumnType columnType, LogicalTypeConverter<T> logicalTypeConverter) {
  public static <T> ConvertedColumnType<T> create(
      final ColumnMetaData columnMetaData,
      final SortingColumn sortingColumnHeader,
      final ParquetSchemaNode.Root schema,
      final LogicalTypeConverter<T> logicalTypeConverter) {
    return create(
        sortingColumnHeader,
        schema.getChild(schema.parsePathElements(columnMetaData.path_in_schema)),
        logicalTypeConverter);
  }

  public static <T> ConvertedColumnType<T> create(
      final SortingColumn sortingColumnHeader,
      final ParquetSchemaNode columnSchemaNode,
      final LogicalTypeConverter<T> logicalTypeConverter) {
    return new ConvertedColumnType<>(
        ColumnType.create(sortingColumnHeader, columnSchemaNode), logicalTypeConverter);
  }

  public int compareNull(final T right) {
    if (right == null) {
      return 0;
    }
    return columnType.sortingColumnHeader().nulls_first ? -1 : 1;
  }

  public ValuesComparer makeValuesComparer(final T right) {
    return switch (columnType.getType()) {
      case BOOLEAN -> {
        final var rightConverted = logicalTypeConverter.toBoolean(right);
        yield (values, index) -> columnType.compare(values.getBoolean(index), rightConverted);
      }
      case INT32 -> {
        final var rightConverted = logicalTypeConverter.toInt32(right);
        yield (values, index) -> columnType.compare(values.getInt32(index), rightConverted);
      }
      case INT64 -> {
        final var rightConverted = logicalTypeConverter.toInt64(right);
        yield (values, index) -> columnType.compare(values.getInt64(index), rightConverted);
      }
      case INT96 -> throw new UnsupportedOperationException("We don't currently support Int96");
      case FLOAT -> {
        final var rightConverted = logicalTypeConverter.toFloat(right);
        yield (values, index) -> columnType.compare(values.getFloat(index), rightConverted);
      }
      case DOUBLE -> {
        final var rightConverted = logicalTypeConverter.toDouble(right);
        yield (values, index) -> columnType.compare(values.getDouble(index), rightConverted);
      }
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
        final var rightConverted = logicalTypeConverter.toByteBuffer(right);
        yield (values, index) -> columnType.compare(values.getByteBuffer(index), rightConverted);
      }
    };
  }

  public ValuesPredicate makeValuesEqualityPredicate(final T right) {
    return switch (columnType.getType()) {
      case BOOLEAN -> {
        final var rightConverted = logicalTypeConverter.toBoolean(right);
        yield (values, index) -> values.getBoolean(index) == rightConverted;
      }
      case INT32 -> {
        final var rightConverted = logicalTypeConverter.toInt32(right);
        yield (values, index) -> values.getInt32(index) == rightConverted;
      }
      case INT64 -> {
        final var rightConverted = logicalTypeConverter.toInt64(right);
        yield (values, index) -> values.getInt64(index) == rightConverted;
      }
      case INT96 -> throw new UnsupportedOperationException("We don't currently support Int96");
      case FLOAT -> {
        final var rightConverted = logicalTypeConverter.toFloat(right);
        yield (values, index) -> values.getFloat(index) == rightConverted;
      }
      case DOUBLE -> {
        final var rightConverted = logicalTypeConverter.toDouble(right);
        yield (values, index) -> values.getDouble(index) == rightConverted;
      }
      case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
        final var rightConverted = logicalTypeConverter.toByteBuffer(right);
        yield (values, index) -> values.getByteBuffer(index).equals(rightConverted);
      }
    };
  }
}
