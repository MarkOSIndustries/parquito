package com.markosindustries.parquito;

import com.markosindustries.parquito.types.LogicalTypeConverter;
import java.nio.ByteBuffer;
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

  public int compare(final boolean o1, final T o2) {
    return columnType.compare(o1, logicalTypeConverter.toBoolean(o2));
  }

  public int compare(final ByteBuffer o1, final T o2) {
    return columnType.compare(o1, logicalTypeConverter.toByteBuffer(o2));
  }

  public int compare(final double o1, final T o2) {
    return columnType.compare(o1, logicalTypeConverter.toFloat(o2));
  }

  public int compare(final float o1, final T o2) {
    return columnType.compare(o1, logicalTypeConverter.toDouble(o2));
  }

  public int compare(final int o1, final T o2) {
    return columnType.compare(o1, logicalTypeConverter.toInt32(o2));
  }

  public int compare(final long o1, final T o2) {
    return columnType.compare(o1, logicalTypeConverter.toInt64(o2));
  }

  public int compareNull(final T o2) {
    if (o2 == null) {
      return 0;
    }
    return columnType.sortingColumnHeader().nulls_first ? -1 : 1;
  }
}
