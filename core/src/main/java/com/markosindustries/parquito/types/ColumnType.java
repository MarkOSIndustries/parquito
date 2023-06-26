package com.markosindustries.parquito.types;

import com.markosindustries.parquito.ParquetSchemaNode;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.LogicalType;

public record ColumnType<ReadAs extends Comparable<ReadAs>>(
    ParquetType<ReadAs> parquetType, ParquetSchemaNode schemaNode) {
  public static ColumnType<?> create(
      final ColumnChunk columnChunkHeader, final ParquetSchemaNode.Root schema) {
    return create(columnChunkHeader, schema.getChild(columnChunkHeader.meta_data.path_in_schema));
  }

  public static ColumnType<?> create(
      final ColumnChunk columnChunkHeader, final ParquetSchemaNode columnSchemaNode) {
    final LogicalType logicalType = columnSchemaNode.getLogicalType();
    final int typeLength = columnSchemaNode.getTypeLength();
    return new ColumnType<>(
        ParquetType.create(columnChunkHeader.meta_data.type, logicalType, typeLength),
        columnSchemaNode);
  }
}
