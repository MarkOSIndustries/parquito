package com.markosindustries.parquito.types;

import com.markosindustries.parquito.ParquetSchemaNode;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.SortingColumn;

public record ColumnType<ReadAs>(
    ParquetType<ReadAs> parquetType,
    ParquetSchemaNode schemaNode,
    SortingColumn sortingColumnHeader) {
  public static ColumnType<?> create(
      final ColumnChunk columnChunkHeader,
      final SortingColumn sortingColumnHeader,
      final ParquetSchemaNode.Root schema) {
    return create(
        columnChunkHeader,
        sortingColumnHeader,
        schema.getChild(columnChunkHeader.meta_data.path_in_schema));
  }

  public static ColumnType<?> create(
      final ColumnChunk columnChunkHeader,
      final SortingColumn sortingColumnHeader,
      final ParquetSchemaNode columnSchemaNode) {
    final LogicalType logicalType = columnSchemaNode.getLogicalType();
    final int typeLength = columnSchemaNode.getTypeLength();
    return new ColumnType<>(
        ParquetType.create(columnChunkHeader.meta_data.type, logicalType, typeLength),
        columnSchemaNode,
        sortingColumnHeader);
  }

  /**
   * We don't implement {@link java.util.Comparator} because we have no need of Serialization etc.
   *
   * @see java.util.Comparator
   * @param o1 Comparable left
   * @param o2 Comparable right
   * @return &lt;0 if o1&lt;o2, 0 if o1==o2, &gt;0 if o1&gt;o2
   */
  public int compare(final ReadAs o1, final ReadAs o2) {
    if (o1 == null) {
      if (o2 == null) {
        return 0;
      }
      return sortingColumnHeader.nulls_first ? -1 : 1;
    }
    if (o2 == null) {
      return sortingColumnHeader.nulls_first ? 1 : -1;
    }

    if (sortingColumnHeader.descending) {
      return parquetType.compare(o2, o1);
    } else {
      return parquetType.compare(o1, o2);
    }
  }
}
