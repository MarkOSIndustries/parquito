package com.markosindustries.parquito;

import com.markosindustries.parquito.types.Comparer;
import java.nio.ByteBuffer;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.SortingColumn;
import org.apache.parquet.format.Type;

public record ColumnType(
    Comparer comparer, ParquetSchemaNode schemaNode, SortingColumn sortingColumnHeader) {
  public static ColumnType create(
      final ColumnMetaData columnMetaData,
      final SortingColumn sortingColumnHeader,
      final ParquetSchemaNode.Root schema) {
    return create(
        sortingColumnHeader,
        schema.getChild(schema.parsePathElements(columnMetaData.path_in_schema)));
  }

  public static ColumnType create(
      final SortingColumn sortingColumnHeader, final ParquetSchemaNode columnSchemaNode) {
    return new ColumnType(
        Comparer.comparerFor(columnSchemaNode.getLogicalType()),
        columnSchemaNode,
        sortingColumnHeader);
  }

  public Type getType() {
    return schemaNode.getElement().getType();
  }

  public int compare(final boolean o1, final boolean o2) {
    if (sortingColumnHeader.descending) {
      return comparer.compareBoolean(o2, o1);
    } else {
      return comparer.compareBoolean(o1, o2);
    }
  }

  public int compare(final ByteBuffer o1, final ByteBuffer o2) {
    if (sortingColumnHeader.descending) {
      return comparer.compareByteBuffer(o2, o1);
    } else {
      return comparer.compareByteBuffer(o1, o2);
    }
  }

  public int compare(final double o1, final double o2) {
    if (sortingColumnHeader.descending) {
      return comparer.compareDouble(o2, o1);
    } else {
      return comparer.compareDouble(o1, o2);
    }
  }

  public int compare(final float o1, final float o2) {
    if (sortingColumnHeader.descending) {
      return comparer.compareFloat(o2, o1);
    } else {
      return comparer.compareFloat(o1, o2);
    }
  }

  public int compare(final int o1, final int o2) {
    if (sortingColumnHeader.descending) {
      return comparer.compareInt32(o2, o1);
    } else {
      return comparer.compareInt32(o1, o2);
    }
  }

  public int compare(final long o1, final long o2) {
    if (sortingColumnHeader.descending) {
      return comparer.compareInt64(o2, o1);
    } else {
      return comparer.compareInt64(o1, o2);
    }
  }
}
