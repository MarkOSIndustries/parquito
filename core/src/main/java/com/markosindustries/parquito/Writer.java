package com.markosindustries.parquito;

import java.util.List;
import org.apache.parquet.format.SchemaElement;

public interface Writer<Value> {
  List<? extends SchemaElement> getRawSchema();

  ParquetSchemaNode.Root getSchemaRoot();

  interface WriteAccumulator {
    ColumnChunkWriter<?> getColumnChunkWriter(final ParquetSchemaPath parquetSchemaPath);

    void enterGroup(final ParquetSchemaNode parquetSchemaNode);

    void leaveGroup(final ParquetSchemaNode parquetSchemaNode);

    void nullGroup(final ParquetSchemaNode parquetSchemaNode);
  }

  interface Shredder<Value> {
    default void shredObject(Object value) {
      shred((Value) value);
    }

    void shred(Value value);

    void shredNull();
  }

  Shredder<Value> makeShredder(WriteAccumulator writeAccumulator);
}
