package com.markosindustries.parquito.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.markosindustries.parquito.ColumnChunkWriter;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Writer;
import java.nio.ByteBuffer;

public interface ProtobufLeafShredder<Value> extends Writer.Shredder<Value> {
  static ProtobufLeafShredder<?> forType(
      final Descriptors.FieldDescriptor fieldDescriptor,
      final ParquetSchemaNode parquetSchemaNode,
      final ColumnChunkWriter<?> columnChunkWriter) {
    // TODO - if we handle repeated/map in here, we can support legacy schemas for those (without
    // the prescribed multi-layer parquet schemas)
    return switch (fieldDescriptor.getType()) {
      case DOUBLE,
              FLOAT,
              INT64,
              UINT64,
              INT32,
              FIXED64,
              FIXED32,
              BOOL,
              STRING,
              UINT32,
              SFIXED32,
              SFIXED64,
              SINT32,
              SINT64 ->
          new ProtobufSimpleLeafShredder<>(columnChunkWriter, parquetSchemaNode);
      case BYTES -> new ProtobufBytesLeafShredder(columnChunkWriter, parquetSchemaNode);
      case ENUM ->
          switch (parquetSchemaNode.getElement().getType()) {
            case INT32 -> new ProtobufEnumAsInt32LeafShredder(columnChunkWriter, parquetSchemaNode);
            case BYTE_ARRAY ->
                new ProtobufEnumAsStringLeafShredder(columnChunkWriter, parquetSchemaNode);
            default ->
                throw new UnsupportedOperationException(
                    "Something probably went wrong in schema conversion - protobuf enums can only be written as int32 or string, but we got "
                        + parquetSchemaNode.getElement());
          };
      case MESSAGE, GROUP ->
          throw new IllegalArgumentException(
              "We shouldn't be shredding a message in a leaf shredder - "
                  + parquetSchemaNode.getElement());
    };
  }

  class ProtobufSimpleLeafShredder<Value> implements ProtobufLeafShredder<Value> {
    private final ColumnChunkWriter<Value> columnChunkWriter;
    private final ParquetSchemaNode schemaNode;

    public ProtobufSimpleLeafShredder(
        final ColumnChunkWriter<Value> columnChunkWriter, final ParquetSchemaNode schemaNode) {
      this.columnChunkWriter = columnChunkWriter;
      this.schemaNode = schemaNode;
    }

    @Override
    public void shred(final Value value) {
      columnChunkWriter.accumulateValue(value);
    }

    @Override
    public void shredNull() {
      columnChunkWriter.accumulateNull(schemaNode.getParent());
    }
  }

  class ProtobufEnumAsInt32LeafShredder
      implements ProtobufLeafShredder<Descriptors.EnumValueDescriptor> {
    private final ColumnChunkWriter<Integer> columnChunkWriter;
    private final ParquetSchemaNode schemaNode;

    public ProtobufEnumAsInt32LeafShredder(
        final ColumnChunkWriter<?> columnChunkWriter, final ParquetSchemaNode schemaNode) {
      //noinspection unchecked
      this.columnChunkWriter = (ColumnChunkWriter<Integer>) columnChunkWriter;
      this.schemaNode = schemaNode;
    }

    @Override
    public void shred(final Descriptors.EnumValueDescriptor value) {
      columnChunkWriter.accumulateValue(value.getNumber());
    }

    @Override
    public void shredNull() {
      columnChunkWriter.accumulateNull(schemaNode.getParent());
    }
  }

  class ProtobufEnumAsStringLeafShredder
      implements ProtobufLeafShredder<Descriptors.EnumValueDescriptor> {
    private final ColumnChunkWriter<String> columnChunkWriter;
    private final ParquetSchemaNode schemaNode;

    public ProtobufEnumAsStringLeafShredder(
        final ColumnChunkWriter<?> columnChunkWriter, final ParquetSchemaNode schemaNode) {
      //noinspection unchecked
      this.columnChunkWriter = (ColumnChunkWriter<String>) columnChunkWriter;
      this.schemaNode = schemaNode;
    }

    @Override
    public void shred(final Descriptors.EnumValueDescriptor value) {
      columnChunkWriter.accumulateValue(value.getName());
    }

    @Override
    public void shredNull() {
      columnChunkWriter.accumulateNull(schemaNode.getParent());
    }
  }

  class ProtobufBytesLeafShredder implements ProtobufLeafShredder<ByteString> {
    private final ColumnChunkWriter<ByteBuffer> columnChunkWriter;
    private final ParquetSchemaNode schemaNode;

    public ProtobufBytesLeafShredder(
        final ColumnChunkWriter<?> columnChunkWriter, final ParquetSchemaNode schemaNode) {
      //noinspection unchecked
      this.columnChunkWriter = (ColumnChunkWriter<ByteBuffer>) columnChunkWriter;
      this.schemaNode = schemaNode;
    }

    @Override
    public void shred(final ByteString bytes) {
      columnChunkWriter.accumulateValue(bytes.asReadOnlyByteBuffer());
    }

    @Override
    public void shredNull() {
      columnChunkWriter.accumulateNull(schemaNode.getParent());
    }
  }
}
