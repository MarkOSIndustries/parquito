package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.MapEntry;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Writer;
import java.util.List;

public class ProtobufMapShredder implements Writer.Shredder<Object> {
  private final Writer.WriteAccumulator writeAccumulator;
  private final ParquetSchemaNode schemaNode;
  private final ParquetSchemaNode keyValueSchemaNode;
  private final ParquetSchemaNode keySchemaNode;
  private final ParquetSchemaNode valueSchemaNode;
  private final Descriptors.FieldDescriptor keyField;
  private final Descriptors.FieldDescriptor valueField;
  private final Writer.Shredder<?> keyShredder;
  private final Writer.Shredder<?> valueShredder;

  public ProtobufMapShredder(
      final Writer.WriteAccumulator writeAccumulator,
      final Descriptors.FieldDescriptor fieldDescriptor,
      final ParquetSchemaNode schemaNode) {
    this.writeAccumulator = writeAccumulator;
    this.schemaNode = schemaNode;
    this.keyValueSchemaNode = schemaNode.getChildAtIndex(0);
    this.keySchemaNode = keyValueSchemaNode.getChildAtIndex(0);
    this.valueSchemaNode = keyValueSchemaNode.getChildAtIndex(1);

    this.keyField = fieldDescriptor.getMessageType().getFields().get(0);
    this.valueField = fieldDescriptor.getMessageType().getFields().get(1);

    this.keyShredder =
        ProtobufShredder.determineAppropriateShredder(writeAccumulator, keyField, keySchemaNode);
    this.valueShredder =
        ProtobufShredder.determineAppropriateShredder(
            writeAccumulator, valueField, valueSchemaNode);
  }

  @Override
  public void shred(final Object map) {
    //noinspection unchecked
    @SuppressWarnings("rawtypes")
    final var mapEntries = (List<MapEntry>) map;
    if (mapEntries.isEmpty()) {
      shredNull();
    } else {
      writeAccumulator.enterGroup(schemaNode); // the actual MAP type field
      for (final var mapEntry : mapEntries) {
        writeAccumulator.enterGroup(keyValueSchemaNode); // the repeated group named "key_value"
        keyShredder.shredObject(mapEntry.getKey()); // the key element
        valueShredder.shredObject(mapEntry.getValue()); // the value element
        writeAccumulator.leaveGroup(keyValueSchemaNode);
      }
      writeAccumulator.leaveGroup(schemaNode);
    }
  }

  @Override
  public void shredNull() {
    writeAccumulator.nullGroup(schemaNode);
  }
}
