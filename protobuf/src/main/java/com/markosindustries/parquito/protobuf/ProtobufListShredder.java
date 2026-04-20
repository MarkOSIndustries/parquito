package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Writer;
import java.util.List;

public class ProtobufListShredder implements Writer.Shredder<List<Object>> {
  private final Writer.WriteAccumulator writeAccumulator;
  private final ParquetSchemaNode schemaNode;
  private final ParquetSchemaNode listSchemaNode;
  private final Writer.Shredder<?> elementShredder;

  public ProtobufListShredder(
      final Writer.WriteAccumulator writeAccumulator,
      final Descriptors.FieldDescriptor field,
      final ParquetSchemaNode schemaNode) {
    this.writeAccumulator = writeAccumulator;
    this.schemaNode = schemaNode;
    this.listSchemaNode = schemaNode.getChildAtIndex(0);
    final var elementSchemaNode = listSchemaNode.getChildAtIndex(0);
    this.elementShredder =
        ProtobufShredder.determineAppropriateShredder(writeAccumulator, field, elementSchemaNode);
  }

  @Override
  public void shred(final List<Object> repeated) {
    writeAccumulator.enterGroup(schemaNode); // the actual LIST type field
    for (final var value : repeated) {
      writeAccumulator.enterGroup(listSchemaNode); // the repeated group named "list"
      elementShredder.shredObject(value);
      writeAccumulator.leaveGroup(listSchemaNode);
    }
    writeAccumulator.leaveGroup(schemaNode);
  }

  @Override
  public void shredNull() {
    writeAccumulator.nullGroup(schemaNode);
  }
}
