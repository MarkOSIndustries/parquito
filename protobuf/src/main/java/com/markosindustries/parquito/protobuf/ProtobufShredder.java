package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.NoOpShredder;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Writer;
import java.util.Arrays;
import org.apache.parquet.format.ConvertedType;

public class ProtobufShredder<Value extends Message> implements Writer.Shredder<Value> {
  private final Writer.Shredder<?>[] fieldShreddersByChildIndex;
  private final Descriptors.FieldDescriptor[] fieldsByChildIndex;
  private final ParquetSchemaNode parquetSchemaNode;
  private final Writer.WriteAccumulator writeAccumulator;

  public ProtobufShredder(
      final Descriptors.Descriptor descriptor,
      final ParquetSchemaNode parquetSchemaNode,
      final Writer.WriteAccumulator writeAccumulator) {
    this.fieldsByChildIndex =
        new Descriptors.FieldDescriptor[parquetSchemaNode.getChildren().length];
    this.fieldShreddersByChildIndex =
        new Writer.Shredder<?>[parquetSchemaNode.getChildren().length];
    this.parquetSchemaNode = parquetSchemaNode;
    this.writeAccumulator = writeAccumulator;
    // Prefill with NoOpReader so that we gracefully handle
    // fields which aren't in the parquet schema
    Arrays.fill(this.fieldShreddersByChildIndex, NoOpShredder.INSTANCE);
    for (final var field : descriptor.getFields()) {
      final var maybeChildIndex = parquetSchemaNode.findIndexOfChildByName(field.getName());
      maybeChildIndex.ifPresent(
          childIndex -> {
            fieldsByChildIndex[childIndex] = field;
            fieldShreddersByChildIndex[childIndex] =
                determineAppropriateShredder(
                    writeAccumulator, field, parquetSchemaNode.getChildAtIndex(childIndex));
          });
    }
  }

  static Writer.Shredder<?> determineAppropriateShredder(
      final Writer.WriteAccumulator writeAccumulator,
      final Descriptors.FieldDescriptor field,
      final ParquetSchemaNode childSchemaNode) {
    final var isMessage = field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE;
    if (field.isRepeated() && childSchemaNode.getConvertedType() == ConvertedType.LIST) {
      // Repeated but not LIST would imply legacy style without the 3 layer list structure
      return new ProtobufListShredder(writeAccumulator, field, childSchemaNode);
    } else if (field.isMapField() && childSchemaNode.getConvertedType() == ConvertedType.MAP) {
      // MapField but not MAP would imply legacy style without the 3 layer map structure
      return new ProtobufMapShredder(writeAccumulator, field, childSchemaNode);
    } else if (isMessage) {
      return new ProtobufShredder<>(field.getMessageType(), childSchemaNode, writeAccumulator);
    } else {
      return ProtobufLeafShredder.forType(
          field, childSchemaNode, writeAccumulator.getColumnChunkWriter(childSchemaNode.getPath()));
    }
  }

  @Override
  public void shred(final Value value) {
    writeAccumulator.enterGroup(parquetSchemaNode);
    for (var childIndex = 0; childIndex < fieldsByChildIndex.length; childIndex++) {
      final var field = fieldsByChildIndex[childIndex];
      if (!field.hasPresence()) {
        final var fieldValue = value.getField(field);
        if (field.hasDefaultValue() && field.getDefaultValue().equals(fieldValue)) {
          fieldShreddersByChildIndex[childIndex].shredNull();
        } else {
          fieldShreddersByChildIndex[childIndex].shredObject(fieldValue);
        }
      } else if (value.hasField(field)) {
        fieldShreddersByChildIndex[childIndex].shredObject(value.getField(field));
      } else {
        fieldShreddersByChildIndex[childIndex].shredNull();
      }
    }
    writeAccumulator.leaveGroup(parquetSchemaNode);
  }

  @Override
  public void shredNull() {
    writeAccumulator.nullGroup(parquetSchemaNode);
  }
}
