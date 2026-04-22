package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import org.apache.parquet.format.ConvertedType;

public class ProtobufMessageWriteTranslator<Value extends Message>
    implements WriteTranslator<Value, Void> {
  private final WriteTranslator<?, ?>[] translatorsByChildIndices;
  private final Descriptors.FieldDescriptor[] fieldsDescriptorsByChildIndex;

  public ProtobufMessageWriteTranslator(
      final Descriptors.Descriptor descriptor, final ParquetSchemaNode parquetSchemaNode) {
    this.fieldsDescriptorsByChildIndex =
        new Descriptors.FieldDescriptor[parquetSchemaNode.getChildren().length];
    this.translatorsByChildIndices =
        new WriteTranslator<?, ?>[parquetSchemaNode.getChildren().length];

    for (final var field : descriptor.getFields()) {
      final var maybeChildIndex = parquetSchemaNode.findIndexOfChildByName(field.getName());
      maybeChildIndex.ifPresent(
          childIndex -> {
            fieldsDescriptorsByChildIndex[childIndex] = field;
            translatorsByChildIndices[childIndex] =
                determineAppropriateTranslator(
                    field, parquetSchemaNode.getChildAtIndex(childIndex));
          });
    }
  }

  static WriteTranslator<?, ?> determineAppropriateTranslator(
      final Descriptors.FieldDescriptor field, final ParquetSchemaNode childSchemaNode) {
    final var isMessage = field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE;
    if (field.isRepeated() && childSchemaNode.getConvertedType() == ConvertedType.LIST) {
      // Repeated but not LIST would imply legacy style without the 3 layer list structure
      return new ProtobufListWriteTranslator(field, childSchemaNode);
    } else if (field.isMapField() && childSchemaNode.getConvertedType() == ConvertedType.MAP) {
      // MapField but not MAP would imply legacy style without the 3 layer map structure
      return new ProtobufMapWriteTranslator(field, childSchemaNode);
    } else if (isMessage) {
      return new ProtobufMessageWriteTranslator<>(field.getMessageType(), childSchemaNode);
    } else {
      return ProtobufLeafWriteTranslator.forType(field, childSchemaNode);
    }
  }

  @Override
  public Object getField(final int childIndex, final Value value) {
    final var fieldDescriptor = fieldsDescriptorsByChildIndex[childIndex];
    if (!fieldDescriptor.hasPresence()) {
      final var fieldValue = value.getField(fieldDescriptor);
      if (fieldDescriptor.hasDefaultValue()
          && fieldDescriptor.getDefaultValue().equals(fieldValue)) {
        return null;
      } else {
        return fieldValue;
      }
    } else if (!value.hasField(fieldDescriptor)) {
      return null;
    }

    return value.getField(fieldDescriptor);
  }

  @Override
  public WriteTranslator<?, ?> forChildIndex(final int childIndex) {
    return translatorsByChildIndices[childIndex];
  }

  @Override
  public Void translate(final Value value) {
    throw new UnsupportedOperationException(
        "A protobuf message cannot be translated, as it is not a leaf node in the schema");
  }
}
