package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import java.util.List;
import java.util.function.Function;
import org.apache.parquet.format.ConvertedType;

public class ProtobufMessageWriteTranslator<Value extends Message>
    implements WriteTranslator<Value, Void> {
  private final WriteTranslator<?, ?>[] translatorsByChildIndices;
  private final Descriptors.FieldDescriptor[] fieldsDescriptorsByChildIndex;
  private final Function<Value, Object>[] fieldGettersByChildIndex;

  public ProtobufMessageWriteTranslator(
      final Descriptors.Descriptor descriptor, final ParquetSchemaNode parquetSchemaNode) {
    this.fieldsDescriptorsByChildIndex =
        new Descriptors.FieldDescriptor[parquetSchemaNode.getChildren().length];
    this.translatorsByChildIndices =
        new WriteTranslator<?, ?>[parquetSchemaNode.getChildren().length];
    this.fieldGettersByChildIndex = new Function[parquetSchemaNode.getChildren().length];

    for (final var field : descriptor.getFields()) {
      final var maybeChildIndex = parquetSchemaNode.findIndexOfChildByName(field.getName());
      maybeChildIndex.ifPresent(
          childIndex -> {
            fieldsDescriptorsByChildIndex[childIndex] = field;
            translatorsByChildIndices[childIndex] =
                determineAppropriateTranslator(
                    field, parquetSchemaNode.getChildAtIndex(childIndex));
            fieldGettersByChildIndex[childIndex] = determineAppropriateFieldGetter(field);
          });
    }
  }

  static WriteTranslator<?, ?> determineAppropriateTranslator(
      final Descriptors.FieldDescriptor field, final ParquetSchemaNode childSchemaNode) {
    if (field.isRepeated() && childSchemaNode.getConvertedType() == ConvertedType.LIST) {
      // Repeated but not LIST would imply legacy style without the 3 layer list structure
      return new ProtobufListWriteTranslator(field, childSchemaNode);
    } else if (field.isMapField() && childSchemaNode.getConvertedType() == ConvertedType.MAP) {
      // MapField but not MAP would imply legacy style without the 3 layer map structure
      return new ProtobufMapWriteTranslator(field, childSchemaNode);
    } else if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
      return new ProtobufMessageWriteTranslator<>(field.getMessageType(), childSchemaNode);
    } else {
      return ProtobufLeafWriteTranslator.forType(field, childSchemaNode);
    }
  }

  static <Value extends Message> Function<Value, Object> determineAppropriateFieldGetter(
      final Descriptors.FieldDescriptor field) {
    if (field.hasPresence()) {
      return value -> value.hasField(field) ? value.getField(field) : null;
    }

    if (field.hasDefaultValue()) {
      final var defaultValue = field.getDefaultValue();
      return value -> {
        final var fieldValue = value.getField(field);
        return defaultValue.equals(fieldValue) ? null : fieldValue;
      };
    }

    if (field.isRepeated()) { // maps and repeateds
      return value -> {
        final var fieldValue = value.getField(field);
        return ((List<?>) fieldValue).isEmpty() ? null : fieldValue;
      };
    }

    return value -> value.getField(field);
  }

  @Override
  public Object getField(final int childIndex, final Value value) {
    return fieldGettersByChildIndex[childIndex].apply(value);
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
