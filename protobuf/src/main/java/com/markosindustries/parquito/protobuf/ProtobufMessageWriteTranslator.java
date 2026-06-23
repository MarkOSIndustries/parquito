package com.markosindustries.parquito.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.WriteTranslator;
import com.markosindustries.parquito.rows.BranchAccumulator;
import com.markosindustries.parquito.rows.LeafAccumulator;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.parquet.format.ConvertedType;

public class ProtobufMessageWriteTranslator implements WriteTranslator<Message> {
  private final WriteTranslator<?>[] translatorsByChildIndices;
  private final ParquetSchemaNode parquetSchemaNode;
  private final Descriptors.FieldDescriptor[] fieldDescriptorsByChildIndices;
  private final FieldLeafVisitor[] fieldLeafVisitorsByChildIndex;

  @FunctionalInterface
  interface FieldLeafVisitor {
    void visit(final Message branch, final LeafAccumulator leafAccumulator);
  }

  @FunctionalInterface
  interface FieldLeafRepeatedVisitor {
    void visit(final List<?> repeated, final LeafAccumulator leafAccumulator);
  }

  public ProtobufMessageWriteTranslator(
      final Descriptors.Descriptor descriptor, final ParquetSchemaNode parquetSchemaNode) {
    this.parquetSchemaNode = parquetSchemaNode;
    this.translatorsByChildIndices = new WriteTranslator[parquetSchemaNode.getChildren().length];
    this.fieldLeafVisitorsByChildIndex =
        new FieldLeafVisitor[parquetSchemaNode.getChildren().length];
    this.fieldDescriptorsByChildIndices =
        new Descriptors.FieldDescriptor[parquetSchemaNode.getChildren().length];

    for (final var field : descriptor.getFields()) {
      final var maybeChildIndex = parquetSchemaNode.findIndexOfChildByName(field.getName());
      maybeChildIndex.ifPresent(
          childIndex -> {
            fieldDescriptorsByChildIndices[childIndex] = field;
            translatorsByChildIndices[childIndex] =
                determineAppropriateTranslator(
                    field, parquetSchemaNode.getChildAtIndex(childIndex));
            if (translatorsByChildIndices[childIndex] == null) {
              fieldLeafVisitorsByChildIndex[childIndex] =
                  determineAppropriateFieldLeafVisitor(
                      field, parquetSchemaNode.getChildAtIndex(childIndex));
            }
          });
    }
  }

  @Override
  public void translate(final Message branch, final BranchAccumulator accumulator) {
    accumulator.branch(
        childAccessor -> {
          for (var childIndex = 0; childIndex < translatorsByChildIndices.length; childIndex++) {
            final var field = fieldDescriptorsByChildIndices[childIndex];
            if (translatorsByChildIndices[childIndex] != null) {
              if (!field.hasPresence() || branch.hasField(field)) {
                translatorsByChildIndices[childIndex].translateUnsafe(
                    branch.getField(field), childAccessor.childBranchAccumulator(childIndex));
              } else {
                childAccessor.childBranchAccumulator(childIndex).accumulateNull();
              }
            } else {
              final var leafAccumulator = childAccessor.childLeafAccumulator(childIndex);
              fieldLeafVisitorsByChildIndex[childIndex].visit(branch, leafAccumulator);
            }
          }
        });
  }

  @Override
  public void translateUnsafe(final Object branch, final BranchAccumulator accumulator) {
    if (branch instanceof final List<?> messages) {
      for (final var message : messages) {
        translate((Message) message, accumulator);
      }
    } else {
      translate((Message) branch, accumulator);
    }
  }

  static WriteTranslator<?> determineAppropriateTranslator(
      final Descriptors.FieldDescriptor field, final ParquetSchemaNode childSchemaNode) {
    if (field.isRepeated() && childSchemaNode.getConvertedType() == ConvertedType.LIST) {
      // Repeated but not LIST would imply legacy style without the 3 layer list structure
      return new ProtobufListWriteTranslator(field, childSchemaNode);
    } else if (field.isMapField() && childSchemaNode.getConvertedType() == ConvertedType.MAP) {
      // MapField but not MAP would imply legacy style without the 3 layer map structure
      return new ProtobufMapWriteTranslator(field, childSchemaNode);
    } else if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
      return new ProtobufMessageWriteTranslator(field.getMessageType(), childSchemaNode);
    } else if (field.getType() == Descriptors.FieldDescriptor.Type.GROUP) {
      throw new UnsupportedOperationException("We don't currently support protobuf GROUPs");
    } else {
      return null;
    }
  }

  static FieldLeafVisitor determineAppropriateFieldLeafVisitor(
      final Descriptors.FieldDescriptor field, final ParquetSchemaNode schemaNode) {
    final FieldLeafVisitor basicVisitor =
        switch (field.getType()) {
          case DOUBLE ->
              (branch, childAccumulator) ->
                  childAccumulator.accumulateDouble((double) branch.getField(field));
          case FLOAT ->
              (branch, childAccumulator) ->
                  childAccumulator.accumulateFloat((float) branch.getField(field));
          case INT64, FIXED64, SFIXED64, SINT64, UINT64 ->
              (branch, childAccumulator) ->
                  childAccumulator.accumulateInt64((long) branch.getField(field));
          case INT32, FIXED32, SFIXED32, SINT32, UINT32 ->
              (branch, childAccumulator) ->
                  childAccumulator.accumulateInt32((int) branch.getField(field));
          case BOOL ->
              (branch, childAccumulator) ->
                  childAccumulator.accumulateBoolean((boolean) branch.getField(field));
          case STRING ->
              (branch, childAccumulator) ->
                  childAccumulator.accumulateByteBuffer(
                      ByteBuffer.wrap(
                          ((String) branch.getField(field)).getBytes(StandardCharsets.UTF_8)));
          case BYTES ->
              (branch, childAccumulator) ->
                  childAccumulator.accumulateByteBuffer(
                      ((ByteString) branch.getField(field)).asReadOnlyByteBuffer());
          case ENUM ->
              switch (schemaNode.getElement().getType()) {
                case INT32 ->
                    (branch, childAccumulator) ->
                        childAccumulator.accumulateInt32(
                            ((Descriptors.EnumValueDescriptor) branch.getField(field)).getNumber());
                case BYTE_ARRAY ->
                    // TODO - we can pre-cache these ByteBuffers in a map
                    (branch, childAccumulator) ->
                        childAccumulator.accumulateByteBuffer(
                            ByteBuffer.wrap(
                                ((Descriptors.EnumValueDescriptor) branch.getField(field))
                                    .getName()
                                    .getBytes(StandardCharsets.UTF_8)));
                default ->
                    throw new UnsupportedOperationException(
                        "Something probably went wrong in schema conversion - protobuf enums can only be written as int32 or string, but we got "
                            + schemaNode.getElement());
              };
          case MESSAGE, GROUP ->
              throw new UnsupportedOperationException(
                  "We shouldn't encounter a MESSAGE or GROUP when looking at a leaf - " + field);
        };

    if (field.hasPresence()) {
      return (branch, childAccumulator) -> {
        if (branch.hasField(field)) {
          basicVisitor.visit(branch, childAccumulator);
        } else {
          childAccumulator.accumulateNull();
        }
      };
    }

    if (field.hasDefaultValue()) {
      final var defaultValue = field.getDefaultValue();
      return (branch, childAccumulator) -> {
        if (!defaultValue.equals(branch.getField(field))) {
          basicVisitor.visit(branch, childAccumulator);
        } else {
          childAccumulator.accumulateNull();
        }
      };
    }

    if (field.isRepeated()) {
      final var repeatedVisitor = determineAppropriateFieldLeafRepeatedVisitor(field, schemaNode);
      return (branch, leafAccumulator) ->
          repeatedVisitor.visit((List<?>) branch.getField(field), leafAccumulator);
    }

    return basicVisitor;
  }

  @SuppressWarnings("unchecked")
  static FieldLeafRepeatedVisitor determineAppropriateFieldLeafRepeatedVisitor(
      final Descriptors.FieldDescriptor field, final ParquetSchemaNode schemaNode) {
    return switch (field.getType()) {
      case DOUBLE ->
          (list, childAccumulator) -> {
            for (final var value : ((List<Double>) list)) {
              childAccumulator.accumulateDouble(value);
            }
          };
      case FLOAT ->
          (list, childAccumulator) -> {
            for (final var value : ((List<Float>) list)) {
              childAccumulator.accumulateFloat(value);
            }
          };
      case INT64, FIXED64, SFIXED64, SINT64, UINT64 ->
          (list, childAccumulator) -> {
            for (final var value : ((List<Long>) list)) {
              childAccumulator.accumulateInt64(value);
            }
          };
      case INT32, FIXED32, SFIXED32, SINT32, UINT32 ->
          (list, childAccumulator) -> {
            for (final var value : ((List<Integer>) list)) {
              childAccumulator.accumulateInt32(value);
            }
          };
      case BOOL ->
          (list, childAccumulator) -> {
            for (final var value : ((List<Boolean>) list)) {
              childAccumulator.accumulateBoolean(value);
            }
          };
      case STRING ->
          (list, childAccumulator) -> {
            for (final var value : ((List<String>) list)) {
              childAccumulator.accumulateByteBuffer(
                  ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
            }
          };
      case BYTES ->
          (list, childAccumulator) -> {
            for (final var value : ((List<ByteString>) list)) {
              childAccumulator.accumulateByteBuffer(value.asReadOnlyByteBuffer());
            }
          };
      case ENUM ->
          switch (schemaNode.getElement().getType()) {
            case INT32 ->
                (list, childAccumulator) -> {
                  for (final var value : ((List<Descriptors.EnumValueDescriptor>) list)) {
                    childAccumulator.accumulateInt32(value.getNumber());
                  }
                };
            case BYTE_ARRAY ->
                (list, childAccumulator) -> {
                  for (final var value : ((List<Descriptors.EnumValueDescriptor>) list)) {
                    // TODO - we can pre-cache these ByteBuffers in a map
                    childAccumulator.accumulateByteBuffer(
                        ByteBuffer.wrap(value.getName().getBytes(StandardCharsets.UTF_8)));
                  }
                };
            default ->
                throw new UnsupportedOperationException(
                    "Something probably went wrong in schema conversion - protobuf enums can only be written as int32 or string, but we got "
                        + schemaNode.getElement());
          };
      case MESSAGE, GROUP ->
          throw new UnsupportedOperationException(
              "We shouldn't encounter a MESSAGE or GROUP when looking at a leaf - " + field);
    };
  }
}
