package com.markosindustries.parquito.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.rows.AbstractFieldVisitor;
import com.markosindustries.parquito.rows.FieldVisitor;
import com.markosindustries.parquito.rows.NoOpFieldVisitor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.parquet.format.ConvertedType;

class ProtobufMessageVisitor extends AbstractFieldVisitor {
  protected final Message.Builder builder;
  private final FieldVisitor[] visitorsByChildIndex;
  private final Consumer<Object> storeValueInParent;

  public ProtobufMessageVisitor(
      final Message.Builder builder,
      final ParquetSchemaNode parquetSchemaNode,
      final Consumer<Object> storeValueInParent) {
    this.builder = builder;
    this.visitorsByChildIndex = new FieldVisitor[parquetSchemaNode.getChildren().length];
    this.storeValueInParent = storeValueInParent;

    // Prefill with NoOpFieldVisitor so that we gracefully handle
    // fields which aren't in the protobuf schema
    Arrays.fill(this.visitorsByChildIndex, NoOpFieldVisitor.INSTANCE);
    for (final var field : builder.getDescriptorForType().getFields()) {
      final var maybeChildIndex = parquetSchemaNode.findIndexOfChildByName(field.getName());
      maybeChildIndex.ifPresent(
          childIndex -> {
            visitorsByChildIndex[childIndex] =
                determineAppropriateFieldVisitor(
                    builder,
                    field,
                    parquetSchemaNode.getChildAtIndex(childIndex),
                    () -> builder.newBuilderForField(field));
          });
    }
  }

  @Override
  public FieldVisitor forChildIndex(final int childIndex) {
    return visitorsByChildIndex[childIndex];
  }

  @Override
  public void endBranch() {
    storeValueInParent.accept(builder.build());
    builder.clear();
  }

  @Override
  public void endRepeated() {}

  private FieldVisitor determineAppropriateFieldVisitor(
      final Message.Builder parentBuilder,
      final Descriptors.FieldDescriptor field,
      final ParquetSchemaNode childSchemaNode,
      Supplier<Message.Builder> newBuilder) {
    final Consumer<Object> storeValueInParent =
        field.isRepeated()
            ? value -> parentBuilder.addRepeatedField(field, mapToProtobuf(field, value))
            : value -> parentBuilder.setField(field, mapToProtobuf(field, value));
    final var isMessage = field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE;
    if (field.isRepeated() && childSchemaNode.getConvertedType() == ConvertedType.LIST) {
      // Repeated but not LIST would imply legacy style without the 3 layer list structure
      return isMessage
          ? ProtobufListVisitor.forMessage(newBuilder, childSchemaNode, storeValueInParent)
          : ProtobufListVisitor.forLeaf(childSchemaNode, storeValueInParent);
    } else if (field.isMapField() && childSchemaNode.getConvertedType() == ConvertedType.MAP) {
      // MapField but not MAP would imply legacy style without the 3 layer map structure
      return new ProtobufMapVisitor(newBuilder, childSchemaNode, storeValueInParent);
    } else if (isMessage) {
      return new ProtobufMessageVisitor(newBuilder.get(), childSchemaNode, storeValueInParent);
    } else {
      return new ProtobufLeafVisitor(storeValueInParent, field.isRepeated());
    }
  }

  private static Object mapToProtobuf(final Descriptors.FieldDescriptor field, final Object value) {
    return switch (field.getType()) {
      case STRING -> mapStringToProtobuf(field, value);
      case BYTES -> mapBytesToProtobuf(field, value);
      case ENUM -> mapEnumsToProtobuf(field, value);
      default -> value;
    };
  }

  @SuppressWarnings("unchecked")
  private static Object mapStringToProtobuf(
      final Descriptors.FieldDescriptor field, final Object value) {
    return ByteString.copyFrom((ByteBuffer) value).toStringUtf8();
  }

  @SuppressWarnings("unchecked")
  private static Object mapBytesToProtobuf(
      final Descriptors.FieldDescriptor field, final Object value) {
    return ByteString.copyFrom((ByteBuffer) value);
  }

  @SuppressWarnings("unchecked")
  private static Object mapEnumsToProtobuf(
      final Descriptors.FieldDescriptor field, final Object value) {
    return mapEnumToProtobuf(field.getEnumType(), value);
  }

  private static Object mapEnumToProtobuf(
      final Descriptors.EnumDescriptor enumType, final Object value) {
    if (value instanceof final ByteBuffer valueAsByteBuffer) {
      final var asString =
          new String(
              valueAsByteBuffer.array(),
              valueAsByteBuffer.arrayOffset() + valueAsByteBuffer.position(),
              valueAsByteBuffer.remaining(),
              StandardCharsets.UTF_8);
      final var asEnum = enumType.findValueByName(asString);
      return asEnum;
    }
    if (value instanceof Integer) {
      return enumType.findValueByNumber((int) value);
    }
    throw new UnsupportedOperationException(
        "Setting a protobuf enum from a " + value.getClass().getName() + " is not supported");
  }
}
