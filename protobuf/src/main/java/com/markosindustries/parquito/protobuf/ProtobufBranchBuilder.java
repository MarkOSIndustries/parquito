package com.markosindustries.parquito.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.rows.BranchBuilder;
import java.nio.ByteBuffer;
import java.util.List;

class ProtobufBranchBuilder<M extends Message> implements BranchBuilder<M> {
  private final Message.Builder builder;
  private final Descriptors.FieldDescriptor[] fieldsByChildIndex;

  public ProtobufBranchBuilder(
      final Message.Builder builder, final Descriptors.FieldDescriptor[] fieldsByChildIndex) {
    this.builder = builder;
    this.fieldsByChildIndex = fieldsByChildIndex;
  }

  @Override
  public void put(final int childFieldIndex, final Object value) {
    if (value == null) {
      return;
    }
    final var field = fieldsByChildIndex[childFieldIndex];
    Object pbExpectedValue = mapToProtobuf(field, value);
    builder.setField(field, pbExpectedValue);
  }

  @Override
  public M build() {
    //noinspection unchecked
    return (M) builder.build();
  }

  private static Object mapToProtobuf(final Descriptors.FieldDescriptor field, final Object value) {
    return switch (field.getType()) {
      case BYTES -> mapBytesToProtobuf(field, value);
      case ENUM -> mapEnumsToProtobuf(field, value);
      default -> value;
    };
  }

  @SuppressWarnings("unchecked")
  private static Object mapBytesToProtobuf(
      final Descriptors.FieldDescriptor field, final Object value) {
    if (field.isRepeated()) {
      return ((List<ByteBuffer>) value).stream().map(ByteString::copyFrom).toList();
    } else {
      return ByteString.copyFrom((ByteBuffer) value);
    }
  }

  @SuppressWarnings("unchecked")
  private static Object mapEnumsToProtobuf(
      final Descriptors.FieldDescriptor field, final Object value) {
    final var enumType = field.getEnumType();
    if (field.isRepeated()) {
      return ((List<Object>) value).stream().map(v -> mapEnumToProtobuf(enumType, v)).toList();
    } else {
      return mapEnumToProtobuf(enumType, value);
    }
  }

  private static Object mapEnumToProtobuf(
      final Descriptors.EnumDescriptor enumType, final Object value) {
    if (value instanceof String) {
      return enumType.findValueByName((String) value);
    }
    if (value instanceof Integer) {
      return enumType.findValueByNumber((int) value);
    }
    throw new UnsupportedOperationException(
        "Setting a protobuf enum from a " + value.getClass().getName() + " is not supported");
  }
}
