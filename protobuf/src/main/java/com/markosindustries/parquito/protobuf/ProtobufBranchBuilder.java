package com.markosindustries.parquito.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.rows.BranchBuilder;
import java.nio.ByteBuffer;

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
      case BYTES -> ByteString.copyFrom((ByteBuffer) value);
      case ENUM -> {
        if (value instanceof String) {
          yield field.getEnumType().findValueByName((String) value);
        }
        if (value instanceof Integer) {
          yield field.getEnumType().findValueByNumber((int) value);
        }
        yield value;
      }
      default -> value;
    };
  }
}
