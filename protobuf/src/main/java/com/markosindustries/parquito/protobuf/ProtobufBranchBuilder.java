package com.markosindustries.parquito.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.rows.BranchBuilder;
import java.nio.ByteBuffer;
import java.util.Map;

class ProtobufBranchBuilder implements BranchBuilder<Message> {
  private final Message.Builder builder;
  private final Map<String, Descriptors.FieldDescriptor> fields;

  public ProtobufBranchBuilder(
      final Message.Builder builder, final Map<String, Descriptors.FieldDescriptor> fields) {
    this.builder = builder;
    this.fields = fields;
  }

  @Override
  public void put(final String key, final Object value) {
    if (value == null) {
      return;
    }
    final var field = fields.get(key);
    Object pbExpectedValue = mapToProtobuf(field, value);
    builder.setField(field, pbExpectedValue);
  }

  @Override
  public Message build() {
    return builder.build();
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
