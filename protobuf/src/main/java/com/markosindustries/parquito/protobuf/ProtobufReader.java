package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import java.util.function.Supplier;

public class ProtobufReader<M extends Message> implements Reader<M> {
  private final ProtobufRowBuilder<M> rowBuilder;

  public static ProtobufReader<DynamicMessage> fromDescriptor(
      final Descriptors.Descriptor descriptor, final ParquetSchemaNode.Root parquetSchemaNode) {
    return new ProtobufReader<>(() -> DynamicMessage.newBuilder(descriptor), parquetSchemaNode);
  }

  public ProtobufReader(
      final Supplier<Message.Builder> newBuilder, final ParquetSchemaNode parquetSchemaNode) {
    this.rowBuilder = new ProtobufRowBuilder<>(newBuilder.get(), parquetSchemaNode);
  }

  @Override
  public RowBuilder<M> rowBuilder() {
    return rowBuilder;
  }

  static class ProtobufRowBuilder<M extends Message> extends ProtobufMessageVisitor
      implements RowBuilder<M> {
    public ProtobufRowBuilder(
        final Message.Builder builder, final ParquetSchemaNode parquetSchemaNode) {
      super(builder, parquetSchemaNode, null);
    }

    @Override
    public void endBranch() {}

    @Override
    public M build() {
      //noinspection unchecked
      final var message = (M) builder.build();
      builder.clear();
      return message;
    }
  }
}
