package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.markosindustries.parquito.NoOpReader;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.SparseArrayIndexMap;
import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import java.util.List;
import java.util.function.Supplier;

public class ProtobufReader<M extends Message> implements Reader<List<M>, M> {
  private final Supplier<Message.Builder> newBuilder;
  private final SparseArrayIndexMap<Descriptors.FieldDescriptor> fieldsById;
  private final SparseArrayIndexMap<Reader<?, ?>> fieldReadersById;

  public static ProtobufReader<DynamicMessage> fromDescriptor(
      final Descriptors.Descriptor descriptor, final ParquetSchemaNode.Root parquetSchemaNode) {
    return new ProtobufReader<>(() -> DynamicMessage.newBuilder(descriptor), parquetSchemaNode);
  }

  public ProtobufReader(
      final Supplier<Message.Builder> newBuilder, final ParquetSchemaNode parquetSchemaNode) {
    this.newBuilder = newBuilder;
    final var builder = newBuilder.get();

    this.fieldsById =
        SparseArrayIndexMap.from(
            builder.getDescriptorForType().getFields(),
            field -> parquetSchemaNode.getChildByName(field.getName()).getElement().field_id,
            Descriptors.FieldDescriptor[]::new);
    this.fieldReadersById =
        SparseArrayIndexMap.from(
            builder.getDescriptorForType().getFields(),
            field -> parquetSchemaNode.getChildByName(field.getName()).getElement().field_id,
            field -> {
              if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                return new ProtobufReader<>(
                    () -> builder.newBuilderForField(field),
                    parquetSchemaNode.getChildByName(field.getName()));
              } else {
                return ProtobufLeafReader.INSTANCE;
              }
            },
            Reader<?, ?>[]::new);
  }

  @Override
  public Reader<?, ?> forChild(final int childFieldId) {
    return fieldReadersById.getOrDefault(childFieldId, NoOpReader.INSTANCE);
  }

  @Override
  public BranchBuilder<M> branchBuilder() {
    return new ProtobufBranchBuilder<>(newBuilder.get(), fieldsById);
  }

  @Override
  public RepeatedBuilder<List<M>, M> repeatedBuilder() {
    return new ProtobufRepeatedBuilder<>();
  }
}
