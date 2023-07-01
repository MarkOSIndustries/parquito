package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.markosindustries.parquito.NoOpReader;
import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ProtobufReader<M extends Message> implements Reader<List<M>, M> {
  private final Supplier<Message.Builder> newBuilder;
  private final Map<String, Descriptors.FieldDescriptor> fields;
  private final Map<String, Reader<?, ?>> fieldReaders;

  public static ProtobufReader<DynamicMessage> fromDescriptor(
      final Descriptors.Descriptor descriptor) {
    return new ProtobufReader<>(() -> DynamicMessage.newBuilder(descriptor));
  }

  public ProtobufReader(final Supplier<Message.Builder> newBuilder) {
    this.newBuilder = newBuilder;
    final var builder = newBuilder.get();
    this.fields =
        builder.getDescriptorForType().getFields().stream()
            .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Function.identity()));
    this.fieldReaders =
        builder.getDescriptorForType().getFields().stream()
            .collect(
                Collectors.toMap(
                    Descriptors.FieldDescriptor::getName,
                    field -> {
                      if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                        return new ProtobufReader<>(() -> builder.newBuilderForField(field));
                      } else {
                        return ProtobufLeafReader.INSTANCE;
                      }
                    }));
  }

  @Override
  public Reader<?, ?> forChild(final String child) {
    return fieldReaders.getOrDefault(child, NoOpReader.INSTANCE);
  }

  @Override
  public BranchBuilder<M> branchBuilder() {
    return new ProtobufBranchBuilder<>(newBuilder.get(), fields);
  }

  @Override
  public RepeatedBuilder<List<M>, M> repeatedBuilder() {
    return new ProtobufRepeatedBuilder<>();
  }
}
