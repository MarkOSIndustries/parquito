package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.markosindustries.parquito.NoOpReader;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class ProtobufReader<M extends Message> implements Reader<List<M>, M> {
  private final Supplier<Message.Builder> newBuilder;
  private final Descriptors.FieldDescriptor[] fieldsByChildIndex;
  private final Reader<?, ?>[] fieldReadersByChildIndex;

  public static ProtobufReader<DynamicMessage> fromDescriptor(
      final Descriptors.Descriptor descriptor, final ParquetSchemaNode.Root parquetSchemaNode) {
    return new ProtobufReader<>(() -> DynamicMessage.newBuilder(descriptor), parquetSchemaNode);
  }

  public ProtobufReader(
      final Supplier<Message.Builder> newBuilder, final ParquetSchemaNode parquetSchemaNode) {
    this.newBuilder = newBuilder;
    final var builder = newBuilder.get();

    this.fieldsByChildIndex =
        new Descriptors.FieldDescriptor[parquetSchemaNode.getChildren().length];
    this.fieldReadersByChildIndex = new Reader<?, ?>[parquetSchemaNode.getChildren().length];
    // Prefill with NoOpReader so that we gracefully handle
    // fields which aren't in the parquet schema
    Arrays.fill(this.fieldReadersByChildIndex, NoOpReader.INSTANCE);
    for (final var field : builder.getDescriptorForType().getFields()) {
      final var maybeChildIndex = parquetSchemaNode.findIndexOfChildByName(field.getName());
      maybeChildIndex.ifPresent(
          childIndex -> {
            fieldsByChildIndex[childIndex] = field;
            fieldReadersByChildIndex[childIndex] =
                field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                    ? new ProtobufReader<>(
                        () -> builder.newBuilderForField(field),
                        parquetSchemaNode.getChildAtIndex(childIndex))
                    : ProtobufLeafReader.INSTANCE;
          });
    }
  }

  @Override
  public Reader<?, ?> forChild(final int childFieldIndex) {
    return fieldReadersByChildIndex[childFieldIndex];
  }

  @Override
  public BranchBuilder<M> branchBuilder() {
    return new ProtobufBranchBuilder<>(newBuilder.get(), fieldsByChildIndex);
  }

  @Override
  public RepeatedBuilder<List<M>, M> repeatedBuilder() {
    return new ProtobufRepeatedBuilder<>();
  }
}
