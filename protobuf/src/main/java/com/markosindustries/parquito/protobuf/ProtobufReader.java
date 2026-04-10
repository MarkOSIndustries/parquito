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
import org.apache.parquet.format.ConvertedType;

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
                determineAppropriateReader(
                    field,
                    parquetSchemaNode.getChildAtIndex(childIndex),
                    () -> builder.newBuilderForField(field));
          });
    }
  }

  private static Reader<?, ?> determineAppropriateReader(
      final Descriptors.FieldDescriptor field,
      final ParquetSchemaNode childSchemaNode,
      Supplier<Message.Builder> newBuilder) {
    final var isMessage = field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE;
    if (field.isRepeated() && childSchemaNode.getConvertedType() == ConvertedType.LIST) {
      // Repeated but not LIST would imply legacy style without the 3 layer list structure
      return isMessage
          ? ProtobufListReader.forMessage(newBuilder, childSchemaNode)
          : ProtobufListReader.forLeaf(childSchemaNode);
    } else if (field.isMapField() && childSchemaNode.getConvertedType() == ConvertedType.MAP) {
      // MapField but not MAP would imply legacy style without the 3 layer map structure
      return new ProtobufMapReader<>(newBuilder, childSchemaNode);
    } else if (isMessage) {
      return new ProtobufReader<>(newBuilder, childSchemaNode);
    } else {
      return ProtobufLeafReader.INSTANCE;
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
