package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Message;
import com.markosindustries.parquito.IdentityBranchBuilder;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.Reader;
import com.markosindustries.parquito.rows.BranchBuilder;
import com.markosindustries.parquito.rows.RepeatedBuilder;
import java.util.List;
import java.util.function.Supplier;
import org.apache.parquet.format.FieldRepetitionType;

public class ProtobufListReader<Value> implements Reader<List<Value>, Value> {
  private final NestedListReader<?> listReader;
  private final IdentityBranchBuilder<Value> branchBuilder = new IdentityBranchBuilder<>();

  private ProtobufListReader(NestedListReader<?> listReader) {
    this.listReader = listReader;
  }

  public static <T extends Message> ProtobufListReader<T> forMessage(
      final Supplier<Message.Builder> newBuilder, final ParquetSchemaNode schemaNode) {
    assert schemaNode.getChildren().length == 1;
    final var listSchemaNode = schemaNode.getChildAtIndex(0);
    assert listSchemaNode.getRepetitionType() == FieldRepetitionType.REPEATED;
    assert listSchemaNode.getChildren().length == 1;
    final var elementSchemaNode = listSchemaNode.getChildAtIndex(0);

    final var listReader =
        new NestedListReader<>(new ProtobufReader<T>(newBuilder, elementSchemaNode));
    return new ProtobufListReader<>(listReader);
  }

  public static ProtobufListReader<Object> forLeaf(final ParquetSchemaNode schemaNode) {
    assert schemaNode.getChildren().length == 1;
    final var listSchemaNode = schemaNode.getChildAtIndex(0);
    assert listSchemaNode.getRepetitionType() == FieldRepetitionType.REPEATED;
    assert listSchemaNode.getChildren().length == 1;

    final var listReader = new NestedListReader<>(ProtobufLeafReader.INSTANCE);
    return new ProtobufListReader<>(listReader);
  }

  @Override
  public Reader<?, ?> forChild(final int childFieldIndex) {
    if (childFieldIndex != 0) {
      throw new IndexOutOfBoundsException("Requested a non-zero child index from a List");
    }
    return listReader;
  }

  @Override
  public BranchBuilder<Value> branchBuilder() {
    return branchBuilder;
  }

  @Override
  public RepeatedBuilder<List<Value>, Value> repeatedBuilder() {
    throw new UnsupportedOperationException(
        "Attempted to build repeated at the top of a LIST structure");
  }

  static class NestedListReader<Value> implements Reader<List<Value>, Value> {
    private final Reader<List<Value>, Value> elementReader;
    private final IdentityBranchBuilder<Value> branchBuilder = new IdentityBranchBuilder<>();

    public NestedListReader(Reader<List<Value>, Value> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public Reader<?, ?> forChild(final int childFieldIndex) {
      return elementReader;
    }

    @Override
    public BranchBuilder<Value> branchBuilder() {
      return branchBuilder;
    }

    @Override
    public RepeatedBuilder<List<Value>, Value> repeatedBuilder() {
      return elementReader.repeatedBuilder();
    }
  }
}
