package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.page.Values;
import com.markosindustries.parquito.rows.AbstractFieldVisitor;
import com.markosindustries.parquito.rows.FieldVisitor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.parquet.format.FieldRepetitionType;

public class ProtobufListVisitor extends AbstractFieldVisitor {
  private final NestedListVisitor listReader;

  private ProtobufListVisitor(NestedListVisitor listVisitor) {
    this.listReader = listVisitor;
  }

  public static ProtobufListVisitor forMessage(
      final Supplier<Message.Builder> newBuilder,
      final ParquetSchemaNode schemaNode,
      final Consumer<Object> storeValueInParent) {
    assert schemaNode.getChildren().length == 1;
    final var listSchemaNode = schemaNode.getChildAtIndex(0);
    assert listSchemaNode.getRepetitionType() == FieldRepetitionType.REPEATED;
    assert listSchemaNode.getChildren().length == 1;
    final var elementSchemaNode = listSchemaNode.getChildAtIndex(0);

    final var listReader =
        new NestedListVisitor(
            new ProtobufMessageVisitor(newBuilder.get(), elementSchemaNode, storeValueInParent));
    return new ProtobufListVisitor(listReader);
  }

  public static ProtobufListVisitor forLeaf(
      final ParquetSchemaNode schemaNode,
      final Descriptors.FieldDescriptor field,
      final Consumer<Object> storeValueInParent) {
    assert schemaNode.getChildren().length == 1;
    final var listSchemaNode = schemaNode.getChildAtIndex(0);
    assert listSchemaNode.getRepetitionType() == FieldRepetitionType.REPEATED;
    assert listSchemaNode.getChildren().length == 1;
    final var elementSchemaNode = listSchemaNode.getChildAtIndex(0);

    final var listReader =
        new NestedListVisitor(
            ProtobufLeafVisitor.create(elementSchemaNode, field, storeValueInParent));
    return new ProtobufListVisitor(listReader);
  }

  @Override
  public FieldVisitor forChildIndex(final int childFieldIndex) {
    if (childFieldIndex != 0) {
      throw new IndexOutOfBoundsException("Requested a non-zero child index from a LIST");
    }
    return listReader;
  }

  @Override
  public void endBranch() {}

  @Override
  public void visit(final int pageIndex, final Values values, final int valueIndex) {
    throw new UnsupportedOperationException("Unexpected value at list group node");
  }

  static class NestedListVisitor extends AbstractFieldVisitor {
    private final FieldVisitor elementVisitor;

    public NestedListVisitor(FieldVisitor elementVisitor) {
      this.elementVisitor = elementVisitor;
    }

    @Override
    public FieldVisitor forChildIndex(final int childFieldIndex) {
      return elementVisitor;
    }

    @Override
    public void visit(final int pageIndex, final Values values, final int valueIndex) {
      throw new UnsupportedOperationException("Unexpected value at list repeated node");
    }

    @Override
    public void endBranch() {}

    @Override
    public void endRepeated() {}
  }
}
