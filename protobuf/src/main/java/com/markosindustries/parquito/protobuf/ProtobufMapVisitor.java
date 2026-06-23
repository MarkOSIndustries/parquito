package com.markosindustries.parquito.protobuf;

import com.google.protobuf.Message;
import com.markosindustries.parquito.ParquetSchemaNode;
import com.markosindustries.parquito.rows.AbstractFieldVisitor;
import com.markosindustries.parquito.rows.FieldVisitor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.parquet.format.FieldRepetitionType;

public class ProtobufMapVisitor extends AbstractFieldVisitor {
  private final ProtobufMessageVisitor mapVisitor;

  public ProtobufMapVisitor(
      final Supplier<Message.Builder> newBuilder,
      final ParquetSchemaNode schemaNode,
      final Consumer<Object> storeValueInParent) {
    assert schemaNode.getChildren().length == 1;
    final var keyValueNode = schemaNode.getChildAtIndex(0);
    assert keyValueNode.getRepetitionType() == FieldRepetitionType.REPEATED;
    assert keyValueNode.getChildren().length == 2;

    // We can immediately hand back to the message visitor, since newBuilder will
    // construct protobuf MapEntry types for a map.
    this.mapVisitor =
        new ProtobufMessageVisitor(newBuilder.get(), keyValueNode, storeValueInParent);
  }

  @Override
  public FieldVisitor forChildIndex(final int childFieldIndex) {
    if (childFieldIndex != 0) {
      throw new IndexOutOfBoundsException("Requested a non-zero child index from a MAP");
    }
    return mapVisitor;
  }

  @Override
  public void endBranch() {}
}
