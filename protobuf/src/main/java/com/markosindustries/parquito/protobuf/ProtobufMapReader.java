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

public class ProtobufMapReader<M extends Message> implements Reader<List<M>, M> {
  private final ProtobufReader<M> mapReader;
  private final IdentityBranchBuilder<M> branchBuilder = new IdentityBranchBuilder<>();

  public ProtobufMapReader(
      final Supplier<Message.Builder> newBuilder, final ParquetSchemaNode schemaNode) {
    assert schemaNode.getChildren().length == 1;
    final var keyValueNode = schemaNode.getChildAtIndex(0);
    assert keyValueNode.getRepetitionType() == FieldRepetitionType.REPEATED;
    assert keyValueNode.getChildren().length == 2;

    // We can immediately hand back to the protobuf reader, since newBuilder will
    // construct protobuf MapEntry types for a map.
    this.mapReader = new ProtobufReader<>(newBuilder, keyValueNode);
  }

  @Override
  public Reader<?, ?> forChild(final int childFieldIndex) {
    if (childFieldIndex != 0) {
      throw new IndexOutOfBoundsException("Requested a non-zero child index from a MAP");
    }
    return mapReader;
  }

  @Override
  public BranchBuilder<M> branchBuilder() {
    return branchBuilder;
  }

  @Override
  public RepeatedBuilder<List<M>, M> repeatedBuilder() {
    throw new UnsupportedOperationException(
        "Attempted to build repeated at the top of a MAP structure");
  }
}
