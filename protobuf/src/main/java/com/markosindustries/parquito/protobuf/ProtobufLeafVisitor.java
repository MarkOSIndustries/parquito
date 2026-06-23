package com.markosindustries.parquito.protobuf;

import com.markosindustries.parquito.rows.FieldVisitor;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class ProtobufLeafVisitor implements FieldVisitor {
  private final Consumer<Object> storeValueInParent;

  ProtobufLeafVisitor(final Consumer<Object> storeValueInParent, final boolean isRepeated) {
    this.storeValueInParent = storeValueInParent;
  }

  @Override
  public FieldVisitor forChildIndex(final int childIndex) {
    throw new UnsupportedOperationException("Leaf nodes don't have children");
  }

  @Override
  public void endBranch() {
    throw new UnsupportedOperationException("Unexpected end of branch in leaf node");
  }

  @Override
  public void endRepeated() {}

  @Override
  public void visit(final int pageIndex, final boolean value) {
    storeValueInParent.accept(value);
  }

  @Override
  public void visit(final int pageIndex, final ByteBuffer value) {
    storeValueInParent.accept(value);
  }

  @Override
  public void visit(final int pageIndex, final float value) {
    storeValueInParent.accept(value);
  }

  @Override
  public void visit(final int pageIndex, final double value) {
    storeValueInParent.accept(value);
  }

  @Override
  public void visit(final int pageIndex, final int value) {
    storeValueInParent.accept(value);
  }

  @Override
  public void visit(final int pageIndex, final long value) {
    storeValueInParent.accept(value);
  }

  @Override
  public void visitNull(final int pageIndex) {}
}
